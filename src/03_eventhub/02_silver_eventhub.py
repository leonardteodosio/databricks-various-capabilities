# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER LOAD: USING DLT.

# COMMAND ----------

# Imports
import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configs
bronze_table = "gdc_dbxtraining.leon_bronze.eventhub_data"
silver_table_name = "eventhub_data"

# Create silver table using DLT
@dlt.table(
  name=silver_table_name,
  comment="Silver eventhub_data from Bronze eventhub_data",
  table_properties={
    "quality": "silver"
  }
)
@dlt.expect_or_drop("valid_deviceId", "deviceId IS NOT NULL AND trim(deviceId) <> ''")
@dlt.expect_or_drop("valid_timestamp", "timestamp IS NOT NULL AND trim(timestamp) <> ''")
@dlt.expect_or_drop("valid_sequence", "sequence IS NOT NULL AND sequence >= 0")
def eventhub_data_silver():
    # Read from Bronze Delta table
    df = spark.read.table(bronze_table)

    # Parse ISO8601 string timestamp into a real timestamp column
    df = df.withColumn(
        "event_ts",
        F.to_timestamp(F.regexp_replace(F.col("timestamp"), "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss.SSS")
    )

    # Normalize strings
    df = (df
          .withColumn("deviceId", F.trim(F.col("deviceId")))
          .withColumn("status", F.upper(F.trim(F.col("status"))))
         )

    # Dedup rule: same (deviceId, sequence) -> keep most recent record
    w = Window.partitionBy("deviceId", "sequence").orderBy(
        F.col("enqueuedTime").desc_nulls_last(),
        F.col("etlLoadTs").desc_nulls_last()
    )

    df_dedup = (df
                .withColumn("_rn", F.row_number().over(w))
                .withColumn("silver_timestamp", F.current_timestamp())
                .filter(F.col("_rn") == 1)
                .drop("_rn")
               )

    # Final Select
    return df_dedup.select(
        "timestamp",
        "event_ts",
        "sequence",
        "deviceId",
        "temperatureC",
        "status",
        "enqueuedTime",
        "offset",
        "sequenceNumber",
        "partitionId",
        "etlLoadTs",
        "silver_timestamp"
    )

# Quarantine table for bad records instead of dropping them
# @dlt.table(
#   name="eventhub_data_quarantine",
#   comment="Rows that failed Silver expectations (invalid deviceId/timestamp/sequence)."
# )
# def eventhub_data_quarantine():
#     df = spark.read.table(bronze_table)
#     df = df.withColumn("deviceId", F.trim(F.col("deviceId")))
#     bad = df.filter(
#         (F.col("deviceId").isNull()) | (F.trim(F.col("deviceId")) == "") |
#         (F.col("timestamp").isNull()) | (F.trim(F.col("timestamp")) == "") |
#         (F.col("sequence").isNull()) | (F.col("sequence") < 0)
#     )
#     return bad
