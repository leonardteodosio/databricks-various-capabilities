# Databricks notebook source
# MAGIC %md
# MAGIC # EVENT HUBS
# MAGIC - Namespace = container for event hubs.
# MAGIC - Make sure to install the event hubs connector to cluster first.

# COMMAND ----------

pip install azure-eventhub

# COMMAND ----------

# DBTITLE 1,Working
# Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Configs
target_table = "gdc_dbxtraining.leon_bronze.eventhub_data"
checkpoint = "dbfs:/tmp/leon/checkpoints/eventhub_data_v3"  # NEW checkpoint to avoid old bad offsets

connection_string = "Endpoint=sb://evhns-gdctraining.servicebus.windows.net/;SharedAccessKeyName=SharedAccessKeyToSendAndListen;SharedAccessKey=ldeCpffK7Btui3LPg9wLF7uVL692Ehj1M+AEhJ3Xwgk=;EntityPath=evh-gdctraining-leon"

# Create schema for JSON payload
payload_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("sequence", LongType(), True),
    StructField("deviceId", StringType(), True),
    StructField("temperatureC", DoubleType(), True),
    StructField("status", StringType(), True),
])

# Starting position: earliest (offset=-1) & include seqNo as a long-compatible value
starting_pos = {
  "offset": -1,
  "seqNo": -1,
  "enqueuedTime": None,
  "isInclusive": True
}

# Create eventhub connection configs
ehConf = {
  "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
  "eventhubs.consumerGroup": "$Default",
  "eventhubs.startingPosition": json.dumps(starting_pos),
  "maxEventsPerTrigger": "10000"
}

# Create target table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
  timestamp STRING,
  sequence BIGINT,
  deviceId STRING,
  temperatureC DOUBLE,
  status STRING,
  enqueuedTime TIMESTAMP,
  offset STRING,
  sequenceNumber BIGINT,
  partitionId STRING,
  etlLoadTs TIMESTAMP
)
USING DELTA
""")

print(f"Reading from eventhub and writing to {target_table}.")

# Stream table using eventhub connector
df_raw = (
    spark.readStream
         .format("eventhubs")
         .options(**ehConf)
         .load()
)

# Parse JSON body data and final select
df_parsed = (
    df_raw
      .withColumn("body_str", col("body").cast("string"))
      .withColumn("payload", from_json(col("body_str"), payload_schema))
      .select(
          col("payload.timestamp").alias("timestamp"),
          col("payload.sequence").alias("sequence"),
          col("payload.deviceId").alias("deviceId"),
          col("payload.temperatureC").alias("temperatureC"),
          col("payload.status").alias("status"),
          col("enqueuedTime").cast("timestamp").alias("enqueuedTime"),
          col("offset").cast("string").alias("offset"),
          col("sequenceNumber").cast("bigint").alias("sequenceNumber"),
          col("partition").cast("string").alias("partitionId"),
          current_timestamp().alias("etlLoadTs")
      )
)

# Function for writing to table
def write_and_log(batch_df, batch_id: int):
    n = batch_df.count()
    print(f"batch_id={batch_id} rows={n}")
    if n > 0:
        (batch_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target_table))

# Write data to table
q = (
    df_parsed.writeStream
        .foreachBatch(write_and_log)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
)

q.awaitTermination()

print(f"Successfully written to {target_table}.")
