# Databricks notebook source
# MAGIC %md
# MAGIC # STREAM WRITE.

# COMMAND ----------

# DBTITLE 1,This is working, although after a stream only - chenge the positioning maybe?
# Imports
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
import json

# Configs
target_table = "gdc_dbxtraining.leon_bronze.stream_batch_unification"
checkpoint = "dbfs:/tmp/leon/checkpoints/unification_stream"

# Build Connection string
connection_string = "Endpoint=sb://evhns-gdctraining.servicebus.windows.net/;SharedAccessKeyName=SharedAccessKeyToSendAndListen;SharedAccessKey=ldeCpffK7Btui3LPg9wLF7uVL692Ehj1M+AEhJ3Xwgk=;EntityPath=evh-gdctraining-leon"

# Create schema for JSON payload
payload_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("sequence", LongType(), True),
    StructField("deviceId", StringType(), True),
    StructField("temperatureC", DoubleType(), True),
    StructField("status", StringType(), True),
])

starting_pos = {
  "offset": -1,
  "seqNo": -1,
  "enqueuedTime": None,
  "isInclusive": True
}

# Eventhub connector config
ehConf = {
  "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connection_string),
  "eventhubs.consumerGroup": "$Default",
  "eventhubs.startingPosition": json.dumps(starting_pos),
  "maxEventsPerTrigger": "10000"
}

# Create table if not exists
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
  eventId STRING,
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

# Read from event hub
df_raw = (
    spark.readStream
         .format("eventhubs")
         .options(**ehConf)
         .load()
)

# Final selects
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
      # EventId generation
      .withColumn(
          "eventId",
          when(
              col("partitionId").isNotNull() & col("offset").isNotNull(),
              sha2(concat_ws("||", lit("eh"), col("partitionId"), col("offset")), 256)
          ).otherwise(
              sha2(concat_ws("||", lit("batch"), col("deviceId"), col("timestamp"), col("sequence")), 256)
          )
      )
      .filter(col("eventId").isNotNull())
      .select(
          "eventId","timestamp","sequence","deviceId","temperatureC","status",
          "enqueuedTime","offset","sequenceNumber","partitionId","etlLoadTs"
      )
)

# Functions
def _get_last_op_metrics(table_name: str):
    """
    Returns (operation, operationMetrics_dict) for the latest commit on the table.
    """
    h = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]
    op = h["operation"]
    metrics = h["operationMetrics"]  # this is a map<string,string>
    return op, metrics

def write_and_log(batch_df, batch_id: int):
    # micro-batch rowcount
    in_rows = batch_df.count()
    print(f"[batch_id={batch_id}] incoming_rows={in_rows}")

    if in_rows == 0:
        return

    # Delta merge by eventId to dedupe + upsert (update existing rows)
    dt = DeltaTable.forName(spark, target_table)

    (dt.alias("t")
       .merge(batch_df.alias("s"), "t.eventId = s.eventId")
       .whenMatchedUpdateAll()
       .whenNotMatchedInsertAll()
       .execute())

    # Pull commit metrics
    op, m = _get_last_op_metrics(target_table)

    inserted = int(m.get("numTargetRowsInserted", "0"))
    updated  = int(m.get("numTargetRowsUpdated", "0"))
    deleted  = int(m.get("numTargetRowsDeleted", "0"))
    matched  = int(m.get("numTargetRowsMatchedUpdated", m.get("numTargetRowsUpdated", "0")))
    output   = int(m.get("numOutputRows", str(inserted + updated)))

    # For MERGE: "inserted" are new, "updated/matched" are merges
    print(
        f"[batch_id={batch_id}] op={op} "
        f"inserted={inserted} merged(updated)={updated} matched={matched} "
        f"deleted={deleted} outputRows={output}"
    )

# Write to target table
q = (
    df_parsed.writeStream
        .foreachBatch(write_and_log)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
)

q.awaitTermination()

print(f"Successfully written to {target_table}.")
