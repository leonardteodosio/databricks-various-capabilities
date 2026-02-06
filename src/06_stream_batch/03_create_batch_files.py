# Databricks notebook source
# Imports
import time, random, datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Configs
storage_account = "dlsdbxtraining001"
container = "landingzone-leon"
base_folder = "stream_batch_unification"

# Pull keys from secrets
adls_key = dbutils.secrets.get(scope="kv-acesskeys-gdc", key="dlsdbxtraining001-accesskey")

# Set adls key
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", adls_key)

# Build Target path: YYYYMMDD_HHMMSS format
file_date = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_%H%M%S")
target_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{base_folder}/file_date={file_date}"

# Generate events
events = []
for i in range(10):  # run for n events
    payload = {
        "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
        "sequence": i,
        "deviceId": f"device-{random.randint(1,5)}",
        "temperatureC": round(random.uniform(20, 35), 2),
        "status": random.choice(["OK", "WARN", "ALERT"])
    }
    print(f"Generated event #{i}: {payload}")
    events.append(Row(**payload))
    time.sleep(2)  # generate every 2 seconds

# Create schema
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("sequence", LongType(), True),
    StructField("deviceId", StringType(), True),
    StructField("temperatureC", DoubleType(), True),
    StructField("status", StringType(), True),
])

df = spark.createDataFrame(events, schema=schema)

# Write Parquet to ADLS
(df.write
   .mode("append")
   .parquet(target_path))

print(f"Successfully written {df.count()} rows to: {target_path}")
display(df)
