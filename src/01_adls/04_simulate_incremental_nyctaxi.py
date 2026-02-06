# Databricks notebook source
# MAGIC %md
# MAGIC # LOAD DUMMY DATA TO ADLS TO SIMULATE INCREMENTAL.

# COMMAND ----------

# DBTITLE 1,Imports & Configs
# Imports
from azureml.opendatasets import NycTlcYellow
from dateutil import parser
from datetime import datetime, timezone
from pyspark.sql import functions as F

# Config
storage_account = "dlsdbxtraining001"
container = "landingzone-leon"
base_relative_path = "nyctaxi/01_yellow_taxi"
dropoff_minutes = 7

# Build destination path
run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
dest_relative_path = f"{base_relative_path}/yellow_taxi_{run_ts}"
landing_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{dest_relative_path}"

# ADLS Auth (Access Key)
access_key = dbutils.secrets.get(
    scope="kv-acesskeys-gdc",
    key="dlsdbxtraining001-accesskey"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    access_key
)

# Read NYC TLC Yellow Taxi
start_date = parser.parse("2018-05-01")
end_date   = parser.parse("2018-06-06")

nyc_tlc = NycTlcYellow(start_date=start_date, end_date=end_date)
nyc_tlc_df = nyc_tlc.to_spark_dataframe()


# COMMAND ----------

# DBTITLE 1,Main: Load data to ADLS
# Find pickup/dropoff columns (case-insensitive)
def find_col(df, name):
    for c in df.columns:
        if c.lower() == name.lower():
            return c
    return None

pickup_col  = find_col(nyc_tlc_df, "tpepPickupDateTime")
dropoff_col = find_col(nyc_tlc_df, "tpepDropoffDateTime")

if pickup_col is None or dropoff_col is None:
    raise ValueError(
        f"Expected pickup/dropoff columns not found. Columns: {nyc_tlc_df.columns}"
    )

# Limit to 2 rows + override timestamps
final_df = (
    nyc_tlc_df
    .limit(2)
    .withColumn(pickup_col, F.current_timestamp())
    .withColumn(
        dropoff_col,
        F.expr(f"current_timestamp() + INTERVAL {dropoff_minutes} MINUTES")
    )
)

# Normalize column name
if pickup_col != "tpepPickupDateTime":
    final_df = final_df.withColumnRenamed(pickup_col, "tpepPickupDateTime")
if dropoff_col != "tpepDropoffDateTime":
    final_df = final_df.withColumnRenamed(dropoff_col, "tpepDropoffDateTime")

# Write Parquet
(
    final_df
    .write
    .mode("append")
    .format("parquet")
    .save(landing_path)
)
print(f"Successfully written 2 rows to: {landing_path}")
display(final_df)
