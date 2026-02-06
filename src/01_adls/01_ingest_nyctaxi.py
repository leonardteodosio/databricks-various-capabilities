# Databricks notebook source
# MAGIC %md 
# MAGIC # LOAD YELLOW TAXI DATA.
# MAGIC - Initial load to ADLS Storage from Azure Open Datasets.

# COMMAND ----------

# DBTITLE 1,Main: Load Yellow Taxi
# Imports
from azureml.opendatasets import NycTlcYellow
from dateutil import parser
from datetime import datetime, timezone

# Configs
storage_account = "dlsdbxtraining001"
container = "landingzone-leon"
base_relative_path = "nyctaxi/01_yellow_taxi"

# Create timestamped folder
run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
dest_relative_path = f"{base_relative_path}/yellow_taxi_{run_ts}"
landing_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{dest_relative_path}"

# Auth (Access Key from Key Vault)
access_key = dbutils.secrets.get(
    scope="kv-acesskeys-gdc",
    key="dlsdbxtraining001-accesskey"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    access_key
)

# Dates
start_date = parser.parse("2018-05-01")
end_date   = parser.parse("2018-06-06")

# Get data
nyc_tlc = NycTlcYellow(start_date=start_date, end_date=end_date)
nyc_tlc_df = nyc_tlc.to_spark_dataframe()

# display(nyc_tlc_df.limit(5))

# Write data
(nyc_tlc_df.write
    .mode("append")
    .format("parquet")
    .save(landing_path)
)
print(f"Data written to: {dest_relative_path}")


# COMMAND ----------

# MAGIC %md 
# MAGIC # LOAD GREEN TAXI DATA.
# MAGIC - Initial load to ADLS Storage from Azure Open Datasets.

# COMMAND ----------

# DBTITLE 1,Main: Load Green Taxi
# Imports
from azureml.opendatasets import NycTlcGreen
from dateutil import parser
from datetime import datetime, timezone

# Configs
storage_account = "dlsdbxtraining001"
container = "landingzone-leon"
base_relative_path = "nyctaxi/02_green_taxi"

# Create timestamped folder
run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
dest_relative_path = f"{base_relative_path}/green_taxi_{run_ts}"
landing_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{dest_relative_path}"

# Auth (Access Key from Key Vault)
access_key = dbutils.secrets.get(
    scope="kv-acesskeys-gdc",
    key="dlsdbxtraining001-accesskey"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    access_key
)

# Dates
start_date = parser.parse("2018-05-01")
end_date   = parser.parse("2018-06-06")

# Get data
nyc_tlc = NycTlcGreen(start_date=start_date, end_date=end_date)
nyc_tlc_df = nyc_tlc.to_spark_dataframe()

# display(nyc_tlc_df.limit(5))

# Write data
(nyc_tlc_df.write
    .mode("append")
    .format("parquet")
    .save(landing_path)
)
print(f"Data written to: {dest_relative_path}")


# COMMAND ----------

# MAGIC %md 
# MAGIC # LOAD INDEPENDENT TAXI DATA.
# MAGIC - Initial load to ADLS Storage from Azure Open Datasets.

# COMMAND ----------

# DBTITLE 1,Main: Load Independent Taxi
# Imports
from azureml.opendatasets import NycTlcFhv
from dateutil import parser
from datetime import datetime, timezone

# Configs
storage_account = "dlsdbxtraining001"
container = "landingzone-leon"
base_relative_path = "nyctaxi/03_independent_taxi"

# Create timestamped folder
run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
dest_relative_path = f"{base_relative_path}/independent_taxi_{run_ts}"
landing_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{dest_relative_path}"

# Auth (Access Key from Key Vault)
access_key = dbutils.secrets.get(
    scope="kv-acesskeys-gdc",
    key="dlsdbxtraining001-accesskey"
)

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    access_key
)

# Dates
start_date = parser.parse("2018-05-01")
end_date   = parser.parse("2018-06-06")

# Get data
nyc_tlc = NycTlcFhv(start_date=start_date, end_date=end_date)
nyc_tlc_df = nyc_tlc.to_spark_dataframe()

# display(nyc_tlc_df.limit(5))

# Write data
(nyc_tlc_df.write
    .mode("append")
    .format("parquet")
    .save(landing_path)
)
print(f"Data written to: {dest_relative_path}")
