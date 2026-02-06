# Databricks notebook source
# MAGIC %md
# MAGIC # BRONZE LOAD: USING AUTOLOADER.
# MAGIC - Incremental load from ADLS Storage to Bronze Table.

# COMMAND ----------

# DBTITLE 1,Imports & Configs
# Imports
from pyspark.sql import functions as F

# Configs
catalog = "gdc_dbxtraining"
schema = "leon_bronze"
storage_account = "dlsdbxtraining001"
container = "landingzone-leon"
base_landing_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/nyctaxi"

table_names = {
    "adls_yellow_taxi": f"{base_landing_path}/01_yellow_taxi",
    "adls_green_taxi": f"{base_landing_path}/02_green_taxi",
    "adls_independent_taxi": f"{base_landing_path}/03_independent_taxi",
}

# Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Set the storage account key
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="kv-acesskeys-gdc", key="dlsdbxtraining001-accesskey")
)

# COMMAND ----------

# DBTITLE 1,Main: Autoloader
# Create Autoloader Function for Bronze ingestion (append-only with Auto Loader)
def autoloader_bronze(dataset: str, source_path: str):
    table_name = f"{catalog}.{schema}.{dataset}"

    # Use DBFS (durable across cluster restarts)
    base_state = f"dbfs:/tmp/leon/nyctaxi/autoloader/{catalog}/{schema}/{dataset}"
    schema_location = f"{base_state}/schema"
    checkpoint_location = f"{base_state}/checkpoint"

    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", schema_location)
            .load(source_path)
            .withColumn("load_timestamp", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
    )

    q = (
        df.writeStream
          .format("delta")
          .option("checkpointLocation", checkpoint_location)
          .outputMode("append")
          .trigger(availableNow=True)
          .toTable(table_name)
    )

    q.awaitTermination()
    print(f"{dataset} ingested into {table_name}")

# Run ingestion
for dataset, path in table_names.items():
    autoloader_bronze(dataset, path)