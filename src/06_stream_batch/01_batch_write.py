# Databricks notebook source
# MAGIC %md
# MAGIC # BATCH WRITE - DATA FROM ADLS.

# COMMAND ----------

# Imports
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Widgets
dbutils.widgets.text("target_table", "gdc_dbxtraining.leon_bronze.stream_batch_unification")
dbutils.widgets.text("file_format", "parquet")
dbutils.widgets.text("csv_header", "true")
dbutils.widgets.text("merge_mode", "merge")

# Configs
target_table = dbutils.widgets.get("target_table").strip()
file_format  = dbutils.widgets.get("file_format").lower().strip()
csv_header   = dbutils.widgets.get("csv_header").lower().strip() == "true"
merge_mode   = dbutils.widgets.get("merge_mode").lower().strip()

# Enforce Unity Catalog context + ensure schema exists
parts = target_table.split(".")
if len(parts) != 3:
    raise ValueError("target_table must be fully qualified: <catalog>.<schema>.<table> (e.g., gdc_dbxtraining.leon_bronze.stream_batch_unification)")

catalog, schema, table = parts

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")

# Target table
target_table_fq = f"{catalog}.{schema}.{table}"
print(f"Taget_table_fq:{target_table_fq}")

# ADLS configs
storage_account = "dlsdbxtraining001"
container = "landingzone-leon"
base_folder = "stream_batch_unification"
adls_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{base_folder}/"

adls_key = dbutils.secrets.get(scope="kv-acesskeys-gdc", key="dlsdbxtraining001-accesskey")
spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", adls_key)

# Functions
def col_or_null(df, name, cast_type=None):
    c = F.col(name) if name in df.columns else F.lit(None)
    return c.cast(cast_type) if cast_type else c

def table_exists(full_name: str) -> bool:
    return spark.catalog.tableExists(full_name)

# Read from ADLS
reader = spark.read
if file_format == "parquet":
    df_raw = reader.parquet(adls_path)
elif file_format == "json":
    df_raw = reader.json(adls_path)
elif file_format == "csv":
    df_raw = reader.option("header", csv_header).csv(adls_path)
else:
    raise ValueError("file_format must be parquet | json | csv")

# Create schema
df = df_raw.select(
    col_or_null(df_raw, "timestamp", "string").alias("timestamp"),
    col_or_null(df_raw, "sequence", "long").alias("sequence"),
    col_or_null(df_raw, "deviceId", "string").alias("deviceId"),
    col_or_null(df_raw, "temperatureC", "double").alias("temperatureC"),
    col_or_null(df_raw, "status", "string").alias("status"),
    F.to_timestamp(col_or_null(df_raw, "enqueuedTime", "string")).alias("enqueuedTime"),
    col_or_null(df_raw, "offset", "string").alias("offset"),
    col_or_null(df_raw, "sequenceNumber", "long").alias("sequenceNumber"),
    col_or_null(df_raw, "partitionId", "string").alias("partitionId"),
    F.coalesce(col_or_null(df_raw, "etlLoadTs", "timestamp"), F.current_timestamp()).alias("etlLoadTs"),
)

# Generate eventId
df = df.withColumn(
    "eventId",
    F.when(
        F.col("partitionId").isNotNull() & F.col("offset").isNotNull(),
        F.sha2(F.concat_ws("||", F.lit("eh"), F.col("partitionId"), F.col("offset")), 256)
    ).otherwise(
        F.sha2(F.concat_ws("||", F.lit("batch"), F.col("deviceId"), F.col("timestamp"), F.col("sequence")), 256)
    )
).filter(F.col("eventId").isNotNull())

df_out = df.select(
    "eventId","timestamp","sequence","deviceId","temperatureC","status",
    "enqueuedTime","offset","sequenceNumber","partitionId","etlLoadTs"
)

# Create table if not exists, else write (merge/append)
if not table_exists(target_table_fq):
    # First run: create table by writing (guarantees correct schema, avoids merge key issues)
    (df_out.write.format("delta")
        .mode("overwrite")
        .saveAsTable(target_table_fq))
    print(f"Created table and loaded data → {target_table_fq}")
else:
    if merge_mode == "append":
        df_out.write.format("delta").mode("append").saveAsTable(target_table_fq)
        print(f"Appended into → {target_table_fq}")
    else:
        spark.sql(f"REFRESH TABLE {target_table_fq}")
        dt = DeltaTable.forName(spark, target_table_fq)
        (dt.alias("t")
          .merge(df_out.alias("s"), "t.eventId = s.eventId")
          .whenMatchedUpdateAll()
          .whenNotMatchedInsertAll()
          .execute())
        print(f"Merged into → {target_table_fq}")

print(f"ADLS path: {adls_path}")
print(f"Rows processed: {df_out.count()}")
print(f"Current catalog/schema: {spark.sql('select current_catalog(), current_schema()').collect()}")