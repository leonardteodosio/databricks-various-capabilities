# Databricks notebook source
# MAGIC %md
# MAGIC # BRONZE TABLE LOAD: USING JDBC CONENCTOR.

# COMMAND ----------

# DBTITLE 1,Imports & Configs
from pyspark.sql import functions as F
import uuid

# JDBC Config
jdbc_url = f"jdbc:sqlserver://sAqldbdbxtraining.database.windows.net:1433;database=sqldb-gdcdbxtraining;"
jdbc_props = {
  "user": "sqladministrator",
  "password": dbutils.secrets.get("kv-acesskeys-gdc", "sqladministrator-password"),
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Set watermark & metadata table names
metadata_tbl = "gdc_dbxtraining.leon_bronze.etl_metadata"
watermark_tbl = "gdc_dbxtraining.leon_bronze.etl_watermark"

run_id = str(uuid.uuid4())

# Load 3 configured tables
targets = ['[leon].[TransactionHistory]','[leon].[SalesOrderDetail]','[leon].[SalesOrderHeader]']

cfg_rows = (
    spark.table(metadata_tbl)
    .filter(F.col("enabled") == True)
    .filter(F.col("source_table_fqn").isin(targets))
    .select("source_table_fqn","watermark_column","target_schema","target_table","lookback_minutes")
    .collect()
)


# COMMAND ----------

# DBTITLE 1,Main: Bronze Load
# Load per table - check watermark & metadata table for each
for cfg in cfg_rows:
    source_table_fqn = cfg["source_table_fqn"]
    watermark_col    = cfg["watermark_column"]
    target_schema    = cfg["target_schema"]
    target_table     = cfg["target_table"]
    lookback_minutes = int(cfg["lookback_minutes"] or 0)

    full_target = f"gdc_dbxtraining.{target_schema}.{target_table}"

    # Get last_successful_watermark
    wm = (
        spark.table(watermark_tbl)
        .filter(F.col("source_table_fqn") == F.lit(source_table_fqn))
        .select("last_successful_watermark")
        .limit(1)
        .collect()
    )
    last_wm = wm[0]["last_successful_watermark"] if wm else None
    if last_wm is None:
        last_wm = "1900-01-01 00:00:00"

    # Apply lookback to reduce chance of missing late updates
    last_wm_expr = f"DATEADD(MINUTE, -{lookback_minutes}, CAST('{last_wm}' AS datetime2))" if lookback_minutes > 0 else f"CAST('{last_wm}' AS datetime2)"

    # Compute stable batch_max at start of run (prevents "moving target" reads)
    batch_max_query = f"(SELECT MAX([{watermark_col}]) AS batch_max FROM {source_table_fqn}) AS t"
    batch_max = spark.read.jdbc(jdbc_url, batch_max_query, properties=jdbc_props).first()["batch_max"]

    if batch_max is None:
        # No rows in table; mark success with no-op update
        spark.sql(f"""
          UPDATE {watermark_tbl}
          SET current_batch_max_watermark = NULL,
              last_run_id = '{run_id}',
              last_run_ts = current_timestamp(),
              last_row_count = 0,
              status = 'SUCCESS',
              updated_at = current_timestamp()
          WHERE source_table_fqn = '{source_table_fqn}'
        """)
        print(f"[SKIP] {source_table_fqn}: empty source table.")
        continue

    # Read incrementally
    where_clause = f"[{watermark_col}] > {last_wm_expr} AND [{watermark_col}] <= CAST('{batch_max}' AS datetime2)"
    src_query = f"(SELECT * FROM {source_table_fqn} WHERE {where_clause}) AS src"

    df = (
        spark.read.jdbc(jdbc_url, src_query, properties=jdbc_props)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_table_fqn", F.lit(source_table_fqn))
        .withColumn("_run_id", F.lit(run_id))
    )

    row_count = df.count()
    if row_count == 0:
        # Nothing new; still advance watermark to batch_max safely
        spark.sql(f"""
          UPDATE {watermark_tbl}
          SET current_batch_max_watermark = TIMESTAMP('{batch_max}'),
              last_successful_watermark = TIMESTAMP('{batch_max}'),
              last_run_id = '{run_id}',
              last_run_ts = current_timestamp(),
              last_row_count = 0,
              status = 'SUCCESS',
              updated_at = current_timestamp()
          WHERE source_table_fqn = '{source_table_fqn}'
        """)
        print(f"[NO-CHANGE] {source_table_fqn}: no new rows. Watermark advanced to {batch_max}.")
        continue

    # Append-only write to Bronze Delta
    (df.write
        .format("delta")
        .mode("append")
        .saveAsTable(full_target)
    )

    # Update watermark state to batch_max
    spark.sql(f"""
      UPDATE {watermark_tbl}
      SET current_batch_max_watermark = TIMESTAMP('{batch_max}'),
          last_successful_watermark = TIMESTAMP('{batch_max}'),
          last_run_id = '{run_id}',
          last_run_ts = current_timestamp(),
          last_row_count = {row_count},
          status = 'SUCCESS',
          updated_at = current_timestamp()
      WHERE source_table_fqn = '{source_table_fqn}'
    """)

    print(f"[OK] {source_table_fqn} → {full_target} | rows={row_count} | new watermark={batch_max}")