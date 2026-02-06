# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE WATERMARK & METADATA TABLE FOR SQL DB INCREMENTAL LOADING.

# COMMAND ----------

# DBTITLE 1,Main : Create tables
# MAGIC %sql
# MAGIC -- Choose your Bronze schema
# MAGIC CREATE SCHEMA IF NOT EXISTS gdc_dbxtraining.leon_bronze;
# MAGIC
# MAGIC -- Create metadata table - What tables should load
# MAGIC CREATE TABLE IF NOT EXISTS gdc_dbxtraining.leon_bronze.etl_metadata (
# MAGIC   source_schema       STRING,
# MAGIC   source_table        STRING,
# MAGIC   source_table_fqn    STRING, 
# MAGIC   watermark_column    STRING,
# MAGIC   load_type           STRING,
# MAGIC   enabled             BOOLEAN,
# MAGIC   target_catalog      STRING,
# MAGIC   target_schema       STRING,
# MAGIC   target_table        STRING,
# MAGIC   lookback_minutes    INT,
# MAGIC   created_at          TIMESTAMP,
# MAGIC   updated_at          TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- Create watermark table - Where each table left off
# MAGIC CREATE TABLE IF NOT EXISTS gdc_dbxtraining.leon_bronze.etl_watermark (
# MAGIC   source_table_fqn            STRING,
# MAGIC   watermark_column            STRING,
# MAGIC   last_successful_watermark   TIMESTAMP,
# MAGIC   current_batch_max_watermark TIMESTAMP,
# MAGIC   last_run_id                 STRING,
# MAGIC   last_run_ts                 TIMESTAMP,
# MAGIC   last_row_count              BIGINT,
# MAGIC   status                      STRING,     -- SUCCESS / FAILED
# MAGIC   updated_at                  TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Initial data for tables
# MAGIC %sql
# MAGIC -- Clear existing rows for these tables if you're re-running setup
# MAGIC DELETE FROM gdc_dbxtraining.leon_bronze.etl_metadata
# MAGIC WHERE source_table_fqn IN ('[leon].[TransactionHistory]','[leon].[SalesOrderDetail]','[leon].[SalesOrderHeader]');
# MAGIC
# MAGIC INSERT INTO gdc_dbxtraining.leon_bronze.etl_metadata (
# MAGIC   source_schema, source_table, source_table_fqn,
# MAGIC   watermark_column, load_type, enabled,
# MAGIC   target_catalog, target_schema, target_table,
# MAGIC   lookback_minutes,
# MAGIC   created_at, updated_at
# MAGIC )
# MAGIC VALUES
# MAGIC ('leon','TransactionHistory','[leon].[TransactionHistory]','ModifiedDate','incremental',true,NULL,'leon_bronze','sql_transactionhistory',10,current_timestamp(),current_timestamp()),
# MAGIC ('leon','SalesOrderDetail','[leon].[SalesOrderDetail]','ModifiedDate','incremental',true,NULL,'leon_bronze','sql_salesorderdetail',10,current_timestamp(),current_timestamp()),
# MAGIC ('leon','SalesOrderHeader','[leon].[SalesOrderHeader]','ModifiedDate','incremental',true,NULL,'leon_bronze','sql_salesorderheader',10,current_timestamp(),current_timestamp());
# MAGIC
# MAGIC -- Seed watermark state if missing
# MAGIC MERGE INTO gdc_dbxtraining.leon_bronze.etl_watermark t
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     source_table_fqn,
# MAGIC     watermark_column,
# MAGIC     TIMESTAMP('1900-01-01 00:00:00') AS last_successful_watermark
# MAGIC   FROM gdc_dbxtraining.leon_bronze.etl_metadata
# MAGIC   WHERE enabled = true
# MAGIC     AND source_table_fqn IN ('[leon].[TransactionHistory]','[leon].[SalesOrderDetail]','[leon].[SalesOrderHeader]')
# MAGIC ) s
# MAGIC ON t.source_table_fqn = s.source_table_fqn
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     source_table_fqn, watermark_column, last_successful_watermark,
# MAGIC     current_batch_max_watermark, last_run_id, last_run_ts, last_row_count, status, updated_at
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     s.source_table_fqn, s.watermark_column, s.last_successful_watermark,
# MAGIC     NULL, NULL, NULL, NULL, 'NEVER_RAN', current_timestamp()
# MAGIC   );
# MAGIC