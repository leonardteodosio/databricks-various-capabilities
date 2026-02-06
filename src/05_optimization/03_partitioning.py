# Databricks notebook source
# MAGIC %md
# MAGIC # OPTIMIZATION: PARTITIONING.
# MAGIC - Partitioning = physically organizes table data into separate folders based on a column value.
# MAGIC - Spark can do faster reads by skipping entire partitions (partition pruning) → fewer files read → faster.
# MAGIC - Avoid too many partitions:
# MAGIC     - Huge partition metadata.
# MAGIC     - Too many small files/directories.
# MAGIC     - Higher scheduling overhead and slower file listing.

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE DEMO TABLES.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Keep shuffle consistent
# MAGIC SET spark.sql.shuffle.partitions = 200;

# COMMAND ----------

# DBTITLE 1,Create source table for partitioned and unpartitioned tables
# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS optimize_03_partitioning;
# MAGIC
# MAGIC -- Create source table for partitioned and unpartitioned tables
# MAGIC CREATE TABLE gdc_dbxtraining.leon_bronze.optimize_03_partitioning (
# MAGIC   txn_id      BIGINT,
# MAGIC   txn_date    DATE,
# MAGIC   customer_id BIGINT,
# MAGIC   product_id  BIGINT,
# MAGIC   amount      DECIMAL(10,2)
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC INSERT OVERWRITE optimize_03_partitioning
# MAGIC SELECT
# MAGIC   id AS txn_id,
# MAGIC   CAST(date_add(DATE '2023-01-01', CAST(pmod(id, 730) AS INT)) AS DATE) AS txn_date,
# MAGIC   pmod(id, 100000) AS customer_id,
# MAGIC   pmod(id, 5000)   AS product_id,
# MAGIC   CAST(rand() * 500 AS DECIMAL(10,2)) AS amount
# MAGIC FROM range(0, 20000000);
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Unpartitioned table
# MAGIC %sql
# MAGIC
# MAGIC -- Create Unpartitioned table
# MAGIC CREATE OR REPLACE TABLE gdc_dbxtraining.leon_bronze.optimize_03_partitioning_unpartitioned
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM gdc_dbxtraining.leon_bronze.optimize_03_partitioning;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create Partitioned table
# MAGIC %sql
# MAGIC
# MAGIC -- Create Partitioned table - by date
# MAGIC CREATE OR REPLACE TABLE gdc_dbxtraining.leon_bronze.optimize_03_partitioning_partitioned
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (txn_date)
# MAGIC AS
# MAGIC SELECT * FROM gdc_dbxtraining.leon_bronze.optimize_03_partitioning;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # RUN QUERIES.

# COMMAND ----------

# DBTITLE 1,Query Unpartitioned Table
# MAGIC %sql
# MAGIC
# MAGIC -- Unpartitioned query
# MAGIC -- Current runtime = 2.94s
# MAGIC SELECT sum(amount) AS revenue
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_03_partitioning_unpartitioned
# MAGIC WHERE txn_date >= DATE '2024-06-01'
# MAGIC   AND txn_date <  DATE '2024-06-08';
# MAGIC

# COMMAND ----------

# DBTITLE 1,Query Partitioned Table
# MAGIC %sql
# MAGIC -- Partitioned
# MAGIC -- Current runtime = 1.94s
# MAGIC SELECT sum(amount) AS revenue
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_03_partitioning_partitioned
# MAGIC WHERE txn_date >= DATE '2024-06-01'
# MAGIC   AND txn_date <  DATE '2024-06-08';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # CHECK EXECUTION PLAN.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check execution plan for unpartitioned
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT sum(amount)
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_03_partitioning_unpartitioned
# MAGIC WHERE txn_date >= DATE '2024-06-01'
# MAGIC   AND txn_date <  DATE '2024-06-08';
# MAGIC
# MAGIC -- In the plan, call out partition filters / partition pruning (you’ll usually see partition predicates tied to txn_date and fewer files scanned).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Check execution plan for partitioned
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT sum(amount)
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_03_partitioning_partitioned
# MAGIC WHERE txn_date >= DATE '2024-06-01'
# MAGIC   AND txn_date <  DATE '2024-06-08';
# MAGIC
# MAGIC -- In the plan, call out partition filters / partition pruning (you’ll usually see partition predicates tied to txn_date and fewer files scanned).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # CLEANUP TABLES IF NEEDED.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS gdc_dbxtraining.leon_bronze.optimize_03_partitioning_partitioned;
# MAGIC -- DROP TABLE IF EXISTS gdc_dbxtraining.leon_bronze.optimize_03_partitioning_unpartitioned;
# MAGIC -- DROP TABLE IF EXISTS gdc_dbxtraining.leon_bronze.optimize_03_partitioning;
# MAGIC