# Databricks notebook source
# MAGIC %md
# MAGIC # OPTIMIZATION: PHOTON.
# MAGIC - Photon = vectorized native execution engine (C++ under the hood) - insted of JVM-based.
# MAGIC - Works best on SQL + Delta + Parquet.

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE DEMO TABLE.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gdc_dbxtraining.leon_bronze;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gdc_dbxtraining.leon_bronze.optimize_06_photon
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   id                                  AS sale_id,
# MAGIC   cast(id % 500000 as bigint)          AS customer_id,
# MAGIC   cast(id % 5000 as int)               AS product_id,
# MAGIC   date_add(date('2023-01-01'), cast(id % 730 as int)) AS sale_date,
# MAGIC   cast((id % 10) + 1 as int)           AS quantity,
# MAGIC   cast((id % 200) * 1.17 as double)    AS unit_price,
# MAGIC   cast((id % 12) as int)               AS region_id,
# MAGIC   cast((id % 30) as int)               AS channel_id
# MAGIC FROM range(10000000);

# COMMAND ----------

# MAGIC %md
# MAGIC # PHOTON DISABLED.
# MAGIC - Query Runtime = 10.15 secs.

# COMMAND ----------

# DBTITLE 1,Query runtime with Photon Disabled
# MAGIC %sql
# MAGIC
# MAGIC -- Run with Photon off
# MAGIC -- Current runtime: 10.15s.
# MAGIC SET spark.databricks.photon.enabled = false;
# MAGIC
# MAGIC UNCACHE TABLE gdc_dbxtraining.leon_bronze.optimize_06_photon;
# MAGIC CLEAR CACHE;
# MAGIC
# MAGIC SELECT
# MAGIC   region_id,
# MAGIC   channel_id,
# MAGIC   date_trunc('month', sale_date) AS sale_month,
# MAGIC   COUNT(*) AS orders,
# MAGIC   SUM(quantity) AS units,
# MAGIC   SUM(quantity * unit_price) AS revenue,
# MAGIC   approx_count_distinct(customer_id) AS approx_customers
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_06_photon
# MAGIC WHERE sale_date >= date('2024-01-01')
# MAGIC   AND sale_date <  date('2024-10-01')
# MAGIC   AND region_id IN (1,2,3,4,5,6,7,8)
# MAGIC GROUP BY region_id, channel_id, date_trunc('month', sale_date)
# MAGIC ORDER BY revenue DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # PHOTON ENABLED.
# MAGIC - Query Runtime = 2.89 secs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.photon.enabled;

# COMMAND ----------

# DBTITLE 1,Query runtime with Photon Enabled
# MAGIC %sql
# MAGIC
# MAGIC -- Run with Photon on
# MAGIC -- Current runtime: 3.45s.
# MAGIC SET spark.databricks.photon.enabled = true;
# MAGIC
# MAGIC UNCACHE TABLE gdc_dbxtraining.leon_bronze.optimize_06_photon;
# MAGIC CLEAR CACHE;
# MAGIC
# MAGIC SELECT
# MAGIC   region_id,
# MAGIC   channel_id,
# MAGIC   date_trunc('month', sale_date) AS sale_month,
# MAGIC   COUNT(*) AS orders,
# MAGIC   SUM(quantity) AS units,
# MAGIC   SUM(quantity * unit_price) AS revenue,
# MAGIC   approx_count_distinct(customer_id) AS approx_customers
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_06_photon
# MAGIC WHERE sale_date >= date('2024-01-01')
# MAGIC   AND sale_date <  date('2024-10-01')
# MAGIC   AND region_id IN (1,2,3,4,5,6,7,8)
# MAGIC GROUP BY region_id, channel_id, date_trunc('month', sale_date)
# MAGIC ORDER BY revenue DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # CHECK EXECUTION PLAN.
# MAGIC - Look for Photon-native execution in the Plan.
# MAGIC
# MAGIC - With Photon On, it uses native vectorized execution, not row-based execution.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Run with Photon off
# MAGIC SET spark.databricks.photon.enabled = false;
# MAGIC
# MAGIC UNCACHE TABLE gdc_dbxtraining.leon_bronze.optimize_06_photon;
# MAGIC CLEAR CACHE;
# MAGIC
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT
# MAGIC   region_id,
# MAGIC   channel_id,
# MAGIC   date_trunc('month', sale_date) AS sale_month,
# MAGIC   COUNT(*) AS orders,
# MAGIC   SUM(quantity) AS units,
# MAGIC   SUM(quantity * unit_price) AS revenue,
# MAGIC   approx_count_distinct(customer_id) AS approx_customers
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_06_photon
# MAGIC WHERE sale_date >= date('2024-01-01')
# MAGIC   AND sale_date <  date('2024-10-01')
# MAGIC   AND region_id IN (1,2,3,4,5,6,7,8)
# MAGIC GROUP BY region_id, channel_id, date_trunc('month', sale_date)
# MAGIC ORDER BY revenue DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Run with Photon on
# MAGIC SET spark.databricks.photon.enabled = false;
# MAGIC
# MAGIC UNCACHE TABLE gdc_dbxtraining.leon_bronze.optimize_06_photon;
# MAGIC CLEAR CACHE;
# MAGIC
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT
# MAGIC   region_id,
# MAGIC   channel_id,
# MAGIC   date_trunc('month', sale_date) AS sale_month,
# MAGIC   COUNT(*) AS orders,
# MAGIC   SUM(quantity) AS units,
# MAGIC   SUM(quantity * unit_price) AS revenue,
# MAGIC   approx_count_distinct(customer_id) AS approx_customers
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_06_photon
# MAGIC WHERE sale_date >= date('2024-01-01')
# MAGIC   AND sale_date <  date('2024-10-01')
# MAGIC   AND region_id IN (1,2,3,4,5,6,7,8)
# MAGIC GROUP BY region_id, channel_id, date_trunc('month', sale_date)
# MAGIC ORDER BY revenue DESC;
# MAGIC