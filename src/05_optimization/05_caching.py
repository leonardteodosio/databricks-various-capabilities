# Databricks notebook source
# MAGIC %md
# MAGIC # OPTIMIZATION: CACHING TABLE.
# MAGIC - Caching is storng in memory(RAM) instead of disk for faster reads.

# COMMAND ----------

# MAGIC %md 
# MAGIC # CREATE DEMO TABLE.

# COMMAND ----------

# DBTITLE 1,Create table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gdc_dbxtraining.leon_bronze.optimize_05_caching;
# MAGIC CREATE SCHEMA IF NOT EXISTS gdc_dbxtraining.leon_bronze;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gdc_dbxtraining.leon_bronze.optimize_05_caching
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   id                                                AS sale_id,
# MAGIC   CASE (id % 6)
# MAGIC     WHEN 0 THEN 'NA'
# MAGIC     WHEN 1 THEN 'EMEA'
# MAGIC     WHEN 2 THEN 'APAC'
# MAGIC     WHEN 3 THEN 'LATAM'
# MAGIC     WHEN 4 THEN 'ANZ'
# MAGIC     ELSE 'INDIA'
# MAGIC   END                                               AS region,
# MAGIC   CASE (id % 8)
# MAGIC     WHEN 0 THEN 'electronics'
# MAGIC     WHEN 1 THEN 'apparel'
# MAGIC     WHEN 2 THEN 'grocery'
# MAGIC     WHEN 3 THEN 'beauty'
# MAGIC     WHEN 4 THEN 'sports'
# MAGIC     WHEN 5 THEN 'home'
# MAGIC     WHEN 6 THEN 'toys'
# MAGIC     ELSE 'automotive'
# MAGIC   END                                               AS product_category,
# MAGIC   CAST(10 + (id % 500) + (rand() * 100) AS DOUBLE)   AS amount,
# MAGIC   date_add(date('2022-01-01'), CAST(id % 730 AS INT)) AS txn_date
# MAGIC FROM range(10000000);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # BEFORE CACHING.

# COMMAND ----------

# DBTITLE 1,Query Runtime: Before Caching
# MAGIC %sql
# MAGIC -- 
# MAGIC SELECT
# MAGIC   region,
# MAGIC   product_category,
# MAGIC   COUNT(*) AS cnt,
# MAGIC   SUM(amount) AS total_amount
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_05_caching
# MAGIC WHERE txn_date BETWEEN '2022-06-01' AND '2022-12-31'
# MAGIC   AND region IN ('NA','EMEA','APAC')
# MAGIC GROUP BY region, product_category
# MAGIC ORDER BY total_amount DESC;
# MAGIC

# COMMAND ----------

import time

query = """
SELECT
  region,
  product_category,
  COUNT(*) AS cnt,
  SUM(amount) AS total_amount
FROM gdc_dbxtraining.leon_bronze.optimize_05_caching
WHERE txn_date BETWEEN '2022-06-01' AND '2022-12-31'
  AND region IN ('NA','EMEA','APAC')
GROUP BY region, product_category
ORDER BY total_amount DESC
"""

def time_query(label, n=3):
    print(f"\n{label}")
    for i in range(1, n+1):
        t0 = time.perf_counter()
        spark.sql(query).collect()   # force execution
        dt = time.perf_counter() - t0
        print(f"Run {i}: {dt:.3f}s")

time_query("Before Cache Table)")


# COMMAND ----------

# MAGIC %md 
# MAGIC # CACHE TABLE.

# COMMAND ----------

# DBTITLE 1,Cache Table
# MAGIC %sql 
# MAGIC
# MAGIC CACHE TABLE gdc_dbxtraining.leon_bronze.optimize_05_caching;
# MAGIC
# MAGIC -- Warm step (forces a scan into cache)
# MAGIC SELECT COUNT(*) FROM gdc_dbxtraining.leon_bronze.optimize_05_caching;

# COMMAND ----------

# MAGIC %md
# MAGIC # AFTER CACHING.

# COMMAND ----------

# DBTITLE 1,Query Runtime: After Caching
time_query("After Cache Table")

# COMMAND ----------

# MAGIC %md
# MAGIC # CLEAR CACHE.

# COMMAND ----------

# DBTITLE 1,Uncache table
# MAGIC %sql
# MAGIC
# MAGIC UNCACHE TABLE gdc_dbxtraining.leon_bronze.optimize_05_caching;
# MAGIC CLEAR CACHE;