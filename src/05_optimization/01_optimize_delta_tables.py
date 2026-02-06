# Databricks notebook source
# MAGIC %md
# MAGIC # OPTIMIZATION: ZORDER & OPTIMIZE.
# MAGIC - Optimize = Compacts small files into fewer bigger files
# MAGIC - ZOrder = optional for Optimize - Data reordering strategy inside 'Optimize', physically cluster the given columns.
# MAGIC     - Data skipping.
# MAGIC     - Fewer bytes scanned.
# MAGIC - Applied by: Running on tables, optionally include ZOrder.
# MAGIC     - OPTIMIZE table1 ZORDER BY(col1, col2,..);

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE DEMO TABLE.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create demo table
# MAGIC DROP TABLE IF EXISTS gdc_dbxtraining.leon_bronze.optimize_01_zorder_optimize;
# MAGIC
# MAGIC CREATE TABLE gdc_dbxtraining.leon_bronze.optimize_01_zorder_optimize
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   CAST((id % 200000) AS BIGINT) AS customer_id,
# MAGIC   DATE_ADD(DATE('2023-01-01'), CAST(id % 365 AS INT)) AS event_date,
# MAGIC   CAST(rand() * 500 AS DOUBLE) AS amount,
# MAGIC   CAST((rand() * 5) + 1 AS INT) AS units,
# MAGIC   CASE
# MAGIC     WHEN (id % 4)=0 THEN 'web'
# MAGIC     WHEN (id % 4)=1 THEN 'store'
# MAGIC     WHEN (id % 4)=2 THEN 'partner'
# MAGIC     ELSE 'mobile'
# MAGIC   END AS channel,
# MAGIC   current_timestamp() AS ingest_ts
# MAGIC FROM range(0, 10000000);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # REPARTITION TO SMALLER FILES.
# MAGIC   - FOR DEMO PURPOSES, TO HIGHLIGHT OPTIMIZE & ZORDER CAPABILITIES.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rewriting into many small files for demo purposes
# MAGIC CREATE OR REPLACE TABLE gdc_dbxtraining.leon_bronze.optimize_01_zorder_optimize
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT /*+ REPARTITION(2000, customer_id, event_date) */
# MAGIC   customer_id,
# MAGIC   event_date,
# MAGIC   amount,
# MAGIC   units,
# MAGIC   channel,
# MAGIC   ingest_ts
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_01_zorder_optimize;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # BEFORE RUNNING OPTIMIZE & ZORDER.
# MAGIC   - CHECK NUM OF FILES & QUERY TIME.
# MAGIC   - NUMFILES: 1746, RUNTIME: 11.96 SECS.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check num of files for the optimize_01_zorder_optimize table
# MAGIC DESCRIBE DETAIL gdc_dbxtraining.leon_bronze.optimize_01_zorder_optimize;
# MAGIC -- Current count: numFiles = 1746

# COMMAND ----------

# Check Query Time
# Query runtime: 11.96 seconds.
from pyspark.sql import functions as F

q = """
SELECT event_date, SUM(amount) AS revenue, SUM(units) AS units
FROM gdc_dbxtraining.leon_bronze.optimize_01_zorder_optimize
WHERE customer_id = 12345
  AND event_date BETWEEN DATE('2023-06-01') AND DATE('2023-06-30')
GROUP BY event_date
ORDER BY event_date
"""

def time_sql(label, sql_text):
    import time
    t0 = time.time()
    df = spark.sql(sql_text)
    df.count()  # action to force execution
    print(f"{label}: {time.time() - t0:.2f}s")

time_sql("Query before Optimize/Zorder runtime", q)

# COMMAND ----------

# MAGIC %md
# MAGIC # RUN OPTIMIZE & ZORDER.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compacts small files into larger ones
# MAGIC OPTIMIZE gdc_dbxtraining.leon_bronze.optimize_01_zorder_optimize;
# MAGIC
# MAGIC -- Multi-dimensional clustering for common filters (customer_id + event_date)
# MAGIC OPTIMIZE gdc_dbxtraining.leon_bronze.optimize_01_zorder_optimize
# MAGIC ZORDER BY (customer_id, event_date);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # AFTER RUNNING OPTIMIZE & ZORDER.
# MAGIC   - CHECK NUM OF FILES & QUERY TIME.
# MAGIC   - NUMFILES: 1, RUNTIME: 1.8 SECS.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check num of files for the optimize_01_zorder_optimize table
# MAGIC DESCRIBE DETAIL gdc_dbxtraining.leon_bronze.optimize_01_zorder_optimize;
# MAGIC -- Current count: numFiles = 1
# MAGIC
# MAGIC

# COMMAND ----------

# Check Query Time
# Query runtime: 1.8 seconds.
from pyspark.sql import functions as F

q = """
SELECT event_date, SUM(amount) AS revenue, SUM(units) AS units
FROM gdc_dbxtraining.leon_bronze.optimize_01_zorder_optimize
WHERE customer_id = 12345
  AND event_date BETWEEN DATE('2023-06-01') AND DATE('2023-06-30')
GROUP BY event_date
ORDER BY event_date
"""

def time_sql(label, sql_text):
    import time
    t0 = time.time()
    df = spark.sql(sql_text)
    df.count()  # action to force execution
    print(f"{label}: {time.time() - t0:.2f}s")

time_sql("Query after Optimize/Zorder runtime", q)