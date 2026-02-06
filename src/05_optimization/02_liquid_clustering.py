# Databricks notebook source
# MAGIC %md
# MAGIC # OPTIMIZATION: LIQUID CLUSTERING.
# MAGIC - Liquid Clustering = gradually/automatically organizes your data files based on query patterns.
# MAGIC - Applied by: On Table creation or 'Alter Table' Existing Table.
# MAGIC     - ALTER TABLE catalog.schema.table_name
# MAGIC     SET TBLPROPERTIES (
# MAGIC       'delta.liquidClustering.enabled' = 'true'
# MAGIC     );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE DEMO TABLE.

# COMMAND ----------

# DBTITLE 1,Create source table for liquid-clustered and standard-partitioned
from pyspark.sql import functions as F

# Create source table for liquid-clustered and standard-partitioned
N_ROWS = 10_000_000
regions = ["NA", "EMEA", "APAC", "LATAM"]
cats = ["Electronics", "Apparel", "Grocery", "Home"]

df = (
    spark.range(N_ROWS)
    .withColumn(
        "region",
        F.element_at(F.array(*[F.lit(x) for x in regions]),
                     (F.rand()*len(regions)+1).cast("int"))
    )
    .withColumn(
        "product_category",
        F.element_at(F.array(*[F.lit(x) for x in cats]),
                     (F.rand()*len(cats)+1).cast("int"))
    )
    .withColumn("order_date", F.date_add(F.lit("2024-01-01"), (F.rand()*365).cast("int")))
    .withColumn("order_month", F.month("order_date"))
    .withColumn("revenue", (F.rand()*500).cast("double"))
)

# Create many files on purpose - so clustering can show benefit
(
    df.repartition(2000, "order_month")
      .write.format("delta")
      .mode("overwrite")
      .saveAsTable("gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_source")
)

spark.sql("ANALYZE TABLE gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_source COMPUTE STATISTICS")


# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE STANDARD PARTITIONED TABLE.

# COMMAND ----------

# DBTITLE 1,Create standard partitioned table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_standard;
# MAGIC
# MAGIC -- Create standard partitioned table
# MAGIC CREATE TABLE gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_standard
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (order_month)
# MAGIC AS
# MAGIC SELECT region, product_category, order_date, order_month, revenue
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_source;
# MAGIC
# MAGIC OPTIMIZE gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_standard;
# MAGIC ANALYZE TABLE gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_standard COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #CREATE LIQUID PARTITIONED TABLE.

# COMMAND ----------

# DBTITLE 1,Create liquid clustering enabled table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_liquid;
# MAGIC
# MAGIC -- Create liquid clustering enabled table
# MAGIC CREATE TABLE gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_liquid
# MAGIC USING DELTA
# MAGIC CLUSTER BY (region, product_category, order_month)
# MAGIC AS
# MAGIC SELECT
# MAGIC   region,
# MAGIC   product_category,
# MAGIC   order_date,
# MAGIC   month(order_date) AS order_month,
# MAGIC   revenue
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_source;
# MAGIC
# MAGIC OPTIMIZE gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_liquid;
# MAGIC ANALYZE TABLE gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_liquid COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # RUN QUERIES - STANDARD VS LIQUID CLUSTERING.
# MAGIC - Partitioning wins when your filter matches the partition column (like month).
# MAGIC - Liquid clustering wins when you filter on multiple non-partition dimensions (like region + product_category) and want fewer files/bytes scanned.

# COMMAND ----------

# DBTITLE 1,Query benchmarks
# Check Query Time for standard partitioned table
from pyspark.sql import functions as F

q = """
SELECT sum(revenue)
FROM gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_standard
WHERE region='APAC' AND product_category='Electronics';
"""

def time_sql(label, sql_text):
    import time
    t0 = time.time()
    df = spark.sql(sql_text)
    df.count()
    print(f"{label}: {time.time() - t0:.2f}s")

time_sql("Query runtime on standard partitioned table", q)

# Check Query Time for liquid clustering enabled table
from pyspark.sql import functions as F

q = """
SELECT sum(revenue)
FROM gdc_dbxtraining.leon_bronze.optimize_02_liquid_cluster_liquid
WHERE region='APAC' AND product_category='Electronics';
"""

def time_sql(label, sql_text):
    import time
    t0 = time.time()
    df = spark.sql(sql_text)
    df.count()
    print(f"{label}: {time.time() - t0:.2f}s")

time_sql("Query runtime on liquid clustered enabled table", q)
