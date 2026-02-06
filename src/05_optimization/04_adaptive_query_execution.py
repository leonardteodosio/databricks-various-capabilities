# Databricks notebook source
# MAGIC %md
# MAGIC # OPTIMIZATION: ADAPTIVE QUERY EXECUTION.
# MAGIC - Adaptive Query Execution = rewrites parts of the plan at runtime to run faster.
# MAGIC   - Uses real shuffle statistics (actual row counts & data sizes) to optimize the rest of the query.
# MAGIC   - Merges small shuffle partitions into larger ones - resulting to less small file overhead.
# MAGIC   - Dynamic Join Strategy Switching - if it detects one table is smaller, it can use Broadcast join.
# MAGIC   - Skew Join Optimization - if it detects data skew on key, itcan split large keys into smaller ones to avoid data skew.

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE DEMO TABLES.

# COMMAND ----------

# DBTITLE 1,Drop tables if exists
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gdc_dbxtraining.leon_bronze;
# MAGIC
# MAGIC DROP TABLE IF EXISTS gdc_dbxtraining.leon_bronze.optimize_04_aqe_fact;
# MAGIC DROP TABLE IF EXISTS gdc_dbxtraining.leon_bronze.optimize_04_aqe_dim;

# COMMAND ----------

# DBTITLE 1,Create fact table
from pyspark.sql import functions as F

catalog = "gdc_dbxtraining"
schema  = "leon_bronze"

fact_tbl = f"{catalog}.{schema}.optimize_04_aqe_fact"

regions = F.array(*[F.lit(x) for x in ["NA","EMEA","APAC","LATAM"]])
cats    = F.array(*[F.lit(x) for x in ["A","B","C","D","E","F"]])

fact = (
    spark.range(0, 10_000_000).toDF("id")
    .withColumn("customer_id", F.pmod(F.col("id") * F.lit(17), F.lit(2_000_000)).cast("int"))

    # Cast element_at index to INT
    .withColumn("region",
        F.element_at(regions, (F.pmod(F.col("id"), F.lit(4)) + F.lit(1)).cast("int"))
    )
    .withColumn("product_category",
        F.element_at(cats, (F.pmod(F.col("id"), F.lit(6)) + F.lit(1)).cast("int"))
    )

    .withColumn("event_date",
        F.date_add(F.to_date(F.lit("2025-01-01")), F.pmod(F.col("id"), F.lit(365)).cast("int"))
    )
    .withColumn("amount", (F.pmod(F.col("id") * F.lit(13), F.lit(10_000)) / F.lit(100.0)).cast("double"))
    .select("id","customer_id","region","product_category","event_date","amount")
)

(fact.write.format("delta").mode("overwrite").saveAsTable(fact_tbl))

spark.sql(f"ANALYZE TABLE {fact_tbl} COMPUTE STATISTICS")
spark.sql(f"ANALYZE TABLE {fact_tbl} COMPUTE STATISTICS FOR COLUMNS customer_id, region, product_category, event_date")

display(spark.table(fact_tbl).limit(5))


# COMMAND ----------

# DBTITLE 1,Create dimension table
from pyspark.sql import functions as F

catalog = "gdc_dbxtraining"
schema  = "leon_bronze"

dim_tbl = f"{catalog}.{schema}.optimize_04_aqe_dim"

tiers = F.array(*[F.lit(x) for x in ["Bronze","Silver","Gold","Platinum"]])
xyz   = F.array(*[F.lit(x) for x in ["X","Y","Z"]])

dim = (
    spark.range(0, 2_000_000).toDF("customer_id")
    .withColumn(
        "customer_tier",
        F.element_at(tiers, (F.pmod(F.col("customer_id"), F.lit(4)) + F.lit(1)).cast("int"))
    )
    .withColumn(
        "signup_date",
        F.date_add(F.to_date(F.lit("2018-01-01")), F.pmod(F.col("customer_id"), F.lit(2000)).cast("int"))
    )
    .withColumn("is_active", (F.pmod(F.col("customer_id"), F.lit(2)) == 0))

    # wide-ish attributes
    .withColumn("attr_01", F.sha2(F.col("customer_id").cast("string"), 256))
    .withColumn("attr_02", (F.col("customer_id") * 3).cast("int"))
    .withColumn("attr_03", (F.col("customer_id") * 7).cast("int"))
    .withColumn("attr_04", F.pmod(F.col("customer_id"), F.lit(1000)).cast("int"))
    .withColumn(
        "attr_05",
        F.element_at(xyz, (F.pmod(F.col("customer_id"), F.lit(3)) + F.lit(1)).cast("int"))
    )
)

(dim.write.format("delta").mode("overwrite").saveAsTable(dim_tbl))

spark.sql(f"ANALYZE TABLE {dim_tbl} COMPUTE STATISTICS")
spark.sql(f"ANALYZE TABLE {dim_tbl} COMPUTE STATISTICS FOR COLUMNS customer_id")

display(spark.table(dim_tbl).limit(5))


# COMMAND ----------

# DBTITLE 1,Join Tables
# MAGIC %sql
# MAGIC -- Wide join + Shuffle-heavy aggregation
# MAGIC SELECT
# MAGIC   f.region,
# MAGIC   f.product_category,
# MAGIC   d.customer_tier,
# MAGIC   date_trunc('month', f.event_date) AS event_month,
# MAGIC   COUNT(*) AS row_cnt,
# MAGIC   SUM(f.amount) AS revenue
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_04_aqe_fact f
# MAGIC JOIN gdc_dbxtraining.leon_bronze.optimize_04_aqe_dim d
# MAGIC   ON f.customer_id = d.customer_id
# MAGIC GROUP BY
# MAGIC   f.region, f.product_category, d.customer_tier, date_trunc('month', f.event_date);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # AQE DISABLED
# MAGIC - Query will run much slower than AQE enabled, and if you view the Spark UI job, it will have many shuffle partitions.

# COMMAND ----------

# DBTITLE 1,Query time: AQE Disabled
# MAGIC %sql
# MAGIC -- Make shuffle “bad on purpose”
# MAGIC SET spark.sql.shuffle.partitions = 2000;
# MAGIC
# MAGIC -- Disable AQE
# MAGIC SET spark.sql.adaptive.enabled = false;
# MAGIC
# MAGIC -- Prove in the plan: you should NOT see "AdaptiveSparkPlan"
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT
# MAGIC   f.region,
# MAGIC   f.product_category,
# MAGIC   d.customer_tier,
# MAGIC   date_trunc('month', f.event_date) AS event_month,
# MAGIC   COUNT(*) AS row_cnt,
# MAGIC   SUM(f.amount) AS revenue
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_04_aqe_fact f
# MAGIC JOIN gdc_dbxtraining.leon_bronze.optimize_04_aqe_dim d
# MAGIC   ON f.customer_id = d.customer_id
# MAGIC GROUP BY
# MAGIC   f.region, f.product_category, d.customer_tier, date_trunc('month', f.event_date);
# MAGIC
# MAGIC -- Run it (watch Spark UI -> SQL / Jobs for shuffle details)
# MAGIC SELECT
# MAGIC   f.region,
# MAGIC   f.product_category,
# MAGIC   d.customer_tier,
# MAGIC   date_trunc('month', f.event_date) AS event_month,
# MAGIC   COUNT(*) AS row_cnt,
# MAGIC   SUM(f.amount) AS revenue
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_04_aqe_fact f
# MAGIC JOIN gdc_dbxtraining.leon_bronze.optimize_04_aqe_dim d
# MAGIC   ON f.customer_id = d.customer_id
# MAGIC GROUP BY
# MAGIC   f.region, f.product_category, d.customer_tier, date_trunc('month', f.event_date);
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # AQE ENABLED
# MAGIC - Query run much much faster and if you view the Spark UI jobs, there is much less/no shuffle partitions.

# COMMAND ----------

# DBTITLE 1,Query time: AQE Enabled
# MAGIC %sql
# MAGIC -- Keep the same “bad” shuffle setting so AQE can correct it
# MAGIC SET spark.sql.shuffle.partitions = 2000;
# MAGIC
# MAGIC -- Enable AQE + coalescing
# MAGIC SET spark.sql.adaptive.enabled = true;
# MAGIC SET spark.sql.adaptive.coalescePartitions.enabled = true;
# MAGIC
# MAGIC -- Optional knobs (leave defaults if you want, but these help make the change obvious)
# MAGIC SET spark.sql.adaptive.advisoryPartitionSizeInBytes = 64m;
# MAGIC
# MAGIC -- Prove in the plan: you SHOULD see "AdaptiveSparkPlan"
# MAGIC -- and often "AQEShuffleRead" / "Coalesced" nodes
# MAGIC EXPLAIN FORMATTED
# MAGIC SELECT
# MAGIC   f.region,
# MAGIC   f.product_category,
# MAGIC   d.customer_tier,
# MAGIC   date_trunc('month', f.event_date) AS event_month,
# MAGIC   COUNT(*) AS row_cnt,
# MAGIC   SUM(f.amount) AS revenue
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_04_aqe_fact f
# MAGIC JOIN gdc_dbxtraining.leon_bronze.optimize_04_aqe_dim d
# MAGIC   ON f.customer_id = d.customer_id
# MAGIC GROUP BY
# MAGIC   f.region, f.product_category, d.customer_tier, date_trunc('month', f.event_date);
# MAGIC
# MAGIC -- Run it again (compare Spark UI metrics vs Run #1)
# MAGIC SELECT
# MAGIC   f.region,
# MAGIC   f.product_category,
# MAGIC   d.customer_tier,
# MAGIC   date_trunc('month', f.event_date) AS event_month,
# MAGIC   COUNT(*) AS row_cnt,
# MAGIC   SUM(f.amount) AS revenue
# MAGIC FROM gdc_dbxtraining.leon_bronze.optimize_04_aqe_fact f
# MAGIC JOIN gdc_dbxtraining.leon_bronze.optimize_04_aqe_dim d
# MAGIC   ON f.customer_id = d.customer_id
# MAGIC GROUP BY
# MAGIC   f.region, f.product_category, d.customer_tier, date_trunc('month', f.event_date);
# MAGIC