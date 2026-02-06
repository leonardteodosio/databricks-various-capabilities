# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER LOAD: USING DLT TABLES.

# COMMAND ----------

# DBTITLE 1,Main: Silver Load Using DLT
# Imports
import dlt
from pyspark.sql import functions as F

# Load Yellow Taxi using DLT
@dlt.table(name="adls_yellow_taxi", comment="Silver ADLS Yellow Taxi (deduped)")
def adls_yellow_taxi():
    return (
        spark.readStream
        .table("gdc_dbxtraining.leon_bronze.adls_yellow_taxi")
        .withColumn("tpepPickupDateTime", F.to_timestamp("tpepPickupDateTime"))
        .withWatermark("tpepPickupDateTime", "1 day")
        .dropDuplicates(["vendorID", "tpepPickupDateTime", "puLocationId", "doLocationId"])
    )

# Load Yellow Taxi using DLT
@dlt.table(name="adls_green_taxi", comment="Silver ADLS Green Taxi (deduped)")
def adls_green_taxi():
    return (
        spark.readStream
        .table("gdc_dbxtraining.leon_bronze.adls_green_taxi")
        .withColumn("lpepPickupDatetime", F.to_timestamp("lpepPickupDatetime"))
        .withWatermark("lpepPickupDatetime", "1 day")
        .dropDuplicates(["vendorID", "lpepPickupDatetime", "puLocationId", "doLocationId"])
    )

# Load Yellow Taxi using DLT
@dlt.table(name="adls_independent_taxi", comment="Silver ADLS Independent Taxi (deduped)")
def adls_independent_taxi():
    return (
        spark.readStream
        .table("gdc_dbxtraining.leon_bronze.adls_independent_taxi")
        .withColumn("pickupDateTime", F.to_timestamp("pickupDateTime"))
        .withColumn("dropOffDateTime", F.to_timestamp("dropOffDateTime"))
        .withWatermark("pickupDateTime", "1 day")
        .dropDuplicates(["dispatchBaseNum", "pickupDateTime", "dropOffDateTime", "puLocationId", "doLocationId"])
    )
