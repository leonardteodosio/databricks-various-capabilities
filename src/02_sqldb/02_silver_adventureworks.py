# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER LOAD: USING DLT.

# COMMAND ----------

# DBTITLE 1,Main: Silver Load Using DLT
import dlt
from pyspark.sql import functions as F

# Silver: sql_transactionhistory
# Keys: TransactionID
@dlt.table(
    name="sql_transactionhistory",
    comment="Silver SQL TransactionHistory (append-only, dedup by TransactionID)"
)
def silver_sql_transactionhistory():
    df = (
        spark.readStream
        .table("gdc_dbxtraining.leon_bronze.sql_transactionhistory")
        .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
        .withWatermark("ModifiedDate", "7 days")   # safer than 1 day for late data
    )
    return df.dropDuplicates(["TransactionID"])


# Silver: sql_salesorderdetail
# Keys: SalesOrderID + SalesOrderDetailID
@dlt.table(
    name="sql_salesorderdetail",
    comment="Silver SQL SalesOrderDetail (append-only, dedup by SalesOrderID+SalesOrderDetailID)"
)
def silver_sql_salesorderdetail():
    df = (
        spark.readStream
        .table("gdc_dbxtraining.leon_bronze.sql_salesorderdetail")
        .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
        .withWatermark("ModifiedDate", "7 days")
    )
    return df.dropDuplicates(["SalesOrderID", "SalesOrderDetailID"])


# Silver: sql_salesorderheader
# Keys: SalesOrderID
@dlt.table(
    name="sql_salesorderheader",
    comment="Silver SQL SalesOrderHeader (append-only, dedup by SalesOrderID)"
)
def silver_sql_salesorderheader():
    df = (
        spark.readStream
        .table("gdc_dbxtraining.leon_bronze.sql_salesorderheader")
        .withColumn("ModifiedDate", F.to_timestamp("ModifiedDate"))
        .withWatermark("ModifiedDate", "7 days")
    )
    return df.dropDuplicates(["SalesOrderID"])
