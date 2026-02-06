# Databricks notebook source
# MAGIC %md
# MAGIC # LOAD DUMMY DATA TO SQL DB TO SIMULATE INCREMENTAL.

# COMMAND ----------

# DBTITLE 1,Configs
# JDBC Connection
jdbc_hostname = "sqldbdbxtraining.database.windows.net"
jdbc_port = 1433
jdbc_database = "sqldb-gdcdbxtraining"

jdbc_url = (
  f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};"
  f"database={jdbc_database};"
  "encrypt=true;"
  "trustServerCertificate=false;"
  "hostNameInCertificate=*.database.windows.net;"
  "loginTimeout=30;"
)

props = {
  "user": "sqladministrator",
  "password": dbutils.secrets.get("kv-acesskeys-gdc", "sqladministrator-password"),
  "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Test connection
# test = (spark.read.format("jdbc")
#   .option("url", jdbc_url)
#   .option("query", "SELECT 1 AS ok")
#   .options(**props)
#   .load()
# )
# display(test)


# COMMAND ----------

# DBTITLE 1,Get max_id from Databricks SalesOrderDetail
from pyspark.sql import functions as F

# Get max_id from Databricks SalesOrderDetail
max_ids_df = (
    spark.read
    .jdbc(
        url=jdbc_url,
        table="leon.SalesOrderDetail",
        properties=props
    )
    .agg(
        F.max("SalesOrderID").alias("max_salesorderid"),
        F.max("SalesOrderDetailID").alias("max_salesorderdetailid")
    )
)

max_ids = max_ids_df.collect()[0]

base_salesorderid = max_ids["max_salesorderid"] or 0
base_salesorderdetailid = max_ids["max_salesorderdetailid"] or 0


# COMMAND ----------

# DBTITLE 1,Create dummy data
from pyspark.sql.types import *

# Create dummy data from max_id +1/+2/+3
data = [
    (
        base_salesorderid + 1,
        base_salesorderdetailid + 1,
        "CTN-9001",
        1,
        707,
        1,
        2100.50,
        0.00,
        2100.50
    ),
    (
        base_salesorderid + 2,
        base_salesorderdetailid + 2,
        "CTN-9002",
        2,
        708,
        1,
        1500.00,
        0.00,
        3000.00
    ),
    (
        base_salesorderid + 3,
        base_salesorderdetailid + 3,
        "CTN-9003",
        1,
        709,
        1,
        999.99,
        0.00,
        999.99
    )
]

# Create Schema
schema = StructType([
    StructField("SalesOrderID", IntegerType(), False),
    StructField("SalesOrderDetailID", IntegerType(), False),
    StructField("CarrierTrackingNumber", StringType(), True),
    StructField("OrderQty", IntegerType(), False),
    StructField("ProductID", IntegerType(), False),
    StructField("SpecialOfferID", IntegerType(), False),
    StructField("UnitPrice", DoubleType(), False),
    StructField("UnitPriceDiscount", DoubleType(), False),
    StructField("LineTotal", DoubleType(), False)
])

# Create dataframe for insertion
df = spark.createDataFrame(data, schema)

df_final = (
    df
    .withColumn("rowguid", F.expr("uuid()"))
    .withColumn("ModifiedDate", F.current_timestamp())
)


# COMMAND ----------

# DBTITLE 1,Insert to sql db using JDB Connection
# Insert to sql db using JDB Connection
(
    df_final
    .write
    .mode("append")
    .jdbc(
        url=jdbc_url,
        table="leon.SalesOrderDetail",
        properties=props
    )
)


# COMMAND ----------

# DBTITLE 1,Verify dummy data insertion
verify_df = (
    spark.read
    .jdbc(
        url=jdbc_url,
        table="leon.SalesOrderDetail",
        properties=connection_properties
    )
    .orderBy(F.col("ModifiedDate").desc())
)

display(verify_df.limit(5))
