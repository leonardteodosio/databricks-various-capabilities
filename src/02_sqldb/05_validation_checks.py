# Databricks notebook source
# DBTITLE 1,Needed checks
# Imports
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType, BooleanType
from pyspark.sql import Row

# Widgets
dbutils.widgets.text("run_id", "") 
dbutils.widgets.text("fail_on_warn", "false")
dbutils.widgets.text("sample_bad_rows", "20")

run_id = dbutils.widgets.get("run_id").strip() or None
fail_on_warn = dbutils.widgets.get("fail_on_warn").lower().strip() == "true"
sample_bad_rows = int(dbutils.widgets.get("sample_bad_rows"))

# Table configs - list required columns and key columns for each table
TABLES = [
    {
        "table": "gdc_dbxtraining.leon_bronze.sql_salesorderdetail",
        "key_cols": ["SalesOrderID", "SalesOrderDetailID"],
        "required_cols": [
            "SalesOrderID","SalesOrderDetailID","OrderQty","ProductID","SpecialOfferID",
            "UnitPrice","UnitPriceDiscount","LineTotal","rowguid","ModifiedDate",
            "_ingest_ts","_source_table_fqn","_run_id"
        ]
    },
    {
        "table": "gdc_dbxtraining.leon_bronze.sql_salesorderheader",
        "key_cols": ["SalesOrderID"],
        "required_cols": [
            "SalesOrderID","RevisionNumber","OrderDate","DueDate","ShipDate","Status",
            "OnlineOrderFlag","SalesOrderNumber","CustomerID",
            "SubTotal","TaxAmt","Freight","TotalDue",
            "rowguid","ModifiedDate","_ingest_ts","_source_table_fqn","_run_id"
        ]
    },
    {
        "table": "gdc_dbxtraining.leon_bronze.sql_transactionhistory",
        "key_cols": ["TransactionID"],
        "required_cols": [
            "TransactionID","ProductID","ReferenceOrderID","ReferenceOrderLineID",
            "TransactionDate","TransactionType","Quantity","ActualCost","ModifiedDate",
            "_ingest_ts","_source_table_fqn","_run_id"
        ]
    },
]

# Functions
def read_table(fqn: str):
    df = spark.table(fqn)
    if run_id:
        # assumes _run_id exists on all 3 tables
        df = df.filter(F.col("_run_id") == run_id)
    return df

def add_result(results, table, check, status, details=None, metric=None):
    results.append({
        "table": table,
        "check": check,
        "status": status,  # PASS / WARN / FAIL
        "metric": metric,
        "details": details
    })

def required_columns_check(df, table, required_cols, results):
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        add_result(results, table, "required_columns_exist", "FAIL", details=f"Missing: {missing}")
        return False
    add_result(results, table, "required_columns_exist", "PASS", details=f"{len(required_cols)} columns present")
    return True

def not_empty_check(df, table, results):
    cnt = df.limit(1).count()
    if cnt == 0:
        add_result(results, table, "not_empty", "FAIL", details="Table has 0 rows (after run_id filter)" if run_id else "Table has 0 rows")
        return False
    add_result(results, table, "not_empty", "PASS")
    return True

def null_key_check(df, table, key_cols, results):
    cond = None
    for c in key_cols:
        ccond = F.col(c).isNull()
        cond = ccond if cond is None else (cond | ccond)
    bad = df.filter(cond)
    bad_count = bad.count()
    if bad_count > 0:
        add_result(results, table, "null_check_key_cols", "FAIL", metric=bad_count, details=f"Nulls found in {key_cols}")
        display(bad.limit(sample_bad_rows))
        return False
    add_result(results, table, "null_check_key_cols", "PASS")
    return True

def duplicate_key_check(df, table, key_cols, results):
    dup = (df.groupBy([F.col(c) for c in key_cols])
             .count()
             .filter(F.col("count") > 1))
    dup_count = dup.count()
    if dup_count > 0:
        add_result(results, table, "duplicate_check_key_cols", "FAIL", metric=dup_count, details=f"Duplicate keys on {key_cols}")
        display(dup.orderBy(F.desc("count")).limit(sample_bad_rows))
        return False
    add_result(results, table, "duplicate_check_key_cols", "PASS")
    return True

def rule_check(df, table, rule_name, bad_condition, results, severity="FAIL"):
    bad = df.filter(bad_condition)
    bad_count = bad.count()
    if bad_count > 0:
        add_result(results, table, rule_name, severity, metric=bad_count, details=f"Bad rows where {rule_name}")
        display(bad.limit(sample_bad_rows))
        return False
    add_result(results, table, rule_name, "PASS")
    return True

def status_rank(s):
    return {"PASS": 0, "WARN": 1, "FAIL": 2}.get(s, 9)

# Run validations
results = []

for cfg in TABLES:
    t = cfg["table"]
    print(f"\n=== Validating: {t} ===")
    df = read_table(t)

    # Check if table is Not Empty
    if not not_empty_check(df, t, results):
        continue

    # Check Required Columns
    if not required_columns_check(df, t, cfg["required_cols"], results):
        continue

    # Check Null key cols
    null_key_check(df, t, cfg["key_cols"], results)

    # Check Duplicate key cols
    duplicate_key_check(df, t, cfg["key_cols"], results)

# Final summary 
schema = StructType([
    StructField("table",   StringType(), True),
    StructField("check",   StringType(), True),
    StructField("status",  StringType(), True),   # PASS / WARN / FAIL
    StructField("metric",  LongType(),   True),   # force numeric type even if all null
    StructField("details", StringType(), True),
])

# Normalize values so types are consistent
rows = []
for r in results:
    rows.append(Row(
        table   = None if r.get("table")   is None else str(r.get("table")),
        check   = None if r.get("check")   is None else str(r.get("check")),
        status  = None if r.get("status")  is None else str(r.get("status")),
        metric  = None if r.get("metric")  is None else int(r.get("metric")),
        details = None if r.get("details") is None else str(r.get("details")),
    ))

res_df = spark.createDataFrame(rows, schema=schema)

res_df = (
    res_df
      .withColumn(
          "status_rank",
          F.when(F.col("status") == "FAIL", F.lit(2))
           .when(F.col("status") == "WARN", F.lit(1))
           .when(F.col("status") == "PASS", F.lit(0))
           .otherwise(F.lit(9))
      )
      .orderBy(F.desc("status_rank"), "table", "check")
      .drop("status_rank")
)

display(res_df)

fail_count = res_df.filter(F.col("status") == "FAIL").count()
warn_count = res_df.filter(F.col("status") == "WARN").count()

if fail_count > 0 or (fail_on_warn and warn_count > 0):
    raise Exception(f"Validation failed. FAIL={fail_count}, WARN={warn_count}, run_id={run_id}")
else:
    print(f"Validation passed. FAIL={fail_count}, WARN={warn_count}, run_id={run_id}")



# COMMAND ----------

# DBTITLE 1,Original checks
# # Imports
# from pyspark.sql import functions as F

# # Widgets
# dbutils.widgets.text("run_id", "")  # optional: validate only one ingestion run_id
# dbutils.widgets.text("fail_on_warn", "false")  # true/false
# dbutils.widgets.text("sample_bad_rows", "20")  # how many bad rows to display per failure

# run_id = dbutils.widgets.get("run_id").strip() or None
# fail_on_warn = dbutils.widgets.get("fail_on_warn").lower().strip() == "true"
# sample_bad_rows = int(dbutils.widgets.get("sample_bad_rows"))

# # Functions
# def read_table(fqn: str):
#     df = spark.table(fqn)
#     if run_id:
#         # assumes _run_id exists on all 3 tables
#         df = df.filter(F.col("_run_id") == run_id)
#     return df

# def add_result(results, table, check, status, details=None, metric=None):
#     results.append({
#         "table": table,
#         "check": check,
#         "status": status,  # PASS / WARN / FAIL
#         "metric": metric,
#         "details": details
#     })

# def required_columns_check(df, table, required_cols, results):
#     missing = [c for c in required_cols if c not in df.columns]
#     if missing:
#         add_result(results, table, "required_columns_exist", "FAIL", details=f"Missing: {missing}")
#         return False
#     add_result(results, table, "required_columns_exist", "PASS", details=f"{len(required_cols)} columns present")
#     return True

# def not_empty_check(df, table, results):
#     cnt = df.limit(1).count()
#     if cnt == 0:
#         add_result(results, table, "not_empty", "FAIL", details="Table has 0 rows (after run_id filter)" if run_id else "Table has 0 rows")
#         return False
#     add_result(results, table, "not_empty", "PASS")
#     return True

# def null_key_check(df, table, key_cols, results):
#     cond = None
#     for c in key_cols:
#         ccond = F.col(c).isNull()
#         cond = ccond if cond is None else (cond | ccond)
#     bad = df.filter(cond)
#     bad_count = bad.count()
#     if bad_count > 0:
#         add_result(results, table, "null_check_key_cols", "FAIL", metric=bad_count, details=f"Nulls found in {key_cols}")
#         display(bad.limit(sample_bad_rows))
#         return False
#     add_result(results, table, "null_check_key_cols", "PASS")
#     return True

# def duplicate_key_check(df, table, key_cols, results):
#     dup = (df.groupBy([F.col(c) for c in key_cols])
#              .count()
#              .filter(F.col("count") > 1))
#     dup_count = dup.count()
#     if dup_count > 0:
#         add_result(results, table, "duplicate_check_key_cols", "FAIL", metric=dup_count, details=f"Duplicate keys on {key_cols}")
#         display(dup.orderBy(F.desc("count")).limit(sample_bad_rows))
#         return False
#     add_result(results, table, "duplicate_check_key_cols", "PASS")
#     return True

# def rule_check(df, table, rule_name, bad_condition, results, severity="FAIL"):
#     bad = df.filter(bad_condition)
#     bad_count = bad.count()
#     if bad_count > 0:
#         add_result(results, table, rule_name, severity, metric=bad_count, details=f"Bad rows where {rule_name}")
#         display(bad.limit(sample_bad_rows))
#         return False
#     add_result(results, table, rule_name, "PASS")
#     return True

# def status_rank(s):
#     return {"PASS": 0, "WARN": 1, "FAIL": 2}.get(s, 9)

# # Table configs - list required columns and key columns for each table
# TABLES = [
#     {
#         "table": "gdc_dbxtraining.leon_bronze.sql_salesorderdetail",
#         "key_cols": ["SalesOrderID", "SalesOrderDetailID"],
#         "required_cols": [
#             "SalesOrderID","SalesOrderDetailID","OrderQty","ProductID","SpecialOfferID",
#             "UnitPrice","UnitPriceDiscount","LineTotal","rowguid","ModifiedDate",
#             "_ingest_ts","_source_table_fqn","_run_id"
#         ],
#         "rules": [
#             ("orderqty_positive", F.col("OrderQty") <= 0, "FAIL"),
#             ("unitprice_non_negative", F.col("UnitPrice") < 0, "FAIL"),
#             ("unitpricediscount_valid", (F.col("UnitPriceDiscount") < 0) | (F.col("UnitPriceDiscount") > 1), "WARN"),
#             ("linetotal_non_negative", F.col("LineTotal") < 0, "FAIL"),
#         ]
#     },
#     {
#         "table": "gdc_dbxtraining.leon_bronze.sql_salesorderheader",
#         "key_cols": ["SalesOrderID"],
#         "required_cols": [
#             "SalesOrderID","RevisionNumber","OrderDate","DueDate","ShipDate","Status",
#             "OnlineOrderFlag","SalesOrderNumber","CustomerID",
#             "SubTotal","TaxAmt","Freight","TotalDue",
#             "rowguid","ModifiedDate","_ingest_ts","_source_table_fqn","_run_id"
#         ],
#         "rules": [
#             ("totaldue_non_negative", F.col("TotalDue") < 0, "FAIL"),
#             ("subtotal_non_negative", F.col("SubTotal") < 0, "FAIL"),
#             ("taxamt_non_negative", F.col("TaxAmt") < 0, "FAIL"),
#             ("freight_non_negative", F.col("Freight") < 0, "FAIL"),
#             ("duedate_gte_orderdate", F.to_timestamp("DueDate") < F.to_timestamp("OrderDate"), "WARN"),
#             ("shipdate_gte_orderdate_when_present",
#              F.col("ShipDate").isNotNull() & (F.to_timestamp("ShipDate") < F.to_timestamp("OrderDate")), "WARN"),
#         ]
#     },
#     {
#         "table": "gdc_dbxtraining.leon_bronze.sql_transactionhistory",
#         "key_cols": ["TransactionID"],
#         "required_cols": [
#             "TransactionID","ProductID","ReferenceOrderID","ReferenceOrderLineID",
#             "TransactionDate","TransactionType","Quantity","ActualCost","ModifiedDate",
#             "_ingest_ts","_source_table_fqn","_run_id"
#         ],
#         "rules": [
#             ("quantity_not_zero", F.col("Quantity") == 0, "WARN"),
#             ("actualcost_non_negative", F.col("ActualCost") < 0, "WARN"),
#             ("transactiontype_allowed",
#              ~F.col("TransactionType").isin(["W","S","P"]), "WARN"),
#         ]
#     },
# ]

# # Run validations
# results = []

# for cfg in TABLES:
#     t = cfg["table"]
#     print(f"\n=== Validating: {t} ===")
#     df = read_table(t)

#     # Check if table is Not Empty
#     if not not_empty_check(df, t, results):
#         continue

#     # Check Required Columns
#     if not required_columns_check(df, t, cfg["required_cols"], results):
#         continue

#     # Check Null key cols
#     null_key_check(df, t, cfg["key_cols"], results)

#     # Check Duplicate key cols
#     duplicate_key_check(df, t, cfg["key_cols"], results)

#     # Additional rules
#     for (rule_name, bad_cond, severity) in cfg["rules"]:
#         rule_check(df, t, rule_name, bad_cond, results, severity=severity)

# # Cross-table referential checks
# detail_tbl = "gdc_dbxtraining.leon_bronze.sql_salesorderdetail"
# header_tbl = "gdc_dbxtraining.leon_bronze.sql_salesorderheader"

# df_d = read_table(detail_tbl).select("SalesOrderID").distinct()
# df_h = read_table(header_tbl).select("SalesOrderID").distinct()

# # Detail must have a header
# missing_headers = df_d.join(df_h, "SalesOrderID", "left_anti")
# mh_count = missing_headers.count()
# if mh_count > 0:
#     add_result(results, detail_tbl, "ref_check_detail_has_header", "WARN", metric=mh_count, details="SalesOrderID in detail not found in header")
#     display(missing_headers.limit(sample_bad_rows))
# else:
#     add_result(results, detail_tbl, "ref_check_detail_has_header", "PASS")

# # Header should have at least one detail
# missing_details = df_h.join(df_d, "SalesOrderID", "left_anti")
# md_count = missing_details.count()
# if md_count > 0:
#     add_result(results, header_tbl, "ref_check_header_has_detail", "WARN", metric=md_count, details="SalesOrderID in header not found in detail")
#     display(missing_details.limit(sample_bad_rows))
# else:
#     add_result(results, header_tbl, "ref_check_header_has_detail", "PASS")
# from pyspark.sql import Row
# from pyspark.sql.types import StructType, StructField, StringType, LongType

# # Final summary 
# schema = StructType([
#     StructField("table",   StringType(), True),
#     StructField("check",   StringType(), True),
#     StructField("status",  StringType(), True),   # PASS / WARN / FAIL
#     StructField("metric",  LongType(),   True),   # force numeric type even if all null
#     StructField("details", StringType(), True),
# ])

# # Normalize values so types are consistent
# rows = []
# for r in results:
#     rows.append(Row(
#         table   = None if r.get("table")   is None else str(r.get("table")),
#         check   = None if r.get("check")   is None else str(r.get("check")),
#         status  = None if r.get("status")  is None else str(r.get("status")),
#         metric  = None if r.get("metric")  is None else int(r.get("metric")),
#         details = None if r.get("details") is None else str(r.get("details")),
#     ))

# res_df = spark.createDataFrame(rows, schema=schema)

# res_df = (
#     res_df
#       .withColumn(
#           "status_rank",
#           F.when(F.col("status") == "FAIL", F.lit(2))
#            .when(F.col("status") == "WARN", F.lit(1))
#            .when(F.col("status") == "PASS", F.lit(0))
#            .otherwise(F.lit(9))
#       )
#       .orderBy(F.desc("status_rank"), "table", "check")
#       .drop("status_rank")
# )

# display(res_df)

# fail_count = res_df.filter(F.col("status") == "FAIL").count()
# warn_count = res_df.filter(F.col("status") == "WARN").count()

# if fail_count > 0 or (fail_on_warn and warn_count > 0):
#     raise Exception(f"Validation failed. FAIL={fail_count}, WARN={warn_count}, run_id={run_id}")
# else:
#     print(f"Validation passed. FAIL={fail_count}, WARN={warn_count}, run_id={run_id}")

