# Databricks notebook source
# MAGIC %md
# MAGIC # APPLYING DATAMASK TO TABLES.
# MAGIC - Mask column/s if a user is not within an authorized group.
# MAGIC - Create function first, then apply to table.
# MAGIC - Cannot be directly applied to view, workaround is use the datamask function when creating views.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run once to create a sample table for datamasking demo
# MAGIC -- CREATE TABLE gdc_dbxtraining.leon_bronze.datamask_sql_salesorderheader
# MAGIC -- AS SELECT * FROM gdc_dbxtraining.leon_bronze.sql_salesorderheader

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Function for datamasking
# MAGIC CREATE OR REPLACE FUNCTION gdc_dbxtraining.leon_bronze.fn_datamask_sql_salesorderheader_id(SalesOrderID INT)
# MAGIC   RETURN 
# MAGIC     CASE 
# MAGIC       WHEN is_account_group_member('RLSCheckGroup1') 
# MAGIC       THEN SalesOrderID ELSE '*****' 
# MAGIC     END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply datamask function to view
# MAGIC ALTER TABLE gdc_dbxtraining.leon_bronze.datamask_sql_salesorderheader 
# MAGIC ALTER COLUMN SalesOrderID 
# MAGIC SET MASK gdc_dbxtraining.leon_bronze.fn_datamask_sql_salesorderheader_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test - If current_user is not a member of the group 'RLSCheckGroup1', datamask will be applied
# MAGIC SELECT * FROM gdc_dbxtraining.leon_bronze.datamask_sql_salesorderheader;

# COMMAND ----------

# MAGIC %md
# MAGIC # APPLYING DATAMASK TO VIEWS
# MAGIC - Mask column/s if a user is not within an authorized group.
# MAGIC - Cannot be directly applied to view, workaround is use the datamask function when creating views.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Datamask functions cannot directly apply to views, workaround is use the datamask function when creating views
# MAGIC CREATE OR REPLACE VIEW gdc_dbxtraining.leon_bronze.datamask_vw_sql_salesorderheader AS
# MAGIC SELECT
# MAGIC     gdc_dbxtraining.leon_bronze.fn_datamask_sql_salesorderheader_id(SalesOrderID) AS SalesOrderID,
# MAGIC     RevisionNumber,
# MAGIC     OrderDate,
# MAGIC     DueDate,
# MAGIC     ShipDate,
# MAGIC     Status,
# MAGIC     OnlineOrderFlag,
# MAGIC     SalesOrderNumber,
# MAGIC     PurchaseOrderNumber,
# MAGIC     AccountNumber,
# MAGIC     CustomerID,
# MAGIC     SalesPersonID,
# MAGIC     TerritoryID,
# MAGIC     BillToAddressID,
# MAGIC     ShipToAddressID,
# MAGIC     ShipMethodID,
# MAGIC     CreditCardID,
# MAGIC     CreditCardApprovalCode,
# MAGIC     CurrencyRateID,
# MAGIC     SubTotal,
# MAGIC     TaxAmt,
# MAGIC     Freight,
# MAGIC     TotalDue,
# MAGIC     Comment,
# MAGIC     rowguid,
# MAGIC     ModifiedDate
# MAGIC FROM gdc_dbxtraining.leon_bronze.sql_salesorderheader;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test - If current_user is not a member of the group 'RLSCheckGroup1', datamask will be applied
# MAGIC SELECT * FROM gdc_dbxtraining.leon_bronze.datamask_vw_sql_salesorderheader