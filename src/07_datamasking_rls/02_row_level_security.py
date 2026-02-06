# Databricks notebook source
# MAGIC %md
# MAGIC # APPLYING ROW-LEVEL SECURITY TO TABLES.
# MAGIC - Filter out rows if a user is not in authorized group.
# MAGIC - Create function first, then apply to table. Can be applied to table - programatically or in catalog explorer.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run once to create sample table to use for row-level security filtering
# MAGIC -- CREATE TABLE gdc_dbxtraining.leon_bronze.rls_adls_yellow_taxi 
# MAGIC -- AS SELECT * FROM gdc_dbxtraining.leon_bronze.adls_yellow_taxi

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Function for row-level filtering
# MAGIC CREATE OR REPLACE FUNCTION gdc_dbxtraining.leon_bronze.fn_rls_yellow_taxi_vendor(vendorID INT)
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('RLSCheckGroup1') THEN TRUE
# MAGIC     ELSE vendorID = 1
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply the row filter to the EXISTING table
# MAGIC -- You can also apply this in the catalog explorer - Go to table -> Overview -> Row Filter
# MAGIC ALTER TABLE gdc_dbxtraining.leon_bronze.rls_adls_yellow_taxi
# MAGIC SET ROW FILTER gdc_dbxtraining.leon_bronze.fn_rls_yellow_taxi_vendor ON (vendorID);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- IF current_user is not a member of the group 'RLSCheckGroup1', rows with vendorID = '2' will be filtered out
# MAGIC SELECT * FROM gdc_dbxtraining.leon_bronze.rls_adls_yellow_taxi WHERE vendorID = '2'

# COMMAND ----------

# MAGIC %md
# MAGIC # APPLYING ROW-LEVEL SECURITY TO VIEWS.
# MAGIC - Filter out rows if a user is not in authorized group.
# MAGIC - Cannot apply directly to views, so we should apply the logic directly to the view.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row-level functions cannot apply directly to views, so we should apply the logic directly to the view
# MAGIC CREATE OR REPLACE VIEW gdc_dbxtraining.leon_bronze.rls_vw_adls_yellow_taxi AS
# MAGIC SELECT *
# MAGIC FROM gdc_dbxtraining.leon_bronze.adls_yellow_taxi
# MAGIC WHERE
# MAGIC   CASE
# MAGIC     WHEN is_account_group_member('RLSCheckGroup1') THEN TRUE
# MAGIC     ELSE vendorID = 1
# MAGIC   END;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- IF current_user is not a member of the group 'RLSCheckGroup1', rows with vendorID = '2' will be filtered out
# MAGIC SELECT * FROM gdc_dbxtraining.leon_bronze.rls_vw_adls_yellow_taxi WHERE vendorID = '2'