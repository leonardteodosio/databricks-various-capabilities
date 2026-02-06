# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE VIEWS FOR DASHBOARD.

# COMMAND ----------

# DBTITLE 1,Create views for dashboard
# MAGIC %sql
# MAGIC -- Create views for dashboard charts
# MAGIC -- Top Pickup Locations (Bar Chart)
# MAGIC CREATE OR REPLACE VIEW gdc_dbxtraining.leon_silver.vw_top_pickup_locations AS
# MAGIC SELECT
# MAGIC     puLocationId,
# MAGIC     COUNT(*) AS trip_count
# MAGIC FROM gdc_dbxtraining.leon_silver.adls_yellow_taxi
# MAGIC GROUP BY puLocationId
# MAGIC ORDER BY trip_count DESC
# MAGIC LIMIT 20;
# MAGIC
# MAGIC -- Tip Analysis (Bar Chart)
# MAGIC CREATE OR REPLACE VIEW gdc_dbxtraining.leon_silver.vw_tip_analysis AS
# MAGIC SELECT
# MAGIC     paymentType,
# MAGIC     ROUND(AVG(tipAmount), 2) AS avg_tip,
# MAGIC     ROUND(SUM(tipAmount), 2) AS total_tips
# MAGIC FROM gdc_dbxtraining.leon_silver.adls_yellow_taxi
# MAGIC GROUP BY paymentType;
# MAGIC