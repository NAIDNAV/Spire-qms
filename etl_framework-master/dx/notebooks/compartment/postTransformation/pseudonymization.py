# Databricks notebook source
# MAGIC %run ../../mdmf/includes/init

# COMMAND ----------

# DBTITLE 1,Application_MEF_Staging transformation views
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW Application_MEF_StagingSQL.V_Customer AS 
# MAGIC SELECT
# MAGIC    c.CustomerID
# MAGIC   ,c.FirstName
# MAGIC   ,c.LastName
# MAGIC FROM Application_MEF_StagingSQL.Customer c;