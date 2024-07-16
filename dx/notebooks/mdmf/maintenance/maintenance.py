# Databricks notebook source
# MAGIC %run ../includes/init

# COMMAND ----------

# MAGIC %run ./maintenanceManager

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

maintenanceManager = MaintenanceManager(spark, compartmentConfig)
output = maintenanceManager.executeMaintenanceTasks()
maintenanceManager.printOutput(output)

# COMMAND ----------

assert output["success"] == True, "Execution of maintenance tasks have failed"

dbutils.notebook.exit(output)
