# Databricks notebook source
# MAGIC %run ../includes/init

# COMMAND ----------

# MAGIC %run ./fwkTableCreatorManager

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

fwkTableCreatorManager = FwkTableCreatorManager(spark, compartmentConfig)
output = fwkTableCreatorManager.createFwkTables()

# COMMAND ----------

dbutils.notebook.exit(output)
