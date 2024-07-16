# Databricks notebook source
# widgets values are sent from ADF pipeline
dbutils.widgets.text("FwkLogParameters", "", "")

# COMMAND ----------

# MAGIC %run ../includes/init

# COMMAND ----------

# MAGIC %run ./preEtlConfigGeneratorManager

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

fwkLogTableParameters = json.loads(dbutils.widgets.get("FwkLogParameters"))

preEtlConfigGeneratorManager = PreEtlConfigGeneratorManager(spark, compartmentConfig)
output = preEtlConfigGeneratorManager.generateConfiguration(fwkLogTableParameters)

# COMMAND ----------

dbutils.notebook.exit(output)
