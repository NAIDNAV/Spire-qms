# Databricks notebook source
# DBTITLE 1,Widgets / notebook parameters
# widgets values are sent from ADF pipeline
dbutils.widgets.text("ForceConfigGeneration", "", "")
dbutils.widgets.text("FwkLogParameters", "", "")

# COMMAND ----------

# MAGIC %run ../includes/init

# COMMAND ----------

# MAGIC %run ./etlConfigGeneratorManager

# COMMAND ----------

# MAGIC %run ./defineReidentFunction

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

fwkLogTableParameters = json.loads(dbutils.widgets.get("FwkLogParameters"))
forceConfigGeneration = dbutils.widgets.get("ForceConfigGeneration")
forceConfigGeneration = True if forceConfigGeneration == "True" else False

etlConfigGeneratorManager = EtlConfigGeneratorManager(spark, compartmentConfig)
output = etlConfigGeneratorManager.generateConfiguration(forceConfigGeneration, fwkLogTableParameters)

# COMMAND ----------

dbutils.notebook.exit(output)
