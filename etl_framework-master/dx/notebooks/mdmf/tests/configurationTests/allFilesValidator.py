# Databricks notebook source
# widgets values are sent from ADF pipeline
dbutils.widgets.text("FwkLogParameters", "", "")

# COMMAND ----------

# MAGIC %run ../../../mdmf/includes/init

# COMMAND ----------

# MAGIC %run ./fileValidatorManager

# COMMAND ----------

import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

fwkLogTableParameters = json.loads(dbutils.widgets.get("FwkLogParameters"))

fileValidatorManager = FileValidatorManager(spark, compartmentConfig)
configsToValidate = fileValidatorManager.getConfigsToValidate(compartmentConfig, fwkLogTableParameters)
output = fileValidatorManager.validate(configsToValidate, fwkLogTableParameters)
fileValidatorManager.printOutput(output)

assert output["valid"] == True, "Validation of configuration files have failed"

# COMMAND ----------

dbutils.notebook.exit(output)
