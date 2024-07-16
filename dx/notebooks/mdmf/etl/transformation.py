# Databricks notebook source
# DBTITLE 1,Widgets / notebook parameters
# widgets values are sent from ADF pipeline
dbutils.widgets.text("DataTransformationParameters", "", "")
dbutils.widgets.text("EntTriggerTime", "", "")
dbutils.widgets.text("EntRunId", "", "")

# COMMAND ----------

# MAGIC %run ../includes/init

# COMMAND ----------

# MAGIC %run ./transformationManager

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

instructionParameters = json.loads(dbutils.widgets.get("DataTransformationParameters"))
entTriggerTime = datetime.strptime(dbutils.widgets.get("EntTriggerTime"), "%Y-%m-%dT%H:%M:%S")
entRunId = dbutils.widgets.get("EntRunId")

transformationManager = TransformationManager(spark, compartmentConfig)
output = transformationManager.executeInstruction(instructionParameters, entRunId, entTriggerTime)

# COMMAND ----------

dbutils.notebook.exit(output)
