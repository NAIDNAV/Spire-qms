# Databricks notebook source
# MAGIC %run ../../mdmf/includes/init

# COMMAND ----------

# MAGIC %run ../../mdmf/includes/dataLakeHelper

# COMMAND ----------

# MAGIC %run ../../mdmf/etl/workflowManager

# COMMAND ----------

if "dbutils" not in globals():
    from notebooks.mdmf.includes.init import triggerADFPipeline

# COMMAND ----------

# DBTITLE 1,Prepare test export files
dataLakeHelper = DataLakeHelper(spark, compartmentConfig)

customerDF = spark.sql("SELECT * FROM Application_MEF_History.Customer")
dataLakeHelper.writeCSV(customerDF, "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/history/Files/Customer.csv", WRITE_MODE_OVERWRITE)

customerAddressDF = spark.sql("SELECT * FROM Application_MEF_History.CustomerAddress")
dataLakeHelper.writeCSV(customerAddressDF, "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/history/Files/CustomerAddress.csv", WRITE_MODE_OVERWRITE)

# COMMAND ----------

# DBTITLE 1,Trigger ADF Pipeline
pipelineName = "PL_StartOrStopTrigger"
parameters = {
        "TriggerNames": ["Weekly"],
        "RuntimeState": "Stopped"
}

triggerADFPipeline(pipelineName, parameters)

# COMMAND ----------

dbutils.widgets.text("ADFTriggerName", "", "")
adfTriggerName = dbutils.widgets.get("ADFTriggerName")

if adfTriggerName in ["Sandbox", "Daily_1am"]:
    stopETL = "true"
else:
    stopETL = "false"

workflowManager = WorkflowManager(spark, compartmentConfig)

output = {
    "workflow": workflowManager.getWorkflowDefinition("Post_Historization_Approval"),
    "stopETL": stopETL
}

dbutils.notebook.exit(output)
