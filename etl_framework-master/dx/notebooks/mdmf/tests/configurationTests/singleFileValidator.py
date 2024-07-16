# Databricks notebook source
# DBTITLE 1,Widgets / notebook parameters
dbutils.widgets.text("FileToValidate", "")
dbutils.widgets.text("ModelAttributesFile", "")
dbutils.widgets.combobox("ValidateAgainstEntityName", "", [
    "Ingestion_Configuration",
    "Transformation_Configuration",
    "Model_Attributes",
    "Model_Relations",
    "Model_Keys",
    "Model_Transformation_Configuration",
    "Export_Configuration",
    "Workflows_Configuration",
    "DQ_Configuration",
    "DQ_Reference_Values",
    "DQ_Reference_Pair_Values"
])
dbutils.widgets.dropdown("AllowInvalidDataTypes", "No", ["Yes", "No"])

# COMMAND ----------

# MAGIC %md
# MAGIC **How to use this notebook**
# MAGIC
# MAGIC 1. Run the cell above to define notebook widgets
# MAGIC 2. Set the widget values - see the "Widget setup" section bellow
# MAGIC 3. Run all cells
# MAGIC
# MAGIC **Widgets setup**
# MAGIC
# MAGIC If you want to validate a configuration file named "Ingestion_Configuration_Mine.json" please set:
# MAGIC * **FileToValidate**: Ingestion_Configuration_Mine.json *(file name)*
# MAGIC * **ValidateAgainstEntityName**: Ingestion_Configuration *(rule-set constant; pick from the dropdown)*
# MAGIC
# MAGIC If you want to validate Model relations or Model keys file, you have to additionally set:
# MAGIC * **ModelAttributesFile**: Attributes_Mine.json *(file name)*
# MAGIC
# MAGIC If the schema of the configuration file expects one of the key's value to be of a BOOLEAN type, but the configuration file contains at least one object with incorrect type, then the configuration file won't be loaded (read) properly - the DataFrame would be either empty or would contain only one row with null values. In such case, set widget **AllowInvalidDataTypes** to 'Yes' and run all cells again.

# COMMAND ----------

# validate inputs
assert dbutils.widgets.get("FileToValidate"), "Please set value of FileToValidate widget"
assert dbutils.widgets.get("ValidateAgainstEntityName"), "Please set value of ValidateAgainstEntityName widget"

ENTITY_NAMES_REQUIRING_ATTRIBUTES_FILE = ['Model_Relations', 'Model_Keys']

assert (
    (
        dbutils.widgets.get("ValidateAgainstEntityName") in ENTITY_NAMES_REQUIRING_ATTRIBUTES_FILE
        and dbutils.widgets.get("ModelAttributesFile")
    )
    or dbutils.widgets.get("ValidateAgainstEntityName") not in ENTITY_NAMES_REQUIRING_ATTRIBUTES_FILE
), "Please set value of ModelAttributesFile widget"

# prepare config to validate and options
configsToValidate = [
    {
        "fileToValidate": dbutils.widgets.get("FileToValidate"),
        "validateAgainstEntityName": dbutils.widgets.get("ValidateAgainstEntityName"),
    }
]

if dbutils.widgets.get("ValidateAgainstEntityName") in ENTITY_NAMES_REQUIRING_ATTRIBUTES_FILE:
    configsToValidate[0]["attributesFile"] = dbutils.widgets.get("ModelAttributesFile")

options = {
    "detailedLogging": True,
    "allowInvalidDataTypes": dbutils.widgets.get("AllowInvalidDataTypes") == "Yes"
}

# COMMAND ----------

# MAGIC %run ../../../mdmf/includes/init

# COMMAND ----------

# MAGIC %run ./fileValidatorManager

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

fileValidatorManager = FileValidatorManager(spark, compartmentConfig)
output = fileValidatorManager.validate(configsToValidate, options)
fileValidatorManager.printOutput(output)
