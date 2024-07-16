# Databricks notebook source
# MAGIC %md
# MAGIC #### How to use this notebook
# MAGIC 1. Provide the proper Database name (e.g. "SDM") and Description in the widgets. These values will be stored in generated config file.
# MAGIC 2. Provide databaseName.tableName of model keys table (e.g. "application_mef_metadata.ldmkeys")
# MAGIC 3. Run the cell below
# MAGIC 4. Download the configuration file

# COMMAND ----------

from pyspark.sql.functions import col, collect_list, concat, concat_ws, count, lit, struct, to_json, when, from_json

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.catalog.clearCache()

spark.sql("USE CATALOG westeurope_spire_platform_dev")

# user variables definitions
USER_OUTPUT_FILE = "dbfs:/FileStore/temp/DQ_Configuration_generated.json"

# read the required parameters from the user
dbutils.widgets.text("table_name", "", "ModelKeys TableName")
dbutils.widgets.text("databaseName", "", "DatabaseName")
dbutils.widgets.text("description", "", "Description")

modelKey_tableName = dbutils.widgets.get("table_name")
databaseName = dbutils.widgets.get("databaseName")
description = dbutils.widgets.get("description")

if (bool(modelKey_tableName) == False) or (bool(description) == False) or (bool(description) == False):
    raise Exception("Please provide the proper input.")

inputDF = spark.read.table(modelKey_tableName)
inputDF = (
    inputDF
    .filter(inputDF.KeyType != "PIdentifier")
    .groupBy(col('Entity'), col('KeyType'))
    .agg(count('Attribute').alias('AttributeCount'),collect_list('Attribute').alias('AttributeList'))
)

# generate output
outputDF = (
    inputDF
    .withColumn('DatabaseName', lit(databaseName))
    .withColumnRenamed('Entity', "EntityName")
    .withColumn('Description', lit(description))
    .withColumn(
        'ExpectationType',
        when(col('AttributeCount')==1, 'expect_column_values_to_be_unique')
        .when(col('AttributeCount')>1, 'expect_compound_columns_to_be_unique')
    )
    .withColumn(
        'KwArgs',
        when(col('AttributeCount')==1, concat(lit('{ "column": "'), col('AttributeList').getItem(0), lit('" }')))
        .when(col('AttributeCount')>1, concat(lit('{ "column_list": '), to_json(col('AttributeList')), lit(' }')))
    )
    .withColumn('Quarantine', lit("False"))
    .withColumn('DQLogOutput', concat_ws(", ", col('AttributeList')))
    .drop("KeyType", "AttributeCount", "AttributeList")
)

outputDF = (
    outputDF.select(
        "DatabaseName", 
        "EntityName", 
        "Description", 
        "ExpectationType", 
        "KwArgs", 
        "Quarantine", 
        "DQLogOutput"
    )
    .orderBy(col('EntityName'))
)

# Convert the nested structure to JSON
outputDF = (
    outputDF
    .groupBy('DatabaseName')
    .agg(
        collect_list(
            struct(
                col('EntityName'),
                col('Description'),
                col('ExpectationType'),
                from_json(col("KwArgs"), "column STRING, column_list ARRAY<STRING>").alias("KwArgs"),
                col('Quarantine'),
                col('DQLogOutput')
            )
        ).alias('Entities')
    )
)

# write final JSON (as one file)
filePathTemp = USER_OUTPUT_FILE + ".tmp"
outputDF.coalesce(1).write.mode("overwrite").format('json').save(filePathTemp)
fileName = [f.name for f in dbutils.fs.ls(filePathTemp) if f.name.endswith(".json")][0]
dbutils.fs.cp(f"{filePathTemp}/{fileName}", USER_OUTPUT_FILE)
dbutils.fs.rm(filePathTemp, True)

# Download File
fileUrl = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/{USER_OUTPUT_FILE.replace('dbfs:/FileStore', 'files')}"
displayHTML(f"<a href='{fileUrl}'>Download the file</a>")
