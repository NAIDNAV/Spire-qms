# Databricks notebook source
# DBTITLE 1,Scorecard DQ Config Generator
from pyspark.sql.functions import col, lit, min, max, concat, when, collect_list, struct, from_json

# user variables definitions
USER_INPUT_FILE = "dbfs:/FileStore/scorecard/ScorecardsReferenceTable.csv"
USER_OUTPUT_FILE = "dbfs:/FileStore/scorecard/DQ_Configuration_Scorecards.json"
DATABASE_NAME = "Application_MEF_StagingSQL"
ENTITY_PREFIX = "DE_"
ENTITY_NAME = "SalesOrderHeader"
DESCRIPTION = "Fill some description here"
EXPECTATION_TYPE = "expect_column_values_to_not_match_condition"
QUARANTINE = False
DQ_LOG_OUTPUT = "SalesOrderID, TotalDue"
SCORE_COLUMN_NAME = "Score"

# read the input file
inputReferenceDF = (
    spark.read.option("header", True)
    .options(delimiter=";")
    .option("nullValue", None)
    .csv(f"{USER_INPUT_FILE}")
)

INFINITY = 999999999999999999

tempReferenceDF = (
    inputReferenceDF
    .withColumn("ScoringMinTemp", when(col("ScoringMin").isNull(), -INFINITY).otherwise(col("ScoringMin")).cast("long"))
    .withColumn("ScoringMaxTemp", when(col("ScoringMax").isNull(), INFINITY).otherwise(col("ScoringMax")).cast("long"))
)

tempReferenceDF = (
    tempReferenceDF
    .groupby("EntityId", "RModelScorecard", "RModelScorecardVersion")
    .agg(min("ScoringMinTemp").alias("ScoringMinTemp"), max("ScoringMaxTemp").alias("ScoringMaxTemp"))
    .filter(f"ScoringMinTemp != {-INFINITY} OR ScoringMaxTemp != {INFINITY}")
    .sort("EntityId", "RModelScorecard", "RModelScorecardVersion")
)

# generate output
simpleOutputReferenceDF = (
    tempReferenceDF
    .withColumn("DatabaseName", lit(DATABASE_NAME))
    .withColumn("EntityName", concat(lit(f"{ENTITY_PREFIX}"), col("EntityId"), lit(f"_{ENTITY_NAME}")))
    .withColumn("Description", lit(DESCRIPTION))
    .withColumn("ExpectationType", lit(EXPECTATION_TYPE))
    .withColumn("KwArgs", concat(
        lit("{ \"condition\": \""),
        when(col("EntityId").isNull(), concat(lit("EntityId"), lit(" is null"))).otherwise(concat(lit("EntityId = '"), col("EntityId"), lit("'"))),
        when(col("RModelScorecard").isNull(), concat(lit(" AND RModelScorecard"), lit(" is null"))).otherwise(concat(lit(" AND RModelScorecard = '"), col("RModelScorecard"), lit("'"))),
        when(col("RModelScorecardVersion").isNull(), concat(lit(" AND RModelScorecardVersion"), lit(" is null"))).otherwise(concat(lit(" AND RModelScorecardVersion = '"), col("RModelScorecardVersion"), lit("'"))),
        when((col("ScoringMinTemp") != -INFINITY) & (col("ScoringMaxTemp") != INFINITY), concat(lit(f" AND ({SCORE_COLUMN_NAME}"), lit(" < "), col("ScoringMinTemp"), lit(" OR "), lit(f"{SCORE_COLUMN_NAME}"), lit(" > "), col("ScoringMaxTemp"), lit(")")))
            .when((col("ScoringMinTemp") == -INFINITY) & (col("ScoringMaxTemp") != INFINITY), concat(lit(f" AND {SCORE_COLUMN_NAME}"), lit(" > "), col("ScoringMaxTemp"), lit("")))
            .when((col("ScoringMinTemp") != -INFINITY) & (col("ScoringMaxTemp") == INFINITY), concat(lit(f" AND {SCORE_COLUMN_NAME}"), lit(" < "), col("ScoringMinTemp"), lit("")))
            .otherwise(lit("")),
        lit("\" }")
    ))
    .withColumn("Quarantine", lit(QUARANTINE))
    .withColumn("DQLogOutput", lit(DQ_LOG_OUTPUT))
)

simpleOutputReferenceDF = simpleOutputReferenceDF.select("DatabaseName", "EntityName", "Description", "ExpectationType", "KwArgs", "Quarantine", "DQLogOutput")

# Convert the nested structure to JSON
outputReferenceDF = (
    simpleOutputReferenceDF
    .groupBy("DatabaseName")
    .agg(
        collect_list(
            struct(
                col("EntityName"),
                col("Description"),
                col("ExpectationType"),
                from_json(col("KwArgs"), "condition STRING").alias("KwArgs"),
                col("Quarantine"),
                col("DQLogOutput")
            )
        ).alias("Entities")
    )
)

# write final JSON (as one file)
filePathTemp = USER_OUTPUT_FILE + ".tmp"
outputReferenceDF.coalesce(1).write.mode("overwrite").json(filePathTemp)
fileName = [f.name for f in dbutils.fs.ls(filePathTemp) if f.name.endswith(".json")][0]
dbutils.fs.cp(f"{filePathTemp}/{fileName}", USER_OUTPUT_FILE)
dbutils.fs.rm(filePathTemp, True)

# Download File
fileUrl = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/{USER_OUTPUT_FILE.replace('dbfs:/FileStore', 'files')}"
displayHTML(f"<a href='{fileUrl}'>Download the file</a>")

# COMMAND ----------

# DBTITLE 1,Display data from output JSON
display(spark.read.option("multiline", "true").json(USER_OUTPUT_FILE))
display(inputReferenceDF)
