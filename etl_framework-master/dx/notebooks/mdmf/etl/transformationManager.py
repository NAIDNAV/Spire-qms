# Databricks notebook source
# MAGIC %run ../includes/dataLakeHelper

# COMMAND ----------

# MAGIC %run ../generator/configGenerators/configGeneratorHelper

# COMMAND ----------

# MAGIC %run ./transformations/aggregate

# COMMAND ----------

# MAGIC %run ./transformations/cast

# COMMAND ----------

# MAGIC %run ./transformations/deduplicate

# COMMAND ----------

# MAGIC %run ./transformations/deriveColumn

# COMMAND ----------

# MAGIC %run ./transformations/filter

# COMMAND ----------

# MAGIC %run ./transformations/join

# COMMAND ----------

# MAGIC %run ./transformations/pseudonymize

# COMMAND ----------

# MAGIC %run ./transformations/renameColumn

# COMMAND ----------

# MAGIC %run ./transformations/replaceNull

# COMMAND ----------

# MAGIC %run ./transformations/select

# COMMAND ----------

# MAGIC %run ./transformations/selectExpression

# COMMAND ----------

# MAGIC %run ./transformations/sqlQuery

# COMMAND ----------

# MAGIC %run ./validator

# COMMAND ----------

import json
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import MapType

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.etl.transformations.aggregate import Aggregate
    from notebooks.mdmf.etl.transformations.cast import Cast
    from notebooks.mdmf.etl.transformations.deduplicate import Deduplicate
    from notebooks.mdmf.etl.transformations.deriveColumn import DeriveColumn
    from notebooks.mdmf.etl.transformations.filter import Filter
    from notebooks.mdmf.etl.transformations.join import Join
    from notebooks.mdmf.etl.transformations.pseudonymize import Pseudonymize
    from notebooks.mdmf.etl.transformations.renameColumn import RenameColumn
    from notebooks.mdmf.etl.transformations.replaceNull import ReplaceNull
    from notebooks.mdmf.etl.transformations.select import Select
    from notebooks.mdmf.etl.transformations.selectExpression import SelectExpression
    from notebooks.mdmf.etl.transformations.sqlQuery import SqlQuery
    from notebooks.mdmf.etl.validator import Validator
    from notebooks.mdmf.includes.dataLakeHelper import DataLakeHelper
    from notebooks.mdmf.includes.frameworkConfig import *


class TransformationManager:
    """TransformationManager reads data from the source location, performs transformations, validates
    data and stores it to the sink location.
    
    TransformationManager is metadata driven – it executes transformation instruction which is sent
    from ADF. Each instruction contains information about exactly one entity – which data should be
    loaded, what transformations should be applied to the data, where the data will be stored and how.

    Public methods:
        executeInstruction
    """

    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._dbutils = globals()["dbutils"] if "dbutils" in globals() else None
        self._dataLakeHelper = DataLakeHelper(spark, compartmentConfig)

        # transformation functions
        self._aggregate = Aggregate()
        self._cast = Cast()
        self._deduplicate = Deduplicate()
        self._deriveColumn = DeriveColumn()
        self._pseudonymize = Pseudonymize()
        self._filter = Filter()
        self._join = Join(spark)
        self._renameColumn = RenameColumn()
        self._replaceNull = ReplaceNull()
        self._select = Select()
        self._selectExpression = SelectExpression()
        self._sqlQuery = SqlQuery(spark)

    def _prepareSinkConfig(self, instructionParameters: MapType) -> MapType:
        sinkInstanceUrl = instructionParameters["SinkInstanceURL"]
        sinkPath = instructionParameters["SinkPath"]

        sinkConfig = {
            "databaseName": instructionParameters["SinkDatabaseName"],
            "tableName": instructionParameters["SinkTableName"],
            "writeMode": instructionParameters["SinkWriteMode"],
            "format": instructionParameters["SinkFormat"].lower(),
            "params": instructionParameters["SinkParams"]
        }

        (sinkConfig["adlsName"], sinkConfig["dataUri"]) = self._dataLakeHelper.getADLSParams(sinkInstanceUrl, sinkPath)

        return sinkConfig
    
    @staticmethod
    def _validateSinkConfig(sinkConfig: MapType):
        assert sinkConfig["format"] in [
                "delta",
                "csv"
            ], f"""The format of the sink data '{sinkConfig["format"]}' is not supported"""

        if sinkConfig["format"] == "delta":
            assert sinkConfig["writeMode"] in [
                    WRITE_MODE_SCD_TYPE1,
                    WRITE_MODE_SCD_TYPE2,
                    WRITE_MODE_SCD_TYPE2_DELTA,
                    WRITE_MODE_SCD_TYPE2_DELETE,
                    WRITE_MODE_SNAPSHOT,
                    WRITE_MODE_OVERWRITE,
                    WRITE_MODE_APPEND
                ], f"""The write mode '{sinkConfig["writeMode"]}' is not supported for Delta"""
        elif sinkConfig["format"] == "csv":
            assert sinkConfig["writeMode"] in [
                    WRITE_MODE_OVERWRITE,
                    WRITE_MODE_APPEND
                ], f"""The write mode '{sinkConfig["writeMode"]}' is not supported for CSV"""
            
    def _prepareSourceConfig(self, instructionParameters: MapType) -> MapType:
        sourceConfig = {
            "databaseName": instructionParameters["SourceDatabaseName"],
            "tableName": instructionParameters["SourceTableName"]
        }

        if (instructionParameters["SourceInstanceURL"] is not None
            and (sourceConfig["databaseName"] is None or sourceConfig["tableName"] is None)
        ):
            sourceInstanceUrl = instructionParameters["SourceInstanceURL"]
            sourcePath = instructionParameters["SourcePath"]

            sourceConfig["format"] = instructionParameters["SourceFormat"].lower()

            (sourceConfig["adlsName"], sourceConfig["dataUri"]) = self._dataLakeHelper.getADLSParams(sourceInstanceUrl, sourcePath)

        return sourceConfig
    
    @staticmethod
    def _validateSourceConfig(sourceConfig: MapType, sinkConfig: MapType):
        if "format" in sourceConfig.keys():
            if sourceConfig["format"] == "csv":
                assert sinkConfig["format"] == "csv", f"""The CSV format of the source data is supported only if sink format is also CSV."""
            else:
                assert sourceConfig["format"] in ["delta", "parquet"], f"""The format of the source data '{sourceConfig["format"]}' is not supported"""

    def _readSourceData(self, sourceConfig: MapType) -> DataFrame:
        if ("databaseName" in sourceConfig.keys()
            and sourceConfig["databaseName"]
            and "tableName" in sourceConfig.keys()
            and sourceConfig["tableName"]
        ):
            sourceDF = self._spark.sql(f"""SELECT * FROM {sourceConfig["databaseName"]}.{sourceConfig["tableName"]}""")
            print("Source loaded from HIVE")
        elif ("format" in sourceConfig.keys()
            and sourceConfig["format"]
            and "dataUri" in sourceConfig.keys()
            and sourceConfig["dataUri"]
        ):
            try:
                sourceDF = self._spark.read.format(sourceConfig["format"]).load(sourceConfig["dataUri"])
            except:
                print(f"""Exception Details
                    Command: spark.read.format("{sourceConfig["format"]}").load("{sourceConfig["dataUri"]}")
                """)
                raise
            print("Source loaded by path")
        else:
            sourceDF = None
            print("Source load skipped")

        return sourceDF
    
    def _executeTransformationsAndValidation(self, sourceDF: DataFrame, sinkConfig: MapType, inputParameters: MapType) -> DataFrame:
        # Quarantined records should not be propagated to the next layer
        
        workingDF = self._validator.filterOutQuarantinedRecords(sourceDF)

        # Apply all transformations

        for function in inputParameters["functions"]:
            # dynamically, based on the transformation name, find class property holding the reference to transformation function
            workingDF = getattr(self, f"""_{function["transformation"]}""").execute(workingDF, function["params"])

        # Validate data

        if self._validator.hasToBeValidated(sinkConfig["databaseName"], sinkConfig["tableName"]):
            workingDF = self._validator.validate(workingDF, sinkConfig["databaseName"], sinkConfig["tableName"])

        return workingDF

    def _writeDataToSink(self, sinkDF: DataFrame, sinkConfig: MapType, inputParameters: MapType, entTriggerTime: datetime, executionStart: datetime) -> MapType:
        statistics = {}
        
        if sinkConfig["format"] == "delta":
            inputParameters["entTriggerTime"] = entTriggerTime
            inputParameters["executionStart"] = executionStart
            
            sinkDF = (
                sinkDF
                .withColumn(INSERT_TIME_COLUMN_NAME, lit(executionStart))
                .withColumn(UPDATE_TIME_COLUMN_NAME, lit(executionStart))
            )
            
            statistics = self._dataLakeHelper.writeData(
                sinkDF,
                sinkConfig["dataUri"],
                sinkConfig["writeMode"],
                sinkConfig["format"],
                inputParameters,
                sinkConfig["databaseName"],
                sinkConfig["tableName"]
            )
        elif sinkConfig["format"] == "csv":
            statistics = self._dataLakeHelper.writeCSV(
                sinkDF,
                sinkConfig["dataUri"],
                sinkConfig["writeMode"],
                sinkConfig["params"]
            )

        return statistics
    
    @staticmethod
    def _createOutput(executionStart: datetime, statistics: MapType = None) -> MapType:
        executionEnd = datetime.now()

        if statistics is None:
            statistics = {
                "recordsInserted": 0,
                "recordsUpdated": 0,
                "recordsDeleted": 0
            }

        output = {
            "duration": (executionEnd - executionStart).seconds,
            **statistics
        }

        return output

    def executeInstruction(self, instructionParameters: MapType, entRunId: str, entTriggerTime: datetime) -> MapType:
        """Executes transformation instruction - reads data from the source location, performs
        transformations, validates data and stores it to the sink location.
        
        Returns:
            Output containing information about execution duration and transformation statistics.
        """
        
        # Initialization
        
        executionStart = datetime.now()
        self._validator = Validator(self._spark, self._compartmentConfig, entRunId, entTriggerTime)

        # Parse parameters

        inputParameters = json.loads(instructionParameters["InputParameters"])

        sinkConfig = self._prepareSinkConfig(instructionParameters)
        self._validateSinkConfig(sinkConfig)

        sourceConfig = self._prepareSourceConfig(instructionParameters)
        self._validateSourceConfig(sourceConfig, sinkConfig)

        # Transformation module used for copying CSV files (export existing CSV to ADLS)

        if sinkConfig["format"] == "csv" and "format" in sourceConfig.keys() and sourceConfig["format"] == "csv":
            self._dbutils.fs.cp(sourceConfig["dataUri"], sinkConfig["dataUri"])
            self._dbutils.fs.rm(sourceConfig["dataUri"])
            return self._createOutput(executionStart)

        # Read source data, transform it and write to sink
        
        sourceDF = self._readSourceData(sourceConfig)
        
        sinkDF = self._executeTransformationsAndValidation(
            sourceDF,
            sinkConfig,
            inputParameters
        )

        statistics = self._writeDataToSink(
            sinkDF,
            sinkConfig,
            inputParameters,
            entTriggerTime,
            executionStart
        )

        # Create output

        output = self._createOutput(executionStart, statistics)

        return output
