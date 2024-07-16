# Databricks notebook source
# MAGIC %run ../includes/dataLakeHelper
# MAGIC

# COMMAND ----------

# MAGIC %run ./configGenerators/configGeneratorHelper

# COMMAND ----------

# MAGIC %run ./configGenerators/metadataIngestionConfigGenerator

# COMMAND ----------

from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import MapType

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.generator.configGenerators.configGeneratorHelper import ConfigGeneratorHelper
    from notebooks.mdmf.generator.configGenerators.metadataIngestionConfigGenerator import MetadataIngestionConfigGenerator
    from notebooks.mdmf.includes.dataLakeHelper import DataLakeHelper
    from notebooks.mdmf.includes.frameworkConfig import *


class PreEtlConfigGeneratorManager:
    """PreEtlConfigGeneratorManager is responsible for generating metadata ingestion and
    framework configuration.
    
    PreEtlConfigGeneratorManager based on compartment configuration notebook (e.g. environmentDevConfig)
    generates configurations and instructions that are used to ingest metadata of database source
    systems (like table names, column names, column data types, etc.). This information is later
    used in EtlConfigGeneratorManager to generate data ingestion, transformation and export instructions.

    Public methods:
        generateConfiguration
    """

    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._sqlContext = globals()["sqlContext"] if "sqlContext" in globals() else None
        self._dataLakeHelper = DataLakeHelper(spark, compartmentConfig)
        self._configGeneratorHelper = ConfigGeneratorHelper(spark, compartmentConfig)
        
        self._metadataIngestionConfigGenerator = None

    def _getFwkTriggerDataFrame(self) -> DataFrame:
        currentFwkTriggerDF = self._compartmentConfig.FWK_TRIGGER_DF
        toBeProcessedExpression = "CASE WHEN RuntimeState IS NULL THEN FALSE ELSE TRUE END AS ToBeProcessed"
        
        # validate trigger configuration

        assert currentFwkTriggerDF.filter("StorageAccount IS NOT NULL AND (PathBeginsWith IS NULL OR PathBeginsWith = '') AND RuntimeState in ('Started', 'Stopped')").count() == 0, (
            "Some Storage Event Triggers have NULL values in column 'PathBeginsWith'. Storage Event Triggers need to contain folder and/or begging of file name in column 'PathBeginsWith'.")

        # if there's no delta table yet, all records have to be processed by trigger creation pipeline

        if "FwkTrigger".lower() not in self._sqlContext.tableNames(self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]):
            return currentFwkTriggerDF.selectExpr("*", toBeProcessedExpression)

        previousFwkTriggerDF = self._spark.sql(f"""SELECT * FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.FwkTrigger""").drop('ToBeProcessed')

        # if the schema have changed, all records have to be processed by trigger creation pipeline

        if currentFwkTriggerDF.schema != previousFwkTriggerDF.schema:
            return currentFwkTriggerDF.selectExpr("*", toBeProcessedExpression)

        # determine which records have changed

        changedDF = currentFwkTriggerDF.subtract(previousFwkTriggerDF)
        unchangedDF = currentFwkTriggerDF.subtract(changedDF)

        fwkTriggerDF = (
            changedDF.selectExpr("*", toBeProcessedExpression)
            .union(unchangedDF.withColumn("ToBeProcessed", lit(False)))
        )

        return fwkTriggerDF

    def _addAuditColumns(self, dataFrame: DataFrame, lastUpdate: datetime) -> DataFrame:
        return (
            dataFrame
            .withColumn("LastUpdate", lit(lastUpdate))
            .withColumn("UpdatedBy", lit(self._configGeneratorHelper.getDatabricksId()))
        )

    def _generateMetadataIngestionConfig(self, configurationOutputPath: str, metadataDeltaTablePath: str, metadataDeltaTableSinkPath: str) -> bool:
        configGenerated = False

        if ("ingestion" in self._compartmentConfig.FWK_METADATA_CONFIG.keys()
            and self._compartmentConfig.FWK_METADATA_CONFIG["ingestion"]
        ):
            # If FwkIngtInstruction table exists then update the records with ActiveFlag = 'N' for source metadata instructions
            self._spark.sql(f"""
                UPDATE {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{FWK_INGT_INST_TABLE} 
                SET LastUpdate = current_timestamp(), ActiveFlag = 'N' 
                WHERE ActiveFlag = 'Y' and FwkSourceEntityId LIKE 'sys.%'
            """)

            for metadataConfig in self._compartmentConfig.FWK_METADATA_CONFIG["ingestion"]:
                sourceType = (
                    self._compartmentConfig.FWK_LINKED_SERVICE_DF
                    .filter(f"""FwkLinkedServiceId = '{metadataConfig["source"]["FwkLinkedServiceId"]}'""")
                    .first()["SourceType"]
                )
                
                if sourceType in ["SQL", "SQLOnPrem", "Oracle"]:
                    configGenerated = True
                   
                    if not self._metadataIngestionConfigGenerator:
                        self._metadataIngestionConfigGenerator = MetadataIngestionConfigGenerator(self._spark, self._compartmentConfig)
                    
                    self._metadataIngestionConfigGenerator.generateConfiguration(
                        metadataConfig, 
                        sourceType, 
                        configurationOutputPath,
                        metadataDeltaTablePath, 
                        metadataDeltaTableSinkPath
                    )

        return configGenerated

    def _createOutput(self, executionStart: datetime, metadataPath: str, metadataIngestionConfigGenerated: bool, executeTriggerCreationPipeline: str) -> MapType:
        generatedConfigFiles = [
            FWK_LAYER_FILE, 
            FWK_TRIGGER_FILE, 
            FWK_LINKED_SERVICE_FILE
        ]

        if metadataIngestionConfigGenerated:
            generatedConfigFiles.append(FWK_ENTITY_FOLDER)
            generatedConfigFiles.append(INGT_OUTPUT_FOLDER)

        metadataInstanceUrl = (
            self._compartmentConfig.FWK_LINKED_SERVICE_DF
            .filter(f"""FwkLinkedServiceId = '{self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["FwkLinkedServiceId"]}'""")
            .first()["InstanceURL"]
        )

        executionEnd = datetime.now()

        output = {
            "Duration": (executionEnd - executionStart).seconds,
            "GeneratedConfigFiles": generatedConfigFiles,
            "GeneratedConfigFilesPath": f"{metadataPath}/Configuration",
            "GeneratedConfigFilesInstanceURL": metadataInstanceUrl,
            "ColumnChangeLogs": "",
            "ExecuteTriggerCreationPipeline": executeTriggerCreationPipeline
        }

        return output

    def generateConfiguration(self, fwkLogTableParameters: dict) -> MapType:
        """Generates metadata ingestion and framework configuration.
        
        Returns:
            Output containing information about execution duration, generated configuration files and column
            change log.
        """
        # Insert record into FwkLog table with the execution module information by pipeline id
        self._configGeneratorHelper.upsertIntoFwkLog(fwkLogTableParameters["EntRunId"], fwkLogTableParameters["Module"], fwkLogTableParameters["ModuleRunId"]
                                               ,fwkLogTableParameters["StartDate"], fwkLogTableParameters["AdfTriggerName"])
        
        # Initialization

        executionStart = datetime.now()

        self._spark.catalog.clearCache()

        # get metadata data storage path and uri
        (metadataPath, metadataUri) = self._configGeneratorHelper.getADLSPathAndUri(self._compartmentConfig.FWK_METADATA_CONFIG["metadata"])
        metadataDeltaTableSinkPath = f"{metadataPath}/Delta"
        metadataDeltaTablePath = f"{metadataUri}/Delta"
        configurationOutputPath = f"{metadataUri}/Configuration"
        
        self._configGeneratorHelper.moveUnprocessedFilesToArchive(configurationOutputPath, executionStart)
        
        # Generate FWK configuration

        self._dataLakeHelper.writeCSV(
            self._compartmentConfig.FWK_LAYER_DF,
            f"{configurationOutputPath}/{FWK_LAYER_FILE}",
            WRITE_MODE_OVERWRITE
        )

        # save the fwkLayerDF as delta table

        self._dataLakeHelper.writeData(
            self._compartmentConfig.FWK_LAYER_DF,
            f"{metadataDeltaTablePath}/{FWK_LAYER_TABLE}",
            WRITE_MODE_OVERWRITE,
            "delta",
            {},
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"],
            FWK_LAYER_TABLE
        )

        fwkTriggerDF = self._getFwkTriggerDataFrame()

        if fwkTriggerDF.filter(col("ToBeProcessed")==True).count() > 0:
            executeTriggerCreationPipeline = "True"
        else:
            executeTriggerCreationPipeline = "False"

        self._dataLakeHelper.writeCSV(
            self._addAuditColumns(fwkTriggerDF, executionStart),
            f"{configurationOutputPath}/{FWK_TRIGGER_FILE}",
            WRITE_MODE_OVERWRITE
        )

        # save the fwkTriggerDF as delta table

        self._dataLakeHelper.writeData(
            self._addAuditColumns(fwkTriggerDF, executionStart),
            f"{metadataDeltaTablePath}/{FWK_TRIGGER_TABLE}",
            WRITE_MODE_OVERWRITE,
            "delta",
            {
                "schemaEvolution": True
            },
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"],
            FWK_TRIGGER_TABLE
        )

        self._dataLakeHelper.writeCSV(
            self._addAuditColumns(self._compartmentConfig.FWK_LINKED_SERVICE_DF, executionStart),
            f"{configurationOutputPath}/{FWK_LINKED_SERVICE_FILE}",
            WRITE_MODE_OVERWRITE
        )

        # save the fwkLinkedServiceDF as delta table

        self._dataLakeHelper.writeData(
            self._addAuditColumns(self._compartmentConfig.FWK_LINKED_SERVICE_DF, executionStart),
            f"{metadataDeltaTablePath}/{FWK_LINKED_SERVICE_TABLE}",
            WRITE_MODE_OVERWRITE,
            "delta",
            {},
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"],
            FWK_LINKED_SERVICE_TABLE
        )

        metadataIngestionConfigGenerated = self._generateMetadataIngestionConfig(
            configurationOutputPath,
            metadataDeltaTablePath,
            metadataDeltaTableSinkPath
        )

        # Create output

        output = self._createOutput(
            executionStart,
            metadataPath,
            metadataIngestionConfigGenerated,
            executeTriggerCreationPipeline
        )

        # Update the record created by the 'upsertIntoFwkLog' activity in the table ‘FwkLog’ by updating the columns ‘PipelineStatus’ to ‘Success’
        self._configGeneratorHelper.upsertIntoFwkLog(fwkLogTableParameters["EntRunId"], fwkLogTableParameters["Module"], fwkLogTableParameters["ModuleRunId"]
                                            ,fwkLogTableParameters["StartDate"], fwkLogTableParameters["AdfTriggerName"])

        return output
