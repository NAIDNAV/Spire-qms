# Databricks notebook source
# MAGIC %run ../includes/dataLakeHelper

# COMMAND ----------

# MAGIC %run ./configGenerators/configGeneratorHelper

# COMMAND ----------

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat
from pyspark.sql.types import MapType

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.generator.configGenerators.configGeneratorHelper import ConfigGeneratorHelper
    from notebooks.mdmf.includes.dataLakeHelper import DataLakeHelper
    from notebooks.mdmf.includes.frameworkConfig import *


class FwkTableCreatorManager:
    """FwkTableCreatorManager is responsible for creating fwkInstruction and Log tables in HIVE.

    It creates the following tables in HIVE:
        FwkEntity,
        FwkIngtInstruction,
        FwkDtInstruction,
        FwkEXPInstruction,
        FwkIngtWatermark,
        FwkLog,
        FwkIngtLog,
        FwkDtLog,
        FwkExpLog
    
    Public methods:
        createFwkTables
    """

    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._dataLakeHelper = DataLakeHelper(spark, compartmentConfig)
        self._configGeneratorHelper = ConfigGeneratorHelper(spark, compartmentConfig)

    def _createOutput(self, executionStart: datetime, createdTables: list, failedTables: list) -> MapType:

        executionEnd = datetime.now()

        output = {
           "executionDuration": (executionEnd - executionStart).seconds,
            "createdTables": createdTables,
            "failedTables": failedTables
        }

        return output

    def createFwkTables(self) -> MapType:
        """Creates fwkInstruction and Log tables in HIVE.

        Returns:
            Output containing the start and end times of the execution, the duration of the execution, and the table creation statistics.
        """

        # Initialization

        executionStart = datetime.now()

        self._spark.catalog.clearCache()

        # get metadata data storage path and uri
        (metadataPath, metadataUri) = self._configGeneratorHelper.getADLSPathAndUri(self._compartmentConfig.FWK_METADATA_CONFIG["metadata"])
        metadataDeltaTablePath = f"{metadataUri}/Delta"
        
        # Create FwkTables
        FwkTables = [
            (FWK_ENTITY_TABLE, FWK_ENTITY_SCHEMA),
            (FWK_INGT_INST_TABLE, f"IngtOutputId BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),{INGT_OUTPUT_SCHEMA}"),
            (FWK_DT_INST_TABLE, f"DtOutputId BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),{DT_OUTPUT_SCHEMA}"),
            (FWK_EXP_INST_TABLE, f"ExpOutputId BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),{EXP_OUTPUT_SCHEMA}"),
            (FWK_INGT_WK_TABLE, f"FwkWatermarkId BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),{INGT_WK_SCHEMA}"),
            (FWK_LOG_TABLE, f"FwkLogId BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),{FWK_LOG_SCHEMA}"),
            (FWK_INGT_LOG_TABLE, f"IngtLogId BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),{FWK_INGT_LOG_SCHEMA}"),
            (FWK_DT_LOG_TABLE, f"IngtLogId BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),{FWK_DT_LOG_SCHEMA}"),
            (FWK_EXP_LOG_TABLE, f"ExpLogId BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),{FWK_EXP_LOG_SCHEMA}")
        ]

        createdTables = []
        failedTables = []

        for tableName, tableSchema in FwkTables:
            try:
                self._spark.sql(f"""
                    CREATE TABLE IF NOT EXISTS {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{tableName} 
                    ({tableSchema})
                    USING DELTA
                    LOCATION "{metadataDeltaTablePath}/{tableName}"
                """)
                createdTables.append(tableName)
            except Exception as e:
                print(e)
                failedTables.append(tableName)

        # create v_FwkEntity view

        createView = f"""
            CREATE OR REPLACE VIEW {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{FWK_ENTITY_VIEW} AS
            SELECT FwkEntityId,
                FwkLinkedServiceId,
                CASE 
                    WHEN Format = 'Database' THEN NULL
                    WHEN Format = 'Parquet' THEN NULL
                    WHEN Format = 'Delta' THEN split(replace(FwkEntityId, '.', ','),',')[size(split(replace(FwkEntityId, '.', ','),',')) - 2]
                    ELSE NULL
                END AS DatabaseName,
                CASE 
                    WHEN Format = 'Database' THEN split(replace(FwkEntityId, '.', ','),',')[size(split(replace(FwkEntityId, '.', ','),',')) - 2]
                    WHEN Format = 'Parquet' THEN NULL
                    WHEN Format = 'Delta' THEN NULL
                    ELSE NULL
                END AS SchemaName,
                CASE 
                    WHEN Format = 'Database' THEN split(replace(FwkEntityId, '.', ','),',')[size(split(replace(FwkEntityId, '.', ','),',')) - 1]
                    WHEN Format = 'Parquet' THEN NULL
                    WHEN Format = 'Delta' THEN split(replace(FwkEntityId, '.', ','),',')[size(split(replace(FwkEntityId, '.', ','),',')) - 1]
                    ELSE FwkEntityId
                END AS EntityName,
                Path,
                Format,
                Params,
                RelativeURL,
                Header01,
                Header02,
                LastUpdate,
                UpdatedBy
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{FWK_ENTITY_TABLE}
        """

        self._spark.sql(createView)
        
        createdTables.append(FWK_ENTITY_VIEW)

        # Create output
        output = self._createOutput(executionStart, createdTables, failedTables)

        return output
