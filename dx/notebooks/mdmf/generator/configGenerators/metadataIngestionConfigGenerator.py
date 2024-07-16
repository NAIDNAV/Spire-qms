# Databricks notebook source
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import MapType

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.generator.configGenerators.configGeneratorHelper import ConfigGeneratorHelper
    from notebooks.mdmf.includes.dataLakeHelper import DataLakeHelper
    from notebooks.mdmf.includes.frameworkConfig import *


class MetadataIngestionConfigGenerator:
    """MetadataIngestionConfigGenerator is responsible for generating metadata ingestion
    configuration.

    It generates IngtOutput.csv and FwkEntity.csv that contain instructions and configuration
    which are later used during ETL to ingest metadata of database source systems (like table
    names, column names, column data types, etc.). This information is later used to generate
    data ingestion instructions and transformation parameters.

    Public methods:
        generateConfiguration
    """

    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._dataLakeHelper = DataLakeHelper(spark, compartmentConfig)
        self._configGeneratorHelper = ConfigGeneratorHelper(spark, compartmentConfig)
    
    @staticmethod
    def _getQuery(sourceType: str) -> str:
        if sourceType in ["SQL", "SQLOnPrem"]:
            return ("SELECT "
                        "s.name + '.' + t.name AS Entity, "
                        "c.name as Attribute, "
                        "ty.name as DataType, "
                        "CASE WHEN pk.column_id is null THEN 'FALSE' ELSE 'TRUE' END AS IsPrimaryKey "
                    "FROM sys.tables t "
                    "INNER JOIN sys.columns c ON t.object_id = c.object_id "
                    "INNER JOIN sys.schemas s ON t.schema_id = s.schema_id "
                    "INNER JOIN sys.types ty on c.system_type_id = ty.user_type_id "
                    "LEFT JOIN (SELECT DISTINCT "
                                    "idxcol.column_id, "
                                    "idxcol.object_id, "
                                    "idx.is_primary_key "
                                "FROM sys.indexes idx "
                                "INNER JOIN sys.index_columns idxcol ON idxcol.index_id = idx.index_id "
                                    "AND idxcol.object_id = idx.object_id "
                                "WHERE idx.is_primary_key = 1 "
                            ") pk ON c.column_id = pk.column_id "
                                    "AND pk.object_id = t.object_id "
                    "WHERE t.is_external = 0 "
                    "ORDER BY t.name, c.object_id;"
                )
        elif sourceType == "Oracle":
            return ("SELECT "
                        "atc.OWNER || '.' || atc.TABLE_NAME AS Entity, "
                        "atc.COLUMN_NAME AS Attribute, "
                        "atc.DATA_TYPE DataType, "
                        "CASE WHEN col.CONSTRAINT_TYPE = 'P' THEN 'TRUE' ELSE 'FALSE' END AS IsPrimaryKey "
                    "FROM all_tab_columns atc "
                    "LEFT JOIN (SELECT "
                                    "cons.CONSTRAINT_NAME, "
                                    "cons.OWNER, "
                                    "cons.TABLE_NAME, "
                                    "cols.COLUMN_NAME, "
                                    "cons.CONSTRAINT_TYPE "
                                "FROM all_constraints cons "
                                "JOIN all_cons_columns cols ON cons.CONSTRAINT_NAME = cols.CONSTRAINT_NAME "
                                    "AND cons.OWNER = cols.OWNER "
                                    "AND cons.TABLE_NAME = cols.TABLE_NAME "
                                    "AND cons.CONSTRAINT_TYPE = 'P' "
                                ") col ON col.OWNER = atc.OWNER "
                                        "AND col.TABLE_NAME = atc.TABLE_NAME "
                                        "AND col.COLUMN_NAME = atc.COLUMN_NAME "
                    "WHERE atc.OWNER NOT IN ('SYSTEM', 'SYS', 'MDSYS', 'OLAPSYS', 'CTXSYS', "
                        "'WMSYS', 'GSMADMIN_INTERNAL', 'XDB', 'LBACSYS', 'ORDSYS', 'ORDDATA') "
                    "ORDER BY Entity, Attribute;"
                    )
        
        assert False, f"Metadata ingestion query can't be determined for source type '{sourceType}'"
    
    def _generateIngtOutput(self, fwkSourceLinkedServiceId: str, sourceType: str) -> DataFrame:
        ingtOutputRecords = []
        lastUpdate = datetime.now()
        insertTime = lastUpdate
        
        # generate ingtOutput record
        metadataTableName = f"{fwkSourceLinkedServiceId}Metadata"
        ingtOutputRecords.append([
            f"sys.{metadataTableName}",
            f"""{self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataTableName}""",
            "Full",
            None,
            None,
            None,
            None,
            None,
            self._getQuery(sourceType),
            FWK_TRIGGER_ID_DEPLOYMENT,
            None,
            None,
            1,
            "N",
            "N" if (fwkSourceLinkedServiceId == "SourceOracle" and self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"] == "Application_MEF_Metadata") else "Y",
            insertTime,
            lastUpdate,
            self._configGeneratorHelper.getDatabricksId()
        ])
        
        return self._spark.createDataFrame(ingtOutputRecords, INGT_OUTPUT_SCHEMA)
    
    def _generateFwkEntities(self, fwkSourceLinkedServiceId: str, metadataDeltaTableSinkPath: str) -> DataFrame:
        fwkEntityRecords = []
        lastUpdate = datetime.now()
        
        # generate source fwkEntity record
        metadataTableName = f"{fwkSourceLinkedServiceId}Metadata"
        fwkEntityRecords.append([
            f"sys.{metadataTableName}",
            fwkSourceLinkedServiceId,
            None,
            "Database",
            None,
            None,
            None,
            None,
            lastUpdate,
            self._configGeneratorHelper.getDatabricksId()
        ])
        
        # generate sink fwkEntity record
        fwkEntityRecords.append([
            f"""{self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataTableName}""",
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["FwkLinkedServiceId"],
            f"{metadataDeltaTableSinkPath}/{metadataTableName}",
            "Parquet",
            None,
            None,
            None,
            None,
            lastUpdate,
            self._configGeneratorHelper.getDatabricksId()
        ])
        
        return self._spark.createDataFrame(fwkEntityRecords, FWK_ENTITY_SCHEMA)
    
    def generateConfiguration(self, metadataConfig: MapType, sourceType: str, configurationOutputPath: str, metadataDeltaTablePath: str, metadataDeltaTableSinkPath: str):
        """Generates metadata ingestion configuration (IngtOutput.csv and FwkEntity.csv)."""

        # generate IngtOutput
        ingtOutputDF = self._generateIngtOutput(
            metadataConfig["source"]["FwkLinkedServiceId"],
            sourceType
        )
        self._dataLakeHelper.writeDataAsParquet(ingtOutputDF, f"{configurationOutputPath}/{INGT_OUTPUT_FOLDER}/", WRITE_MODE_APPEND)

        # save the ingtOutputDF as delta table
        self._dataLakeHelper.writeData(
            ingtOutputDF, 
            f"{metadataDeltaTablePath}/{FWK_INGT_INST_TABLE}/",
            WRITE_MODE_APPEND, 
            "delta", 
            {}, 
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"], 
            FWK_INGT_INST_TABLE
        )

        # generate FwkEntities
        fwkEntitiesDF = self._generateFwkEntities(
            metadataConfig["source"]["FwkLinkedServiceId"],
            metadataDeltaTableSinkPath
        )
        self._dataLakeHelper.writeDataAsParquet(fwkEntitiesDF, f"{configurationOutputPath}/{FWK_ENTITY_FOLDER}/", WRITE_MODE_APPEND)

        # save the fwkEntitiesDF as delta table
        
        self._dataLakeHelper.writeData(
            fwkEntitiesDF, 
            f"{metadataDeltaTablePath}/{FWK_ENTITY_TABLE}/",
            WRITE_MODE_SCD_TYPE1, 
            "delta", 
            {"keyColumns": ["FwkEntityId"]},
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"], 
            FWK_ENTITY_TABLE
        )