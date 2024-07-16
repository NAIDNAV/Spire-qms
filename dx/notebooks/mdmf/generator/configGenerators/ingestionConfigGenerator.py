# Databricks notebook source
import json
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, split, trim, explode, when, substring_index
from pyspark.sql.types import MapType

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.generator.configGenerators.configGeneratorHelper import ConfigGeneratorHelper
    from notebooks.mdmf.includes.dataLakeHelper import DataLakeHelper
    from notebooks.mdmf.includes.frameworkConfig import *


class IngestionConfigGenerator:
    """IngestionConfigGenerator is responsible for generating ingestion configuration.

    It generates IngtOutput.csv and FwkEntity.csv that contain instructions and configuration
    which are later used during ETL to ingest data. It also generates metadata information about 
    all entities (tables) that are created as output (sink) of these ingestion instructions.

    Public methods:
        generateConfiguration
    """

    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._sqlContext = globals()["sqlContext"] if "sqlContext" in globals() else None
        self._dataLakeHelper = DataLakeHelper(spark, compartmentConfig)
        self._configGeneratorHelper = ConfigGeneratorHelper(spark, compartmentConfig)

    def _getWmkDataType(self, sourceSystemMetadataTableName: str, sourceType: str, fwkSourceEntityId: str, wmkColumnName: str) -> str:
        dataTypeDF = self._getDataType(sourceSystemMetadataTableName, fwkSourceEntityId, wmkColumnName)
        return self._processDataType(dataTypeDF, sourceType, wmkColumnName, sourceSystemMetadataTableName, fwkSourceEntityId)

    def _getDataType(self, sourceSystemMetadataTableName: str, fwkSourceEntityId: str, wmkColumnName: str):
        return self._spark.sql(f"""
            SELECT DataType
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{sourceSystemMetadataTableName}
            WHERE Entity = '{fwkSourceEntityId}' AND Attribute = '{wmkColumnName}'
        """)

    def _processDataType(self, dataType, sourceType: str, wmkColumnName: str, sourceSystemMetadataTableName: str, fwkSourceEntityId: str) -> str:
        if dataType.count() > 0:
            dataType = dataType.first()["DataType"].lower()
        else:
            assert False, (
                f"""WmkDataType of '{wmkColumnName}' column from '{fwkSourceEntityId}' table of '{sourceSystemMetadataTableName}' source system could not be found""")
            return -1

        if sourceType in ["SQL", "SQLOnPrem"]:
            if dataType.lower() == "datetime":
                return "datetime"
            elif dataType in ["varchar", "char", "nvarchar"]:
                return "stringDatetime"
            elif dataType in ["int", "integer", "tinyint", "smallint", "mediumint", "bigint"]:
                return "numeric"
        elif sourceType == "Oracle":
            if dataType.startswith("timestamp") or dataType in ["date", "datetime"]:
                return "datetime"
            elif dataType.startswith("varchar2"):
                return "stringDatetime"
            elif dataType.startswith("number") or dataType in ["binary_float", "binary_double"]:
                return "numeric"

        assert False, (
            f"""WmkDataType '{dataType}' of '{wmkColumnName}' column from '{fwkSourceEntityId}' table of '{sourceSystemMetadataTableName}' source system is not supported""")
        return -1

    @staticmethod
    def _getSinkTableNameFromIngestionConfigurationEntity(sourceEntity: str) -> str:
        return sourceEntity.split(".")[-1]

    def _generateIngtOutput(self, metadataIngestionTable: str, sinkDatabaseName: str, fwkSourceLinkedServiceId: str, fwkTriggerId: str, sourceSystemMetadataTableName: str, archiveLinkedServiceIdInput: str, archivePathInput: str) -> DataFrame:
        ingtOutputRecords = []
        lastUpdate = datetime.now()
        insertTime = lastUpdate
        
        sourceType = (
            self._compartmentConfig.FWK_LINKED_SERVICE_DF
            .filter(f"FwkLinkedServiceId = '{fwkSourceLinkedServiceId}'")
            .first()["SourceType"]
        )
        
        tables = (
            self._spark.sql(f"""
                SELECT *,
                    CASE WHEN Path IS NOT NULL THEN row_number() OVER (PARTITION BY FwkLinkedServiceId, Path ORDER BY ArchivePath ASC) ELSE 1 END AS BatchNumber,
                    CASE WHEN Path IS NOT NULL AND ArchivePath IS NOT NULL THEN row_number() OVER (PARTITION BY FwkLinkedServiceId, Path, isnull(ArchivePath) ORDER BY ArchivePath ASC) ELSE 1 END AS MultipleArchive
                FROM (
                    SELECT
                        Entity, Params, MetadataParams, TypeLoad, WmkColumnName, SelectedColumnNames, TableHint, QueryHint, Query, Path, FwkLinkedServiceId, 
                        Params.archivePath as ArchivePath, Params.archiveLinkedServiceId as ArchiveLinkedServiceId, IsMandatory
                    FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataIngestionTable}
                    WHERE FwkLinkedServiceId = '{fwkSourceLinkedServiceId}'
                )
            """)
            .rdd.collect()
        )

        for table in tables:
            fwkSinkEntityId = f"{sinkDatabaseName}.None"
            try:
                fwkSourceEntityId = table["Entity"]
                
                # multiple archiving
                
                assert table["MultipleArchive"] <= 1, (
                    f"""Multiple archiving, source file '{table["Path"]}' is archived more then once. Please modify 'Ingestion configuration' file""")

                # read Params

                if table["Params"]:
                    params = table["Params"]
                else:
                    params = {}
            
                if "sinkEntityName" in params and params["sinkEntityName"]:
                    tableName = params["sinkEntityName"]
                else:
                    tableName = self._getSinkTableNameFromIngestionConfigurationEntity(fwkSourceEntityId)

                fwkSinkEntityId = f"{sinkDatabaseName}.{tableName}"
                
                # read MetadataParams

                if table["MetadataParams"]:
                    metadataParams = table["MetadataParams"]
                else:
                    metadataParams = {}

                wmkDataType = None
                if table["TypeLoad"] == "Delta":
                    if "wmkDataType" in metadataParams and metadataParams["wmkDataType"]:
                        wmkDataType = metadataParams["wmkDataType"]
                    else:
                        wmkDataType = self._getWmkDataType(
                            sourceSystemMetadataTableName,
                            sourceType,
                            fwkSourceEntityId,
                            table["WmkColumnName"]
                        )
                
                if table["SelectedColumnNames"] == "#primaryColumns":
                    if "primaryColumns" in metadataParams:
                        selectedColumnNames = metadataParams["primaryColumns"]
                    else:
                        primaryColumns = self._spark.sql(f"""
                            SELECT Attribute
                            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{sourceSystemMetadataTableName}
                            WHERE Entity = '{fwkSourceEntityId}' AND IsPrimaryKey = True 
                        """).rdd.map(lambda x: x[0])
                        selectedColumnNames = ", ".join(primaryColumns.take(primaryColumns.count()))
                else:
                    selectedColumnNames = table["SelectedColumnNames"]

                archivePath = params["archivePath"] if "archivePath" in params and params["archivePath"] else archivePathInput
                archiveLinkedServiceId = params["archiveLinkedServiceId"] if "archiveLinkedServiceId" in params and params["archiveLinkedServiceId"] else archiveLinkedServiceIdInput
                
                foundLinkedServiceDF = self._compartmentConfig.FWK_LINKED_SERVICE_DF.filter(f"FwkLinkedServiceId = '{archiveLinkedServiceId}'")
                
                if archiveLinkedServiceId and not archivePath: 
                    assert False, "Please defined both properties 'archivePath' and 'archiveLinkedServiceId' in 'CONFIG'."
                elif archiveLinkedServiceId and archivePath:
                    if foundLinkedServiceDF.count() == 0:
                        assert False, f"""Defined property 'archiveLinkedServiceId': '{archiveLinkedServiceId}' in 'CONFIG' is not listed in 'FWK_LINKED_SERVICE_DF'."""
                    elif sourceType != 'ADLS' or foundLinkedServiceDF.first()["SourceType"] != 'SFTP':
                        assert False, f"""Defined property to archive data from  source {sourceType} to sink {foundLinkedServiceDF.first()["SourceType"]} in 'CONFIG' is not SUPPORTED."""

                # generate ingtOutput record

                ingtOutputRecords.append([
                    fwkSourceEntityId,
                    fwkSinkEntityId,
                    table["TypeLoad"],
                    table["WmkColumnName"],
                    wmkDataType,
                    selectedColumnNames,
                    table["TableHint"],
                    table["QueryHint"],
                    table["Query"],
                    (params["fwkTriggerId"] if "fwkTriggerId" in params and params["fwkTriggerId"] else fwkTriggerId),
                    archivePath,
                    archiveLinkedServiceId,
                    table["BatchNumber"],
                    "Y" if table["IsMandatory"] else "N",
                    "Y",
                    insertTime,
                    lastUpdate,
                    self._configGeneratorHelper.getDatabricksId()
                ])
            except:
                print(f"""Exception details
                    Source linked service: {fwkSourceLinkedServiceId}
                    Source entity: {fwkSourceEntityId}
                    Sink entity: {fwkSinkEntityId}
                """)
                raise
        
        return self._spark.createDataFrame(ingtOutputRecords, INGT_OUTPUT_SCHEMA)

    def _generateFwkEntities(self, metadataIngestionTable: str, sinkDatabaseName: str, fwkLinkedServiceId: str, logFwkLinkedServiceId: str, logPath: str, sinkPath: str, fwkSourceLinkedServiceId: str) -> DataFrame:
        fwkEntityRecords = []
        lastUpdate = datetime.now()
        
        sourceType = (
            self._compartmentConfig.FWK_LINKED_SERVICE_DF
            .filter(f"FwkLinkedServiceId = '{fwkSourceLinkedServiceId}'")
            .first()["SourceType"]
        )
        
        entities = (
            self._spark.sql(f"""
                SELECT Entity, Path, Params, SourceParams
                FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataIngestionTable}
                WHERE FwkLinkedServiceId = '{fwkSourceLinkedServiceId}'
            """)
            .rdd.collect()
        )

        for entity in entities:
            tableName = None
            
            if sourceType in ["SQL", "SQLOnPrem", "Oracle", "Snowflake"]:
                sourceFormat = "Database"
            elif sourceType == "OData":
                sourceFormat = "OData"
            elif sourceType in ["FileShare", "SFTP", "ADLS"]:
                extension = entity["Path"].split(".")[-1].upper() 
                if extension in ["CSV", "ZIP", "TXT", "CMA"]:
                    sourceFormat = "CSV"
                elif extension in ["XLS", "XLSX"]:
                    sourceFormat = "EXCEL"
                elif extension == "XML":
                    sourceFormat = "XML"
            
            sourceParamsJson = '{}' if entity["SourceParams"] == None else json.loads(str(entity["SourceParams"]))
            sourceParamsStr = entity["SourceParams"]
           
            logFwkLinkedServiceIdParam = sourceParamsJson["logFwkLinkedServiceId"] if "logFwkLinkedServiceId" in sourceParamsJson else logFwkLinkedServiceId
            logPathParam = sourceParamsJson["logPath"] if "logPath" in sourceParamsJson else logPath
            
            foundLinkedServiceDF = self._compartmentConfig.FWK_LINKED_SERVICE_DF.filter(f"FwkLinkedServiceId = '{logFwkLinkedServiceIdParam}'")

            if (logFwkLinkedServiceIdParam and not logPathParam) or (not logFwkLinkedServiceIdParam and logPathParam): 
                assert False, "Please defined both properties 'logPath' and 'logFwkLinkedServiceId' in 'CONFIG'."
            elif (logFwkLinkedServiceIdParam and logPathParam):
                if foundLinkedServiceDF.count() == 0:
                    assert False, f"""Defined property 'logFwkLinkedServiceId': '{logFwkLinkedServiceIdParam}' in 'CONFIG' is not listed in 'FWK_LINKED_SERVICE_DF'."""
                else:
                    logInstanceURL = foundLinkedServiceDF.first()["InstanceURL"]
            else:
                logInstanceURL = None
                logPathParam = None            
            
            if (sourceParamsStr is not None and "enableSkipIncompatibleRow" in sourceParamsJson and logFwkLinkedServiceIdParam and logPathParam):
                sourceParamsStr = sourceParamsStr.rstrip()[:-1]   
                sourceParamsStr = f"""{sourceParamsStr}, "logSettingsInstanceURL": "{logInstanceURL}", "logSettingsPath": "{logPathParam}"}}"""
                
            try:
                # generate source fwkEntity record
                fwkEntityRecords.append([
                    entity["Entity"],
                    fwkSourceLinkedServiceId,
                    entity["Path"],
                    sourceFormat,
                    (None if sourceFormat == "Database" else sourceParamsStr),
                    None,
                    None,
                    None,
                    lastUpdate,
                    self._configGeneratorHelper.getDatabricksId()
                ])

                # generate sink fwkEntity record
                            
                if entity["Params"] and "sinkEntityName" in entity["Params"] and entity["Params"]["sinkEntityName"]:
                    tableName = entity["Params"]["sinkEntityName"]
                
                if tableName == None:
                    tableName = self._getSinkTableNameFromIngestionConfigurationEntity(entity["Entity"])

                fwkEntityRecords.append([
                    f"{sinkDatabaseName}.{tableName}",
                    fwkLinkedServiceId,
                    f"{sinkPath}/{tableName}",
                    "Parquet",
                    None,
                    None,
                    None,
                    None,
                    lastUpdate,
                    self._configGeneratorHelper.getDatabricksId()
                ])
            except:
                print(f"""Exception details
                    Source linked service: {fwkSourceLinkedServiceId}
                    Source entity: {entity["Entity"]}
                    Sink linked service: {fwkLinkedServiceId}
                    Sink entity: {sinkDatabaseName}.{tableName}
                """)
                raise
        
        return self._spark.createDataFrame(fwkEntityRecords, FWK_ENTITY_SCHEMA).distinct()
    
    def _generateAttributes(self, metadataIngestionTable: str, sinkDatabaseName: str, fwkLinkedServiceId: str, sourceSystemMetadataTableName: str) -> DataFrame:
        attributeRecords = []
        
        # join ingestion config with source system metadata to collect all column names and primary key flags
        
        metadataTableExists = sourceSystemMetadataTableName.lower() in self._sqlContext.tableNames(self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"])
        if metadataTableExists == True:
            attributesLeftPartSQL = f"""LEFT JOIN (SELECT * FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{sourceSystemMetadataTableName} WHERE cast(IsPrimaryKey as boolean)) AS sm ON im.Entity = sm.Entity"""
            attributesColumnAttribute = "sm.Attribute"
            attributesColumnIsPrimaryKey = "cast(sm.IsPrimaryKey as boolean)"
        else:
            attributesLeftPartSQL = ""
            attributesColumnAttribute = "NULL"
            attributesColumnIsPrimaryKey = "NULL"
        
        attributesDF = (
            self._spark.sql(f"""
                SELECT 
                    im.Entity as EntityName,
                    {attributesColumnAttribute} AS Attribute,
                    {attributesColumnIsPrimaryKey} AS IsPrimaryKey,
                    im.SelectedColumnNames,
                    im.Params,
                    im.MetadataParams
                FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataIngestionTable} AS im
                {attributesLeftPartSQL}
                WHERE im.FwkLinkedServiceId = '{fwkLinkedServiceId}'
            """)
            .withColumn("primaryColumns", split(col("MetadataParams.primaryColumns"), ","))
            .withColumn(
                "EntityName", 
                when(col("Params.sinkEntityName").isNull(), substring_index(col("EntityName"), '.', -1))
                .otherwise(col("Params.sinkEntityName"))
            )
            .drop("Params", "MetadataParams")
        )
        
        # override primary keys coming from the source system metadata if user specified primary keys in MetadataParams.primaryColumns
        # in ingestion configuration file
        
        attributesWithPrimaryColumnsDF = (
            attributesDF.filter("primaryColumns IS NOT NULL")
            .dropDuplicates(["EntityName"])
            .withColumn("Attribute", explode("primaryColumns"))
            .withColumn("Attribute", trim(col("Attribute")))
            .withColumn("IsPrimaryKey", lit(True))
        )

        attributesDF = (
            attributesDF.filter("primaryColumns IS NULL")
            .union(attributesWithPrimaryColumnsDF)
        )
        
        attributes = attributesDF.rdd.collect()
        
        for attribute in attributes:
            # attribute (column) will be present in landing only if there are no SelectedColumnNames specified in ingestion
            # configuration file or this attribute is listed in SelectedColumnNames
            
            if attribute["SelectedColumnNames"] == "#primaryColumns":
                # if user specified #primaryColumns as SelectedColumnNames, determine which columns are primary
                
                primaryColumns = (
                    attributesDF
                    .filter(f"""EntityName = '{attribute["EntityName"]}' AND IsPrimaryKey = True""")
                    .rdd.map(lambda x: x["Attribute"])
                )
                selectedColumnNames = primaryColumns.take(primaryColumns.count())
            elif attribute["SelectedColumnNames"]:
                selectedColumnNames = attribute["SelectedColumnNames"].replace(" ", "").split(",")
            else:
                selectedColumnNames = []
            
            if not selectedColumnNames or attribute["Attribute"] in selectedColumnNames:
                # generate sink Attribute record
                attributeRecords.append([
                    sinkDatabaseName,
                    attribute["EntityName"],
                    attribute["Attribute"],
                    attribute["IsPrimaryKey"],
                    False if attribute["Attribute"] else None,
                    METADATA_ATTRIBUTES_INGESTION_KEY
                ])

        return self._spark.createDataFrame(attributeRecords, METADATA_ATTRIBUTES_SCHEMA)
    
    def generateConfiguration(self, metadataConfig: MapType, configurationOutputPath: str, metadataDeltaTablePath: str) -> DataFrame:
        """Generates ingestion configuration (IngtOutput.csv and FwkEntity.csv) and
        metadata information about all sink entities.

        Returns:
            Metadata information about all sink entities.
        """

        (sinkPath, sinkUri) = self._configGeneratorHelper.getADLSPathAndUri(metadataConfig["sink"])
        
        # register source system metadata in hive
        sourceSystemMetadataTableName = f"""{metadataConfig["source"]["FwkLinkedServiceId"]}Metadata"""
        sourceSystemMetadataTablePath = f"{metadataDeltaTablePath}/{sourceSystemMetadataTableName}"
        if (self._dataLakeHelper.fileExists(sourceSystemMetadataTablePath)):
            self._spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{sourceSystemMetadataTableName} 
                USING PARQUET 
                LOCATION '{sourceSystemMetadataTablePath}'
            """)

        # generate IngtOutput
        ingtOutputDF = self._generateIngtOutput(
            metadataConfig["metadata"]["ingestionTable"],
            metadataConfig["sink"]["databaseName"],
            metadataConfig["source"]["FwkLinkedServiceId"],
            metadataConfig["FwkTriggerId"],
            sourceSystemMetadataTableName,
            metadataConfig["sink"]["archiveLinkedServiceId"] if "archiveLinkedServiceId" in metadataConfig["sink"].keys() else None,
            metadataConfig["sink"]["archivePath"] if "archivePath" in metadataConfig["sink"].keys() else None,
        )
        self._dataLakeHelper.writeDataAsParquet(
            ingtOutputDF,
            f"{configurationOutputPath}/{INGT_OUTPUT_FOLDER}/",
            WRITE_MODE_APPEND
        )

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
            metadataConfig["metadata"]["ingestionTable"],
            metadataConfig["sink"]["databaseName"],
            metadataConfig["sink"]["FwkLinkedServiceId"],
            metadataConfig["sink"]["logFwkLinkedServiceId"] if "logFwkLinkedServiceId" in metadataConfig["sink"].keys() else None,
            metadataConfig["sink"]["logPath"] if "logPath" in metadataConfig["sink"].keys() else None,
            sinkPath,
            metadataConfig["source"]["FwkLinkedServiceId"]
        )
        self._dataLakeHelper.writeDataAsParquet(
            fwkEntitiesDF,
            f"{configurationOutputPath}/{FWK_ENTITY_FOLDER}/",
            WRITE_MODE_APPEND
        )

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
        
        # generate Attributes
        attributesDF = self._generateAttributes(
            metadataConfig["metadata"]["ingestionTable"],
            metadataConfig["sink"]["databaseName"],
            metadataConfig["source"]["FwkLinkedServiceId"],
            sourceSystemMetadataTableName
        )
        return attributesDF
