# Databricks notebook source
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.types import MapType
from pyspark.sql.functions import lit
from typing import Tuple

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.generator.configGenerators.configGeneratorHelper import ConfigGeneratorHelper
    from notebooks.mdmf.includes.dataLakeHelper import DataLakeHelper
    from notebooks.mdmf.includes.frameworkConfig import *


class ExportConfigGenerator:
    """ExportConfigGenerator is responsible for generating export configuration.

    It generates DtOutput.csv and FwkEntity.csv that contain instructions and configuration which
    are later used during ETL to transform data. If data should be exported to external systems
    (FileShare, SFTP, SQL), ExpOutput.csv is also generated which is used during ETL to copy data.

    Public methods:
        generateConfiguration
    """

    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._sqlContext = globals()["sqlContext"] if "sqlContext" in globals() else None
        self._dataLakeHelper = DataLakeHelper(spark, compartmentConfig)
        self._configGeneratorHelper = ConfigGeneratorHelper(spark, compartmentConfig)

    def _getEntityName(self, exportPath: str) -> str:
        sStart = exportPath.rfind("/") + 1
        sEnd = exportPath.rfind(".")
        if sEnd == -1: sEnd = len(exportPath)
        return exportPath[sStart:sEnd]
    
    def _getDtExportPathAndName(self, table: Row) -> Tuple[str, str]:
        if table["Path"]:
            # exporting to ADLS, FS, SFTP
            exportPath = table["Path"]
        else:
            # exporting to Database
            exportPath = f"""{table["Entity"]}.csv"""

        exportName = self._getEntityName(exportPath)

        return (exportPath, exportName)

    def _generateDtOutput(self, metadataExportTable: str, fwkLinkedServiceId: str, sinkSourceType: str, fwkLayerId: str, fwkTriggerId: str) -> DataFrame:
        dtOutputRecords = []
        lastUpdate = datetime.now()
        insertTime = lastUpdate
        
        tables = (
            self._spark.sql(f"""SELECT * FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataExportTable}""")
            .rdd.collect()
        )

        for table in tables:
            try:
                exportName = None

                (exportPath, exportName) = self._getDtExportPathAndName(table)

                fwkSourceEntityId = None
                inputParameters = f"""{{ "keyColumns": [], "partitionColumns": [], "functions": [{{"transformation": "sqlQuery", "params": "{table["Query"]}"}}] }}"""
                
                # Exporting existing CSV file
                if table["FwkLinkedServiceId"] and table["SourcePath"]:
                    if sinkSourceType == "ADLS":
                        # Exporting to ADLS via transformation instruction
                        fwkSourceEntityId = f"""{table["FwkLinkedServiceId"]}.{exportName}"""
                        inputParameters = f"""{{ "keyColumns": [], "partitionColumns": [], "functions": [] }}"""
                    elif sinkSourceType in ["SFTP", "FileShare"]:
                        # There's no need for transformation instruction when exporting to SFTP or FileShare.
                        # Export will be facilitated by export instruction.
                        continue
                    else:
                        assert False, f"Export of existing CSV file is not supported if sink type is '{sinkSourceType}'"

                # generate dtOutput record
                dtOutputRecords.append([
                    fwkSourceEntityId,
                    f"{fwkLinkedServiceId}.{exportName}",
                    inputParameters,
                    WRITE_MODE_OVERWRITE,
                    fwkTriggerId,
                    fwkLayerId,
                    1,
                    "Y",
                    insertTime,
                    lastUpdate,
                    self._configGeneratorHelper.getDatabricksId()
                ])
            except:
                print(f"""Exception details
                    Sink entity: {fwkLinkedServiceId}.{exportName}
                """)
                raise
        
        return self._spark.createDataFrame(dtOutputRecords, DT_OUTPUT_SCHEMA)

    def _generateFwkEntities(self, metadataExportTable: str, fwkLinkedServiceId: str, sinkSourceType: str, path: str) -> DataFrame:
        fwkEntityRecords = []
        lastUpdate = datetime.now()
        
        tables = (
            self._spark.sql(f"""SELECT * FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataExportTable}""")
            .rdd.collect()
        )

        for table in tables:
            try:
                exportName = None

                (exportPath, exportName) = self._getDtExportPathAndName(table)

                if table["Params"]:
                    configParam = table["Params"]
                else: 
                    configParam = """{"header":true,"nullValue":null}"""

                # Exporting existing CSV file
                if table["FwkLinkedServiceId"] and table["SourcePath"]:
                    if sinkSourceType == "ADLS":
                        # Exporting to ADLS via transformation instruction
                        # generate source fwkEntity record
                        fwkEntityRecords.append([
                            f"""{table["FwkLinkedServiceId"]}.{exportName}""",
                            table["FwkLinkedServiceId"],
                            table["SourcePath"],
                            "CSV",
                            None,
                            None,
                            None,
                            None,
                            lastUpdate,
                            self._configGeneratorHelper.getDatabricksId()
                        ])
                    elif sinkSourceType in ["SFTP", "FileShare"]:
                        # There's no need for transformation instruction when exporting to SFTP or FileShare.
                        # Export will be facilitated by export instruction.
                        continue
                    else:
                        assert False, f"Export of existing CSV file is not supported if sink type is '{sinkSourceType}'"

                # generate sink fwkEntity record
                fwkEntityRecords.append([
                    f"{fwkLinkedServiceId}.{exportName}",
                    fwkLinkedServiceId,
                    f"{path}/{exportPath}",
                    "CSV",
                    configParam,
                    None,
                    None,
                    None,
                    lastUpdate,
                    self._configGeneratorHelper.getDatabricksId()
                ])
            except Exception as e:
                print(f"""Exception details
                    Sink entity: {fwkLinkedServiceId}.{exportName}
                """)
                raise Exception(f"An error occurred while processing the table '{fwkLinkedServiceId}.{exportName}'. Original error: {e}")
        
        return self._spark.createDataFrame(fwkEntityRecords, FWK_ENTITY_SCHEMA)

    def _generateExpOutput(self, metadataExportTable: str, fwkLinkedServiceId: str, fwkSinkLinkedServiceId: str, sinkSourceType: str, fwkTriggerId: str) -> DataFrame:
        expOutputRecords = []
        lastUpdate = datetime.now()
        insertTime = lastUpdate
        
        tables = (
            self._spark.sql(f"""SELECT * FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataExportTable}""")
            .rdd.collect()
        )

        for table in tables:
            try:
                exportName = None
                fwkSinkEntityId = None

                (exportPath, exportName) = self._getDtExportPathAndName(table)

                fwkSourceEntityId = f"{fwkLinkedServiceId}.{exportName}"

                if table["Path"]:
                    # exporting to FS, SFTP - assign the id automatically
                    fwkSinkEntityId = f"{fwkSinkLinkedServiceId}.{exportName}"
                else:
                    # exporting to Database - take the id from export config as it represents schema.table
                    # but add EXP prefix so it won't conflict with ingestion source entity
                    fwkSinkEntityId = f"""EXP.{table["Entity"]}"""

                # Exporting existing CSV file
                if table["FwkLinkedServiceId"] and table["SourcePath"]:
                    if sinkSourceType in ["SFTP", "FileShare"]:
                        fwkSourceEntityId = f"""{table["FwkLinkedServiceId"]}.{exportName}"""
                    else:
                        assert False, f"Export of existing CSV file is not supported if sink type is '{sinkSourceType}'"

                # generate dtOutput record
                expOutputRecords.append([
                    fwkSourceEntityId,
                    fwkSinkEntityId,
                    fwkTriggerId,
                    "Y",
                    insertTime,
                    lastUpdate,
                    self._configGeneratorHelper.getDatabricksId()
                ])
            except:
                print(f"""Exception details
                    Source entity: {fwkLinkedServiceId}.{exportName}
                    Sink entity: {fwkSinkEntityId}
                """)
                raise
        
        return self._spark.createDataFrame(expOutputRecords, EXP_OUTPUT_SCHEMA)

    def _generateExpFwkEntities(self, metadataExportTable: str, fwkSinkLinkedServiceId: str, sinkSourceType: str, path: str) -> DataFrame:
        fwkEntityRecords = []
        lastUpdate = datetime.now()
        
        tables = (
            self._spark.sql(f"""SELECT * FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataExportTable}""")
            .rdd.collect()
        )

        for table in tables:
            try:
                fwkEntityId = None

                if table["Path"]:
                    # exporting to FS, SFTP
                    (exportPath, exportName) = self._getDtExportPathAndName(table)

                    fwkEntityId = f"{fwkSinkLinkedServiceId}.{exportName}"
                    entityPath = f"{path}/{exportPath}"
                    entityFormat = "CSV"
                    params = None
                else:
                    # exporting to Database
                    fwkEntityId = f"""EXP.{table["Entity"]}"""
                    entityPath = None
                    entityFormat = "Database"
                    if table["PreCopyScript"]:
                        params = f'{{"preCopyScript":"{table["PreCopyScript"]}"}}'
                    else:
                        params = None

                # Exporting existing CSV file
                if table["FwkLinkedServiceId"] and table["SourcePath"]:
                    if sinkSourceType in ["SFTP", "FileShare"]:
                        # generate source fwkEntity record
                        fwkEntityRecords.append([
                            f"""{table["FwkLinkedServiceId"]}.{exportName}""",
                            table["FwkLinkedServiceId"],
                            table["SourcePath"],
                            "CSV",
                            None,
                            None,
                            None,
                            None,
                            lastUpdate,
                            self._configGeneratorHelper.getDatabricksId()
                        ])
                    else:
                        assert False, f"Export of existing CSV file is not supported if sink type is '{sinkSourceType}'"

                # generate sink fwkEntity record
                fwkEntityRecords.append([
                    fwkEntityId,
                    fwkSinkLinkedServiceId,
                    entityPath,
                    entityFormat,
                    params,
                    None,
                    None,
                    None,
                    lastUpdate,
                    self._configGeneratorHelper.getDatabricksId()
                ])
            except:
                print(f"""Exception details
                    Linked service: {fwkSinkLinkedServiceId}
                    FwkEntityId: {fwkEntityId}
                """)
                raise
        
        return self._spark.createDataFrame(fwkEntityRecords, FWK_ENTITY_SCHEMA)
    
    def generateConfiguration(self, metadataConfig: MapType, configurationOutputPath: str, metadataDeltaTablePath: str) -> bool:
        """Generates export configuration (DtOutput.csv and FwkEntity.csv). If data should be
        exported to external systems (FileShare, SFTP, SQL), ExpOutput.csv is also generated.

        Returns:
            Flag whether ExpOutput.csv was generated.
        """

        # determine sink
        sinkSourceType = (
            self._compartmentConfig.FWK_LINKED_SERVICE_DF
            .filter(f"""FwkLinkedServiceId = '{metadataConfig["sink"]["FwkLinkedServiceId"]}'""")
            .first()["SourceType"]
        )

        if sinkSourceType == "ADLS":
            metadataADLSConfig = metadataConfig["sink"]
        elif sinkSourceType in ["SFTP", "FileShare", "SQL"]:
            assert "tempSink" in metadataConfig["sink"].keys(), (
                "'tempSink' is required to be defined under 'sink' when exporting data to SFTP or FileShare or SQL")
            metadataADLSConfig = metadataConfig["sink"]["tempSink"]

        (sinkPath, sinkUri) = self._configGeneratorHelper.getADLSPathAndUri(metadataADLSConfig)

        # generate DtOutput
        dtOutputDF = self._generateDtOutput(
            metadataConfig["metadata"]["exportTable"],
            metadataADLSConfig["FwkLinkedServiceId"],
            sinkSourceType,
            metadataConfig["FwkLayerId"],
            metadataConfig["FwkTriggerId"]
        )
        self._dataLakeHelper.writeDataAsParquet(dtOutputDF, f"{configurationOutputPath}/{DT_OUTPUT_FOLDER}/", WRITE_MODE_APPEND)

        # save the dtOutputDF as delta table
        self._dataLakeHelper.writeData(
            dtOutputDF, 
            f"{metadataDeltaTablePath}/{FWK_DT_INST_TABLE}/",
            WRITE_MODE_APPEND, 
            "delta", 
            {}, 
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"], 
            FWK_DT_INST_TABLE
        )
        
        # generate FwkEntities
        fwkEntitiesDF = self._generateFwkEntities(
            metadataConfig["metadata"]["exportTable"],
            metadataADLSConfig["FwkLinkedServiceId"],
            sinkSourceType,
            sinkPath
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

        # create instructions for moving CSVs from temp ADLS sink to SFTP or FileShare
        
        if sinkSourceType in ["SFTP", "FileShare", "SQL"]:
            # generate ExpOutput
            expOutputDF = self._generateExpOutput(
                metadataConfig["metadata"]["exportTable"],
                metadataADLSConfig["FwkLinkedServiceId"],
                metadataConfig["sink"]["FwkLinkedServiceId"],
                sinkSourceType,
                metadataConfig["FwkTriggerId"]
            )
            self._dataLakeHelper.writeDataAsParquet(expOutputDF, f"{configurationOutputPath}/{EXP_OUTPUT_FOLDER}/", WRITE_MODE_APPEND)

            # save the expOutputDF as delta table
            self._dataLakeHelper.writeData(
                expOutputDF, 
                f"{metadataDeltaTablePath}/{FWK_EXP_INST_TABLE}/",
                WRITE_MODE_APPEND, 
                "delta", 
                {}, 
                self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"], 
                FWK_EXP_INST_TABLE
            )
            expOutputGenerated = True
            
            # generate ExpFwkEntities
            fwkEntitiesDF = self._generateExpFwkEntities(
                metadataConfig["metadata"]["exportTable"],
                metadataConfig["sink"]["FwkLinkedServiceId"],
                sinkSourceType,
                metadataConfig["sink"]["dataPath"] if "dataPath" in metadataConfig["sink"].keys() else None
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
        else:
            expOutputGenerated = False
            
        return expOutputGenerated
