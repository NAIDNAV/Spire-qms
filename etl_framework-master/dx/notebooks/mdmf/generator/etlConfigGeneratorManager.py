# Databricks notebook source
# MAGIC %run ../includes/dataLakeHelper

# COMMAND ----------

# MAGIC %run ./configGenerators/configGeneratorHelper

# COMMAND ----------

# MAGIC %run ./configGenerators/exportConfigGenerator

# COMMAND ----------

# MAGIC %run ./configGenerators/ingestionConfigGenerator

# COMMAND ----------

# MAGIC %run ./configGenerators/modelTransformationsConfigGenerator

# COMMAND ----------

# MAGIC %run ./configGenerators/transformationsConfigGenerator

# COMMAND ----------

# MAGIC %run ../etl/validator

# COMMAND ----------

# MAGIC %run ../maintenance/maintenanceManager

# COMMAND ----------

from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, MapType

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.etl.validator import Validator
    from notebooks.mdmf.generator.configGenerators.configGeneratorHelper import ConfigGeneratorHelper
    from notebooks.mdmf.generator.configGenerators.exportConfigGenerator import ExportConfigGenerator
    from notebooks.mdmf.generator.configGenerators.ingestionConfigGenerator import IngestionConfigGenerator
    from notebooks.mdmf.generator.configGenerators.modelTransformationsConfigGenerator import ModelTransformationsConfigGenerator
    from notebooks.mdmf.generator.configGenerators.transformationsConfigGenerator import TransformationsConfigGenerator
    from notebooks.mdmf.includes.dataLakeHelper import DataLakeHelper
    from notebooks.mdmf.includes.frameworkConfig import *
    from notebooks.mdmf.maintenance.maintenanceManager import MaintenanceManager


class EtlConfigGeneratorManager:
    """EtlConfigGeneratorManager is responsible for generating ingestion, transformations,
    model transformations and export configuration.
    
    EtlConfigGeneratorManager reads configuration input files provided by compartment and
    stores them as DeltaTables. Based on the content of these input files and compartment
    configuration notebook (e.g. environmentDevConfig) it generates configurations and 
    instructions that are later used during ETL to ingest, transform and export data.

    Public methods:
        generateConfiguration
    """

    INGESTION_CONFIGURATION_PROCESSED_FLAG = "IngestionConfiguration_processed"
    TRANSFORMATION_CONFIGURATION_PROCESSED_FLAG = "TransformationConfiguration_processed"

    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._sqlContext = globals()["sqlContext"] if "sqlContext" in globals() else None
        self._dataLakeHelper = DataLakeHelper(spark, compartmentConfig)
        self._configGeneratorHelper = ConfigGeneratorHelper(spark, compartmentConfig)
        self._validator = Validator(spark, compartmentConfig, None, None)
        self._maintenanceManager = MaintenanceManager(spark, compartmentConfig)
        
        self._ingestionConfigGenerator = None
        self._modelTransformationsConfigGenerator = None
        self._transformationsConfigGenerator = None
        self._exportConfigGenerator = None

    def _writeAttributesData(self, attributesDF: DataFrame, metadataDeltaTablePath: str):
        self._dataLakeHelper.writeData(
            attributesDF,
            f"{metadataDeltaTablePath}/{METADATA_ATTRIBUTES}",
            WRITE_MODE_APPEND,
            "delta",
            {},
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"],
            METADATA_ATTRIBUTES
        )

    def _isSectionConfiguredInFwkMetadataConfig(self, sectionName: str) -> bool:
        if (sectionName in self._compartmentConfig.FWK_METADATA_CONFIG.keys()
            and self._compartmentConfig.FWK_METADATA_CONFIG[sectionName]
        ):
            return True
        else:
            return False

    def _saveIngestionMetadataConfiguration(self, metadataDeltaTablePath: str) -> bool:
        newConfigurationData = False

        if self._isSectionConfiguredInFwkMetadataConfig("ingestion"):
            # Multiple ingestion sections can use the same ingestion config (JSON), therefore it's necessary
            # to remember which config was already stored as new delta version not to repeat it multiple times
            updatedIngestionTables = []

            for metadataConfig in self._compartmentConfig.FWK_METADATA_CONFIG["ingestion"]:
                ingestionTable = metadataConfig["metadata"]["ingestionTable"]
            
                if ingestionTable not in updatedIngestionTables:
                    # Save config and check if there's new data
                    newConfigurationData = self._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion(
                        metadataConfig["metadata"]["ingestionFile"],
                        METADATA_INGESTION_CONFIGURATION_SCHEMA,
                        metadataDeltaTablePath,
                        ingestionTable
                    ) or newConfigurationData

                    updatedIngestionTables.append(ingestionTable)

        return newConfigurationData

    def _saveModelTransformationsMetadataConfiguration(self, metadataDeltaTablePath: str) -> bool:
        newConfigurationData = False

        if self._isSectionConfiguredInFwkMetadataConfig("modelTransformations"):
            # Multiple model transformations can use the same transformation config (JSON), therefore it's necessary
            # to remember which config was already stored as new delta version not to repeat it multiple times
            updatedTransformationTables = []

            for metadataConfig in self._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"]:
                # Save attributes config and check if there's new data
                newAttributesData = self._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion(
                    metadataConfig["metadata"]["attributesFile"],
                    METADATA_MODEL_ATTRIBUTES_SCHEMA,
                    metadataDeltaTablePath,
                    metadataConfig["metadata"]["attributesTable"]
                )

                if newAttributesData:
                    self._configGeneratorHelper.setFwkFlag(f"""{metadataConfig["metadata"]["attributesTable"]}_processed""", "false")

                # Save remaining configs and check if there's new data
                newKeysData = self._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion(
                    metadataConfig["metadata"]["keysFile"],
                    METADATA_MODEL_KEYS_SCHEMA,
                    metadataDeltaTablePath,
                    metadataConfig["metadata"]["keysTable"]
                )
                newRelationsData = self._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion(
                    metadataConfig["metadata"]["relationsFile"],
                    METADATA_MODEL_RELATIONS_SCHEMA,
                    metadataDeltaTablePath,
                    metadataConfig["metadata"]["relationsTable"]
                )
                newEntitiesData = self._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion(
                    metadataConfig["metadata"]["entitiesFile"],
                    METADATA_MODEL_ENTITIES_SCHEMA,
                    metadataDeltaTablePath,
                    metadataConfig["metadata"]["entitiesTable"]
                )

                newTransformationData = False
                if ("transformationFile" in metadataConfig["metadata"].keys()
                    and "transformationTable" in metadataConfig["metadata"].keys()
                ):
                    transformationTable = metadataConfig["metadata"]["transformationTable"]
                    
                    if transformationTable not in updatedTransformationTables:
                        # Save config and check if there's new data
                        newTransformationData = self._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion(
                            metadataConfig["metadata"]["transformationFile"],
                            METADATA_MODEL_TRANSFORMATION_CONFIGURATION_SCHEMA,
                            metadataDeltaTablePath,
                            transformationTable
                        )

                        if newTransformationData:
                            self._configGeneratorHelper.setFwkFlag(f"""{transformationTable}_processed""", "false")

                        updatedTransformationTables.append(transformationTable)

                newConfigurationData = (
                    newKeysData or
                    newRelationsData or
                    newEntitiesData or
                    newTransformationData or
                    newConfigurationData
                )
        
        return newConfigurationData

    def _saveTransformationsMetadataConfiguration(self, metadataDeltaTablePath: str) -> bool:
        newConfigurationData = False

        if self._isSectionConfiguredInFwkMetadataConfig("transformations"):
            # Multiple transformations can use the same transformation config (JSON), therefore it's necessary
            # to remember which config was already stored as new delta version not to repeat it multiple times
            updatedTransformationTables = []
            
            for metadataConfig in self._compartmentConfig.FWK_METADATA_CONFIG["transformations"]:
                transformationTable = metadataConfig["metadata"]["transformationTable"]

                if transformationTable not in updatedTransformationTables:
                    # Save config and check if there's new data
                    newConfigurationData = self._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion(
                        metadataConfig["metadata"]["transformationFile"],
                        METADATA_TRANSFORMATION_CONFIGURATION_SCHEMA,
                        metadataDeltaTablePath,
                        transformationTable
                    ) or newConfigurationData

                    if newConfigurationData:
                        self._configGeneratorHelper.setFwkFlag(f"""{transformationTable}_processed""", "false")

                    updatedTransformationTables.append(transformationTable)
        
        return newConfigurationData

    def _saveExportMetadataConfiguration(self, metadataDeltaTablePath: str) -> bool:
        newConfigurationData = False
        
        if self._isSectionConfiguredInFwkMetadataConfig("export"):
            for metadataConfig in self._compartmentConfig.FWK_METADATA_CONFIG["export"]:
                # Save config and check if there's new data
                newConfigurationData = self._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion(
                    metadataConfig["metadata"]["exportFile"],
                    METADATA_EXPORT_CONFIGURATION_SCHEMA,
                    metadataDeltaTablePath,
                    metadataConfig["metadata"]["exportTable"]
                ) or newConfigurationData
        
        return newConfigurationData

    def _determineWhetherToProcessIngestionConfiguration(self, metadataDeltaTablePath: str, forceConfigGeneration: bool) -> bool:
        # If there's no ingestion section in the FWK metadata config,
        # do not process ingestion configuration
        if not self._isSectionConfiguredInFwkMetadataConfig("ingestion"):
            return False
        
        # Save ingestion configuration
        newIngestionConfigurationData = self._saveIngestionMetadataConfiguration(metadataDeltaTablePath)

        # Process ingestion configuration if:
        # there's new data or previous processing was not successful
        processIngestionConfiguration = (
            forceConfigGeneration
            or newIngestionConfigurationData
            or self._configGeneratorHelper.getFwkFlag(self.INGESTION_CONFIGURATION_PROCESSED_FLAG) != "true"
        )

        return processIngestionConfiguration

    def _determineWhetherToProcessTransformationConfiguration(self, metadataDeltaTablePath: str, forceConfigGeneration: bool, processIngestionConfiguration: bool) -> bool:
        # If there are no transformation sections in the FWK metadata config,
        # do not process transformation configuration
        if (not self._isSectionConfiguredInFwkMetadataConfig("modelTransformations")
            and not self._isSectionConfiguredInFwkMetadataConfig("transformations")
            and not self._isSectionConfiguredInFwkMetadataConfig("export")
        ):
            return False
        
        # Save transformation configuration
        try:
            newModelTransformationsConfigurationData = self._saveModelTransformationsMetadataConfiguration(metadataDeltaTablePath)
            newTransformationsConfigurationData = self._saveTransformationsMetadataConfiguration(metadataDeltaTablePath)
            newExportConfigurationData = self._saveExportMetadataConfiguration(metadataDeltaTablePath)
        except:
            # If saving of one of the metadata failed, set FWK flag to FALSE to force processing transformation
            # configuration with next run
            self._configGeneratorHelper.setFwkFlag(self.TRANSFORMATION_CONFIGURATION_PROCESSED_FLAG, "false")
            raise

        # Process transformation configuration if:
        # ingestion will be processed or there's new transformation data or previous processing was not successful
        processTransformationConfiguration = (
            forceConfigGeneration
            or processIngestionConfiguration
            or newModelTransformationsConfigurationData
            or newTransformationsConfigurationData
            or newExportConfigurationData
            or self._configGeneratorHelper.getFwkFlag(self.TRANSFORMATION_CONFIGURATION_PROCESSED_FLAG) != "true"
        )
        
        return processTransformationConfiguration

    def _generateIngestionConfiguration(self, processIngestionConfiguration: bool, configurationOutputPath: str, metadataDeltaTablePath: str):
        if (processIngestionConfiguration
            and self._isSectionConfiguredInFwkMetadataConfig("ingestion")
        ):
            # If FwkIngtInstruction table exists then update the records with ActiveFlag = 'N' for ingestion instructions
            self._spark.sql(f"""
                UPDATE {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{FWK_INGT_INST_TABLE} 
                SET LastUpdate = current_timestamp(), ActiveFlag = 'N' 
                WHERE ActiveFlag = 'Y' and (FwkSourceEntityId IS NULL OR FwkSourceEntityId NOT LIKE 'sys.%')
            """)

            if not self._ingestionConfigGenerator:
                self._ingestionConfigGenerator = IngestionConfigGenerator(self._spark, self._compartmentConfig)
            
            for metadataConfig in self._compartmentConfig.FWK_METADATA_CONFIG["ingestion"]:
                attributesDF = self._ingestionConfigGenerator.generateConfiguration(
                    metadataConfig,
                    configurationOutputPath,
                    metadataDeltaTablePath
                )
                self._writeAttributesData(attributesDF, metadataDeltaTablePath)

    def _generateModelTransformationsConfiguration(self, processTransformationConfiguration: bool, configurationOutputPath: str, metadataDeltaTablePath: str) -> ArrayType:
        columnChangeLogs = []

        if self._isSectionConfiguredInFwkMetadataConfig("modelTransformations"):
            transformationTableProcessedFlags = []

            if not self._modelTransformationsConfigGenerator:
                self._modelTransformationsConfigGenerator = ModelTransformationsConfigGenerator(self._spark, self._compartmentConfig)

            for metadataConfig in self._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"]:
                attributesProcessed = f"""{metadataConfig["metadata"]["attributesTable"]}_processed"""
                processAttributesConfiguration = self._configGeneratorHelper.getFwkFlag(attributesProcessed) != "true"
                
                if processAttributesConfiguration:
                    columnChangeLogs += self._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata(metadataConfig)
                    self._configGeneratorHelper.setFwkFlag(attributesProcessed, "true")

                if processTransformationConfiguration:
                    # Check if transformationTable exists in the metadataConfig
                    runMaintenanceManager = False
                    if "transformationTable" in metadataConfig["metadata"].keys():
                        transformationTableName = metadataConfig["metadata"]["transformationTable"]
                        
                        if self._configGeneratorHelper.getFwkFlag(f"""{transformationTableName}_processed""") == "false":
                            runMaintenanceManager = self._configGeneratorHelper.isDeltaTableChanged(
                                metadataDeltaTablePath, 
                                transformationTableName, 
                                f"""Key='{metadataConfig["metadata"]["key"]}'""", 
                                ["Params.partitionColumns"]
                            )
                        
                            if transformationTableName not in transformationTableProcessedFlags:
                                transformationTableProcessedFlags.append(transformationTableName)

                    if runMaintenanceManager:
                        print(f"""Running maintenance manager for {transformationTableName} config with {metadataConfig["metadata"]["key"]} key.""")

                    attributesDF = self._modelTransformationsConfigGenerator.generateConfiguration(
                        metadataConfig,
                        configurationOutputPath,
                        metadataDeltaTablePath,
                        runMaintenanceManager
                    )
                    self._writeAttributesData(attributesDF, metadataDeltaTablePath)


            for transformationTableName in transformationTableProcessedFlags:
                self._configGeneratorHelper.setFwkFlag(f"""{transformationTableName}_processed""", "true")

        return columnChangeLogs
    
    def _generateTransformationsConfiguration(self, processTransformationConfiguration: bool, configurationOutputPath: str, metadataDeltaTablePath: str):
        if (processTransformationConfiguration
            and self._isSectionConfiguredInFwkMetadataConfig("transformations")
        ):
            transformationTableProcessedFlags = []
            if not self._transformationsConfigGenerator:
                self._transformationsConfigGenerator = TransformationsConfigGenerator(self._spark, self._compartmentConfig)

            # Sort transformations by DtOrder. They have to be processed in this order because attributes data are
            # created based on attribute data of the previous layer (and transformation performed on the current layer)
            for metadataConfig in self._compartmentConfig.FWK_METADATA_CONFIG["transformations"]:
                metadataConfig["DtOrder"] = (
                    self._compartmentConfig.FWK_LAYER_DF
                    .filter(f"""FwkLayerId == '{metadataConfig["FwkLayerId"]}'""")
                    .first()["DtOrder"]
                )
            
            self._compartmentConfig.FWK_METADATA_CONFIG["transformations"].sort(key=lambda x: x["DtOrder"])

            for metadataConfig in self._compartmentConfig.FWK_METADATA_CONFIG["transformations"]:
                runMaintenanceManager = False
                transformationTableName = metadataConfig["metadata"]["transformationTable"]
                
                if self._configGeneratorHelper.getFwkFlag(f"""{transformationTableName}_processed""") == "false":
                    runMaintenanceManager = self._configGeneratorHelper.isDeltaTableChanged(
                        metadataDeltaTablePath, 
                        transformationTableName, 
                        f"""Key='{metadataConfig["metadata"]["key"]}'""", 
                        ["Params.partitionColumns"]
                    )

                    if transformationTableName not in transformationTableProcessedFlags:
                        transformationTableProcessedFlags.append(transformationTableName)

                if runMaintenanceManager:
                    print(f"""Running maintenance manager for {transformationTableName} config with {metadataConfig["metadata"]["key"]} key.""")
                
                attributesDF = self._transformationsConfigGenerator.generateConfiguration(
                    metadataConfig,
                    configurationOutputPath,
                    metadataDeltaTablePath,
                    runMaintenanceManager
                )
                self._writeAttributesData(attributesDF, metadataDeltaTablePath)

            for transformationTableName in transformationTableProcessedFlags:
                self._configGeneratorHelper.setFwkFlag(f"""{transformationTableName}_processed""", "true")

    def _generateExportConfiguration(self, processTransformationConfiguration: bool, configurationOutputPath: str, metadataDeltaTablePath: str) -> bool:
        expOutputGenerated = False

        if (processTransformationConfiguration
            and self._isSectionConfiguredInFwkMetadataConfig("export")
        ):
            # If FwkEXPInstruction table exists then update the records with ActiveFlag = 'N' for export instructions
            self._spark.sql(f"""
                UPDATE {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{FWK_EXP_INST_TABLE} 
                SET LastUpdate = current_timestamp(), ActiveFlag = 'N' 
                WHERE ActiveFlag = 'Y'
            """)

            if not self._exportConfigGenerator:
                self._exportConfigGenerator = ExportConfigGenerator(self._spark, self._compartmentConfig)
            
            for metadataConfig in self._compartmentConfig.FWK_METADATA_CONFIG["export"]:
                expOutputGenerated = self._exportConfigGenerator.generateConfiguration(
                    metadataConfig,
                    configurationOutputPath,
                    metadataDeltaTablePath
                ) or expOutputGenerated

        return expOutputGenerated

    def _createOutput(self, executionStart: datetime, metadataPath: str, processIngestionConfiguration: bool, processTransformationConfiguration: bool, expOutputGenerated: bool, columnChangeLogs: ArrayType) -> MapType:
        generatedConfigFiles = []
        if processIngestionConfiguration or processTransformationConfiguration:
            generatedConfigFiles.append(FWK_ENTITY_FOLDER)
        if processIngestionConfiguration:
            generatedConfigFiles.append(INGT_OUTPUT_FOLDER)
        if processTransformationConfiguration:
            generatedConfigFiles.append(DT_OUTPUT_FOLDER)
        if expOutputGenerated:
            generatedConfigFiles.append(EXP_OUTPUT_FOLDER)

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
            "ColumnChangeLogs": '\n'.join(columnChangeLogs),
            "ExecuteTriggerCreationPipeline": "False"
        }

        return output

    def generateConfiguration(self, forceConfigGeneration: bool, fwkLogTableParameters: dict) -> MapType:
        """Stores configuration input files and generates ingestion, transformations, model transformations
        and export configuration.
        
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
        metadataDeltaTablePath = f"{metadataUri}/Delta"
        configurationOutputPath = f"{metadataUri}/Configuration"
        
        self._configGeneratorHelper.moveUnprocessedFilesToArchive(configurationOutputPath, executionStart)
        self._configGeneratorHelper.createFwkFlagsTable(metadataDeltaTablePath)
        self._maintenanceManager.createMaintenanceTable(metadataDeltaTablePath)

        # Determine which configuration to process

        processIngestionConfiguration = self._determineWhetherToProcessIngestionConfiguration(metadataDeltaTablePath, forceConfigGeneration)

        # Once it was determined that ingestion configuration should be processed
        # set the respective FWK flag to FALSE so even if this run fails
        # processing will be retried the next time
        if processIngestionConfiguration:
            self._configGeneratorHelper.setFwkFlag(self.INGESTION_CONFIGURATION_PROCESSED_FLAG, "false")

        processTransformationConfiguration = self._determineWhetherToProcessTransformationConfiguration(
            metadataDeltaTablePath,
            forceConfigGeneration,
            processIngestionConfiguration
        )
       
        # Once it was determined that transformation configuration should be processed
        # set the respective FWK flag to FALSE so even if this run fails
        # processing will be retried the next time
        if processTransformationConfiguration:
            self._configGeneratorHelper.setFwkFlag(self.TRANSFORMATION_CONFIGURATION_PROCESSED_FLAG, "false")

        # Clean up old Attributes data

        attributesTable = f"""{self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}"""

        if self._spark.catalog.tableExists(attributesTable):
            if processIngestionConfiguration:
                self._spark.sql(f""" 
                    DELETE FROM {attributesTable} 
                    WHERE MGT_ConfigTable = '{METADATA_ATTRIBUTES_INGESTION_KEY}'
                """)
            
            if processTransformationConfiguration:
                self._spark.sql(f"""
                    DELETE FROM {attributesTable} 
                    WHERE MGT_ConfigTable = '{METADATA_ATTRIBUTES_TRANSFORMATION_KEY}'
                """)

        # Generate FWK configuration

        self._generateIngestionConfiguration(
            processIngestionConfiguration,
            configurationOutputPath,
            metadataDeltaTablePath
        )

        if processTransformationConfiguration:
            # If FwkDtInstruction table exists then update the records with ActiveFlag = 'N' for transformation instructions
            self._spark.sql(f"""
                UPDATE {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{FWK_DT_INST_TABLE} 
                SET LastUpdate = current_timestamp(), ActiveFlag = 'N' 
                WHERE ActiveFlag = 'Y'
            """)

        columnChangeLogs = self._generateModelTransformationsConfiguration(
            processTransformationConfiguration,
            configurationOutputPath,
            metadataDeltaTablePath
        )
        
        self._generateTransformationsConfiguration(
            processTransformationConfiguration,
            configurationOutputPath,
            metadataDeltaTablePath
        )
        
        expOutputGenerated = self._generateExportConfiguration(
            processTransformationConfiguration,
            configurationOutputPath,
            metadataDeltaTablePath
        )

        # Store DQ configuration and introduce validation status

        dqConfigHasChanged = self._validator.saveDqMetadataConfig(
            self._compartmentConfig.METADATA_DQ_CONFIGURATION_FILE,
            metadataDeltaTablePath
        )
        self._validator.saveDqRefMetadataConfig(
            self._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE,
            metadataDeltaTablePath
        )
        self._validator.saveDqRefPairMetadataConfig(
            self._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE,
            metadataDeltaTablePath
        )

        self._validator.createDQLogTable(metadataDeltaTablePath)

        if (dqConfigHasChanged
            or processIngestionConfiguration
            or processTransformationConfiguration
        ):
            self._validator.addValidationStatusForTablesInHive()

        # Create output

        output = self._createOutput(
            executionStart,
            metadataPath,
            processIngestionConfiguration,
            processTransformationConfiguration,
            expOutputGenerated,
            columnChangeLogs
        )

        # Update FWK flags

        if processIngestionConfiguration:
            self._configGeneratorHelper.setFwkFlag(self.INGESTION_CONFIGURATION_PROCESSED_FLAG, "true")
        
        if processTransformationConfiguration:
            self._configGeneratorHelper.setFwkFlag(self.TRANSFORMATION_CONFIGURATION_PROCESSED_FLAG, "true")

        # Update the record created by the 'upsertIntoFwkLog' activity in the table ‘FwkLog’ by updating the columns ‘PipelineStatus’ to ‘Success’
        self._configGeneratorHelper.upsertIntoFwkLog(fwkLogTableParameters["EntRunId"], fwkLogTableParameters["Module"], fwkLogTableParameters["ModuleRunId"]
                                            ,fwkLogTableParameters["StartDate"], fwkLogTableParameters["AdfTriggerName"])
        
        return output
