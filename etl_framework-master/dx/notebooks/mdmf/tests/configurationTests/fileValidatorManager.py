# Databricks notebook source
# MAGIC %run ../../../mdmf/includes/dataLakeHelper

# COMMAND ----------

# MAGIC %run ../../../mdmf/generator/configGenerators/configGeneratorHelper

# COMMAND ----------

# MAGIC %run ../../../mdmf/etl/validator

# COMMAND ----------

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, MapType
import re

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.etl.validator import Validator
    from notebooks.mdmf.generator.configGenerators.configGeneratorHelper import ConfigGeneratorHelper
    from notebooks.mdmf.includes.frameworkConfig import *


class FileValidatorManager:
    """FileValidatorManager is responsible for validation of configuration files provided
    by compartment.

    Public methods:
        getConfigsToValidate
        printOutput
        validate
    """

    def __init__(self, spark: SparkSession, compartmentConfig: MapType, initialize: bool = True):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._display = globals()["display"] if "display" in globals() else None

        self._configGeneratorHelper = ConfigGeneratorHelper(self._spark, self._compartmentConfig)
        
        validatorParams = {
            "validationConfigurationTable": Validator.METADATA_CONFIGURATION_FILE_VALIDATION_TABLE,
            "logToDatabase": False,
        }
        self._validator = Validator(self._spark, self._compartmentConfig, None, None, validatorParams)

        self._executionStart = datetime.now()

        if initialize:
            self._initialize()

    def _initialize(self):
        (metadataPath, metadataUri) = self._configGeneratorHelper.getADLSPathAndUri(self._compartmentConfig.FWK_METADATA_CONFIG["metadata"])
        metadataDeltaTablePath = f"{metadataUri}/Delta"

        # store configuration validation config file
        self._validator.saveConfigFileMetadataConfig(self._compartmentConfig.METADATA_CONFIGURATION_FILE_VALIDATION_FILE, metadataDeltaTablePath)

        # prepare temp tables for cross validation
        self._compartmentConfig.FWK_LINKED_SERVICE_DF.createOrReplaceTempView("FwkLinkedService")
        self._compartmentConfig.FWK_TRIGGER_DF.createOrReplaceTempView("FwkTrigger")
    
    @staticmethod
    def _getConfigsToValidateFromFwkMetadataConfig(compartmentConfig: MapType, sectionName: str, fileAttribute: str, validateAgainstEntityName: str) -> ArrayType:
        configsToValidate = []
        
        # check if section exists
        if sectionName in compartmentConfig.FWK_METADATA_CONFIG.keys():
            distinctFiles = []
            for metadataConfig in compartmentConfig.FWK_METADATA_CONFIG[sectionName]:
                # check if fileAttribute exists (may be optional)
                if fileAttribute in metadataConfig["metadata"].keys():
                    configFile = metadataConfig["metadata"][fileAttribute]
                    
                    if configFile not in distinctFiles: 
                        configToValidate = {
                            "fileToValidate": configFile,
                            "validateAgainstEntityName": validateAgainstEntityName
                        }

                        if validateAgainstEntityName in ["Model_Relations", "Model_Keys"]:
                            configToValidate["attributesFile"] = metadataConfig["metadata"]["attributesFile"]
                        
                        configsToValidate.append(configToValidate)
                        distinctFiles.append(configFile)

        return configsToValidate

    @staticmethod
    def _getConfigToValidateFromCompartmentConfigAttr(compartmentConfig: MapType, fileAttribute: str, validateAgainstEntityName: str) -> ArrayType:
        configsToValidate = []
        compartmentConfigAttrValue = getattr(compartmentConfig, fileAttribute, None)

        if compartmentConfigAttrValue:    
            if isinstance(compartmentConfigAttrValue, str):
                configsToValidate.append({
                    "fileToValidate": compartmentConfigAttrValue,
                    "validateAgainstEntityName": validateAgainstEntityName
                })
            elif isinstance(compartmentConfigAttrValue, list):
                 for item in compartmentConfigAttrValue:
                    configsToValidate.append({
                        "fileToValidate": item,
                        "validateAgainstEntityName": validateAgainstEntityName
                    })

        return configsToValidate

    def getConfigsToValidate(self, compartmentConfig: MapType, fwkLogTableParameters: dict) -> ArrayType:
        """Collects all configuration files provided by compartment.
        
        Returns:
            Array containing information about configuration files and respective entity name
            against the file should be validated.
        """

        # Insert record into FwkLog table with the execution module information by pipeline id
        self._configGeneratorHelper.upsertIntoFwkLog(fwkLogTableParameters["EntRunId"], fwkLogTableParameters["Module"], fwkLogTableParameters["ModuleRunId"]
                                       ,fwkLogTableParameters["StartDate"], fwkLogTableParameters["AdfTriggerName"])

        configsToValidate = []

        configsToValidate += self._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "ingestion", "ingestionFile", "Ingestion_Configuration")
        configsToValidate += self._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "transformations", "transformationFile", "Transformation_Configuration")
        configsToValidate += self._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "modelTransformations", "attributesFile", "Model_Attributes")
        configsToValidate += self._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "modelTransformations", "keysFile", "Model_Keys")
        configsToValidate += self._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "modelTransformations", "relationsFile", "Model_Relations")
        configsToValidate += self._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "modelTransformations", "transformationFile", "Model_Transformation_Configuration")
        configsToValidate += self._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "export", "exportFile", "Export_Configuration")
        configsToValidate += self._getConfigToValidateFromCompartmentConfigAttr(compartmentConfig, "WORKFLOWS_CONFIGURATION_FILE", "Workflows_Configuration")
        configsToValidate += self._getConfigToValidateFromCompartmentConfigAttr(compartmentConfig, "METADATA_DQ_CONFIGURATION_FILE", "DQ_Configuration")
        configsToValidate += self._getConfigToValidateFromCompartmentConfigAttr(compartmentConfig, "METADATA_DQ_REFERENCE_VALUES_FILE", "DQ_Reference_Values")
        configsToValidate += self._getConfigToValidateFromCompartmentConfigAttr(compartmentConfig, "METADATA_DQ_REFERENCE_PAIR_VALUES_FILE", "DQ_Reference_Pair_Values")

        return configsToValidate
    
    @staticmethod
    def _printConfigFiles(title: str, configFiles: ArrayType):
        if configFiles:
            print(title)

            for configFile in configFiles:
                print(f"   - {configFile}")
    
    @staticmethod
    def printOutput(output: MapType):
        "Pretty prints the result of the validation."

        print("\n\033[1mRESULT\x1b[0m")
        print("\033[1m------\x1b[0m")
        box = ("\x1b[42m  \x1b[0m" if output["valid"] else "\x1b[41m  \x1b[0m")
        print(f"""Success: {box} {output["valid"]}""")
        print(f"""Duration: {output["duration"]}s""")
        FileValidatorManager._printConfigFiles("Files with error:", output["configsByStatus"]["error"])
        FileValidatorManager._printConfigFiles("Invalid files:", output["configsByStatus"]["invalid"])
        FileValidatorManager._printConfigFiles("Valid files:", output["configsByStatus"]["valid"])

    @staticmethod
    def _createOutput(executionStart: datetime, configsByStatus: MapType) -> MapType:
        executionEnd = datetime.now()

        output = {
            "duration": (executionEnd - executionStart).seconds,
            "valid": len(configsByStatus["invalid"]) == 0 and len(configsByStatus["error"]) == 0,
            "configsByStatus": configsByStatus
        }

        return output

    def validate(self, configsToValidate: ArrayType, fwkLogTableParameters: dict, options: MapType = {}) -> MapType:
        """Validates configuration files.

        Configuration files are validated against expectations defined in
        ConfigurationFileValidation and data quality log is displayed.

        Returns:
            Output containing information about execution duration and validation details.
        """

        # set option defaults
        if "detailedLogging" not in options.keys():
            options["detailedLogging"] = False

        if "allowInvalidDataTypes" not in options.keys():
            options["allowInvalidDataTypes"] = False
        
        # clear cache because file to validate is cached when running this code more than once
        self._spark.catalog.clearCache()

        configsByStatus = {
            "valid": [],
            "invalid": [],
            "error": []
        }

        for configToValidate in configsToValidate:
            try:
                configFileToValidate = configToValidate["fileToValidate"]
                validateAgainstEntityName = configToValidate["validateAgainstEntityName"]

                # check if expectations are defined
                if options["detailedLogging"]:
                    assert self._validator.hasToBeValidated("FileStore", validateAgainstEntityName), (
                        f"There are no expectations defined for EntityName '{validateAgainstEntityName}'")

                # register attributes table for cross validation
                if "attributesFile" in configToValidate.keys():
                    attributesDF = self._configGeneratorHelper.readMetadataConfig(
                        configToValidate["attributesFile"],
                        METADATA_MODEL_ATTRIBUTES_SCHEMA
                    )
                    attributesDF.createOrReplaceTempView("Attributes")

                # get config schema
                configSchema = globals()[f"METADATA_{validateAgainstEntityName.upper()}_SCHEMA"]

                if options["detailedLogging"]:
                    print("\nExpected config schema:")
                    print(configSchema)

                # read MetadataParams/Params structs as string, so validator can use previously defined rules
                configSchema = re.sub(r"(?s)Params STRUCT<.*\s{20}>", "Params STRING", configSchema)
                configSchema = re.sub(r"(?s)MetadataParams STRUCT<.*\s{12}>", "MetadataParams STRING", configSchema)
                configSchema = re.sub(r"(?s)Params STRUCT<.*\s{12}>", "Params STRING", configSchema)

                if options["allowInvalidDataTypes"]:
                    # read boolean values as string, so the type can be checked
                    configSchema = configSchema.replace("BOOLEAN", "STRING")

                # read config to validate
                configDF = self._configGeneratorHelper.readMetadataConfig(
                    configFileToValidate,
                    configSchema
                )

                assert configDF.count(), f"Configuration file '{configFileToValidate}' either does not exist or is empty or does not match the config schema. Try to set widget AllowInvalidDataTypes to 'Yes'"

                # validate
                print(f"\nValidation log for '{configFileToValidate}':")
                configDF = self._validator.validate(configDF, "FileStore", validateAgainstEntityName)

                # display config with validation status
                if options["detailedLogging"]:
                    print("\nContent of the validated file with validation status:")
                    self._display(configDF)

                if configDF.count() == 1:
                    row = configDF.first()
                    noNullValues = 0
                    for column in configDF.columns:
                        if row[column]:
                            noNullValues += 1

                    assert noNullValues > 1, f"It seems that configuration file '{configFileToValidate}' does not match the config schema. Try to set widget AllowInvalidDataTypes to 'Yes'"

                if configDF.filter(f"""{VALIDATION_STATUS_COLUMN_NAME} <> 'VALID'""").count() > 0:
                    configsByStatus["invalid"].append(configFileToValidate)
                    print("\x1b[41m  \x1b[0m\x1b[31m INVALID\x1b[0m\n")
                else:
                    configsByStatus["valid"].append(configFileToValidate)
                    print("\x1b[42m  \x1b[0m\x1b[32m VALID\x1b[0m\n")
            except Exception as e:
                configsByStatus["error"].append(configFileToValidate)
                print(f"\x1b[31m{e}\x1b[0m")
                print("\x1b[41m  \x1b[0m\x1b[31m ERROR\x1b[0m\n")

        # create output
        output = self._createOutput(self._executionStart, configsByStatus)

        # Update the record created by the 'upsertIntoFwkLog' activity in the table ‘FwkLog’ by updating the columns ‘PipelineStatus’ to ‘Success’
        self._configGeneratorHelper.upsertIntoFwkLog(fwkLogTableParameters["EntRunId"], fwkLogTableParameters["Module"], fwkLogTableParameters["ModuleRunId"]
                                       ,fwkLogTableParameters["StartDate"], fwkLogTableParameters["AdfTriggerName"])

        return output
