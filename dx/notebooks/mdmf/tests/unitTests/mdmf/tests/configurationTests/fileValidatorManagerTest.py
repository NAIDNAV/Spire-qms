# Databricks notebook source
import unittest
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from unittest.mock import MagicMock, ANY

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.tests.configurationTests.fileValidatorManager import FileValidatorManager
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../../../compartment/includes/compartmentConfig

# COMMAND ----------

# MAGIC %run ../../../../configurationTests/fileValidatorManager

# COMMAND ----------

# MAGIC %run ../../../../../includes/frameworkConfig

# COMMAND ----------

METADATA_ENTITY1_SCHEMA = "Schema1"
METADATA_ENTITY2_SCHEMA = "Schema2"

class FileValidatorManagerTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()
        
        self._compartmentConfig = MagicMock()
        self._compartmentConfig.FWK_METADATA_CONFIG = {
            "metadata": {
                "databaseName": "Application_MEF_Metadata"
            }
        }

    def setUp(self):
        self._fileValidatorManager = FileValidatorManager(self._spark, self._compartmentConfig, False)
        self._fileValidatorManager._spark = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # initialize tests

    def test_initialize(self):
        # arrange
        self._fileValidatorManager._configGeneratorHelper = MagicMock()
        self._fileValidatorManager._configGeneratorHelper.getADLSPathAndUri.return_value = ("metadataPath", "metadataUri")

        self._fileValidatorManager._validator = MagicMock()

        self._fileValidatorManager._compartmentConfig.FWK_LINKED_SERVICE_DF = MagicMock()
        self._fileValidatorManager._compartmentConfig.FWK_TRIGGER_DF = MagicMock()

        # act
        self._fileValidatorManager._initialize()

        # assert
        self._fileValidatorManager._configGeneratorHelper.getADLSPathAndUri.assert_called_once_with(self._fileValidatorManager._compartmentConfig.FWK_METADATA_CONFIG["metadata"])

        self._fileValidatorManager._validator.saveConfigFileMetadataConfig.assert_called_once_with(
            self._fileValidatorManager._compartmentConfig.METADATA_CONFIGURATION_FILE_VALIDATION_FILE,
            "metadataUri/Delta"
        )

        self._fileValidatorManager._compartmentConfig.FWK_LINKED_SERVICE_DF.createOrReplaceTempView.assert_called_once_with("FwkLinkedService")
        self._fileValidatorManager._compartmentConfig.FWK_TRIGGER_DF.createOrReplaceTempView.assert_called_once_with("FwkTrigger")


    # getConfigsToValidateFromFwkMetadataConfig tests

    def test_getConfigsToValidateFromFwkMetadataConfig_ingestion(self):
        # arrange
        compartmentConfig = MagicMock()
        compartmentConfig.FWK_METADATA_CONFIG = {
            "ingestion": [
                {
                    "otherKeysDoNotMatter": "...",
                    "metadata": {
                        "ingestionFile": "Ingestion_Configuration.json",
                    }
                },
                {
                    "otherKeysDoNotMatter": "...",
                    "metadata": {
                        "ingestionFile": "Ingestion_Configuration.json",
                    }
                },
                {
                    "otherKeysDoNotMatter": "...",
                    "metadata": {
                        "ingestionFile": "Ingestion_Configuration_Files.json",
                    }
                }
            ]
        }

        expectedConfigsToValidate = [
            {
                "fileToValidate": "Ingestion_Configuration.json",
                "validateAgainstEntityName": "Ingestion_Configuration"
            },
            {
                "fileToValidate": "Ingestion_Configuration_Files.json",
                "validateAgainstEntityName": "Ingestion_Configuration"
            }
        ]

        # act
        configsToValidate = FileValidatorManager._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "ingestion", "ingestionFile", "Ingestion_Configuration")

        # assert
        self.assertEqual(configsToValidate, expectedConfigsToValidate)

    def test_getConfigsToValidateFromFwkMetadataConfig_transformations(self):
        # arrange
        compartmentConfig = MagicMock()
        compartmentConfig.FWK_METADATA_CONFIG = {
            "transformations": [
                {
                    "otherKeysDoNotMatter": "...",
                    "metadata": {
                        "transformationFile": "Transformation_Configuration.json",
                    }
                },
                {
                    "otherKeysDoNotMatter": "...",
                    "metadata": {
                        "transformationFile": "Transformation_Configuration.json",
                    }
                },
                {
                    "otherKeysDoNotMatter": "...",
                    "metadata": {
                        "transformationFile": "Transformation_Configuration.json",
                    }
                }
            ]
        }

        expectedConfigsToValidate = [
            {
                "fileToValidate": "Transformation_Configuration.json",
                "validateAgainstEntityName": "Transformation_Configuration"
            }
        ]

        # act
        configsToValidate = FileValidatorManager._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "transformations", "transformationFile", "Transformation_Configuration")

        # assert
        self.assertEqual(configsToValidate, expectedConfigsToValidate)

    def test_getConfigsToValidateFromFwkMetadataConfig_modelTransformations(self):
        # arrange
        compartmentConfig = MagicMock()
        compartmentConfig.FWK_METADATA_CONFIG = {
            "modelTransformations": [
                {
                    "otherKeysDoNotMatter": "...",
                    "metadata": {
                        "attributesFile": "OneHOUSE_LDM_Attributes.json",
                        "keysFile": "OneHOUSE_LDM_Keys.json",
                        "relationsFile": "OneHOUSE_LDM_Relations.json",
                        "transformationFile": "Model_Transformation_Configuration.json",
                    }
                },
                {
                    "otherKeysDoNotMatter": "...",
                    "metadata": {
                        "attributesFile": "OneHOUSE_Dim_Attributes.json",
                        "keysFile": "OneHOUSE_Dim_Keys.json",
                        "relationsFile": "OneHOUSE_Dim_Relations.json"
                    }
                }
            ]
        }

        expectedConfigsToValidate_attributes = [
            {
                "fileToValidate": "OneHOUSE_LDM_Attributes.json",
                "validateAgainstEntityName": "Model_Attributes"
            },
            {
                "fileToValidate": "OneHOUSE_Dim_Attributes.json",
                "validateAgainstEntityName": "Model_Attributes"
            }
        ]

        expectedConfigsToValidate_keys = [
            {
                "fileToValidate": "OneHOUSE_LDM_Keys.json",
                "validateAgainstEntityName": "Model_Keys",
                "attributesFile": "OneHOUSE_LDM_Attributes.json"
            },
            {
                "fileToValidate": "OneHOUSE_Dim_Keys.json",
                "validateAgainstEntityName": "Model_Keys",
                "attributesFile": "OneHOUSE_Dim_Attributes.json"
            }
        ]

        expectedConfigsToValidate_relations = [
            {
                "fileToValidate": "OneHOUSE_LDM_Relations.json",
                "validateAgainstEntityName": "Model_Relations",
                "attributesFile": "OneHOUSE_LDM_Attributes.json"
            },
            {
                "fileToValidate": "OneHOUSE_Dim_Relations.json",
                "validateAgainstEntityName": "Model_Relations",
                "attributesFile": "OneHOUSE_Dim_Attributes.json"
            }
        ]

        expectedConfigsToValidate_transformation = [
            {
                "fileToValidate": "Model_Transformation_Configuration.json",
                "validateAgainstEntityName": "Model_Transformation_Configuration"
            }
        ]

        # act
        configsToValidate_attributes = FileValidatorManager._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "modelTransformations", "attributesFile", "Model_Attributes")
        configsToValidate_keys = FileValidatorManager._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "modelTransformations", "keysFile", "Model_Keys")
        configsToValidate_relations = FileValidatorManager._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "modelTransformations", "relationsFile", "Model_Relations")
        configsToValidate_transformation = FileValidatorManager._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "modelTransformations", "transformationFile", "Model_Transformation_Configuration")

        # assert
        self.assertEqual(configsToValidate_attributes, expectedConfigsToValidate_attributes)
        self.assertEqual(configsToValidate_keys, expectedConfigsToValidate_keys)
        self.assertEqual(configsToValidate_relations, expectedConfigsToValidate_relations)
        self.assertEqual(configsToValidate_transformation, expectedConfigsToValidate_transformation)

    def test_getConfigsToValidateFromFwkMetadataConfig_export(self):
        # arrange
        compartmentConfig = MagicMock()
        compartmentConfig.FWK_METADATA_CONFIG = {
            "export": [
                {
                    "otherKeysDoNotMatter": "...",
                    "metadata": {
                        "exportFile": "Export_ADLS.json",
                    }
                },
                {
                    "otherKeysDoNotMatter": "...",
                    "metadata": {
                        "exportFile": "Export_FS.json",
                    }
                }
            ]
        }

        expectedConfigsToValidate = [
            {
                "fileToValidate": "Export_ADLS.json",
                "validateAgainstEntityName": "Export_Configuration"
            },
            {
                "fileToValidate": "Export_FS.json",
                "validateAgainstEntityName": "Export_Configuration"
            }
        ]

        # act
        configsToValidate = FileValidatorManager._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "export", "exportFile", "Export_Configuration")

        # assert
        self.assertEqual(configsToValidate, expectedConfigsToValidate)

    def test_getConfigsToValidateFromFwkMetadataConfig_missingSection(self):
        # arrange
        compartmentConfig = MagicMock()
        compartmentConfig.FWK_METADATA_CONFIG = {
            "ingestion": [
                {
                    "otherKeysDoNotMatter": "...",
                    "metadata": {
                        "ingestionFile": "Ingestion_Configuration.json",
                    }
                }
            ]
        }

        expectedConfigsToValidate = []

        # act
        configsToValidate = FileValidatorManager._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "ingestion123", "ingestionFile", "Ingestion_Configuration")

        # assert
        self.assertEqual(configsToValidate, expectedConfigsToValidate)

    def test_getConfigsToValidateFromFwkMetadataConfig_emptySection(self):
        # arrange
        compartmentConfig = MagicMock()
        compartmentConfig.FWK_METADATA_CONFIG = {
            "ingestion": []
        }

        expectedConfigsToValidate = []

        # act
        configsToValidate = FileValidatorManager._getConfigsToValidateFromFwkMetadataConfig(compartmentConfig, "ingestion", "ingestionFile", "Ingestion_Configuration")

        # assert
        self.assertEqual(configsToValidate, expectedConfigsToValidate)

    
    # getConfigToValidateFromCompartmentConfigAttr tests

    def test_getConfigToValidateFromCompartmentConfigAttr(self):
        # arrange
        compartmentConfig = MagicMock()
        compartmentConfig.METADATA_DQ_CONFIGURATION_FILE = ["DQ_Configuration.json","DQ_Configuration1.json"]
        compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE = "DQ_Reference_Values.json"
        compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE = "DQ_Reference_Pair_Values.json"

        expectedConfigsToValidate_dqConfig = [
            {
                "fileToValidate": "DQ_Configuration.json",
                "validateAgainstEntityName": "DQ_Configuration"
            },
            {
                "fileToValidate": "DQ_Configuration1.json",
                "validateAgainstEntityName": "DQ_Configuration"
            }
        ]

        expectedConfigsToValidate_dqReferenceValues = [
            {
                "fileToValidate": "DQ_Reference_Values.json",
                "validateAgainstEntityName": "DQ_Reference_Values"
            }
        ]

        expectedConfigsToValidate_dqReferencePairValues = [
            {
                "fileToValidate": "DQ_Reference_Pair_Values.json",
                "validateAgainstEntityName": "DQ_Reference_Pair_Values"
            }
        ]

        # act
        configsToValidate_dqConfig = FileValidatorManager._getConfigToValidateFromCompartmentConfigAttr(compartmentConfig, "METADATA_DQ_CONFIGURATION_FILE", "DQ_Configuration")
        configsToValidate_dqReferenceValues = FileValidatorManager._getConfigToValidateFromCompartmentConfigAttr(compartmentConfig, "METADATA_DQ_REFERENCE_VALUES_FILE", "DQ_Reference_Values")
        configsToValidate_dqReferencePairValues = FileValidatorManager._getConfigToValidateFromCompartmentConfigAttr(compartmentConfig, "METADATA_DQ_REFERENCE_PAIR_VALUES_FILE", "DQ_Reference_Pair_Values")

        # assert
        self.assertEqual(configsToValidate_dqConfig, expectedConfigsToValidate_dqConfig)
        self.assertEqual(configsToValidate_dqReferenceValues, expectedConfigsToValidate_dqReferenceValues)
        self.assertEqual(configsToValidate_dqReferencePairValues, expectedConfigsToValidate_dqReferencePairValues)

    def test_getConfigToValidateFromCompartmentConfigAttr_missingAttribute(self):
        # arrange
        class CompartmentConfig:
            a = 1

        compartmentConfig = CompartmentConfig()

        expectedConfigsToValidate = []

        # act
        configsToValidate = FileValidatorManager._getConfigToValidateFromCompartmentConfigAttr(compartmentConfig, "METADATA_DQ_CONFIGURATION_FILE_123", "DQ_Configuration")

        # assert
        self.assertEqual(configsToValidate, expectedConfigsToValidate)


    # getConfigsToValidate tests

    def test_getConfigsToValidate(self):
        # arrange
        fileValidatorManager = FileValidatorManager(self._spark, self._compartmentConfig, False)

        compartmentConfig = MagicMock()
        compartmentConfig.FWK_METADATA_CONFIG = {
            "ingestion": []
        }

        fileValidatorManager._getConfigsToValidateFromFwkMetadataConfig = MagicMock(return_value=["A"])
        fileValidatorManager._getConfigToValidateFromCompartmentConfigAttr = MagicMock(return_value=["B"])

        # act
        configsToValidate = fileValidatorManager.getConfigsToValidate(compartmentConfig)

        # assert
        self.assertEqual(fileValidatorManager._getConfigsToValidateFromFwkMetadataConfig.call_count, 7)
        fileValidatorManager._getConfigsToValidateFromFwkMetadataConfig.assert_any_call(compartmentConfig, "ingestion", "ingestionFile", "Ingestion_Configuration")
        fileValidatorManager._getConfigsToValidateFromFwkMetadataConfig.assert_any_call(compartmentConfig, "transformations", "transformationFile", "Transformation_Configuration")
        fileValidatorManager._getConfigsToValidateFromFwkMetadataConfig.assert_any_call(compartmentConfig, "modelTransformations", "attributesFile", "Model_Attributes")
        fileValidatorManager._getConfigsToValidateFromFwkMetadataConfig.assert_any_call(compartmentConfig, "modelTransformations", "keysFile", "Model_Keys")
        fileValidatorManager._getConfigsToValidateFromFwkMetadataConfig.assert_any_call(compartmentConfig, "modelTransformations", "relationsFile", "Model_Relations")
        fileValidatorManager._getConfigsToValidateFromFwkMetadataConfig.assert_any_call(compartmentConfig, "modelTransformations", "relationsFile", "Model_Relations")
        fileValidatorManager._getConfigsToValidateFromFwkMetadataConfig.assert_any_call(compartmentConfig, "export", "exportFile", "Export_Configuration")

        self.assertEqual(fileValidatorManager._getConfigToValidateFromCompartmentConfigAttr.call_count, 4)
        fileValidatorManager._getConfigToValidateFromCompartmentConfigAttr.assert_any_call(compartmentConfig, "WORKFLOWS_CONFIGURATION_FILE", "Workflows_Configuration")
        fileValidatorManager._getConfigToValidateFromCompartmentConfigAttr.assert_any_call(compartmentConfig, "METADATA_DQ_CONFIGURATION_FILE", "DQ_Configuration")
        fileValidatorManager._getConfigToValidateFromCompartmentConfigAttr.assert_any_call(compartmentConfig, "METADATA_DQ_REFERENCE_VALUES_FILE", "DQ_Reference_Values")
        fileValidatorManager._getConfigToValidateFromCompartmentConfigAttr.assert_any_call(compartmentConfig, "METADATA_DQ_REFERENCE_PAIR_VALUES_FILE", "DQ_Reference_Pair_Values")

        self.assertEqual(configsToValidate, ["A","A","A","A","A","A","A","B","B","B","B"])

    
    # createOutput tests

    def test_createOutput_valid(self):
        # arrange
        executionStart = datetime.now()
        configsByStatus = {
            "valid": ["File1.json", "File2.json"],
            "invalid": [],
            "error": []
        }

        expectedOutput = {
            "duration": ANY,
            "valid": True,
            "configsByStatus": {
                "valid": ["File1.json", "File2.json"],
                "invalid": [],
                "error": []
            }
        }

        # act
        output = self._fileValidatorManager._createOutput(executionStart, configsByStatus)

        # assert
        self.assertEqual(output, expectedOutput)

        self.assertIsInstance(output["duration"], int)

    def test_createOutput_invalid(self):
        # arrange
        executionStart = datetime.now()
        configsByStatus = {
            "valid": ["File1.json", "File2.json"],
            "invalid": ["File3.json"],
            "error": ["File4.json"]
        }

        expectedOutput = {
            "duration": ANY,
            "valid": False,
            "configsByStatus": {
                "valid": ["File1.json", "File2.json"],
                "invalid": ["File3.json"],
                "error": ["File4.json"]
            }
        }

        # act
        output = self._fileValidatorManager._createOutput(executionStart, configsByStatus)

        # assert
        self.assertEqual(output, expectedOutput)

        self.assertIsInstance(output["duration"], int)


    # validate tests

    def test_validate_noOptions(self):
        # arrange
        self._fileValidatorManager._spark.catalog.clearCache = MagicMock()
        self._fileValidatorManager._configGeneratorHelper.readMetadataConfig = MagicMock()
        
        def validate(validationDF: DataFrame, databaseName: str, tableName: str) -> DataFrame:            
            if tableName == "Entity1":
                return self._spark.createDataFrame(
                    [
                        (1, "A", "B", "VALID"),
                        (2, "C", "D", "VALID"),
                        (3, "E", "F", "INVALID")
                    ],
                    ["id", "col1", "col2", VALIDATION_STATUS_COLUMN_NAME]
                )
            elif tableName == "Entity2":
                return self._spark.createDataFrame(
                    [
                        (1, "X", "VALID"),
                        (2, "Y", "VALID"),
                        (3, "Z", "VALID")
                    ],
                    ["id", "colA", VALIDATION_STATUS_COLUMN_NAME]
                )
        
        self._fileValidatorManager._validator.validate = MagicMock(side_effect=validate)

        expectedOutput = MagicMock()
        self._fileValidatorManager._createOutput = MagicMock(return_value=expectedOutput)

        configsToValidate = [
            {
                "fileToValidate": "File1.json",
                "validateAgainstEntityName": "Entity1",
                "attributesFile": "AttributesFile.json"
            },
            {
                "fileToValidate": "File2.json",
                "validateAgainstEntityName": "Entity2"
            }
        ]

        # act
        output = self._fileValidatorManager.validate(configsToValidate)

        # assert
        self._fileValidatorManager._spark.catalog.clearCache.assert_called_once()

        self.assertEqual(self._fileValidatorManager._configGeneratorHelper.readMetadataConfig.call_count, 3)
        self._fileValidatorManager._configGeneratorHelper.readMetadataConfig.assert_any_call("AttributesFile.json", METADATA_MODEL_ATTRIBUTES_SCHEMA)
        self._fileValidatorManager._configGeneratorHelper.readMetadataConfig.assert_any_call("File1.json", METADATA_ENTITY1_SCHEMA)
        self._fileValidatorManager._configGeneratorHelper.readMetadataConfig.assert_any_call("File2.json", METADATA_ENTITY2_SCHEMA)

        self.assertEqual(self._fileValidatorManager._validator.validate.call_count, 2)
        self._fileValidatorManager._validator.validate.assert_any_call(ANY, "FileStore", "Entity1")
        self._fileValidatorManager._validator.validate.assert_any_call(ANY, "FileStore", "Entity2")

        self._fileValidatorManager._createOutput.assert_called_once_with(
            ANY,
            {
                "valid": ["File2.json"],
                "invalid": ["File1.json"],
                "error": []
            }
        )

        self.assertEqual(output, expectedOutput)

    def test_validate_options(self):
        # arrange
        self._fileValidatorManager._spark.catalog.clearCache = MagicMock()
        self._fileValidatorManager._configGeneratorHelper.readMetadataConfig = MagicMock()
        
        def validate(validationDF: DataFrame, databaseName: str, tableName: str) -> DataFrame:            
            if tableName == "Entity1":
                return self._spark.createDataFrame(
                    [
                        (1, "A", "B", "VALID"),
                        (2, "C", "D", "VALID"),
                        (3, "E", "F", "INVALID")
                    ],
                    ["id", "col1", "col2", VALIDATION_STATUS_COLUMN_NAME]
                )
            elif tableName == "Entity2":
                return self._spark.createDataFrame(
                    [
                        (1, "X", "VALID"),
                        (2, "Y", "VALID"),
                        (3, "Z", "VALID")
                    ],
                    ["id", "colA", VALIDATION_STATUS_COLUMN_NAME]
                )
        
        self._fileValidatorManager._validator.validate = MagicMock(side_effect=validate)
        self._fileValidatorManager._validator.hasToBeValidated = MagicMock(return_value=True)

        expectedOutput = MagicMock()
        self._fileValidatorManager._createOutput = MagicMock(return_value=expectedOutput)

        configsToValidate = [
            {
                "fileToValidate": "File1.json",
                "validateAgainstEntityName": "Entity1",
                "attributesFile": "AttributesFile.json"
            },
            {
                "fileToValidate": "File2.json",
                "validateAgainstEntityName": "Entity2"
            }
        ]

        options = {
            "detailedLogging": True
        }

        # act
        output = self._fileValidatorManager.validate(configsToValidate, options)

        # assert
        self._fileValidatorManager._spark.catalog.clearCache.assert_called_once()

        self.assertEqual(self._fileValidatorManager._configGeneratorHelper.readMetadataConfig.call_count, 3)
        self._fileValidatorManager._configGeneratorHelper.readMetadataConfig.assert_any_call("AttributesFile.json", METADATA_MODEL_ATTRIBUTES_SCHEMA)
        self._fileValidatorManager._configGeneratorHelper.readMetadataConfig.assert_any_call("File1.json", METADATA_ENTITY1_SCHEMA)
        self._fileValidatorManager._configGeneratorHelper.readMetadataConfig.assert_any_call("File2.json", METADATA_ENTITY2_SCHEMA)

        self.assertEqual(self._fileValidatorManager._validator.validate.call_count, 2)
        self._fileValidatorManager._validator.validate.assert_any_call(ANY, "FileStore", "Entity1")
        self._fileValidatorManager._validator.validate.assert_any_call(ANY, "FileStore", "Entity2")

        self._fileValidatorManager._createOutput.assert_called_once_with(
            ANY,
            {
                "valid": ["File2.json"],
                "invalid": ["File1.json"],
                "error": []
            }
        )

        self.assertEqual(output, expectedOutput)
