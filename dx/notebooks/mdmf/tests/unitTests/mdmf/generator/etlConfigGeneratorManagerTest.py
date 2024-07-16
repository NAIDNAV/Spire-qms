# Databricks notebook source
import unittest
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import ArrayType, MapType
from unittest.mock import MagicMock, ANY

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.generator.etlConfigGeneratorManager import EtlConfigGeneratorManager
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../../compartment/includes/compartmentConfig

# COMMAND ----------

# MAGIC %run ../../../../generator/etlConfigGeneratorManager

# COMMAND ----------

# MAGIC %run ../../../../includes/frameworkConfig

# COMMAND ----------

class EtlConfigGeneratorManagerTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()
        self._compartmentConfig = MagicMock()
        self._compartmentConfig.FWK_METADATA_CONFIG = {
            "metadata": {
                "FwkLinkedServiceId": "MetadataADLS",
                "containerName": "metadata",
                "databaseName": "Application_MEF_Metadata"
            }
        }

        self._INGESTION = [
            {
                "FwkLayerId": "Landing",
                "FwkTriggerId": "Sandbox",
                "source": {
                    "FwkLinkedServiceId": "SourceSQL"
                },
                "sink": {
                    "FwkLinkedServiceId": "LandingADLS",
                    "containerName": "asmefdevxx",
                    "dataPath": "/SQL",
                    "databaseName": "Application_MEF_LandingSQL"
                },
                "metadata": {
                    "ingestionFile": "Ingestion_Configuration.json",
                    "ingestionTable": "IngestionConfiguration"
                }
            },
            {
                "FwkLayerId": "Landing",
                "FwkTriggerId": "Sandbox",
                "source": {
                    "FwkLinkedServiceId": "SourceOracle"
                },
                "sink": {
                    "FwkLinkedServiceId": "LandingADLS",
                    "containerName": "asmefdevxx",
                    "dataPath": "/Oracle",
                    "databaseName": "Application_MEF_LandingOracle"
                },
                "metadata": {
                    "ingestionFile": "Ingestion_Configuration.json",
                    "ingestionTable": "IngestionConfiguration"
                }
            }
        ]

        self._MODEL_TRANSFORMATIONS = [
            {
                "FwkLayerId": "SDM",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "CompartmentADLS",
                    "containerName": "dldata",
                    "dataPath": "/SDM",
                    "databaseName": "Application_MEF_SDM"
                },
                "metadata": {
                    "attributesFile": "OneHOUSE_LDM_Attributes.json",
                    "attributesTable": "LDMAttributes",
                    "keysFile": "OneHOUSE_LDM_Keys.json",
                    "keysTable": "LDMKeys",
                    "relationsFile": "OneHOUSE_LDM_Relations.json",
                    "relationsTable": "LDMRelations",
                    "entitiesFile": "OneHOUSE_LDM_Entities.json",
                    "entitiesTable": "LDMEntities",
                    "transformationFile": "Model_Transformation_Configuration.json",
                    "transformationTable": "ModelTransformationConfiguration",
                    "key": "SDM"
                }
            },
            {
                "FwkLayerId": "Dimensional",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "CompartmentADLS",
                    "containerName": "dldata",
                    "dataPath": "/dimensional",
                    "databaseName": "Application_MEF_Dimensional"
                },
                "metadata": {
                    "attributesFile": "OneHOUSE_Dim_Attributes.json",
                    "attributesTable": "DimAttributes",
                    "keysFile": "OneHOUSE_Dim_Keys.json",
                    "keysTable": "DimKeys",
                    "relationsFile": "OneHOUSE_Dim_Relations.json",
                    "relationsTable": "DimRelations",
                    "entitiesFile": "OneHOUSE_Dim_Entities.json",
                    "entitiesTable": "DimEntities"
                }
            }
        ]

        self._TRANSFORMATIONS = [
            {
                "FwkLayerId": "Pseudonymization",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "StagingADLS",
                    "containerName": "dldata",
                    "dataPath": "/SQL",
                    "databaseName": "Application_MEF_StagingSQL"
                },
                "metadata": {
                    "transformationFile": "Transformation_Configuration.json",
                    "transformationTable": "TransformationConfiguration",
                    "key": "LandingSQLPseudonymization",
                    "copyRest": True
                }
            },
            {
                "FwkLayerId": "Historization",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "CompartmentADLS",
                    "containerName": "dldata",
                    "dataPath": "/history",
                    "databaseName": "Application_MEF_History"
                },
                "metadata": {
                    "transformationFile": "Transformation_Configuration.json",
                    "transformationTable": "TransformationConfiguration",
                    "key": "StagingHistorization",
                    "copyRest": True
                }
            }
        ]

        self._EXPORT = [
            {
                "FwkLayerId": "Export",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "ExportADLS",
                    "containerName": "dldata",
                    "dataPath": "/ExportADLS"
                },
                "metadata": {
                    "exportFile": "ExportADLS_Export.json",
                    "exportTable": "ExportADLSExport"
                }
            },
            {
                "FwkLayerId": "Export",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "ExportSFTP",
                    "dataPath": "/export",
                    "tempSink": {
                        "FwkLinkedServiceId": "CompartmentADLS",
                        "containerName": "dldata",
                        "dataPath": "/TempExportSFTP"
                    }
                },
                "metadata": {
                    "exportFile": "ExportSFTP_Export.json",
                    "exportTable": "ExportSFTPExport"
                }
            }
        ]

    def setUp(self):
        self._etlConfigGeneratorManager = EtlConfigGeneratorManager(self._spark, self._compartmentConfig)
        self._etlConfigGeneratorManager._spark = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # writeAttributesData tests

    def test_writeAttributesData(self):
        # arrange
        attributesDF = MagicMock()
        self._etlConfigGeneratorManager._dataLakeHelper.writeData = MagicMock()

        # act
        self._etlConfigGeneratorManager._writeAttributesData(attributesDF, "metadataDeltaTablePath")

        # assert
        self._etlConfigGeneratorManager._dataLakeHelper.writeData.assert_called_once_with(
            attributesDF,
            f"metadataDeltaTablePath/{METADATA_ATTRIBUTES}",
            WRITE_MODE_APPEND,
            "delta",
            { },
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"],
            METADATA_ATTRIBUTES
        )
    

    # isSectionConfiguredInFwkMetadataConfig tests

    def test_isSectionConfiguredInFwkMetadataConfig_sectionDefined_notEmpty(self):
        # arrange
        compartmentConfig = MagicMock()
        compartmentConfig.FWK_METADATA_CONFIG = {
            "transformations": [
                {
                    "content": "someValues"
                }
            ],
            "modelTransformations": [
                {
                    "content": "someValues"
                }
            ]
        }

        etlConfigGeneratorManager = EtlConfigGeneratorManager(self._spark, compartmentConfig)

        # act
        result = etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig("transformations")

        # assert
        self.assertEqual(result, True)

    def test_isSectionConfiguredInFwkMetadataConfig_sectionDefined_empty(self):
        # arrange
        compartmentConfig = MagicMock()
        compartmentConfig.FWK_METADATA_CONFIG = {
            "transformations": [],
            "modelTransformations": [
                {
                    "content": "someValues"
                }
            ]
        }

        etlConfigGeneratorManager = EtlConfigGeneratorManager(self._spark, compartmentConfig)

        # act
        result = etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig("transformations")

        # assert
        self.assertEqual(result, False)

    def test_isSectionConfiguredInFwkMetadataConfig_sectionNotDefined(self):
        # arrange
        compartmentConfig = MagicMock()
        compartmentConfig.FWK_METADATA_CONFIG = {
            "modelTransformations": [
                {
                    "content": "someValues"
                }
            ]
        }

        etlConfigGeneratorManager = EtlConfigGeneratorManager(self._spark, compartmentConfig)

        # act
        result = etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig("transformations")

        # assert
        self.assertEqual(result, False)


    # saveIngestionMetadataConfiguration tests

    def test_saveIngestionMetadataConfiguration_newConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"] = self._INGESTION

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(return_value=True)

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, True)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 1)

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_called_once_with(
            "Ingestion_Configuration.json",
            METADATA_INGESTION_CONFIGURATION_SCHEMA,
            "metadataDeltaTablePath",
            "IngestionConfiguration"
        )

    def test_saveIngestionMetadataConfiguration_twoSectionsUsingDifferentIngestionConfig(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"] = self._INGESTION

        secondSection = self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"][1]
        secondSection["metadata"]["ingestionFile"] = "Ingestion_Configuration_2.json"
        secondSection["metadata"]["ingestionTable"] = "IngestionConfiguration2"

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(return_value=True)

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, True)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 2)

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "Ingestion_Configuration.json",
            METADATA_INGESTION_CONFIGURATION_SCHEMA,
            "metadataDeltaTablePath",
            "IngestionConfiguration"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "Ingestion_Configuration_2.json",
            METADATA_INGESTION_CONFIGURATION_SCHEMA,
            "metadataDeltaTablePath",
            "IngestionConfiguration2"
        )

    def test_saveIngestionMetadataConfiguration_onlyOneNewConfig(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"] = self._INGESTION

        secondSection = self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"][1]
        secondSection["metadata"]["ingestionFile"] = "Ingestion_Configuration_2.json"
        secondSection["metadata"]["ingestionTable"] = "IngestionConfiguration2"

        def saveMetadataConfig(configPath: str, configSchema: str, dataPath: str, tableName: str) -> bool:
            if configPath == "Ingestion_Configuration_2.json":
                return True
            else:
                return False

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(side_effect=saveMetadataConfig)

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, True)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 2)

    def test_saveIngestionMetadataConfiguration_noNewConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"] = self._INGESTION

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(return_value=False)

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, False)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 1)

    def test_saveIngestionMetadataConfiguration_noConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"] = None

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock()

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, False)

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_not_called()

    
    # saveModelTransformationsMetadataConfiguration tests

    def test_saveModelTransformationsMetadataConfiguration_allNewConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"] = self._MODEL_TRANSFORMATIONS

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, True)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 9)
        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 3)

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "OneHOUSE_LDM_Attributes.json",
            METADATA_MODEL_ATTRIBUTES_SCHEMA,
            "metadataDeltaTablePath",
            "LDMAttributes"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_any_call(
            "LDMAttributes_processed",
            "false"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "OneHOUSE_LDM_Keys.json",
            METADATA_MODEL_KEYS_SCHEMA,
            "metadataDeltaTablePath",
            "LDMKeys"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "OneHOUSE_LDM_Relations.json",
            METADATA_MODEL_RELATIONS_SCHEMA,
            "metadataDeltaTablePath",
            "LDMRelations"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "OneHOUSE_LDM_Entities.json",
            METADATA_MODEL_ENTITIES_SCHEMA,
            "metadataDeltaTablePath",
            "LDMEntities"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "Model_Transformation_Configuration.json",
            METADATA_MODEL_TRANSFORMATION_CONFIGURATION_SCHEMA,
            "metadataDeltaTablePath",
            "ModelTransformationConfiguration"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_any_call(
            "ModelTransformationConfiguration_processed",
            "false"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "OneHOUSE_Dim_Attributes.json",
            METADATA_MODEL_ATTRIBUTES_SCHEMA,
            "metadataDeltaTablePath",
            "DimAttributes"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_any_call(
            "DimAttributes_processed",
            "false"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "OneHOUSE_Dim_Keys.json",
            METADATA_MODEL_KEYS_SCHEMA,
            "metadataDeltaTablePath",
            "DimKeys"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "OneHOUSE_Dim_Relations.json",
            METADATA_MODEL_RELATIONS_SCHEMA,
            "metadataDeltaTablePath",
            "DimRelations"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "OneHOUSE_Dim_Entities.json",
            METADATA_MODEL_ENTITIES_SCHEMA,
            "metadataDeltaTablePath",
            "DimEntities"
        )

    def test_saveModelTransformationsMetadataConfiguration_onlyLDMAttributesIsNew(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"] = self._MODEL_TRANSFORMATIONS

        def saveMetadataConfig(configPath: str, configSchema: str, dataPath: str, tableName: str) -> bool:
            if configPath == "OneHOUSE_LDM_Attributes.json":
                return True
            else:
                return False

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(side_effect=saveMetadataConfig)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, False)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 9)
        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 1)

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_any_call(
            "LDMAttributes_processed",
            "false"
        )

    def test_saveModelTransformationsMetadataConfiguration_onlyLDMRelationsIsNew(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"] = self._MODEL_TRANSFORMATIONS

        def saveMetadataConfig(configPath: str, configSchema: str, dataPath: str, tableName: str) -> bool:
            if configPath == "OneHOUSE_LDM_Relations.json":
                return True
            else:
                return False

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(side_effect=saveMetadataConfig)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, True)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 9)
        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 0)

    def test_saveModelTransformationsMetadataConfiguration_twoSectionsUsingSameModelTransformationConfig(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"] = self._MODEL_TRANSFORMATIONS
        
        secondSection = self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"][1]
        secondSection["metadata"]["transformationFile"] = "Model_Transformation_Configuration.json"
        secondSection["metadata"]["transformationTable"] = "ModelTransformationConfiguration"

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 9)

    def test_saveModelTransformationsMetadataConfiguration_twoSectionsUsingDifferentModelTransformationConfig(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"] = self._MODEL_TRANSFORMATIONS
        
        secondSection = self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"][1]
        secondSection["metadata"]["transformationFile"] = "Model_Transformation_Configuration_2.json"
        secondSection["metadata"]["transformationTable"] = "ModelTransformationConfiguration2"

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 10)

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "Model_Transformation_Configuration.json",
            METADATA_MODEL_TRANSFORMATION_CONFIGURATION_SCHEMA,
            "metadataDeltaTablePath",
            "ModelTransformationConfiguration"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "Model_Transformation_Configuration_2.json",
            METADATA_MODEL_TRANSFORMATION_CONFIGURATION_SCHEMA,
            "metadataDeltaTablePath",
            "ModelTransformationConfiguration2"
        )

    def test_saveModelTransformationsMetadataConfiguration_noNewConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"] = self._MODEL_TRANSFORMATIONS
        
        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, False)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 9)
        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 0)

    def test_saveModelTransformationsMetadataConfiguration_noConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"] = None

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock()
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, False)

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_not_called()
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_not_called()


    # saveTransformationsMetadataConfiguration tests

    def test_saveTransformationsMetadataConfiguration_newConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"] = self._TRANSFORMATIONS

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, True)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 1)
        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 1)

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "Transformation_Configuration.json",
            METADATA_TRANSFORMATION_CONFIGURATION_SCHEMA,
            "metadataDeltaTablePath",
            "TransformationConfiguration"
        )

    def test_saveTransformationsMetadataConfiguration_twoSectionsUsingDifferentTransformationConfig(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"] = self._TRANSFORMATIONS

        secondSection = self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"][1]
        secondSection["metadata"]["transformationFile"] = "Transformation_Configuration_2.json"
        secondSection["metadata"]["transformationTable"] = "TransformationConfiguration2"

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, True)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 2)
        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 2)

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "Transformation_Configuration.json",
            METADATA_TRANSFORMATION_CONFIGURATION_SCHEMA,
            "metadataDeltaTablePath",
            "TransformationConfiguration"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "Transformation_Configuration_2.json",
            METADATA_TRANSFORMATION_CONFIGURATION_SCHEMA,
            "metadataDeltaTablePath",
            "TransformationConfiguration2"
        )

    def test_saveTransformationsMetadataConfiguration_onlyOneNewConfig(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"] = self._TRANSFORMATIONS

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        secondSection = self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"][1]
        secondSection["metadata"]["transformationFile"] = "Transformation_Configuration_2.json"
        secondSection["metadata"]["transformationTable"] = "TransformationConfiguration2"

        def saveMetadataConfig(configPath: str, configSchema: str, dataPath: str, tableName: str) -> bool:
            if configPath == "Transformation_Configuration_2.json":
                return True
            else:
                return False

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(side_effect=saveMetadataConfig)

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, True)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 2)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 1)


    def test_saveTransformationsMetadataConfiguration_noNewConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"] = self._MODEL_TRANSFORMATIONS
        
        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(return_value=False)

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, False)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 1)

    def test_saveTransformationsMetadataConfiguration_noConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"] = None

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock()

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, False)

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_not_called()

    
    # saveExportMetadataConfiguration tests

    def test_saveExportMetadataConfiguration_newConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["export"] = self._EXPORT

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(return_value=True)

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveExportMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, True)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 2)

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "ExportADLS_Export.json",
            METADATA_EXPORT_CONFIGURATION_SCHEMA,
            "metadataDeltaTablePath",
            "ExportADLSExport"
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_any_call(
            "ExportSFTP_Export.json",
            METADATA_EXPORT_CONFIGURATION_SCHEMA,
            "metadataDeltaTablePath",
            "ExportSFTPExport"
        )

    def test_saveExportMetadataConfiguration_onlyOneNewExportConfig(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["export"] = self._EXPORT

        def saveMetadataConfig(configPath: str, configSchema: str, dataPath: str, tableName: str) -> bool:
            if configPath == "ExportADLS_Export.json":
                return True
            else:
                return False

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(side_effect=saveMetadataConfig)

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveExportMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, True)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 2)

    def test_saveExportMetadataConfiguration_noNewConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["export"] = self._EXPORT

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock(return_value=False)

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveExportMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, False)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.call_count, 2)

    def test_saveExportMetadataConfiguration_noConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["export"] = None

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion = MagicMock()

        # act
        newConfigurationData = self._etlConfigGeneratorManager._saveExportMetadataConfiguration("metadataDeltaTablePath")

        # assert
        self.assertEqual(newConfigurationData, False)

        self._etlConfigGeneratorManager._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion.assert_not_called()


    # determineWhetherToProcessIngestionConfiguration tests

    def test_determineWhetherToProcessIngestionConfiguration_ingestionSectionNotDefined(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration = MagicMock()
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock()

        forceConfigGeneration = True

        # act
        processIngestionConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration("metadataDeltaTablePath", forceConfigGeneration)

        # assert
        self.assertEqual(processIngestionConfiguration, False)

        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.assert_called_once_with("ingestion")
        self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration.assert_not_called()
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_not_called()

    def test_determineWhetherToProcessIngestionConfiguration_newConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock()

        forceConfigGeneration = False

        # act
        processIngestionConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration("metadataDeltaTablePath", forceConfigGeneration)

        # assert
        self.assertEqual(processIngestionConfiguration, True)

        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.assert_called_once_with("ingestion")
        self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_not_called()
    
    def test_determineWhetherToProcessIngestionConfiguration_configurationNotProcessed(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock(return_value="false")

        forceConfigGeneration = False

        # act
        processIngestionConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration("metadataDeltaTablePath", forceConfigGeneration)

        # assert
        self.assertEqual(processIngestionConfiguration, True)

        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.assert_called_once_with("ingestion")
        self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_called_once_with(
            self._etlConfigGeneratorManager.INGESTION_CONFIGURATION_PROCESSED_FLAG
        )

    def test_determineWhetherToProcessIngestionConfiguration_noNewConfigurationAndAlreadyProcessed(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock(return_value="true")

        forceConfigGeneration = False

        # act
        processIngestionConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration("metadataDeltaTablePath", forceConfigGeneration)

        # assert
        self.assertEqual(processIngestionConfiguration, False)

        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.assert_called_once_with("ingestion")
        self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_called_once_with(
            self._etlConfigGeneratorManager.INGESTION_CONFIGURATION_PROCESSED_FLAG
        )

    def test_determineWhetherToProcessIngestionConfiguration_forceConfigGeneration(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock(return_value="true")

        forceConfigGeneration = True

        # act
        processIngestionConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration("metadataDeltaTablePath", forceConfigGeneration)

        # assert
        self.assertEqual(processIngestionConfiguration, True)

        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.assert_called_once_with("ingestion")
        self._etlConfigGeneratorManager._saveIngestionMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_not_called()

    
    # determineWhetherToProcessTransformationConfiguration tests

    def test_determineWhetherToProcessTransformationConfiguration_processingIngestionConfiguration_noTransformationSectionDefined(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration = MagicMock()
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration = MagicMock()
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration = MagicMock()
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock()

        processIngestionConfiguration = True
        forceConfigGeneration = True

        # act
        processTransformationConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration(
            "metadataDeltaTablePath",
            forceConfigGeneration,
            processIngestionConfiguration
        )

        # assert
        self.assertEqual(processTransformationConfiguration, False)

        self.assertEqual(self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.call_count, 3)
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.assert_any_call("modelTransformations")
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.assert_any_call("transformations")
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.assert_any_call("export")

        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration.assert_not_called()
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration.assert_not_called()
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration.assert_not_called()
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_not_called()
    
    def test_determineWhetherToProcessTransformationConfiguration_processingIngestionConfiguration_allTransformationSectionDefined(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock()

        processIngestionConfiguration = True
        forceConfigGeneration = False

        # act
        processTransformationConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration(
            "metadataDeltaTablePath",
            forceConfigGeneration,
            processIngestionConfiguration
        )

        # assert
        self.assertEqual(processTransformationConfiguration, True)

        self.assertEqual(self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.call_count, 1)
        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_not_called()

    def test_determineWhetherToProcessTransformationConfiguration_processingIngestionConfiguration_atLeastOneTransformationSectionDefined(self):
        # arrange
        def isSectionConfiguredInFwkMetadataConfig(sectionName: str) -> bool:
            if sectionName in ("modelTransformations", "transformations"):
                return False
            elif sectionName == "export":
                return True

        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(side_effect=isSectionConfiguredInFwkMetadataConfig)
        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock()

        processIngestionConfiguration = True
        forceConfigGeneration = False

        # act
        processTransformationConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration(
            "metadataDeltaTablePath",
            forceConfigGeneration,
            processIngestionConfiguration
        )

        # assert
        self.assertEqual(processTransformationConfiguration, True)

        self.assertEqual(self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.call_count, 3)
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.assert_any_call("modelTransformations")
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.assert_any_call("transformations")
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.assert_any_call("export")

        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_not_called()

    def test_determineWhetherToProcessTransformationConfiguration_newModelTransformationConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock()

        processIngestionConfiguration = False
        forceConfigGeneration = False

        # act
        processTransformationConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration(
            "metadataDeltaTablePath",
            forceConfigGeneration,
            processIngestionConfiguration
        )

        # assert
        self.assertEqual(processTransformationConfiguration, True)

        self.assertEqual(self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.call_count, 1)
        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_not_called()

    def test_determineWhetherToProcessTransformationConfiguration_newTransformationConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=True)

        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock()

        processIngestionConfiguration = False
        forceConfigGeneration = False

        # act
        processTransformationConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration(
            "metadataDeltaTablePath",
            forceConfigGeneration,
            processIngestionConfiguration
        )

        # assert
        self.assertEqual(processTransformationConfiguration, True)

        self.assertEqual(self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.call_count, 1)
        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_not_called()

    def test_determineWhetherToProcessTransformationConfiguration_newExportConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=True)

        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock()

        processIngestionConfiguration = False
        forceConfigGeneration = False

        # act
        processTransformationConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration(
            "metadataDeltaTablePath",
            forceConfigGeneration,
            processIngestionConfiguration
        )

        # assert
        self.assertEqual(processTransformationConfiguration, True)

        self.assertEqual(self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.call_count, 1)
        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_not_called()
    
    def test_determineWhetherToProcessTransformationConfiguration_configurationNotProcessed(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=True)

        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock(return_value="false")

        processIngestionConfiguration = False
        forceConfigGeneration = False

        # act
        processTransformationConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration(
            "metadataDeltaTablePath",
            forceConfigGeneration,
            processIngestionConfiguration
        )

        # assert
        self.assertEqual(processTransformationConfiguration, True)

        self.assertEqual(self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.call_count, 1)
        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_called_once_with(
            self._etlConfigGeneratorManager.TRANSFORMATION_CONFIGURATION_PROCESSED_FLAG
        )

    def test_determineWhetherToProcessTransformationConfiguration_noNewConfigurationAndAlreadyProcessed(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=True)

        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock(return_value="true")

        processIngestionConfiguration = False
        forceConfigGeneration = False

        # act
        processTransformationConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration(
            "metadataDeltaTablePath",
            forceConfigGeneration,
            processIngestionConfiguration
        )

        # assert
        self.assertEqual(processTransformationConfiguration, False)

        self.assertEqual(self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.call_count, 1)
        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_called_once_with(
            self._etlConfigGeneratorManager.TRANSFORMATION_CONFIGURATION_PROCESSED_FLAG
        )

    def test_determineWhetherToProcessTransformationConfiguration_forceConfigGeneration(self):
        # arrange
        self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig = MagicMock(return_value=True)

        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock(return_value="true")

        processIngestionConfiguration = False
        forceConfigGeneration = True

        # act
        processTransformationConfiguration = self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration(
            "metadataDeltaTablePath",
            forceConfigGeneration,
            processIngestionConfiguration
        )

        # assert
        self.assertEqual(processTransformationConfiguration, True)

        self.assertEqual(self._etlConfigGeneratorManager._isSectionConfiguredInFwkMetadataConfig.call_count, 1)
        self._etlConfigGeneratorManager._saveModelTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveTransformationsMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._saveExportMetadataConfiguration.assert_called_once_with("metadataDeltaTablePath")
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_not_called()

    
    # generateIngestionConfiguration tests

    def test_generateIngestionConfiguration_generatesConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"] = [
            {
                "FwkLayerId": "Landing",
                "FwkTriggerId": "Sandbox",
                "source": {
                    "FwkLinkedServiceId": "SourceSQL"
                },
                "sink": {
                    "FwkLinkedServiceId": "LandingADLS",
                    "containerName": "asmefdevxx",
                    "dataPath": "/SQL",
                    "databaseName": "Application_MEF_LandingSQL"
                }
            },
            {
                "FwkLayerId": "Landing",
                "FwkTriggerId": "Sandbox",
                "source": {
                    "FwkLinkedServiceId": "SourceOracle"
                },
                "sink": {
                    "FwkLinkedServiceId": "LandingADLS",
                    "containerName": "asmefdevxx",
                    "dataPath": "/Oracle",
                    "databaseName": "Application_MEF_LandingOracle"
                }
            }
        ]

        sourceSQLAttributesDF = self._spark.createDataFrame([
            ["Application_MEF_LandingSQL", "Customer", "CustomerID", True, False, "AttributesTable"],
			["Application_MEF_LandingSQL", "Customer", "FirstName", False, False, "AttributesTable"],
			["Application_MEF_LandingSQL", "Customer", "LastName", False, False, "AttributesTable"],
            ["Application_MEF_LandingSQL", "Address", "AddressID", True, False, "AttributesTable"],
            ["Application_MEF_LandingSQL", "Address", "City", False, False, "AttributesTable"],
            ["Application_MEF_LandingSQL", "CustomerAddress", "CustomerID", True, False, "AttributesTable"],
            ["Application_MEF_LandingSQL", "CustomerAddress", "AddressID", True, False, "AttributesTable"]
        ], METADATA_ATTRIBUTES_SCHEMA)

        sourceOracleAttributesDF = self._spark.createDataFrame([
            ["Application_MEF_LandingOracle", "SalesOrderHeader", "OrderID", True, False, "AttributesTable"],
            ["Application_MEF_LandingOracle", "SalesOrderHeader", "CustomerID", False, False, "AttributesTable"],
			["Application_MEF_LandingOracle", "SalesOrderHeader", "Amount", False, False, "AttributesTable"]
        ], METADATA_ATTRIBUTES_SCHEMA)

        def generateConfiguration(metadataConfig: MapType, configurationOutputPath: str, metadataDeltaTablePath: str) -> DataFrame:
            if metadataConfig["source"]["FwkLinkedServiceId"] == "SourceSQL":
                return sourceSQLAttributesDF
            elif metadataConfig["source"]["FwkLinkedServiceId"] == "SourceOracle":
                return sourceOracleAttributesDF

        self._etlConfigGeneratorManager._ingestionConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._ingestionConfigGenerator.generateConfiguration.side_effect = generateConfiguration

        self._etlConfigGeneratorManager._writeAttributesData = MagicMock()

        processIngestionConfiguration = True

        # act
        self._etlConfigGeneratorManager._generateIngestionConfiguration(
            processIngestionConfiguration,
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        # assert
        self.assertEqual(self._etlConfigGeneratorManager._ingestionConfigGenerator.generateConfiguration.call_count, 2)

        self._etlConfigGeneratorManager._ingestionConfigGenerator.generateConfiguration.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"][0],
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )
        self._etlConfigGeneratorManager._ingestionConfigGenerator.generateConfiguration.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"][1],
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        self.assertEqual(self._etlConfigGeneratorManager._writeAttributesData.call_count, 2)
        self._etlConfigGeneratorManager._writeAttributesData.assert_any_call(
            sourceSQLAttributesDF,
            "metadataDeltaTablePath"
        )
        self._etlConfigGeneratorManager._writeAttributesData.assert_any_call(
            sourceOracleAttributesDF,
            "metadataDeltaTablePath"
        )

    def test_generateIngestionConfiguration_processFalse(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"] = [
            {},
            {}
        ]

        self._etlConfigGeneratorManager._ingestionConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._writeAttributesData = MagicMock()

        processIngestionConfiguration = False

        # act
        self._etlConfigGeneratorManager._generateIngestionConfiguration(
            processIngestionConfiguration,
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        # assert
        self._etlConfigGeneratorManager._ingestionConfigGenerator.generateConfiguration.assert_not_called()
        self._etlConfigGeneratorManager._writeAttributesData.assert_not_called()

    def test_generateIngestionConfiguration_noIngestion(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"] = None

        self._etlConfigGeneratorManager._ingestionConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._writeAttributesData = MagicMock()

        processIngestionConfiguration = True

        # act
        self._etlConfigGeneratorManager._generateIngestionConfiguration(
            processIngestionConfiguration,
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        # assert
        self._etlConfigGeneratorManager._ingestionConfigGenerator.generateConfiguration.assert_not_called()
        self._etlConfigGeneratorManager._writeAttributesData.assert_not_called()


    # generateModelTransformationsConfiguration tests

    def test_generateModelTransformationsConfiguration_generatesConfigurationAndAltersTables(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"] = self._MODEL_TRANSFORMATIONS
        metadataConfig = self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"][0]

        sdmAttributesDF = self._spark.createDataFrame([
            ["Application_MEF_SDM", "Customer", "WK_Customer_Identifier", True, True, "AttributesTable"],
            ["Application_MEF_SDM", "Customer", "CustomerID", False, False, "AttributesTable"],
			["Application_MEF_SDM", "Customer", "FirstName", False, False, "AttributesTable"],
			["Application_MEF_SDM", "Customer", "LastName", False, False, "AttributesTable"],
            ["Application_MEF_SDM", "Address", "WK_Address_Identifier", True, True, "AttributesTable"],
            ["Application_MEF_SDM", "Address", "AddressID", False, False, "AttributesTable"],
            ["Application_MEF_SDM", "Address", "City", False, False, "AttributesTable"],
            ["Application_MEF_SDM", "CustomerAddress", "WK_CustomerAddress_Identifier", True, True, "AttributesTable"],
            ["Application_MEF_SDM", "CustomerAddress", "WK_Customer_Identifier", False, False, "AttributesTable"],
            ["Application_MEF_SDM", "CustomerAddress", "CustomerID", False, False, "AttributesTable"],
            ["Application_MEF_SDM", "CustomerAddress", "AddressID", False, False, "AttributesTable"]
        ], METADATA_ATTRIBUTES_SCHEMA)

        dimAttributesDF = self._spark.createDataFrame([
            ["Application_MEF_Dimensional", "SalesOrderHeader", "WK_Order_Identifier", True, True, "AttributesTable"],
            ["Application_MEF_Dimensional", "SalesOrderHeader", "OrderID", False, False, "AttributesTable"],
            ["Application_MEF_Dimensional", "SalesOrderHeader", "CustomerID", False, False, "AttributesTable"],
			["Application_MEF_Dimensional", "SalesOrderHeader", "Amount", False, False, "AttributesTable"]
        ], METADATA_ATTRIBUTES_SCHEMA)

        def generateConfiguration(metadataConfig: MapType, configurationOutputPath: str, runMaintenanceManager: bool) -> DataFrame:
            if metadataConfig["FwkLayerId"] == "SDM":
                return sdmAttributesDF
            elif metadataConfig["FwkLayerId"] == "Dimensional":
                return dimAttributesDF

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration.side_effect = generateConfiguration

        self._etlConfigGeneratorManager._writeAttributesData = MagicMock()

        def getFwkFlag(flag: str) -> str:
            if flag == "LDMAttributes_processed":
                return "false"
            elif flag == "DimAttributes_processed":
                return "false"
            elif flag == "ModelTransformationConfiguration_processed":
                return "false"

        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock(side_effect=getFwkFlag)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        modelTransformationConfigurationChangedForSDM = MagicMock()

        def isDeltaTableChanged(dataPath: str, deltaTableName: str, filterExpression: str, columnsToCompare: ArrayType) -> bool:
            if deltaTableName == "ModelTransformationConfiguration":
                return modelTransformationConfigurationChangedForSDM

        self._etlConfigGeneratorManager._configGeneratorHelper.isDeltaTableChanged = MagicMock(side_effect=isDeltaTableChanged)

        sdmChanges = ["Change 1 for Customer", "Change 2 for Customer", "Change 1 for Address"]
        dimChanges = ["Change 1 for SalesOrderHeader"]

        def createOrAlterDeltaTables(metadataConfig: MapType) -> ArrayType:
            if metadataConfig["FwkLayerId"] == "SDM":
                return sdmChanges
            elif metadataConfig["FwkLayerId"] == "Dimensional":
                return dimChanges

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.side_effect = createOrAlterDeltaTables

        expectedColumnChangeLogs = sdmChanges + dimChanges

        processTransformationConfiguration = True

        # act
        columnChangeLogs = self._etlConfigGeneratorManager._generateModelTransformationsConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        # assert
        self.assertEqual(self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration.call_count, 2)

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"][0],
            "configurationOutputPath",
            modelTransformationConfigurationChangedForSDM
        )
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"][1],
            "configurationOutputPath",
            False
        )

        self.assertEqual(self._etlConfigGeneratorManager._writeAttributesData.call_count, 2)

        self._etlConfigGeneratorManager._writeAttributesData.assert_any_call(
            sdmAttributesDF,
            "metadataDeltaTablePath"
        )
        self._etlConfigGeneratorManager._writeAttributesData.assert_any_call(
            dimAttributesDF,
            "metadataDeltaTablePath"
        )

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.call_count, 3)

        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_any_call(
            "LDMAttributes_processed"
        )
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_any_call(
            "DimAttributes_processed"
        )
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_any_call(
            "ModelTransformationConfiguration_processed"
        )

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.isDeltaTableChanged.call_count, 1)

        self._etlConfigGeneratorManager._configGeneratorHelper.isDeltaTableChanged.assert_any_call(
            "metadataDeltaTablePath", 
            "ModelTransformationConfiguration", 
            f"""Key='{metadataConfig["metadata"]["key"]}'""", 
            ["Params.partitionColumns"]
        )

        self.assertEqual(self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.call_count, 2)

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"][0]
        )
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"][1]
        )

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 3)

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_any_call(
            "LDMAttributes_processed", "true"
        )
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_any_call(
            "DimAttributes_processed", "true"
        )
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_any_call(
            "ModelTransformationConfiguration_processed", "true"
        )

        self.assertEqual(columnChangeLogs, expectedColumnChangeLogs)

    def test_generateModelTransformationsConfiguration_generatesConfigurationOnly(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"] = self._MODEL_TRANSFORMATIONS

        sdmAttributesDF = self._spark.createDataFrame([
            ["Application_MEF_SDM", "Customer", "WK_Customer_Identifier", True, True, "AttributesTable"],
            ["Application_MEF_SDM", "Customer", "CustomerID", False, False, "AttributesTable"],
			["Application_MEF_SDM", "Customer", "FirstName", False, False, "AttributesTable"],
			["Application_MEF_SDM", "Customer", "LastName", False, False, "AttributesTable"],
            ["Application_MEF_SDM", "Address", "WK_Address_Identifier", True, True, "AttributesTable"],
            ["Application_MEF_SDM", "Address", "AddressID", False, False, "AttributesTable"],
            ["Application_MEF_SDM", "Address", "City", False, False, "AttributesTable"],
            ["Application_MEF_SDM", "CustomerAddress", "WK_CustomerAddress_Identifier", True, True, "AttributesTable"],
            ["Application_MEF_SDM", "CustomerAddress", "WK_Customer_Identifier", False, False, "AttributesTable"],
            ["Application_MEF_SDM", "CustomerAddress", "CustomerID", False, False, "AttributesTable"],
            ["Application_MEF_SDM", "CustomerAddress", "AddressID", False, False, "AttributesTable"]
        ], METADATA_ATTRIBUTES_SCHEMA)

        dimAttributesDF = self._spark.createDataFrame([
            ["Application_MEF_Dimensional", "SalesOrderHeader", "WK_Order_Identifier", True, True, "AttributesTable"],
            ["Application_MEF_Dimensional", "SalesOrderHeader", "OrderID", False, False, "AttributesTable"],
            ["Application_MEF_Dimensional", "SalesOrderHeader", "CustomerID", False, False, "AttributesTable"],
			["Application_MEF_Dimensional", "SalesOrderHeader", "Amount", False, False, "AttributesTable"]
        ], METADATA_ATTRIBUTES_SCHEMA)

        def generateConfiguration(metadataConfig: MapType, configurationOutputPath: str, runMaintenanceManager: bool) -> DataFrame:
            if metadataConfig["FwkLayerId"] == "SDM":
                return sdmAttributesDF
            elif metadataConfig["FwkLayerId"] == "Dimensional":
                return dimAttributesDF

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration.side_effect = generateConfiguration

        self._etlConfigGeneratorManager._writeAttributesData = MagicMock()

        def getFwkFlag(flag: str) -> str:
            if flag == "LDMAttributes_processed":
                return "true"
            elif flag == "DimAttributes_processed":
                return "true"
            elif flag == "ModelTransformationConfiguration_processed":
                return None

        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock(side_effect=getFwkFlag)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        self._etlConfigGeneratorManager._configGeneratorHelper.isDeltaTableChanged = MagicMock(return_value=False)
        modelTransformationConfigurationChangedForSDM = False

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata = MagicMock()

        processTransformationConfiguration = True

        # act
        columnChangeLogs = self._etlConfigGeneratorManager._generateModelTransformationsConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        # assert
        self.assertEqual(self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration.call_count, 2)

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"][0],
            "configurationOutputPath",
            modelTransformationConfigurationChangedForSDM
        )
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"][1],
            "configurationOutputPath",
            modelTransformationConfigurationChangedForSDM
        )

        self.assertEqual(self._etlConfigGeneratorManager._writeAttributesData.call_count, 2)

        self._etlConfigGeneratorManager._writeAttributesData.assert_any_call(
            sdmAttributesDF,
            "metadataDeltaTablePath"
        )
        self._etlConfigGeneratorManager._writeAttributesData.assert_any_call(
            dimAttributesDF,
            "metadataDeltaTablePath"
        )

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.call_count, 3)

        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_any_call(
            "LDMAttributes_processed"
        )
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_any_call(
            "DimAttributes_processed"
        )
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_any_call(
            "ModelTransformationConfiguration_processed"
        )

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.isDeltaTableChanged.call_count, 0)

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.assert_not_called()
        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 0)

        self.assertEqual(columnChangeLogs, [])

    def test_generateModelTransformationsConfiguration_altersTablesOnly(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"] = self._MODEL_TRANSFORMATIONS

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration = MagicMock()

        self._etlConfigGeneratorManager._writeAttributesData = MagicMock()

        def getFwkFlag(flag: str) -> str:
            if flag == "LDMAttributes_processed":
                return "false"
            elif flag == "DimAttributes_processed":
                return "false"

        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock(side_effect=getFwkFlag)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        sdmChanges = ["Change 1 for Customer", "Change 2 for Customer", "Change 1 for Address"]
        dimChanges = ["Change 1 for SalesOrderHeader"]

        def createOrAlterDeltaTables(metadataConfig: MapType) -> ArrayType:
            if metadataConfig["FwkLayerId"] == "SDM":
                return sdmChanges
            elif metadataConfig["FwkLayerId"] == "Dimensional":
                return dimChanges

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.side_effect = createOrAlterDeltaTables

        expectedColumnChangeLogs = sdmChanges + dimChanges

        processTransformationConfiguration = False

        # act
        columnChangeLogs = self._etlConfigGeneratorManager._generateModelTransformationsConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        # assert
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration.assert_not_called()
        self._etlConfigGeneratorManager._writeAttributesData.assert_not_called()

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.call_count, 2)

        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_any_call(
            "LDMAttributes_processed"
        )
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_any_call(
            "DimAttributes_processed"
        )

        self.assertEqual(self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.call_count, 2)

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"][0]
        )
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"][1]
        )

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 2)

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_any_call(
            "LDMAttributes_processed", "true"
        )
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_any_call(
            "DimAttributes_processed", "true"
        )

        self.assertEqual(columnChangeLogs, expectedColumnChangeLogs)

    def test_generateModelTransformationsConfiguration_altersTablesForDimLayerOnly(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"] = self._MODEL_TRANSFORMATIONS

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration = MagicMock()

        self._etlConfigGeneratorManager._writeAttributesData = MagicMock()

        def getFwkFlag(flag: str) -> str:
            if flag == "LDMAttributes_processed":
                return "true"
            elif flag == "DimAttributes_processed":
                return "false"

        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock(side_effect=getFwkFlag)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        dimChanges = ["Change 1 for SalesOrderHeader"]

        def createOrAlterDeltaTables(metadataConfig: MapType) -> ArrayType:
            if metadataConfig["FwkLayerId"] == "Dimensional":
                return dimChanges

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.side_effect = createOrAlterDeltaTables

        expectedColumnChangeLogs = dimChanges

        processTransformationConfiguration = False

        # act
        columnChangeLogs = self._etlConfigGeneratorManager._generateModelTransformationsConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        # assert
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration.assert_not_called()
        self._etlConfigGeneratorManager._writeAttributesData.assert_not_called()

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.call_count, 2)

        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_any_call(
            "LDMAttributes_processed"
        )
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_any_call(
            "DimAttributes_processed"
        )

        self.assertEqual(self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.call_count, 1)

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"][1]
        )

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 1)

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_any_call(
            "DimAttributes_processed", "true"
        )

        self.assertEqual(columnChangeLogs, expectedColumnChangeLogs)

    def test_generateModelTransformationsConfiguration_noModelTransformations(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["modelTransformations"] = None

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration = MagicMock()

        self._etlConfigGeneratorManager._writeAttributesData = MagicMock()

        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock()
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata = MagicMock()

        processTransformationConfiguration = True

        # act
        columnChangeLogs = self._etlConfigGeneratorManager._generateModelTransformationsConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        # assert
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.generateConfiguration.assert_not_called()
        self._etlConfigGeneratorManager._writeAttributesData.assert_not_called()

        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_not_called()
        self._etlConfigGeneratorManager._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata.assert_not_called()
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_not_called()

        self.assertEqual(columnChangeLogs, [])

    
    # generateTransformationsConfiguration tests

    def test_generateTransformationsConfiguration_generatesConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"] = self._TRANSFORMATIONS

        self._etlConfigGeneratorManager._compartmentConfig.FWK_LAYER_DF = self._spark.createDataFrame([
            ["Pseudonymization", 1],
            ["Historization", 2],
        ], "FwkLayerId STRING, DtOrder INTEGER")

        pseudoAttributesDF = self._spark.createDataFrame([
            ["Application_MEF_StagingSQL", "Customer", "CustomerID", True, False, "AttributesTable"],
			["Application_MEF_StagingSQL", "Customer", "FirstName", False, False, "AttributesTable"],
			["Application_MEF_StagingSQL", "Customer", "LastName", False, False, "AttributesTable"],
            ["Application_MEF_StagingSQL", "Address", "AddressID", True, False, "AttributesTable"],
            ["Application_MEF_StagingSQL", "Address", "City", False, False, "AttributesTable"],
            ["Application_MEF_StagingSQL", "CustomerAddress", "CustomerID", True, False, "AttributesTable"],
            ["Application_MEF_StagingSQL", "CustomerAddress", "AddressID", True, False, "AttributesTable"]
        ], METADATA_ATTRIBUTES_SCHEMA)

        historizationAttributesDF = self._spark.createDataFrame([
            ["Application_MEF_History", "Customer", "CustomerID", True, False, "AttributesTable"],
			["Application_MEF_History", "Customer", "FirstName", False, False, "AttributesTable"],
			["Application_MEF_History", "Customer", "LastName", False, False, "AttributesTable"],
            ["Application_MEF_History", "Customer", IS_ACTIVE_RECORD_COLUMN_NAME, False, False, "AttributesTable"],
            ["Application_MEF_History", "Address", "AddressID", True, False, "AttributesTable"],
            ["Application_MEF_History", "Address", "City", False, False, "AttributesTable"],
            ["Application_MEF_History", "Address", IS_ACTIVE_RECORD_COLUMN_NAME, False, False, "AttributesTable"],
            ["Application_MEF_History", "CustomerAddress", "CustomerID", True, False, "AttributesTable"],
            ["Application_MEF_History", "CustomerAddress", "AddressID", True, False, "AttributesTable"],
            ["Application_MEF_History", "CustomerAddress", IS_ACTIVE_RECORD_COLUMN_NAME, False, False, "AttributesTable"],
        ], METADATA_ATTRIBUTES_SCHEMA)

        def generateConfiguration(metadataConfig: MapType, configurationOutputPath: str, runMaintenanceManager: bool) -> DataFrame:
            if metadataConfig["FwkLayerId"] == "Pseudonymization":
                return pseudoAttributesDF
            elif metadataConfig["FwkLayerId"] == "Historization":
                return historizationAttributesDF

        self._etlConfigGeneratorManager._transformationsConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._transformationsConfigGenerator.generateConfiguration.side_effect = generateConfiguration

        transformationConfigurationChanged = MagicMock()
        def isDeltaTableChanged(dataPath: str, deltaTableName: str, filterExpression: str, columnsToCompare: ArrayType) -> bool:
            if deltaTableName == "TransformationConfiguration":
                return transformationConfigurationChanged
            
        self._etlConfigGeneratorManager._configGeneratorHelper.isDeltaTableChanged = MagicMock(side_effect=isDeltaTableChanged)

        def getFwkFlag(key: str) -> str:
            if key == "TransformationConfiguration_processed":
                return "false"

        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock(side_effect=getFwkFlag)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        self._etlConfigGeneratorManager._writeAttributesData = MagicMock()

        processTransformationConfiguration = True

        # act
        columnChangeLogs = self._etlConfigGeneratorManager._generateTransformationsConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        # assert
        self.assertEqual(self._etlConfigGeneratorManager._transformationsConfigGenerator.generateConfiguration.call_count, 2)

        calls = self._etlConfigGeneratorManager._transformationsConfigGenerator.generateConfiguration.call_args_list

        self.assertEqual(calls[0].args[0]["FwkLayerId"], "Pseudonymization")
        self.assertEqual(calls[1].args[0]["FwkLayerId"], "Historization")

        self._etlConfigGeneratorManager._transformationsConfigGenerator.generateConfiguration.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"][0],
            "configurationOutputPath",
            transformationConfigurationChanged
        )
        self._etlConfigGeneratorManager._transformationsConfigGenerator.generateConfiguration.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"][1],
            "configurationOutputPath",
            transformationConfigurationChanged
        )

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.isDeltaTableChanged.call_count, 2)
        self._etlConfigGeneratorManager._configGeneratorHelper.isDeltaTableChanged.assert_any_call(
            "metadataDeltaTablePath", 
            "TransformationConfiguration", 
            f"""Key='{self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"][0]["metadata"]["key"]}'""",
            ["Params.partitionColumns"]
        )
        self._etlConfigGeneratorManager._configGeneratorHelper.isDeltaTableChanged.assert_any_call(
            "metadataDeltaTablePath", 
            "TransformationConfiguration", 
            f"""Key='{self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"][1]["metadata"]["key"]}'""",
            ["Params.partitionColumns"]
        )

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.call_count, 2)
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_any_call(
            "TransformationConfiguration_processed"
        )
        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.assert_any_call(
            "TransformationConfiguration_processed"
        )

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 1)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_any_call(
            "TransformationConfiguration_processed",
            "true"
        )

        self.assertEqual(self._etlConfigGeneratorManager._writeAttributesData.call_count, 2)

        self._etlConfigGeneratorManager._writeAttributesData.assert_any_call(
            pseudoAttributesDF,
            "metadataDeltaTablePath"
        )
        self._etlConfigGeneratorManager._writeAttributesData.assert_any_call(
            historizationAttributesDF,
            "metadataDeltaTablePath"
        )

    def test_generateTransformationsConfiguration_generatesConfigurationBasedOnDtOrder(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"] = self._TRANSFORMATIONS

        self._etlConfigGeneratorManager._compartmentConfig.FWK_LAYER_DF = self._spark.createDataFrame([
            ["Pseudonymization", 10],
            ["Historization", 2],
        ], "FwkLayerId STRING, DtOrder INTEGER")

        self._etlConfigGeneratorManager._transformationsConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._transformationsConfigGenerator.generateConfiguration = MagicMock()

        self._etlConfigGeneratorManager._configGeneratorHelper.isDeltaTableChanged = MagicMock()

        def getFwkFlag(key: str) -> str:
            if key == "TransformationConfiguration_processed":
                return None

        self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag = MagicMock(side_effect=getFwkFlag)
        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        self._etlConfigGeneratorManager._writeAttributesData = MagicMock()

        processTransformationConfiguration = True

        # act
        columnChangeLogs = self._etlConfigGeneratorManager._generateTransformationsConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        # assert
        self.assertEqual(self._etlConfigGeneratorManager._transformationsConfigGenerator.generateConfiguration.call_count, 2)

        calls = self._etlConfigGeneratorManager._transformationsConfigGenerator.generateConfiguration.call_args_list

        self.assertEqual(calls[0].args[0]["FwkLayerId"], "Historization")
        self.assertEqual(calls[1].args[0]["FwkLayerId"], "Pseudonymization")

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.isDeltaTableChanged.call_count,0)

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.getFwkFlag.call_count, 2)
        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 0)

    def test_generateTransformationsConfiguration_processFalse(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"] = self._TRANSFORMATIONS

        self._etlConfigGeneratorManager._transformationsConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._transformationsConfigGenerator.generateConfiguration = MagicMock()

        self._etlConfigGeneratorManager._writeAttributesData = MagicMock()

        processTransformationConfiguration = False

        # act
        columnChangeLogs = self._etlConfigGeneratorManager._generateTransformationsConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        # assert
        self._etlConfigGeneratorManager._transformationsConfigGenerator.generateConfiguration.assert_not_called()
        self._etlConfigGeneratorManager._writeAttributesData.assert_not_called()

    def test_generateTransformationsConfiguration_noTransformations(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["transformations"] = None

        self._etlConfigGeneratorManager._transformationsConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._transformationsConfigGenerator.generateConfiguration = MagicMock()

        self._etlConfigGeneratorManager._writeAttributesData = MagicMock()

        processTransformationConfiguration = True

        # act
        columnChangeLogs = self._etlConfigGeneratorManager._generateTransformationsConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        # assert
        self._etlConfigGeneratorManager._transformationsConfigGenerator.generateConfiguration.assert_not_called()
        self._etlConfigGeneratorManager._writeAttributesData.assert_not_called()

    
    # generateExportConfiguration tests

    def test_generateExportConfiguration_generatesConfiguration(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["export"] = self._EXPORT

        def generateConfiguration(metadataConfig: MapType, configurationOutputPath: str) -> bool:
            if metadataConfig["metadata"]["exportFile"] == "ExportADLS_Export.json":
                return False
            elif metadataConfig["metadata"]["exportFile"] == "ExportSFTP_Export.json":
                return True

        self._etlConfigGeneratorManager._exportConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._exportConfigGenerator.generateConfiguration.side_effect = generateConfiguration

        processTransformationConfiguration = True

        # act
        expOutputGenerated = self._etlConfigGeneratorManager._generateExportConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath"
        )

        # assert
        self.assertEqual(self._etlConfigGeneratorManager._exportConfigGenerator.generateConfiguration.call_count, 2)

        self._etlConfigGeneratorManager._exportConfigGenerator.generateConfiguration.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["export"][0],
            "configurationOutputPath"
        )
        self._etlConfigGeneratorManager._exportConfigGenerator.generateConfiguration.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["export"][1],
            "configurationOutputPath"
        )

        self.assertEqual(expOutputGenerated, True)

    def test_generateExportConfiguration_generatesConfigurationNoExportOutput(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["export"] = self._EXPORT

        def generateConfiguration(metadataConfig: MapType, configurationOutputPath: str) -> bool:
            if metadataConfig["metadata"]["exportFile"] == "ExportADLS_Export.json":
                return False
            elif metadataConfig["metadata"]["exportFile"] == "ExportSFTP_Export.json":
                return False

        self._etlConfigGeneratorManager._exportConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._exportConfigGenerator.generateConfiguration.side_effect = generateConfiguration

        processTransformationConfiguration = True

        # act
        expOutputGenerated = self._etlConfigGeneratorManager._generateExportConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath"
        )

        # assert
        self.assertEqual(self._etlConfigGeneratorManager._exportConfigGenerator.generateConfiguration.call_count, 2)

        self._etlConfigGeneratorManager._exportConfigGenerator.generateConfiguration.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["export"][0],
            "configurationOutputPath"
        )
        self._etlConfigGeneratorManager._exportConfigGenerator.generateConfiguration.assert_any_call(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["export"][1],
            "configurationOutputPath"
        )

        self.assertEqual(expOutputGenerated, False)

    def test_generateExportConfiguration_processFalse(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["export"] = self._EXPORT

        self._etlConfigGeneratorManager._exportConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._exportConfigGenerator.generateConfiguration = MagicMock()

        processTransformationConfiguration = False

        # act
        expOutputGenerated = self._etlConfigGeneratorManager._generateExportConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath"
        )

        # assert
        self._etlConfigGeneratorManager._exportConfigGenerator.generateConfiguration.assert_not_called()

        self.assertEqual(expOutputGenerated, False)

    def test_generateExportConfiguration_noExport(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["export"] = None

        self._etlConfigGeneratorManager._exportConfigGenerator = MagicMock()
        self._etlConfigGeneratorManager._exportConfigGenerator.generateConfiguration = MagicMock()

        processTransformationConfiguration = True

        # act
        expOutputGenerated = self._etlConfigGeneratorManager._generateExportConfiguration(
            processTransformationConfiguration,
            "configurationOutputPath"
        )

        # assert
        self._etlConfigGeneratorManager._exportConfigGenerator.generateConfiguration.assert_not_called()

        self.assertEqual(expOutputGenerated, False)


    # createOutput tests

    def test_createOutput_allConfigs(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF = self._spark.createDataFrame([
            ["MetadataADLS", "https://metadata.dfs.core.windows.net/"],
        ], "FwkLinkedServiceId STRING, InstanceURL STRING")

        executionStart = datetime.now()
        processIngestionConfiguration = True
        processTransformationConfiguration = True
        expOutputGenerated = True
        columnChangeLogs = ["Change 1 for Customer", "Change 2 for Customer", "Change 1 for Address", "Change 1 for SalesOrderHeader"]

        expectedOutput = {
            "Duration": ANY,
            "GeneratedConfigFiles": [
                FWK_ENTITY_FOLDER,
                INGT_OUTPUT_FOLDER,
                DT_OUTPUT_FOLDER,
                EXP_OUTPUT_FOLDER
            ],
            "GeneratedConfigFilesPath": "testMetadataPath/Configuration",
            "GeneratedConfigFilesInstanceURL": "https://metadata.dfs.core.windows.net/",
            "ColumnChangeLogs": '\n'.join(columnChangeLogs),
            "ExecuteTriggerCreationPipeline": "False"
        }

        # act
        output = self._etlConfigGeneratorManager._createOutput(
            executionStart,
            "testMetadataPath",
            processIngestionConfiguration,
            processTransformationConfiguration,
            expOutputGenerated,
            columnChangeLogs
        )

        # assert
        del output["Duration"]
        del expectedOutput["Duration"]
        self.assertEqual(output, expectedOutput)

    def test_createOutput_ingestionOnly(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF = self._spark.createDataFrame([
            ["MetadataADLS", "https://metadata.dfs.core.windows.net/"],
        ], "FwkLinkedServiceId STRING, InstanceURL STRING")

        executionStart = datetime.now()
        processIngestionConfiguration = True
        processTransformationConfiguration = False
        expOutputGenerated = False
        columnChangeLogs = []

        expectedOutput = {
            "Duration": ANY,
            "GeneratedConfigFiles": [
                FWK_ENTITY_FOLDER,
                INGT_OUTPUT_FOLDER
            ],
            "GeneratedConfigFilesPath": "testMetadataPath/Configuration",
            "GeneratedConfigFilesInstanceURL": "https://metadata.dfs.core.windows.net/",
            "ColumnChangeLogs": "",
            "ExecuteTriggerCreationPipeline": "False"
        }

        # act
        output = self._etlConfigGeneratorManager._createOutput(
            executionStart,
            "testMetadataPath",
            processIngestionConfiguration,
            processTransformationConfiguration,
            expOutputGenerated,
            columnChangeLogs
        )

        # assert
        del output["Duration"]
        del expectedOutput["Duration"]
        self.assertEqual(output, expectedOutput)

    def test_createOutput_transformationOnly(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF = self._spark.createDataFrame([
            ["MetadataADLS", "https://metadata.dfs.core.windows.net/"],
        ], "FwkLinkedServiceId STRING, InstanceURL STRING")

        executionStart = datetime.now()
        processIngestionConfiguration = False
        processTransformationConfiguration = True
        expOutputGenerated = False
        columnChangeLogs = []

        expectedOutput = {
            "Duration": ANY,
            "GeneratedConfigFiles": [
                FWK_ENTITY_FOLDER,
                DT_OUTPUT_FOLDER
            ],
            "GeneratedConfigFilesPath": "testMetadataPath/Configuration",
            "GeneratedConfigFilesInstanceURL": "https://metadata.dfs.core.windows.net/",
            "ColumnChangeLogs": "",
            "ExecuteTriggerCreationPipeline": "False"
        }

        # act
        output = self._etlConfigGeneratorManager._createOutput(
            executionStart,
            "testMetadataPath",
            processIngestionConfiguration,
            processTransformationConfiguration,
            expOutputGenerated,
            columnChangeLogs
        )

        # assert
        del output["Duration"]
        del expectedOutput["Duration"]
        self.assertEqual(output, expectedOutput)

    def test_createOutput_transformationAndExportOnly(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF = self._spark.createDataFrame([
            ["MetadataADLS", "https://metadata.dfs.core.windows.net/"],
        ], "FwkLinkedServiceId STRING, InstanceURL STRING")

        executionStart = datetime.now()
        processIngestionConfiguration = False
        processTransformationConfiguration = True
        expOutputGenerated = True
        columnChangeLogs = []

        expectedOutput = {
            "Duration": ANY,
            "GeneratedConfigFiles": [
                FWK_ENTITY_FOLDER,
                DT_OUTPUT_FOLDER,
                EXP_OUTPUT_FOLDER
            ],
            "GeneratedConfigFilesPath": "testMetadataPath/Configuration",
            "GeneratedConfigFilesInstanceURL": "https://metadata.dfs.core.windows.net/",
            "ColumnChangeLogs": "",
            "ExecuteTriggerCreationPipeline": "False"
        }

        # act
        output = self._etlConfigGeneratorManager._createOutput(
            executionStart,
            "testMetadataPath",
            processIngestionConfiguration,
            processTransformationConfiguration,
            expOutputGenerated,
            columnChangeLogs
        )

        # assert
        del output["Duration"]
        del expectedOutput["Duration"]
        self.assertEqual(output, expectedOutput)

    def test_createOutput_noConfigs(self):
        # arrange
        self._etlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF = self._spark.createDataFrame([
            ["MetadataADLS", "https://metadata.dfs.core.windows.net/"],
        ], "FwkLinkedServiceId STRING, InstanceURL STRING")

        executionStart = datetime.now()
        processIngestionConfiguration = False
        processTransformationConfiguration = False
        expOutputGenerated = False
        columnChangeLogs = []

        expectedOutput = {
            "Duration": ANY,
            "GeneratedConfigFiles": [],
            "GeneratedConfigFilesPath": "testMetadataPath/Configuration",
            "GeneratedConfigFilesInstanceURL": "https://metadata.dfs.core.windows.net/",
            "ColumnChangeLogs": "",
            "ExecuteTriggerCreationPipeline": "False"
        }

        # act
        output = self._etlConfigGeneratorManager._createOutput(
            executionStart,
            "testMetadataPath",
            processIngestionConfiguration,
            processTransformationConfiguration,
            expOutputGenerated,
            columnChangeLogs
        )

        # assert
        del output["Duration"]
        del expectedOutput["Duration"]
        self.assertEqual(output, expectedOutput)

    
    # generateConfiguration tests

    def test_generateConfiguration_configsGenerated_dqConfigDidNotChange(self):
        # arrange
        self._etlConfigGeneratorManager._configGeneratorHelper.getADLSPathAndUri = MagicMock(return_value=("testMetadataPath", "testMetadataUri"))
        self._etlConfigGeneratorManager._configGeneratorHelper.moveUnprocessedFilesToArchive = MagicMock()
        self._etlConfigGeneratorManager._configGeneratorHelper.createFwkFlagsTable = MagicMock()
        self._etlConfigGeneratorManager._maintenanceManager.createMaintenanceTable = MagicMock()

        processIngestionConfiguration = True
        processTransformationConfiguration = True
        columnChangeLogs = ["Change 1", "Change 2"]
        expOutputGenerated = True

        self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration = MagicMock(return_value=processIngestionConfiguration)
        self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration = MagicMock(return_value=processTransformationConfiguration)

        self._etlConfigGeneratorManager._spark.catalog.tableExists.return_value = True

        self._etlConfigGeneratorManager._generateIngestionConfiguration = MagicMock()
        self._etlConfigGeneratorManager._generateModelTransformationsConfiguration = MagicMock(return_value=columnChangeLogs)
        self._etlConfigGeneratorManager._generateTransformationsConfiguration = MagicMock()
        self._etlConfigGeneratorManager._generateExportConfiguration = MagicMock(return_value=expOutputGenerated)

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_CONFIGURATION_FILE = "DQ Configuration.json"
        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE = "DQ Reference values.json"
        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE = "DQ Reference pair values.json"

        self._etlConfigGeneratorManager._validator.saveDqMetadataConfig = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._validator.saveDqRefMetadataConfig = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._validator.saveDqRefPairMetadataConfig = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._validator.createDQLogTable = MagicMock()
        self._etlConfigGeneratorManager._validator.addValidationStatusForTablesInHive = MagicMock()

        self._etlConfigGeneratorManager._createOutput = MagicMock()

        forceConfigGeneration = False

        # act
        output = self._etlConfigGeneratorManager.generateConfiguration(forceConfigGeneration)

        # assert
        self._etlConfigGeneratorManager._spark.catalog.clearCache.assert_called_once()

        self._etlConfigGeneratorManager._configGeneratorHelper.getADLSPathAndUri.assert_called_once_with(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["metadata"]
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.moveUnprocessedFilesToArchive.assert_called_once_with(
            "testMetadataUri/Configuration",
            ANY
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.createFwkFlagsTable.assert_called_once_with("testMetadataUri/Delta")
        self._etlConfigGeneratorManager._maintenanceManager.createMaintenanceTable.assert_called_once_with("testMetadataUri/Delta")

        self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration.assert_called_once_with(
            "testMetadataUri/Delta",
            False
        )

        self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration.assert_called_once_with(
            "testMetadataUri/Delta",
            False,
            processIngestionConfiguration
        )

        self.assertEqual(self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.call_count, 4)

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_has_calls([
            [(self._etlConfigGeneratorManager.INGESTION_CONFIGURATION_PROCESSED_FLAG, "false"),],
            [(self._etlConfigGeneratorManager.TRANSFORMATION_CONFIGURATION_PROCESSED_FLAG, "false"),],
            [(self._etlConfigGeneratorManager.INGESTION_CONFIGURATION_PROCESSED_FLAG, "true"),],
            [(self._etlConfigGeneratorManager.TRANSFORMATION_CONFIGURATION_PROCESSED_FLAG, "true"),]
        ])

        attributesTable = f"""{self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}"""

        self._etlConfigGeneratorManager._spark.catalog.tableExists.assert_called_once_with(attributesTable)

        self.assertEqual(self._etlConfigGeneratorManager._spark.sql.call_count, 2)
        
        sqlCommand = self._etlConfigGeneratorManager._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"""DELETE FROM {attributesTable}""", sqlCommand)
        self.assertIn(f"""WHERE MGT_ConfigTable = '{METADATA_ATTRIBUTES_INGESTION_KEY}'""", sqlCommand)

        sqlCommand = self._etlConfigGeneratorManager._spark.sql.call_args_list[1].args[0]
        self.assertIn(f"""DELETE FROM {attributesTable}""", sqlCommand)
        self.assertIn(f"""WHERE MGT_ConfigTable = '{METADATA_ATTRIBUTES_TRANSFORMATION_KEY}'""", sqlCommand)

        self._etlConfigGeneratorManager._generateIngestionConfiguration.assert_called_once_with(
            processIngestionConfiguration,
            "testMetadataUri/Configuration",
            "testMetadataUri/Delta"
        )

        self._etlConfigGeneratorManager._generateModelTransformationsConfiguration.assert_called_once_with(
            processTransformationConfiguration,
            "testMetadataUri/Configuration",
            "testMetadataUri/Delta"
        )
        
        self._etlConfigGeneratorManager._generateTransformationsConfiguration.assert_called_once_with(
            processTransformationConfiguration,
            "testMetadataUri/Configuration",
            "testMetadataUri/Delta"
        )
        
        self._etlConfigGeneratorManager._generateExportConfiguration.assert_called_once_with(
            processTransformationConfiguration,
            "testMetadataUri/Configuration"
        )

        self.assertEqual(self._etlConfigGeneratorManager._validator.saveDqMetadataConfig.call_count, 1)
        self.assertEqual(self._etlConfigGeneratorManager._validator.saveDqRefMetadataConfig.call_count, 1)
        self.assertEqual(self._etlConfigGeneratorManager._validator.saveDqRefPairMetadataConfig.call_count, 1)

        self._etlConfigGeneratorManager._validator.saveDqMetadataConfig.assert_any_call(
            self._compartmentConfig.METADATA_DQ_CONFIGURATION_FILE,
            "testMetadataUri/Delta"
        )
        self._etlConfigGeneratorManager._validator.saveDqRefMetadataConfig.assert_any_call(
            self._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE,
            "testMetadataUri/Delta"
        )
        self._etlConfigGeneratorManager._validator.saveDqRefPairMetadataConfig.assert_any_call(
            self._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE,
            "testMetadataUri/Delta"
        )

        self._etlConfigGeneratorManager._validator.createDQLogTable.assert_called_once_with(
            "testMetadataUri/Delta"
        )

        self._etlConfigGeneratorManager._validator.addValidationStatusForTablesInHive.assert_called_once()

        self._etlConfigGeneratorManager._createOutput.assert_called_once_with(
            ANY,
            "testMetadataPath",
            processIngestionConfiguration,
            processTransformationConfiguration,
            expOutputGenerated,
            columnChangeLogs
        )

        self.assertEqual(output, self._etlConfigGeneratorManager._createOutput.return_value)

    def test_generateConfiguration_noConfigsGenerated_dqConfigDidChange(self):
        # arrange
        self._etlConfigGeneratorManager._configGeneratorHelper.getADLSPathAndUri = MagicMock(return_value=("testMetadataPath", "testMetadataUri"))
        self._etlConfigGeneratorManager._configGeneratorHelper.moveUnprocessedFilesToArchive = MagicMock()
        self._etlConfigGeneratorManager._configGeneratorHelper.createFwkFlagsTable = MagicMock()
        self._etlConfigGeneratorManager._maintenanceManager.createMaintenanceTable = MagicMock()

        processIngestionConfiguration = False
        processTransformationConfiguration = False
        columnChangeLogs = []
        expOutputGenerated = False

        self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration = MagicMock(return_value=processIngestionConfiguration)
        self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration = MagicMock(return_value=processTransformationConfiguration)

        self._etlConfigGeneratorManager._spark.catalog.tableExists.return_value = True

        self._etlConfigGeneratorManager._generateIngestionConfiguration = MagicMock()
        self._etlConfigGeneratorManager._generateModelTransformationsConfiguration = MagicMock(return_value=columnChangeLogs)
        self._etlConfigGeneratorManager._generateTransformationsConfiguration = MagicMock()
        self._etlConfigGeneratorManager._generateExportConfiguration = MagicMock(return_value=expOutputGenerated)

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_CONFIGURATION_FILE = "DQ Configuration.json"
        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE = "DQ Reference values.json"
        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE = "DQ Reference pair values.json"

        self._etlConfigGeneratorManager._validator.saveDqMetadataConfig = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._validator.saveDqRefMetadataConfig = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._validator.saveDqRefPairMetadataConfig = MagicMock(return_value=True)
        self._etlConfigGeneratorManager._validator.createDQLogTable = MagicMock()
        self._etlConfigGeneratorManager._validator.addValidationStatusForTablesInHive = MagicMock()

        self._etlConfigGeneratorManager._createOutput = MagicMock()

        forceConfigGeneration = False

        # act
        output = self._etlConfigGeneratorManager.generateConfiguration(forceConfigGeneration)

        # assert
        self._etlConfigGeneratorManager._spark.catalog.clearCache.assert_called_once()

        self._etlConfigGeneratorManager._configGeneratorHelper.getADLSPathAndUri.assert_called_once_with(
            self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["metadata"]
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.moveUnprocessedFilesToArchive.assert_called_once_with(
            "testMetadataUri/Configuration",
            ANY
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.createFwkFlagsTable.assert_called_once_with("testMetadataUri/Delta")
        self._etlConfigGeneratorManager._maintenanceManager.createMaintenanceTable.assert_called_once_with("testMetadataUri/Delta")

        self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration.assert_called_once_with(
            "testMetadataUri/Delta",
            False
        )

        self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration.assert_called_once_with(
            "testMetadataUri/Delta",
            False,
            processIngestionConfiguration
        )

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag.assert_not_called()

        attributesTable = f"""{self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}"""

        self._etlConfigGeneratorManager._spark.catalog.tableExists.assert_called_once_with(attributesTable)

        self.assertEqual(self._etlConfigGeneratorManager._spark.sql.call_count, 0)

        self._etlConfigGeneratorManager._generateIngestionConfiguration.assert_called_once_with(
            processIngestionConfiguration,
            "testMetadataUri/Configuration",
            "testMetadataUri/Delta"
        )

        self._etlConfigGeneratorManager._generateModelTransformationsConfiguration.assert_called_once_with(
            processTransformationConfiguration,
            "testMetadataUri/Configuration",
            "testMetadataUri/Delta"
        )
        
        self._etlConfigGeneratorManager._generateTransformationsConfiguration.assert_called_once_with(
            processTransformationConfiguration,
            "testMetadataUri/Configuration",
            "testMetadataUri/Delta"
        )
        
        self._etlConfigGeneratorManager._generateExportConfiguration.assert_called_once_with(
            processTransformationConfiguration,
            "testMetadataUri/Configuration"
        )

        self.assertEqual(self._etlConfigGeneratorManager._validator.saveDqMetadataConfig.call_count, 1)
        self.assertEqual(self._etlConfigGeneratorManager._validator.saveDqRefMetadataConfig.call_count, 1)
        self.assertEqual(self._etlConfigGeneratorManager._validator.saveDqRefPairMetadataConfig.call_count, 1)

        self._etlConfigGeneratorManager._validator.saveDqMetadataConfig.assert_any_call(
            self._compartmentConfig.METADATA_DQ_CONFIGURATION_FILE,
            "testMetadataUri/Delta"
        )
        self._etlConfigGeneratorManager._validator.saveDqRefMetadataConfig.assert_any_call(
            self._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE,
            "testMetadataUri/Delta"
        )
        self._etlConfigGeneratorManager._validator.saveDqRefPairMetadataConfig.assert_any_call(
            self._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE,
            "testMetadataUri/Delta"
        )

        self._etlConfigGeneratorManager._validator.createDQLogTable.assert_called_once_with(
            "testMetadataUri/Delta"
        )

        self._etlConfigGeneratorManager._validator.addValidationStatusForTablesInHive.assert_called_once()

        self._etlConfigGeneratorManager._createOutput.assert_called_once_with(
            ANY,
            "testMetadataPath",
            processIngestionConfiguration,
            processTransformationConfiguration,
            expOutputGenerated,
            columnChangeLogs
        )

        self.assertEqual(output, self._etlConfigGeneratorManager._createOutput.return_value)

    def test_generateConfiguration_noConfigsGenerated_dqConfigDidNotChange(self):
        # arrange
        self._etlConfigGeneratorManager._configGeneratorHelper.getADLSPathAndUri = MagicMock(return_value=("testMetadataPath", "testMetadataUri"))
        self._etlConfigGeneratorManager._configGeneratorHelper.moveUnprocessedFilesToArchive = MagicMock()
        self._etlConfigGeneratorManager._configGeneratorHelper.createFwkFlagsTable = MagicMock()
        self._etlConfigGeneratorManager._maintenanceManager.createMaintenanceTable = MagicMock()

        processIngestionConfiguration = False
        processTransformationConfiguration = False
        columnChangeLogs = []
        expOutputGenerated = False

        self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration = MagicMock(return_value=processIngestionConfiguration)
        self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration = MagicMock(return_value=processTransformationConfiguration)

        self._etlConfigGeneratorManager._generateIngestionConfiguration = MagicMock()
        self._etlConfigGeneratorManager._generateModelTransformationsConfiguration = MagicMock(return_value=columnChangeLogs)
        self._etlConfigGeneratorManager._generateTransformationsConfiguration = MagicMock()
        self._etlConfigGeneratorManager._generateExportConfiguration = MagicMock(return_value=expOutputGenerated)

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_CONFIGURATION_FILE = "DQ Configuration.json"
        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE = "DQ Reference values.json"
        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE = "DQ Reference pair values.json"

        self._etlConfigGeneratorManager._validator.saveMetadataConfig = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._validator.createDQLogTable = MagicMock()
        self._etlConfigGeneratorManager._validator.addValidationStatusForTablesInHive = MagicMock()

        self._etlConfigGeneratorManager._createOutput = MagicMock()

        forceConfigGeneration = True

        # act
        output = self._etlConfigGeneratorManager.generateConfiguration(forceConfigGeneration)

        # assert
        self._etlConfigGeneratorManager._spark.catalog.clearCache.assert_called_once()

        self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration.assert_called_once_with(
            "testMetadataUri/Delta",
            True
        )

        self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration.assert_called_once_with(
            "testMetadataUri/Delta",
            True,
            processIngestionConfiguration
        )

        self._etlConfigGeneratorManager._validator.addValidationStatusForTablesInHive.assert_not_called()

    def test_generateConfiguration_ingestionConfigurationProcessed(self):
        # arrange
        self._etlConfigGeneratorManager._configGeneratorHelper.getADLSPathAndUri = MagicMock(return_value=("testMetadataPath", "testMetadataUri"))
        self._etlConfigGeneratorManager._configGeneratorHelper.moveUnprocessedFilesToArchive = MagicMock()
        self._etlConfigGeneratorManager._configGeneratorHelper.createFwkFlagsTable = MagicMock()
        self._etlConfigGeneratorManager._maintenanceManager.createMaintenanceTable = MagicMock()

        processIngestionConfiguration = True
        processTransformationConfiguration = False
        columnChangeLogs = []
        expOutputGenerated = False

        self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration = MagicMock(return_value=processIngestionConfiguration)
        self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration = MagicMock(return_value=processTransformationConfiguration)

        self._etlConfigGeneratorManager._spark.catalog.tableExists.return_value = True

        self._etlConfigGeneratorManager._generateIngestionConfiguration = MagicMock()
        self._etlConfigGeneratorManager._generateModelTransformationsConfiguration = MagicMock(return_value=columnChangeLogs)
        self._etlConfigGeneratorManager._generateTransformationsConfiguration = MagicMock()
        self._etlConfigGeneratorManager._generateExportConfiguration = MagicMock(return_value=expOutputGenerated)

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_CONFIGURATION_FILE = "DQ Configuration.json"
        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE = "DQ Reference values.json"
        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE = "DQ Reference pair values.json"

        self._etlConfigGeneratorManager._validator.saveMetadataConfig = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._validator.createDQLogTable = MagicMock()
        self._etlConfigGeneratorManager._validator.addValidationStatusForTablesInHive = MagicMock()

        self._etlConfigGeneratorManager._createOutput = MagicMock()

        forceConfigGeneration = False

        # act
        output = self._etlConfigGeneratorManager.generateConfiguration(forceConfigGeneration)

        # assert
        self._etlConfigGeneratorManager._spark.catalog.clearCache.assert_called_once()

        attributesTable = f"""{self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}"""

        self._etlConfigGeneratorManager._spark.catalog.tableExists.assert_called_once_with(attributesTable)

        self.assertEqual(self._etlConfigGeneratorManager._spark.sql.call_count, 1)
        
        sqlCommand = self._etlConfigGeneratorManager._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"""DELETE FROM {attributesTable}""", sqlCommand)
        self.assertIn(f"""WHERE MGT_ConfigTable = '{METADATA_ATTRIBUTES_INGESTION_KEY}'""", sqlCommand)

    def test_generateConfiguration_ingestionTransformationProcessed(self):
        # arrange
        self._etlConfigGeneratorManager._configGeneratorHelper.getADLSPathAndUri = MagicMock(return_value=("testMetadataPath", "testMetadataUri"))
        self._etlConfigGeneratorManager._configGeneratorHelper.moveUnprocessedFilesToArchive = MagicMock()
        self._etlConfigGeneratorManager._configGeneratorHelper.createFwkFlagsTable = MagicMock()
        self._etlConfigGeneratorManager._maintenanceManager.createMaintenanceTable = MagicMock()

        processIngestionConfiguration = False
        processTransformationConfiguration = True
        columnChangeLogs = []
        expOutputGenerated = False

        self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration = MagicMock(return_value=processIngestionConfiguration)
        self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration = MagicMock(return_value=processTransformationConfiguration)

        self._etlConfigGeneratorManager._spark.catalog.tableExists.return_value = True

        self._etlConfigGeneratorManager._generateIngestionConfiguration = MagicMock()
        self._etlConfigGeneratorManager._generateModelTransformationsConfiguration = MagicMock(return_value=columnChangeLogs)
        self._etlConfigGeneratorManager._generateTransformationsConfiguration = MagicMock()
        self._etlConfigGeneratorManager._generateExportConfiguration = MagicMock(return_value=expOutputGenerated)

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_CONFIGURATION_FILE = "DQ Configuration.json"
        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE = "DQ Reference values.json"
        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE = "DQ Reference pair values.json"

        self._etlConfigGeneratorManager._validator.saveMetadataConfig = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._validator.createDQLogTable = MagicMock()
        self._etlConfigGeneratorManager._validator.addValidationStatusForTablesInHive = MagicMock()

        self._etlConfigGeneratorManager._createOutput = MagicMock()

        forceConfigGeneration = False

        # act
        output = self._etlConfigGeneratorManager.generateConfiguration(forceConfigGeneration)

        # assert
        self._etlConfigGeneratorManager._spark.catalog.clearCache.assert_called_once()

        attributesTable = f"""{self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}"""

        self._etlConfigGeneratorManager._spark.catalog.tableExists.assert_called_once_with(attributesTable)

        self.assertEqual(self._etlConfigGeneratorManager._spark.sql.call_count, 1)
        
        sqlCommand = self._etlConfigGeneratorManager._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"""DELETE FROM {attributesTable}""", sqlCommand)
        self.assertIn(f"""WHERE MGT_ConfigTable = '{METADATA_ATTRIBUTES_TRANSFORMATION_KEY}'""", sqlCommand)

    def test_generateConfiguration_attributesTableDoesNotExist(self):
        # arrange
        self._etlConfigGeneratorManager._configGeneratorHelper.getADLSPathAndUri = MagicMock(return_value=("testMetadataPath", "testMetadataUri"))
        self._etlConfigGeneratorManager._configGeneratorHelper.moveUnprocessedFilesToArchive = MagicMock()
        self._etlConfigGeneratorManager._configGeneratorHelper.createFwkFlagsTable = MagicMock()
        self._etlConfigGeneratorManager._maintenanceManager.createMaintenanceTable = MagicMock()

        processIngestionConfiguration = True
        processTransformationConfiguration = True
        columnChangeLogs = []
        expOutputGenerated = False

        self._etlConfigGeneratorManager._determineWhetherToProcessIngestionConfiguration = MagicMock(return_value=processIngestionConfiguration)
        self._etlConfigGeneratorManager._determineWhetherToProcessTransformationConfiguration = MagicMock(return_value=processTransformationConfiguration)

        self._etlConfigGeneratorManager._spark.catalog.tableExists.return_value = False

        self._etlConfigGeneratorManager._generateIngestionConfiguration = MagicMock()
        self._etlConfigGeneratorManager._generateModelTransformationsConfiguration = MagicMock(return_value=columnChangeLogs)
        self._etlConfigGeneratorManager._generateTransformationsConfiguration = MagicMock()
        self._etlConfigGeneratorManager._generateExportConfiguration = MagicMock(return_value=expOutputGenerated)

        self._etlConfigGeneratorManager._configGeneratorHelper.setFwkFlag = MagicMock()

        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_CONFIGURATION_FILE = "DQ Configuration.json"
        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE = "DQ Reference values.json"
        self._etlConfigGeneratorManager._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE = "DQ Reference pair values.json"

        self._etlConfigGeneratorManager._validator.saveMetadataConfig = MagicMock(return_value=False)
        self._etlConfigGeneratorManager._validator.createDQLogTable = MagicMock()
        self._etlConfigGeneratorManager._validator.addValidationStatusForTablesInHive = MagicMock()

        self._etlConfigGeneratorManager._createOutput = MagicMock()

        forceConfigGeneration = False

        # act
        output = self._etlConfigGeneratorManager.generateConfiguration(forceConfigGeneration)

        # assert
        self._etlConfigGeneratorManager._spark.catalog.clearCache.assert_called_once()

        attributesTable = f"""{self._etlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}"""

        self._etlConfigGeneratorManager._spark.catalog.tableExists.assert_called_once_with(attributesTable)

        self.assertEqual(self._etlConfigGeneratorManager._spark.sql.call_count, 0)
