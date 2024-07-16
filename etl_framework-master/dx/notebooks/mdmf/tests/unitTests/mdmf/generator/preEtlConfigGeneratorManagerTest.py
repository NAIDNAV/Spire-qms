# Databricks notebook source
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from unittest.mock import MagicMock, ANY

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.generator.preEtlConfigGeneratorManager import PreEtlConfigGeneratorManager
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../generator/preEtlConfigGeneratorManager

# COMMAND ----------

# MAGIC %run ../../../../includes/frameworkConfig

# COMMAND ----------

class PreEtlConfigGeneratorManagerTest(unittest.TestCase):

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

    def setUp(self):
        self._preEtlConfigGeneratorManager = PreEtlConfigGeneratorManager(self._spark, self._compartmentConfig)
        self._preEtlConfigGeneratorManager._spark = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # getFwkTriggerDataFrame tests

    def test_getFwkTriggerDataFrame_invalidConfiguration(self):
        # arrange
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_TRIGGER_DF = self._spark.createDataFrame([
            ["MarketStorageEvent", "MarketStorageEvent", None, "dlzasmefdevxx", "", ".csv", "Stopped"],
        ], "ADFTriggerName STRING, FwkTriggerId STRING, Frequency STRING, StorageAccount STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING")

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Storage Event Triggers .*PathBeginsWith.*"):
            self._preEtlConfigGeneratorManager._getFwkTriggerDataFrame()
    
    def test_getFwkTriggerDataFrame_emptyEnvironment_allTriggersShouldBeProcessedExceptOfInternal(self):
        # arrange
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_TRIGGER_DF = self._spark.createDataFrame([
            ["Sandbox", "Sandbox", None, None, None, None, None],
            ["Manual", "Manual", None, None, None, None, None],
            ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "Day", None, None, None, "Stopped"],
            ["Daily", "Daily", "Day", None, None, None, "Stopped"],
            ["Weekly", "Weekly", "Week", None, None, None, "Stopped"],
            ["MarketStorageEvent", "MarketStorageEvent", None, "dlzasmefdevxx", "eventFolder/eventFile", ".csv", "Stopped"],
        ], "ADFTriggerName STRING, FwkTriggerId STRING, Frequency STRING, StorageAccount STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING")

        self._preEtlConfigGeneratorManager._sqlContext.tableNames = MagicMock(return_value=[])

        expectedFwkTriggerDF = self._spark.createDataFrame([
            ["Sandbox", "Sandbox", None, None, None, None, None, False],
            ["Manual", "Manual", None, None, None, None, None, False],
            ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "Day", None, None, None, "Stopped", True],
            ["Daily", "Daily", "Day", None, None, None, "Stopped", True],
            ["Weekly", "Weekly", "Week", None, None, None, "Stopped", True],
            ["MarketStorageEvent", "MarketStorageEvent", None, "dlzasmefdevxx", "eventFolder/eventFile", ".csv", "Stopped", True],
        ], "ADFTriggerName STRING, FwkTriggerId STRING, Frequency STRING, StorageAccount STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING, ToBeProcessed BOOLEAN")

        # act
        fwkTriggerDF = self._preEtlConfigGeneratorManager._getFwkTriggerDataFrame()

        # assert
        self.assertEqual(
            fwkTriggerDF.sort("ADFTriggerName").rdd.collect(),
            expectedFwkTriggerDF.sort("ADFTriggerName").rdd.collect()
        )

        self._preEtlConfigGeneratorManager._sqlContext.tableNames.assert_called_once_with(self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"])

    def test_getFwkTriggerDataFrame_schemaChanged_allTriggersShouldBeProcessedExceptOfInternal(self):
        # arrange
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_TRIGGER_DF = self._spark.createDataFrame([
            ["Sandbox", "Sandbox", None, None, None, None, None],
            ["Manual", "Manual", None, None, None, None, None],
            ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "Day", None, None, None, "Stopped"],
            ["Daily", "Daily", "Day", None, None, None, "Stopped"],
            ["Weekly", "Weekly", "Week", None, None, None, "Stopped"],
            ["MarketStorageEvent", "MarketStorageEvent", None, "dlzasmefdevxx", "eventFolder/eventFile", ".csv", "Stopped"],
        ], "ADFTriggerName STRING, FwkTriggerId STRING, Frequency STRING, StorageAccount STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING")

        self._preEtlConfigGeneratorManager._sqlContext.tableNames = MagicMock(return_value=["fwktrigger"])

        previousFwkTriggerDF =  self._spark.createDataFrame([
            ["Sandbox", None, None, None, None, None],
            ["Manual", None, None, None, None, None],
            ["MDMF_Execute_PL_TriggerCreation", "Day", None, None, None, "Stopped"],
            ["Daily", "Day", None, None, None, "Stopped"],
            ["Weekly", "Week", None, None, None, "Stopped"],
            ["MarketStorageEvent", None, "dlzasmefdevxx", "eventFolder/eventFile", ".csv", "Stopped"],
        ], "FwkTriggerId STRING, Frequency STRING, StorageAccount STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING")

        self._preEtlConfigGeneratorManager._spark.sql.return_value = previousFwkTriggerDF

        expectedFwkTriggerDF = self._spark.createDataFrame([
            ["Sandbox", "Sandbox", None, None, None, None, None, False],
            ["Manual", "Manual", None, None, None, None, None, False],
            ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "Day", None, None, None, "Stopped", True],
            ["Daily", "Daily", "Day", None, None, None, "Stopped", True],
            ["Weekly", "Weekly", "Week", None, None, None, "Stopped", True],
            ["MarketStorageEvent", "MarketStorageEvent", None, "dlzasmefdevxx", "eventFolder/eventFile", ".csv", "Stopped", True],
        ], "ADFTriggerName STRING, FwkTriggerId STRING, Frequency STRING, StorageAccount STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING, ToBeProcessed BOOLEAN")

        # act
        fwkTriggerDF = self._preEtlConfigGeneratorManager._getFwkTriggerDataFrame()

        # assert
        self.assertEqual(
            fwkTriggerDF.sort("ADFTriggerName").rdd.collect(),
            expectedFwkTriggerDF.sort("ADFTriggerName").rdd.collect()
        )

        self._preEtlConfigGeneratorManager._sqlContext.tableNames.assert_called_once_with(self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"])

        self._preEtlConfigGeneratorManager._spark.sql.assert_called_once_with(f"""SELECT * FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.FwkTrigger""")

    def test_getFwkTriggerDataFrame_fewRecordsChanged_changedTriggersShouldBeProcessedExceptOfInternal(self):
        # arrange
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_TRIGGER_DF = self._spark.createDataFrame([
            ["Sandbox", "Sandbox", None, None, None, None, None],
            ["Manual", "Manual", None, None, None, None, None],
            ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "Day", None, None, None, "Stopped"],
            ["Daily", "Daily", "Day", None, None, None, "Stopped"],
            ["Weekly", "Weekly", "Week", None, None, None, "Stopped"],
            ["MarketStorageEvent", "MarketStorageEvent", None, "dlzasmefdevxx", "eventFolder/eventFile", ".csv", "Stopped"],
        ], "ADFTriggerName STRING, FwkTriggerId STRING, Frequency STRING, StorageAccount STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING")

        self._preEtlConfigGeneratorManager._sqlContext.tableNames = MagicMock(return_value=["fwktrigger"])

        previousFwkTriggerDF =  self._spark.createDataFrame([
            ["Sandbox", "Sandbox_old_value", None, None, None, None, None],
            ["Manual", "Manual", None, None, None, None, None],
            ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "Day", None, None, None, "Stopped"],
            ["Daily", "Daily_old_value", "Day", None, None, None, "Stopped"],
            ["Weekly", "Weekly", "Week", None, None, None, "Stopped"],
            ["MarketStorageEvent", "MarketStorageEvent_old_value", None, "dlzasmefdevxx", "eventFolder/eventFile", ".csv", "Stopped"],
        ], "ADFTriggerName STRING, FwkTriggerId STRING, Frequency STRING, StorageAccount STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING")

        self._preEtlConfigGeneratorManager._spark.sql.return_value = previousFwkTriggerDF

        expectedFwkTriggerDF = self._spark.createDataFrame([
            ["Sandbox", "Sandbox", None, None, None, None, None, False],
            ["Manual", "Manual", None, None, None, None, None, False],
            ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "Day", None, None, None, "Stopped", False],
            ["Daily", "Daily", "Day", None, None, None, "Stopped", True],
            ["Weekly", "Weekly", "Week", None, None, None, "Stopped", False],
            ["MarketStorageEvent", "MarketStorageEvent", None, "dlzasmefdevxx", "eventFolder/eventFile", ".csv", "Stopped", True],
        ], "ADFTriggerName STRING, FwkTriggerId STRING, Frequency STRING, StorageAccount STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING, ToBeProcessed BOOLEAN")

        # act
        fwkTriggerDF = self._preEtlConfigGeneratorManager._getFwkTriggerDataFrame()

        # assert
        self.assertEqual(
            fwkTriggerDF.sort("ADFTriggerName").rdd.collect(),
            expectedFwkTriggerDF.sort("ADFTriggerName").rdd.collect()
        )

        self._preEtlConfigGeneratorManager._sqlContext.tableNames.assert_called_once_with(self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"])

        self._preEtlConfigGeneratorManager._spark.sql.assert_called_once_with(f"""SELECT * FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.FwkTrigger""")

    def test_getFwkTriggerDataFrame_noRecordChanged_noTriggerShouldBeProcessed(self):
        # arrange
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_TRIGGER_DF = self._spark.createDataFrame([
            ["Sandbox", "Sandbox", None, None, None, None, None],
            ["Manual", "Manual", None, None, None, None, None],
            ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "Day", None, None, None, "Stopped"],
            ["Daily", "Daily", "Day", None, None, None, "Stopped"],
            ["Weekly", "Weekly", "Week", None, None, None, "Stopped"],
            ["MarketStorageEvent", "MarketStorageEvent", None, "dlzasmefdevxx", "eventFolder/eventFile", ".csv", "Stopped"],
        ], "ADFTriggerName STRING, FwkTriggerId STRING, Frequency STRING, StorageAccount STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING")

        self._preEtlConfigGeneratorManager._sqlContext.tableNames = MagicMock(return_value=["fwktrigger"])

        previousFwkTriggerDF =  self._spark.createDataFrame([
            ["Sandbox", "Sandbox", None, None, None, None, None],
            ["Manual", "Manual", None, None, None, None, None],
            ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "Day", None, None, None, "Stopped"],
            ["Daily", "Daily", "Day", None, None, None, "Stopped"],
            ["Weekly", "Weekly", "Week", None, None, None, "Stopped"],
            ["MarketStorageEvent", "MarketStorageEvent", None, "dlzasmefdevxx", "eventFolder/eventFile", ".csv", "Stopped"],
        ], "ADFTriggerName STRING, FwkTriggerId STRING, Frequency STRING, StorageAccount STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING")

        self._preEtlConfigGeneratorManager._spark.sql.return_value = previousFwkTriggerDF

        expectedFwkTriggerDF = self._spark.createDataFrame([
            ["Sandbox", "Sandbox", None, None, None, None, None, False],
            ["Manual", "Manual", None, None, None, None, None, False],
            ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "Day", None, None, None, "Stopped", False],
            ["Daily", "Daily", "Day", None, None, None, "Stopped", False],
            ["Weekly", "Weekly", "Week", None, None, None, "Stopped", False],
            ["MarketStorageEvent", "MarketStorageEvent", None, "dlzasmefdevxx", "eventFolder/eventFile", ".csv", "Stopped", False],
        ], "ADFTriggerName STRING, FwkTriggerId STRING, Frequency STRING, StorageAccount STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING, ToBeProcessed BOOLEAN")

        # act
        fwkTriggerDF = self._preEtlConfigGeneratorManager._getFwkTriggerDataFrame()

        # assert
        self.assertEqual(
            fwkTriggerDF.sort("ADFTriggerName").rdd.collect(),
            expectedFwkTriggerDF.sort("ADFTriggerName").rdd.collect()
        )

        self._preEtlConfigGeneratorManager._sqlContext.tableNames.assert_called_once_with(self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"])

        self._preEtlConfigGeneratorManager._spark.sql.assert_called_once_with(f"""SELECT * FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.FwkTrigger""")
    

    # saveFwkTriggerDataFrameAsDeltaTable tests

    def test_saveFwkTriggerDataFrameAsDeltaTable(self):
        # arrange
        metadataDeltaTablePath = "testMetadataUri/Delta"

        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_TRIGGER_DF = MagicMock()
        self._preEtlConfigGeneratorManager._dataLakeHelper = MagicMock()

        # act
        self._preEtlConfigGeneratorManager._saveFwkTriggerDataFrameAsDeltaTable(metadataDeltaTablePath)

        # assert
        self._preEtlConfigGeneratorManager._dataLakeHelper.writeData.assert_called_once_with(
            self._compartmentConfig.FWK_TRIGGER_DF,
            f"{metadataDeltaTablePath}/FwkTrigger",
            WRITE_MODE_OVERWRITE,
            "delta",
            {
                "schemaEvolution": True
            },
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"],
            "FwkTrigger"
        )


    # addAuditColumns tests

    def test_addAuditColumns(self):
        # arrange
        testDF = self._spark.createDataFrame([
            ["Value A1", "Value A2"],
            ["Value B2", "Value B2"],
            ["Value C1", "Value C2"],
        ], "Column1 STRING, Column2 STRING")

        testDate = datetime.now()

        self._preEtlConfigGeneratorManager._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedResultDF = self._spark.createDataFrame([
            ["Value A1", "Value A2", testDate, "test_id"],
            ["Value B2", "Value B2", testDate, "test_id"],
            ["Value C1", "Value C2", testDate, "test_id"],
        ], "Column1 STRING, Column2 STRING, LastUpdate TIMESTAMP, UpdatedBy STRING")
        
        # act
        resultDF = self._preEtlConfigGeneratorManager._addAuditColumns(testDF, testDate)

        # assert
        self.assertEqual(resultDF.rdd.collect(), expectedResultDF.rdd.collect())

    
    # generateMetadataIngestionConfig

    def test_generateMetadataIngestionConfig_noConfigGenerated(self):
        # arrange
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF = self._spark.createDataFrame([
            ["SourceFS", "FileShare"],
            ["LandingADLS", "ADLS"],
        ], "FwkLinkedServiceId STRING, SourceType STRING")

        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"] = [
            {
                "source": {
                    "FwkLinkedServiceId": "SourceFS"
                }
            },
            {
                "source": {
                    "FwkLinkedServiceId": "LandingADLS"
                }
            }
        ]

        # act
        configGenerated = self._preEtlConfigGeneratorManager._generateMetadataIngestionConfig("configurationOutputPath", "metadataDeltaTablePath")

        # assert
        self.assertEqual(configGenerated, False)
        self.assertEqual(self._preEtlConfigGeneratorManager._metadataIngestionConfigGenerator, None)
    
    def test_generateMetadataIngestionConfig_configGenerated(self):
        # arrange
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF = self._spark.createDataFrame([
            ["SourceSQL", "SQL"],
            ["SourceOracle", "Oracle"],
            ["SourceFS", "FileShare"],
            ["LandingADLS", "ADLS"],
        ], "FwkLinkedServiceId STRING, SourceType STRING")

        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["ingestion"] = [
            {
                "source": {
                    "FwkLinkedServiceId": "SourceSQL"
                }
            },
            {
                "source": {
                    "FwkLinkedServiceId": "SourceOracle"
                }
            },
            {
                "source": {
                    "FwkLinkedServiceId": "LandingADLS"
                }
            }
        ]

        self._preEtlConfigGeneratorManager._metadataIngestionConfigGenerator = MagicMock()

        # act
        configGenerated = self._preEtlConfigGeneratorManager._generateMetadataIngestionConfig("configurationOutputPath", "metadataDeltaTablePath")

        # assert
        self.assertEqual(configGenerated, True)

        self.assertEqual(self._preEtlConfigGeneratorManager._metadataIngestionConfigGenerator.generateConfiguration.call_count, 2)

        self._preEtlConfigGeneratorManager._metadataIngestionConfigGenerator.generateConfiguration.assert_any_call(
            {
                "source": {
                    "FwkLinkedServiceId": "SourceSQL"
                }
            },
            "SQL",
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )

        self._preEtlConfigGeneratorManager._metadataIngestionConfigGenerator.generateConfiguration.assert_any_call(
            {
                "source": {
                    "FwkLinkedServiceId": "SourceOracle"
                }
            },
            "Oracle",
            "configurationOutputPath",
            "metadataDeltaTablePath"
        )
    

    # createOutput tests

    def test_createOutput_includingMetadataIngestion(self):
        # arrange
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF = self._spark.createDataFrame([
            ["MetadataADLS", "https://metadata.dfs.core.windows.net/"],
        ], "FwkLinkedServiceId STRING, InstanceURL STRING")

        executeTriggerCreationPipeline = "True"

        executionStart = datetime.now()
        metadataIngestionConfigGenerated = True

        expectedOutput = {
            "Duration": ANY,
            "GeneratedConfigFiles": [
                FWK_LAYER_FILE, 
                FWK_TRIGGER_FILE, 
                FWK_LINKED_SERVICE_FILE,
                FWK_ENTITY_FOLDER,
                INGT_OUTPUT_FOLDER
            ],
            "GeneratedConfigFilesPath": "testMetadataPath/Configuration",
            "GeneratedConfigFilesInstanceURL": "https://metadata.dfs.core.windows.net/",
            "ColumnChangeLogs": "",
            "ExecuteTriggerCreationPipeline": "True"
        }

        # act
        output = self._preEtlConfigGeneratorManager._createOutput(executionStart, "testMetadataPath", metadataIngestionConfigGenerated, executeTriggerCreationPipeline)

        # assert
        del output["Duration"]
        del expectedOutput["Duration"]
        self.assertEqual(output, expectedOutput)

    def test_createOutput_excludingMetadataIngestion(self):
        # arrange
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF = self._spark.createDataFrame([
            ["MetadataADLS", "https://metadata.dfs.core.windows.net/"],
        ], "FwkLinkedServiceId STRING, InstanceURL STRING")

        executeTriggerCreationPipeline = "False"

        executionStart = datetime.now()
        metadataIngestionConfigGenerated = False

        expectedOutput = {
            "Duration": ANY,
            "GeneratedConfigFiles": [
                FWK_LAYER_FILE, 
                FWK_TRIGGER_FILE, 
                FWK_LINKED_SERVICE_FILE
            ],
            "GeneratedConfigFilesPath": "testMetadataPath/Configuration",
            "GeneratedConfigFilesInstanceURL": "https://metadata.dfs.core.windows.net/",
            "ColumnChangeLogs": "",
            "ExecuteTriggerCreationPipeline": "False"
        }

        # act
        output = self._preEtlConfigGeneratorManager._createOutput(executionStart, "testMetadataPath", metadataIngestionConfigGenerated, executeTriggerCreationPipeline)

        # assert
        del output["Duration"]
        del expectedOutput["Duration"]
        self.assertEqual(output, expectedOutput)


    # generateConfiguration tests

    def test_generateConfiguration_toBeProcessed(self):
        # arrange
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_LAYER_DF = MagicMock()
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF = MagicMock()

        self._preEtlConfigGeneratorManager._configGeneratorHelper.getADLSPathAndUri = MagicMock(return_value=("testMetadataPath", "testMetadataUri"))
        self._preEtlConfigGeneratorManager._configGeneratorHelper.moveUnprocessedFilesToArchive = MagicMock()
        self._preEtlConfigGeneratorManager._dataLakeHelper.writeCSV = MagicMock()

        fwkTriggerDF = self._spark.createDataFrame([
            ["MarketStorageEvent",  True],
        ], "ADFTriggerName STRING, ToBeProcessed BOOLEAN")
        self._preEtlConfigGeneratorManager._getFwkTriggerDataFrame = MagicMock(return_value=fwkTriggerDF)
        self._preEtlConfigGeneratorManager._saveFwkTriggerDataFrameAsDeltaTable = MagicMock()

        self._preEtlConfigGeneratorManager._addAuditColumns = MagicMock()
        self._preEtlConfigGeneratorManager._generateMetadataIngestionConfig = MagicMock(return_value=False)
        self._preEtlConfigGeneratorManager._createOutput = MagicMock()

        # act
        output = self._preEtlConfigGeneratorManager.generateConfiguration()

        # assert

        self._preEtlConfigGeneratorManager._spark.catalog.clearCache.assert_called_once()

        self._preEtlConfigGeneratorManager._configGeneratorHelper.getADLSPathAndUri.assert_called_once_with(
            self._preEtlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["metadata"]
        )

        self._preEtlConfigGeneratorManager._configGeneratorHelper.moveUnprocessedFilesToArchive.assert_called_once_with(
            "testMetadataUri/Configuration",
            ANY
        )

        self.assertEqual(self._preEtlConfigGeneratorManager._dataLakeHelper.writeCSV.call_count, 3)

        self._preEtlConfigGeneratorManager._dataLakeHelper.writeCSV.assert_any_call(
            self._preEtlConfigGeneratorManager._compartmentConfig.FWK_LAYER_DF,
            f"testMetadataUri/Configuration/{FWK_LAYER_FILE}",
            WRITE_MODE_OVERWRITE
        )

        self.assertEqual(self._preEtlConfigGeneratorManager._addAuditColumns.call_count, 2)
        
        self._preEtlConfigGeneratorManager._getFwkTriggerDataFrame.assert_called_once()

        self._preEtlConfigGeneratorManager._addAuditColumns.assert_any_call(
            fwkTriggerDF,
            ANY
        )

        self._preEtlConfigGeneratorManager._dataLakeHelper.writeCSV.assert_any_call(
            ANY,
            f"testMetadataUri/Configuration/{FWK_TRIGGER_FILE}",
            WRITE_MODE_OVERWRITE
        )

        self._preEtlConfigGeneratorManager._addAuditColumns.assert_any_call(
            self._preEtlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF,
            ANY
        )

        self._preEtlConfigGeneratorManager._dataLakeHelper.writeCSV.assert_any_call(
            ANY,
            f"testMetadataUri/Configuration/{FWK_LINKED_SERVICE_FILE}",
            WRITE_MODE_OVERWRITE
        )

        self._preEtlConfigGeneratorManager._generateMetadataIngestionConfig.assert_called_once_with(
            "testMetadataUri/Configuration",
            "testMetadataPath/Delta",
        )

        self._preEtlConfigGeneratorManager._saveFwkTriggerDataFrameAsDeltaTable.assert_called_once_with(
            "testMetadataUri/Delta"
        )

        self._preEtlConfigGeneratorManager._createOutput.assert_called_once_with(
            ANY,
            "testMetadataPath",
            self._preEtlConfigGeneratorManager._generateMetadataIngestionConfig.return_value,
            "True"
        )

        self.assertEqual(output, self._preEtlConfigGeneratorManager._createOutput.return_value)

    def test_generateConfiguration_noToBeProcessed(self):
        # arrange
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_LAYER_DF = MagicMock()
        self._preEtlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF = MagicMock()

        self._preEtlConfigGeneratorManager._configGeneratorHelper.getADLSPathAndUri = MagicMock(return_value=("testMetadataPath", "testMetadataUri"))
        self._preEtlConfigGeneratorManager._configGeneratorHelper.moveUnprocessedFilesToArchive = MagicMock()
        self._preEtlConfigGeneratorManager._dataLakeHelper.writeCSV = MagicMock()

        fwkTriggerDF = self._spark.createDataFrame([
            ["MarketStorageEvent",  False],
        ], "ADFTriggerName STRING, ToBeProcessed BOOLEAN")
        self._preEtlConfigGeneratorManager._getFwkTriggerDataFrame = MagicMock(return_value=fwkTriggerDF)
        self._preEtlConfigGeneratorManager._saveFwkTriggerDataFrameAsDeltaTable = MagicMock()

        self._preEtlConfigGeneratorManager._addAuditColumns = MagicMock()
        self._preEtlConfigGeneratorManager._generateMetadataIngestionConfig = MagicMock(return_value=True)
        self._preEtlConfigGeneratorManager._createOutput = MagicMock()

        # act
        output = self._preEtlConfigGeneratorManager.generateConfiguration()

        # assert
        self._preEtlConfigGeneratorManager._spark.catalog.clearCache.assert_called_once()

        self._preEtlConfigGeneratorManager._configGeneratorHelper.getADLSPathAndUri.assert_called_once_with(
            self._preEtlConfigGeneratorManager._compartmentConfig.FWK_METADATA_CONFIG["metadata"]
        )

        self._preEtlConfigGeneratorManager._configGeneratorHelper.moveUnprocessedFilesToArchive.assert_called_once_with(
            "testMetadataUri/Configuration",
            ANY
        )

        self.assertEqual(self._preEtlConfigGeneratorManager._dataLakeHelper.writeCSV.call_count, 3)

        self._preEtlConfigGeneratorManager._dataLakeHelper.writeCSV.assert_any_call(
            self._preEtlConfigGeneratorManager._compartmentConfig.FWK_LAYER_DF,
            f"testMetadataUri/Configuration/{FWK_LAYER_FILE}",
            WRITE_MODE_OVERWRITE
        )

        self.assertEqual(self._preEtlConfigGeneratorManager._addAuditColumns.call_count, 2)
        
        self._preEtlConfigGeneratorManager._getFwkTriggerDataFrame.assert_called_once()

        self._preEtlConfigGeneratorManager._addAuditColumns.assert_any_call(
            fwkTriggerDF,
            ANY
        )

        self._preEtlConfigGeneratorManager._dataLakeHelper.writeCSV.assert_any_call(
            ANY,
            f"testMetadataUri/Configuration/{FWK_TRIGGER_FILE}",
            WRITE_MODE_OVERWRITE
        )

        self._preEtlConfigGeneratorManager._addAuditColumns.assert_any_call(
            self._preEtlConfigGeneratorManager._compartmentConfig.FWK_LINKED_SERVICE_DF,
            ANY
        )

        self._preEtlConfigGeneratorManager._dataLakeHelper.writeCSV.assert_any_call(
            ANY,
            f"testMetadataUri/Configuration/{FWK_LINKED_SERVICE_FILE}",
            WRITE_MODE_OVERWRITE
        )

        self._preEtlConfigGeneratorManager._generateMetadataIngestionConfig.assert_called_once_with(
            "testMetadataUri/Configuration",
            "testMetadataPath/Delta",
        )

        self._preEtlConfigGeneratorManager._saveFwkTriggerDataFrameAsDeltaTable.assert_called_once_with(
            "testMetadataUri/Delta"
        )

        self._preEtlConfigGeneratorManager._createOutput.assert_called_once_with(
            ANY,
            "testMetadataPath",
            self._preEtlConfigGeneratorManager._generateMetadataIngestionConfig.return_value,
            "False"
        )

        self.assertEqual(output, self._preEtlConfigGeneratorManager._createOutput.return_value)

