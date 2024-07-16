# Databricks notebook source
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.generator.configGenerators.metadataIngestionConfigGenerator import MetadataIngestionConfigGenerator
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../../generator/configGenerators/metadataIngestionConfigGenerator

# COMMAND ----------

# MAGIC %run ../../../../../includes/frameworkConfig

# COMMAND ----------

class MetadataIngestionConfigGeneratorTest(unittest.TestCase):

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
        self._metadataIngestionConfigGenerator = MetadataIngestionConfigGenerator(self._spark, self._compartmentConfig)
        self._metadataIngestionConfigGenerator._spark = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # getQuery tests

    def test_getQuery_withinSourceTypes(self):
        # act
        metadataIngestionQuery = self._metadataIngestionConfigGenerator._getQuery("SQL")

        # assert
        self.assertIn("FROM sys.tables", metadataIngestionQuery)
        
        # act
        metadataIngestionQuery = self._metadataIngestionConfigGenerator._getQuery("Oracle")

        # assert
        self.assertIn("FROM all_tab_columns", metadataIngestionQuery)
        
    def test_getQuery_outsideSourceTypes(self):
        # act
        with self.assertRaises(AssertionError) as context:
            self._metadataIngestionConfigGenerator._getQuery('unexpectedSourceType')
        
        # assert
        self.assertTrue("Metadata ingestion query can't be determined for source type 'unexpectedSourceType'" in str(context.exception))


    # generateIngtOutput tests
    
    def test_generateIngtOutput(self):
        # arrange
        self._metadataIngestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="databricks_id")
        self._metadataIngestionConfigGenerator._getQuery = MagicMock(return_value="Test_query")

        self._metadataIngestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        expectedIngtOutputDF = self._spark.createDataFrame([
            [
                "sys.SourceSystemNameMetadata",
                f"""{self._metadataIngestionConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.SourceSystemNameMetadata""",
                "Full",
                None,
                None,
                None,
                None,
                None,
                "Test_query",
                FWK_TRIGGER_ID_DEPLOYMENT,
                None,
                None,
                1,
                "N",
                "Y",
                datetime.now(),
                datetime.now(),
                "databricks_id"
            ]
        ], INGT_OUTPUT_SCHEMA)
        
        # act
        ingtOutputDF = self._metadataIngestionConfigGenerator._generateIngtOutput("SourceSystemName", "SQL")

        # assert
        self.assertEqual(
            ingtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedIngtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect()
        )
        
        self._metadataIngestionConfigGenerator._getQuery.assert_called_once_with("SQL")

        self.assertEqual(self._metadataIngestionConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 1)

    def test_generateIngtOutput_OracleOnMef(self):
        # arrange
        self._metadataIngestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="databricks_id")
        self._metadataIngestionConfigGenerator._getQuery = MagicMock(return_value="Test_query")

        self._metadataIngestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        expectedIngtOutputDF = self._spark.createDataFrame([
            [
                "sys.SourceOracleMetadata",
                f"""{self._metadataIngestionConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.SourceOracleMetadata""",
                "Full",
                None,
                None,
                None,
                None,
                None,
                "Test_query",
                FWK_TRIGGER_ID_DEPLOYMENT,
                None,
                None,
                1,
                "N",
                "N",
                datetime.now(),
                datetime.now(),
                "databricks_id"
            ]
        ], INGT_OUTPUT_SCHEMA)
        
        # act
        ingtOutputDF = self._metadataIngestionConfigGenerator._generateIngtOutput("SourceOracle", "Oracle")

        # assert
        self.assertEqual(
            ingtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedIngtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect()
        )
        
        self._metadataIngestionConfigGenerator._getQuery.assert_called_once_with("Oracle")

        self.assertEqual(self._metadataIngestionConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 1)

    def test_generateIngtOutput_OracleNotOnMef(self):
        # arrange
        compartmentConfig = MagicMock()
        compartmentConfig.FWK_METADATA_CONFIG = {
            "metadata": {
                "FwkLinkedServiceId": "MetadataADLS",
                "containerName": "metadata",
                "databaseName": "Application_Not_MEF_Metadata"
            }
        }

        metadataIngestionConfigGenerator = MetadataIngestionConfigGenerator(self._spark, compartmentConfig)
        metadataIngestionConfigGenerator._spark = MagicMock()

        metadataIngestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="databricks_id")
        metadataIngestionConfigGenerator._getQuery = MagicMock(return_value="Test_query")

        metadataIngestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        expectedIngtOutputDF = self._spark.createDataFrame([
            [
                "sys.SourceOracleMetadata",
                f"""{metadataIngestionConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.SourceOracleMetadata""",
                "Full",
                None,
                None,
                None,
                None,
                None,
                "Test_query",
                FWK_TRIGGER_ID_DEPLOYMENT,
                None,
                None,
                1,
                "N",
                "Y",
                datetime.now(),
                datetime.now(),
                "databricks_id"
            ]
        ], INGT_OUTPUT_SCHEMA)

        # act
        ingtOutputDF = metadataIngestionConfigGenerator._generateIngtOutput("SourceOracle", "Oracle")

        # assert
        self.assertEqual(
            ingtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedIngtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect()
        )
                
        metadataIngestionConfigGenerator._getQuery.assert_called_once_with("Oracle")

        self.assertEqual(metadataIngestionConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 1)


    # generateFwkEntities tests

    def test_generateFwkEntities(self):
        # arrange
        self._metadataIngestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="databricks_id")

        self._metadataIngestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "sys.SourceSystemNameMetadata",
                "SourceSystemName",
                None,
                "Database",
                None,
                None,
                None,
                None,
                datetime.now(),
                "databricks_id"
            ], [
                f"""{self._metadataIngestionConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.SourceSystemNameMetadata""",
                self._metadataIngestionConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["FwkLinkedServiceId"],
                "test_path/SourceSystemNameMetadata",
                "Parquet",
                None,
                None,
                None,
                None,
                datetime.now(),
                "databricks_id"
            ]
        ], FWK_ENTITY_SCHEMA)

        # act
        fwkEntitiesDF = self._metadataIngestionConfigGenerator._generateFwkEntities("SourceSystemName", "test_path")

        # assert
        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").rdd.collect()
        )

        self.assertEqual(self._metadataIngestionConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 2)


    # generateConfiguration tests
    
    def test_generateConfiguration(self):
        # arrange
        self._metadataIngestionConfigGenerator._generateIngtOutput = MagicMock()
        self._metadataIngestionConfigGenerator._generateFwkEntities = MagicMock()
        self._metadataIngestionConfigGenerator._dataLakeHelper.fileExists = MagicMock(return_value=True)
        self._metadataIngestionConfigGenerator._dataLakeHelper.writeDataAsParquet = MagicMock()

        metadataConfig = {
            "source": {"FwkLinkedServiceId": "sourceId"},
            "sink": {"FwkLinkedServiceId": "sinkId"}
        }

        # act
        self._metadataIngestionConfigGenerator.generateConfiguration(
            metadataConfig,
            "testType",
            "testOutputPath",
            "testSinkPath"
        )

        # assert
        self._metadataIngestionConfigGenerator._generateIngtOutput.assert_called_once_with(
            "sourceId",
            "testType"
        )
        self._metadataIngestionConfigGenerator._generateFwkEntities.assert_called_once_with(
            "sourceId",
            "testSinkPath"
        )
        self._metadataIngestionConfigGenerator._dataLakeHelper.writeDataAsParquet.assert_any_call(
            self._metadataIngestionConfigGenerator._generateIngtOutput.return_value,
            f"testOutputPath/{INGT_OUTPUT_FOLDER}/", WRITE_MODE_APPEND
        )
        self._metadataIngestionConfigGenerator._dataLakeHelper.writeDataAsParquet.assert_any_call(
            self._metadataIngestionConfigGenerator._generateFwkEntities.return_value,
            f"testOutputPath/{FWK_ENTITY_FOLDER}/", WRITE_MODE_APPEND
        )

