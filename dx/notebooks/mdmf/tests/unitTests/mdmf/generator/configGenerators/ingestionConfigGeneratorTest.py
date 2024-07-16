# Databricks notebook source
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.generator.configGenerators.ingestionConfigGenerator import IngestionConfigGenerator
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../../generator/configGenerators/ingestionConfigGenerator

# COMMAND ----------

# MAGIC %run ../../../../../includes/frameworkConfig

# COMMAND ----------

class IngestionConfigGeneratorTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

        self._compartmentConfig = MagicMock()
        self._compartmentConfig.FWK_LINKED_SERVICE_DF = self._spark.createDataFrame([
            ["SourceSQL", "SQL", None, None, None, "connectionString", None],
            ["SourceSFTP", "SFTP", "InstanceURL", None, "UserName", "SecretName", None],
            ["LogsADLS", "ADLS", "InstanceURL", None, None, None, None],
        ], "FwkLinkedServiceId STRING, SourceType STRING, InstanceURL STRING, Port STRING, UserName STRING, SecretName STRING, IPAddress STRING")
        self._compartmentConfig.FWK_METADATA_CONFIG = {
            "metadata": {
                "FwkLinkedServiceId": "MetadataADLS",
                "containerName": "metadata",
                "databaseName": "Application_MEF_Metadata"
            }
        }
        self._compartmentConfig.FWK_METADATA_CONFIG = {
            "metadata": {
                "FwkLinkedServiceId": "MetadataADLS",
                "containerName": "metadata",
                "databaseName": "Application_MEF_Metadata"
            }
        }

    def setUp(self):
        self._ingestionConfigGenerator = IngestionConfigGenerator(self._spark, self._compartmentConfig)
        self._ingestionConfigGenerator._spark = MagicMock()
        self._ingestionConfigGenerator._sqlContext = MagicMock()
        self._ingestionConfigGenerator._configGeneratorHelper._DeltaTable = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # getWmkDataType tests

    def test_getWmkDataType_general(self):
        # arrange
        dataTypeDF = self._spark.createDataFrame([["integer"]], "DataType STRING")
        self._ingestionConfigGenerator._getDataType = MagicMock(return_value=dataTypeDF)

        expectedResult = MagicMock()
        self._ingestionConfigGenerator._processDataType = MagicMock(return_value=expectedResult)
        
        # act
        result = self._ingestionConfigGenerator._getWmkDataType("SourceSQLMetadata", "SQL", "entityId", "columnName")
        
        # assert
        self.assertEqual(result, expectedResult)

        self._ingestionConfigGenerator._getDataType.assert_called_once_with("SourceSQLMetadata", "entityId", "columnName")
        self._ingestionConfigGenerator._processDataType.assert_called_once_with(dataTypeDF, "SQL", "columnName", "SourceSQLMetadata", "entityId")

    def test_getWmkDataType_SQL(self):
        # arrange
        sourceType = "integer"
        self._ingestionConfigGenerator._getDataType = MagicMock(return_value=self._spark.createDataFrame([[sourceType]], "DataType STRING"))
        
        # act
        result = self._ingestionConfigGenerator._getWmkDataType("SourceSQLMetadata", "SQL", "entityId", "columnName")
        
        # assert
        self.assertEqual(result, "numeric")

    def test_getWmkDataType_SQLOnPrem(self):
        # arrange
        sourceType = "datetime"
        self._ingestionConfigGenerator._getDataType = MagicMock(return_value=self._spark.createDataFrame([[sourceType]], "DataType STRING"))
        
        # act
        result = self._ingestionConfigGenerator._getWmkDataType("SourceSQLMetadata", "SQLOnPrem", "entityId", "columnName")
        
        # assert
        self.assertEqual(result, "datetime")

    def test_getWmkDataType_Oracle(self):
        # arrange
        sourceType = "timestamp"
        self._ingestionConfigGenerator._getDataType = MagicMock(return_value=self._spark.createDataFrame([[sourceType]], "DataType STRING"))
        
        # act
        result = self._ingestionConfigGenerator._getWmkDataType("SourceSQLMetadata", "Oracle", "entityId", "columnName")
        
        # assert
        self.assertEqual(result, "datetime")

    def test_getWmkDataType_NoDataType(self):
        # arrange
        self._ingestionConfigGenerator._getDataType = MagicMock(return_value=self._spark.createDataFrame([], "DataType STRING"))
        
        # act & assert
        with self.assertRaises(AssertionError):
            self._ingestionConfigGenerator._getWmkDataType("SourceSQLMetadata", "SQL", "entityId", "columnName")

    def test_getWmkDataType_InvalidDataType(self):
        # arrange
        sourceType = "invalid"
        self._ingestionConfigGenerator._getDataType = MagicMock(return_value=self._spark.createDataFrame([[sourceType]], "DataType STRING"))
        
        # act & assert
        with self.assertRaises(AssertionError):
            self._ingestionConfigGenerator._getWmkDataType("SourceSQLMetadata", "SQL", "entityId", "columnName")


    # getDataType tests

    def test_getDataType(self):
        # arrange
        expectedResult = MagicMock()
        self._ingestionConfigGenerator._spark.sql.return_value = expectedResult

        # act
        result = self._ingestionConfigGenerator._getDataType("SourceSQLMetadata", "entityId", "columnName")

        # assert
        self.assertEqual(result, expectedResult)

        self._ingestionConfigGenerator._spark.sql.assert_called_once()

        sqlCommand = self._ingestionConfigGenerator._spark.sql.call_args_list[0].args[0]
        self.assertIn("SELECT DataType", sqlCommand)
        self.assertIn(f"""FROM {self._ingestionConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.SourceSQLMetadata""", sqlCommand)
        self.assertIn(f"""WHERE Entity = 'entityId' AND Attribute = 'columnName'""", sqlCommand)


    # getSinkTableNameFromIngestionConfigurationEntity tests

    def test_getSinkTableNameFromIngestionConfigurationEntity(self):
        # act
        result = self._ingestionConfigGenerator._getSinkTableNameFromIngestionConfigurationEntity("db.schema.table")

        # assert
        self.assertEqual(result, "table")

        # act
        result = self._ingestionConfigGenerator._getSinkTableNameFromIngestionConfigurationEntity("table")

        # assert
        self.assertEqual(result, "table")


    # generateIngtOutput tests

    def test_generateIngtOutput_tables(self):
        # arrange
        tablesDF = self._spark.createDataFrame(
            [
                ["SalesLT.Customer", (None, None, None), (None, None), "Delta", "ModifiedDate", None, "TableHint", "QueryHint", None, None, "SourceSQL", None, None, 1, 1],
                ["SalesLT.Customer", ("CustomerFull", "TriggerIDOverride", None), ("PrimaryColumnFromConfig1, PrimaryColumnFromConfig2", None), "Full", None, "#primaryColumns", None, None, "SELECT * FROM ...", None, "SourceSQL", None, None, 1, 1],
                ["SalesLT.Customer", ("CustomerFull2", "TriggerIDOverride", None), None, "Full", None, "#primaryColumns", None, None, "SELECT * FROM ...", None, "SourceSQL", None, None, 1, 1],
                ["SalesLT.Address", None, (None, "userDefinedType"), "Delta", "ModifiedDate", "Col1, Col2, Col3", None, None, None, None, "SourceSQL", None, None, 1, 1]
            ],
            """
            Entity STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >,
            MetadataParams STRUCT<
                primaryColumns STRING,
                wmkDataType STRING
            >,
            TypeLoad STRING, WmkColumnName STRING, SelectedColumnNames STRING, TableHint STRING, QueryHint STRING, Query STRING, Path STRING, FwkLinkedServiceId STRING, ArchivePath STRING, IsMandatory BOOLEAN, BatchNumber INTEGER, MultipleArchive INTEGER
            """
        )

        primaryColumnsDF = self._spark.createDataFrame([
            ["PrimaryColumnFromMetadata1"],
            ["PrimaryColumnFromMetadata2"]
        ], "Attribute STRING")

        def sqlQuery(command: str):
            if "SELECT *" in command:
                return tablesDF
            elif "SELECT Attribute" in command:
                return primaryColumnsDF

        self._ingestionConfigGenerator._spark.sql.side_effect = sqlQuery
        self._ingestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        self._ingestionConfigGenerator._getWmkDataType = MagicMock(return_value="datetime")

        self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedIngtOutputDF = self._spark.createDataFrame([
            [
                "SalesLT.Customer",
                "LandingSQL.Customer",
                "Delta",
                "ModifiedDate",
                "datetime",
                None,
                "TableHint",
                "QueryHint",
                None,
                "TriggerID",
                None,
                None,
                1,
                "N",
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                "SalesLT.Customer",
                "LandingSQL.CustomerFull",
                "Full",
                None,
                None,
                "PrimaryColumnFromConfig1, PrimaryColumnFromConfig2",
                None,
                None,
                "SELECT * FROM ...",
                "TriggerIDOverride",
                None,
                None,
                1,
                "N",
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                "SalesLT.Customer",
                "LandingSQL.CustomerFull2",
                "Full",
                None,
                None,
                "PrimaryColumnFromMetadata1, PrimaryColumnFromMetadata2",
                None,
                None,
                "SELECT * FROM ...",
                "TriggerIDOverride",
                None,
                None,
                1,
                "N",
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                "SalesLT.Address",
                "LandingSQL.Address",
                "Delta",
                "ModifiedDate",
                "userDefinedType",
                "Col1, Col2, Col3",
                None,
                None,
                None,
                "TriggerID",
                None,
                None,
                1,
                "N",
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
        ], INGT_OUTPUT_SCHEMA)

        # act
        ingtOutputDF = self._ingestionConfigGenerator._generateIngtOutput("metadataIngestionTable", "LandingSQL", "SourceSQL", "TriggerID", "SourceSQLMetadata", None, None)

        # assert
        self.assertEqual(
            ingtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedIngtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect()
        )

        self._ingestionConfigGenerator._getWmkDataType.assert_called_once_with(
            "SourceSQLMetadata",
            "SQL",
            "SalesLT.Customer",
            "ModifiedDate"
        )

        self.assertEqual(self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 4)
    
    def test_generateIngtOutput_files(self):
        # arrange
        tablesDF = self._spark.createDataFrame(
            [
                ["ExcelSheet1", (None, None, "path/archive"), None, "Full", None, None, None, None, None, "path/excel.xlsx", "SourceSFTP", "path/archive", True,  3, 1],
                ["ExcelSheet2", ("ExcelSheet2Override", None, None), None, "Full", None, None, None, None, None, "path/excel.xlsx", "SourceSFTP", None, False, 1, 1],
                ["ExcelSheet3", (None, None, None), None, "Full", None, None, None, None, None, "path/excel.xlsx", "SourceSFTP", None, None, 2, 1],
            ],
            """
            Entity STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >,
            MetadataParams STRUCT<
                primaryColumns STRING,
                wmkDataType STRING
            >,
            TypeLoad STRING, WmkColumnName STRING, SelectedColumnNames STRING, TableHint STRING, QueryHint STRING, Query STRING, Path STRING, FwkLinkedServiceId STRING, ArchivePath STRING, IsMandatory BOOLEAN, BatchNumber INTEGER, MultipleArchive INTEGER
            """
        )

        self._ingestionConfigGenerator._spark.sql.return_value = tablesDF
        self._ingestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        self._ingestionConfigGenerator._getWmkDataType = MagicMock()

        self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedIngtOutputDF = self._spark.createDataFrame([
            [
                "ExcelSheet1",
                "LandingFiles.ExcelSheet1",
                "Full",
                None,
                None,
                None,
                None,
                None,
                None,
                "TriggerID",
                "path/archive",
                None,
                3,
                "Y",
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                "ExcelSheet2",
                "LandingFiles.ExcelSheet2Override",
                "Full",
                None,
                None,
                None,
                None,
                None,
                None,
                "TriggerID",
                None,
                None,
                1,
                "N",
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                "ExcelSheet3",
                "LandingFiles.ExcelSheet3",
                "Full",
                None,
                None,
                None,
                None,
                None,
                None,
                "TriggerID",
                None,
                None,
                2,
                "N",
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
        ], INGT_OUTPUT_SCHEMA)

        # act
        ingtOutputDF = self._ingestionConfigGenerator._generateIngtOutput("metadataIngestionTable", "LandingFiles", "SourceSFTP", "TriggerID", "SourceSFTPMetadata", None, None)

        # assert
        self.assertEqual(
            ingtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedIngtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect()
        )

        self._ingestionConfigGenerator._getWmkDataType.assert_not_called()

        self.assertEqual(self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 3)

    def test_generateIngtOutput_files_multipleArchivePaths(self):
        # arrange
        tablesDF = self._spark.createDataFrame(
            [
                ["ExcelSheet1", (None, None, "path/archive"), None, "Full", None, None, None, None, None, "path/excel.xlsx", "SourceSFTP", "path/archive", None, 3, 3],
                ["ExcelSheet2", (None, None, "path/archive"), None, "Full", None, None, None, None, None, "path/excel.xlsx", "SourceSFTP", None, None,  1, 1],
                ["ExcelSheet3", (None, None, "path/archive"), None, "Full", None, None, None, None, None, "path/excel.xlsx", "SourceSFTP", None, True, 2, 2],
            ],
            """
            Entity STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >,
            MetadataParams STRUCT<
                primaryColumns STRING,
                wmkDataType STRING
            >,
            TypeLoad STRING, WmkColumnName STRING, SelectedColumnNames STRING, TableHint STRING, QueryHint STRING, Query STRING, Path STRING, FwkLinkedServiceId STRING, ArchivePath STRING, IsMandatory BOOLEAN, BatchNumber INTEGER, MultipleArchive INTEGER
            """
        )

        self._ingestionConfigGenerator._spark.sql.return_value = tablesDF

        self._ingestionConfigGenerator._getWmkDataType = MagicMock()

        self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        # act & assert
        with self.assertRaisesRegex(AssertionError, f".* source file 'path/excel.xlsx' is archived more then once.*"):
            self._ingestionConfigGenerator._generateIngtOutput("metadataIngestionTable", "LandingFiles", "SourceSFTP", "TriggerID", "SourceSFTPMetadata", None, None)

        self._ingestionConfigGenerator._getWmkDataType.assert_not_called()

        self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId.assert_not_called()

        def test_generateIngtOutput_files_archivePathandLinkedServiceDefined(self):
            # arrange
            tablesDF = self._spark.createDataFrame(
                [
                    ["ExcelSheet1", (None, None, "path/archive"), None, "Full", None, None, None, None, None, "path/excel.xlsx", "SourceADLS", "path/archive", "SinkSFTP", 3, 3],
                    ["ExcelSheet2", (None, None, "path/archive"), None, "Full", None, None, None, None, None, "path/excel.xlsx", "SourceSFTP", None, None,  1, 1],
                    ["ExcelSheet3", (None, None, "path/archive"), None, "Full", None, None, None, None, None, "path/excel.xlsx", "SourceSFTP", None, True, 2, 2],
                ],
                """
                Entity STRING,
                Params STRUCT<
                    sinkEntityName STRING,
                    fwkTriggerId STRING,
                    archivePath STRING
                >,
                MetadataParams STRUCT<
                    primaryColumns STRING,
                    wmkDataType STRING
                >,
                TypeLoad STRING, WmkColumnName STRING, SelectedColumnNames STRING, TableHint STRING, QueryHint STRING, Query STRING, Path STRING, FwkLinkedServiceId STRING, ArchivePath STRING, IsMandatory BOOLEAN, BatchNumber INTEGER, MultipleArchive INTEGER
                """
            )

            self._ingestionConfigGenerator._spark.sql.return_value = tablesDF

            self._ingestionConfigGenerator._getWmkDataType = MagicMock()

            self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

            expectedIngtOutputDF = self._spark.createDataFrame([
                [
                    "ExcelSheet1",
                    "LandingFiles.ExcelSheet1",
                    "Full",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "TriggerID",
                    "path/archive",
                    "SinkSFTP",
                    3,
                    "Y",
                    "Y",
                    datetime.now(),
                    datetime.now(),
                    "test_id"
                ],
                [
                    "ExcelSheet2",
                    "LandingFiles.ExcelSheet2Override",
                    "Full",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "TriggerID",
                    None,
                    None,
                    1,
                    "N",
                    "Y",
                    datetime.now(),
                    datetime.now(),
                    "test_id"
                ],
                [
                    "ExcelSheet3",
                    "LandingFiles.ExcelSheet3",
                    "Full",
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    "TriggerID",
                    None,
                    None,
                    2,
                    "N",
                    "Y",
                    datetime.now(),
                    datetime.now(),
                    "test_id"
                ],
            ], INGT_OUTPUT_SCHEMA)

            # act
            ingtOutputDF = self._ingestionConfigGenerator._generateIngtOutput("metadataIngestionTable", "LandingFiles", "SourceSFTP", "TriggerID", "SourceSFTPMetadata", None, None)

            # assert
            self.assertEqual(
                ingtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect(),
                expectedIngtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect()
            )

            self._ingestionConfigGenerator._getWmkDataType.assert_not_called()

            self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId.assert_not_called()
        

    # generateFwkEntities tets

    def test_generateFwkEntities_tables(self):
        # arrange
        self._ingestionConfigGenerator._spark.sql.return_value = self._spark.createDataFrame(
            [
                ["SalesLT.Customer", None, None, None],
                ["SalesLT.Customer", None, ("CustomerFull", None, None), None],
            ],
            """
            Entity STRING,
            Path STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >, 
            SourceParams STRING
            """
        )

        self._ingestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "SalesLT.Customer",
                "SourceSQL",
                None,
                "Database",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "LandingSQL.Customer",
                "LinkedServiceId",
                "path/to/data/Customer",
                "Parquet",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "LandingSQL.CustomerFull",
                "LinkedServiceId",
                "path/to/data/CustomerFull",
                "Parquet",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
        ], FWK_ENTITY_SCHEMA)

        # act
        fwkEntitiesDF = self._ingestionConfigGenerator._generateFwkEntities("metadataIngestionTable", "LandingSQL", "LinkedServiceId", None, None, "path/to/data", "SourceSQL") 

        # assert
        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").sort("FwkEntityId").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").sort("FwkEntityId").rdd.collect()
        )
        
        self.assertEqual(self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 4)

    def test_generateFwkEntities_files(self):
        # arrange
        self._ingestionConfigGenerator._spark.sql.return_value = self._spark.createDataFrame(
            [
                ["ExcelSheet1", "path/excel.xlsx", None, '{ "source" : "params" }'],
                ["ExcelSheet2", "path/excel.xlsx", ("ExcelSheet2Override", None, None), '{ "source" : "params" }'],
            ],
            """
            Entity STRING,
            Path STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >, 
            SourceParams STRING
            """
        )
        
        self._ingestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        # sourceParamsJson = { "source" : "params" }

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "ExcelSheet1",
                "SourceSFTP",
                "path/excel.xlsx",
                "EXCEL",
                '{ "source" : "params" }',
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "LandingFiles.ExcelSheet1",
                "LinkedServiceId",
                "path/to/data/ExcelSheet1",
                "Parquet",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "ExcelSheet2",
                "SourceSFTP",
                "path/excel.xlsx",
                "EXCEL",
               '{ "source" : "params" }',
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "LandingFiles.ExcelSheet2Override",
                "LinkedServiceId",
                "path/to/data/ExcelSheet2Override",
                "Parquet",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
        ], FWK_ENTITY_SCHEMA)

        # act
        fwkEntitiesDF = self._ingestionConfigGenerator._generateFwkEntities("metadataIngestionTable", "LandingFiles", "LinkedServiceId", None, None, "path/to/data", "SourceSFTP")

        # assert
        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").sort("FwkEntityId").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").sort("FwkEntityId").rdd.collect()
        )
        
        self.assertEqual(self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 4)

    def test_generateFwkEntities_filesForExistingLogFwkLinkedServiceIdAndLogPath(self):
        # arrange
        self._ingestionConfigGenerator._spark.sql.return_value = self._spark.createDataFrame(
            [
                ["ExcelSheet2", "path/excel.xlsx", ("ExcelSheet2Override", None, None), '{"enableSkipIncompatibleRow": "true"}'],
            ],
            """
            Entity STRING,
            Path STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >, 
            SourceParams STRING
            """
        )

        self._ingestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "ExcelSheet2",
                "SourceSFTP",
                "path/excel.xlsx",
                "EXCEL",
                '{"enableSkipIncompatibleRow": "true", "logSettingsInstanceURL": "InstanceURL", "logSettingsPath": "containerName/logPath/"}',
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "LandingFiles.ExcelSheet2Override",
                "LinkedServiceId",
                "path/to/data/ExcelSheet2Override",
                "Parquet",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
        ], FWK_ENTITY_SCHEMA)

        # act
        fwkEntitiesDF = self._ingestionConfigGenerator._generateFwkEntities("metadataIngestionTable", "LandingFiles", "LinkedServiceId", "SourceSFTP", "containerName/logPath/", "path/to/data", "SourceSFTP")    

        # assert
        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").sort("FwkEntityId").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").sort("FwkEntityId").rdd.collect()
        )
        
        self.assertEqual(self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 2)
    
    def test_generateFwkEntities_filesForExistingLogFwkLinkedServiceIdAndLogPathFromIngestionConfig(self):
        # arrange
        self._ingestionConfigGenerator._spark.sql.return_value = self._spark.createDataFrame(
            [
                ["ExcelSheet2", "path/excel.xlsx", ("ExcelSheet2Override", None, None), '{"enableSkipIncompatibleRow": "true", "logFwkLinkedServiceId": "LogsADLS", "logPath": "containerName/logPathDefinedInIngestionConfig/"}'],
            ],
            """
            Entity STRING,
            Path STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >, 
            SourceParams STRING
            """
        )

        self._ingestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "ExcelSheet2",
                "SourceSFTP",
                "path/excel.xlsx",
                "EXCEL",
                '{"enableSkipIncompatibleRow": "true", "logFwkLinkedServiceId": "LogsADLS", "logPath": "containerName/logPathDefinedInIngestionConfig/", "logSettingsInstanceURL": "InstanceURL", "logSettingsPath": "containerName/logPathDefinedInIngestionConfig/"}',
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "LandingFiles.ExcelSheet2Override",
                "LinkedServiceId",
                "path/to/data/ExcelSheet2Override",
                "Parquet",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
        ], FWK_ENTITY_SCHEMA)

        # act
        fwkEntitiesDF = self._ingestionConfigGenerator._generateFwkEntities("metadataIngestionTable", "LandingFiles", "LinkedServiceId", "SourceSFTP", "containerName/logPath/", "path/to/data", "SourceSFTP")    

        # assert
        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").sort("FwkEntityId").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").sort("FwkEntityId").rdd.collect()
        )
        
        self.assertEqual(self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 2)

    def test_generateFwkEntities_filesForIncorrectLogFwkLinkedServiceId(self):
        # arrange
        self._ingestionConfigGenerator._spark.sql.return_value = self._spark.createDataFrame(
            [
                ["ExcelSheet2", "path/excel.xlsx", ("ExcelSheet2Override", None, None), '{"enableSkipIncompatibleRow": true}'],
            ],
            """
            Entity STRING,
            Path STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >, 
            SourceParams STRING
            """
        )

        self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Defined property 'logFwkLinkedServiceId': 'IncorrectSourceSFTP' in 'CONFIG' is not listed in 'FWK_LINKED_SERVICE_DF'."):
            self._ingestionConfigGenerator._generateFwkEntities("metadataIngestionTable", "LandingFiles", "LinkedServiceId", "IncorrectSourceSFTP", "containerName/logPath/", "path/to/data", "SourceSFTP")    

    def test_generateFwkEntities_filesForMissingLogPath(self):
        # arrange
        self._ingestionConfigGenerator._spark.sql.return_value = self._spark.createDataFrame(
            [
                ["ExcelSheet2", "path/excel.xlsx", ("ExcelSheet2Override", None, None), '{"enableSkipIncompatibleRow": true}'],
            ],
            """
            Entity STRING,
            Path STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >, 
            SourceParams STRING
            """
        )

        self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Please defined both properties 'logPath' and 'logFwkLinkedServiceId' in 'CONFIG'."):
            self._ingestionConfigGenerator._generateFwkEntities("metadataIngestionTable", "LandingFiles", "LinkedServiceId", "SourceSFTP", None, "path/to/data", "SourceSFTP")

    def test_generateFwkEntities_filesForMissingLogFwkLinkedServiceId(self):
        # arrange
        self._ingestionConfigGenerator._spark.sql.return_value = self._spark.createDataFrame(
            [
                ["ExcelSheet2", "path/excel.xlsx", ("ExcelSheet2Override", None, None), '{"enableSkipIncompatibleRow": true}'],
            ],
            """
            Entity STRING,
            Path STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >, 
            SourceParams STRING
            """
        )

        self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Please defined both properties 'logPath' and 'logFwkLinkedServiceId' in 'CONFIG'."):
            self._ingestionConfigGenerator._generateFwkEntities("metadataIngestionTable", "LandingFiles", "LinkedServiceId", None, "containerName/logPath/", "path/to/data", "SourceSFTP")

    def test_generateFwkEntities_filesForEnableSkipIncompatibleRowAndMissingLogFwkLinkedServiceIdAndMissingLogPath(self):
        # arrange
        self._ingestionConfigGenerator._spark.sql.return_value = self._spark.createDataFrame(
            [
                ["ExcelSheet2", "path/excel.xlsx", ("ExcelSheet2Override", None, None), '{"enableSkipIncompatibleRow": true}'],
            ],
            """
            Entity STRING,
            Path STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >, 
            SourceParams STRING
            """
        )

        self._ingestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "ExcelSheet2",
                "SourceSFTP",
                "path/excel.xlsx",
                "EXCEL",
                '{"enableSkipIncompatibleRow": true}',
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "LandingFiles.ExcelSheet2Override",
                "LinkedServiceId",
                "path/to/data/ExcelSheet2Override",
                "Parquet",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
        ], FWK_ENTITY_SCHEMA)
        
        # act
        fwkEntitiesDF = self._ingestionConfigGenerator._generateFwkEntities("metadataIngestionTable", "LandingFiles", "LinkedServiceId", None, None, "path/to/data", "SourceSFTP")    

        # assert
        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").sort("FwkEntityId").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").sort("FwkEntityId").rdd.collect()
        )
        
        self.assertEqual(self._ingestionConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 2)


    # generateAttributes tests
    
    def test_generateAttributes_tables(self):
        # arrange
        self._ingestionConfigGenerator._sqlContext.tableNames.return_value = ["SourceSQLMetadata".lower()]

        self._ingestionConfigGenerator._spark.sql.return_value = self._spark.createDataFrame(
            [
                ["Customer", "CustomerID", True, None, None, None],
                ["Customer", "CustomerID", True, "#primaryColumns", ("CustomerOverride", None, None), ("CustomerID, SystemID", None)],
                ["Address", "AddressID", True, None, None, ("AddressID, SystemID", None)],
                ["SalesOrderHeader", "OrderID", True, None, None, ("OrderID", None)],
                ["SalesOrderHeader", "Market", True, None, None, ("OrderID", None)],
                ["TableWithoutPrimaryKeys", None, None, None, None, None],
            ],
            """
            EntityName STRING, Attribute STRING, IsPrimaryKey BOOLEAN, SelectedColumnNames STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >,
            MetadataParams STRUCT<
                primaryColumns STRING,
                wmkDataType STRING
            >
            """
        )

        expectedAttributesDF = self._spark.createDataFrame([
            ["LandingSQL", "Customer", "CustomerID", True, False, METADATA_ATTRIBUTES_INGESTION_KEY],
            ["LandingSQL", "CustomerOverride", "CustomerID", True, False, METADATA_ATTRIBUTES_INGESTION_KEY],
            ["LandingSQL", "CustomerOverride", "SystemID", True, False, METADATA_ATTRIBUTES_INGESTION_KEY],
            ["LandingSQL", "Address", "AddressID", True, False, METADATA_ATTRIBUTES_INGESTION_KEY],
            ["LandingSQL", "Address", "SystemID", True, False, METADATA_ATTRIBUTES_INGESTION_KEY],
            ["LandingSQL", "SalesOrderHeader", "OrderID", True, False, METADATA_ATTRIBUTES_INGESTION_KEY],
            ["LandingSQL", "TableWithoutPrimaryKeys", None, None, None, METADATA_ATTRIBUTES_INGESTION_KEY],
        ], METADATA_ATTRIBUTES_SCHEMA)

        self._ingestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        # act
        attributesDF = self._ingestionConfigGenerator._generateAttributes("metadataIngestionTable", "LandingSQL", "SourceSQL", "SourceSQLMetadata")

        # assert
        self.assertEqual(
            attributesDF.sort("DatabaseName", "EntityName", "Attribute").rdd.collect(),
            expectedAttributesDF.sort("DatabaseName", "EntityName", "Attribute").rdd.collect()
        )

        self._ingestionConfigGenerator._spark.sql.assert_called_once()
        sqlCommand = self._ingestionConfigGenerator._spark.sql.call_args_list[0].args[0]
        self.assertIn("LEFT JOIN (SELECT * FROM Application_MEF_Metadata.SourceSQLMetadata WHERE cast(IsPrimaryKey as boolean))", sqlCommand)

    def test_generateAttributes_files(self):
        # arrange
        self._ingestionConfigGenerator._sqlContext.tableNames.return_value = []

        self._ingestionConfigGenerator._spark.sql.return_value = self._spark.createDataFrame(
            [
                ["ExcelSheet1", None, None, None, None, None],
                ["ExcelSheet2", None, None, None, ("ExcelSheet2Override", None, None), ("Col1, Col2", None)],
            ],
            """
            EntityName STRING, Attribute STRING, IsPrimaryKey BOOLEAN, SelectedColumnNames STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING
            >,
            MetadataParams STRUCT<
                primaryColumns STRING,
                wmkDataType STRING
            >
            """
        )

        expectedAttributesDF = self._spark.createDataFrame([
            ["LandingSFTP", "ExcelSheet1", None, None, None, METADATA_ATTRIBUTES_INGESTION_KEY],
            ["LandingSFTP", "ExcelSheet2Override", "Col1", True, False, METADATA_ATTRIBUTES_INGESTION_KEY],
            ["LandingSFTP", "ExcelSheet2Override", "Col2", True, False, METADATA_ATTRIBUTES_INGESTION_KEY],
        ], METADATA_ATTRIBUTES_SCHEMA)

        self._ingestionConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        # act
        attributesDF = self._ingestionConfigGenerator._generateAttributes("metadataIngestionTable", "LandingSFTP", "SourceSFTP", "SourceSFTPMetadata")

        # assert
        self.assertEqual(
            attributesDF.sort("DatabaseName", "EntityName", "Attribute").rdd.collect(),
            expectedAttributesDF.sort("DatabaseName", "EntityName", "Attribute").rdd.collect()
        )

        self._ingestionConfigGenerator._spark.sql.assert_called_once()
        sqlCommand = self._ingestionConfigGenerator._spark.sql.call_args_list[0].args[0]
        self.assertNotIn("LEFT JOIN Application_MEF_Metadata.SourceSFTPMetadata", sqlCommand)


    # generateConfiguration tests

    def test_generateConfiguration_registerSourceSystemMetadata(self):
        # arrange
        self._ingestionConfigGenerator._configGeneratorHelper.getADLSPathAndUri = MagicMock(
            return_value=("testSinkPath", "testSinkUri")
        )
        self._ingestionConfigGenerator._dataLakeHelper.fileExists = MagicMock(return_value=True)
        self._ingestionConfigGenerator._dataLakeHelper.writeDataAsParquet = MagicMock()

        ingestionDF = self._spark.createDataFrame([["ingtOutput"],], "Test STRING")
        self._ingestionConfigGenerator._generateIngtOutput = MagicMock(return_value=ingestionDF)

        fwkEntitiesDF = self._spark.createDataFrame([["entities"],], "Test STRING")
        self._ingestionConfigGenerator._generateFwkEntities = MagicMock(return_value=fwkEntitiesDF)

        attributesDF = MagicMock()
        self._ingestionConfigGenerator._generateAttributes = MagicMock(return_value=attributesDF)

        metadataConfig = {
            "FwkTriggerId": "triggerId",
            "source": {
                "FwkLinkedServiceId": "sourceServiceId"
            },
            "sink": {
                "databaseName": "sinkDatabase",
                "FwkLinkedServiceId": "sinkServiceId"
            },
            "metadata": {
                "ingestionFile": "Ingestion_Configuration.json",
                "ingestionTable": "IngestionConfiguration"
            }
        }
        
        # act
        resultDF = self._ingestionConfigGenerator.generateConfiguration(metadataConfig, "output/path", "delta/table/path")
        
        # assert
        self.assertEqual(resultDF, attributesDF)

        self._ingestionConfigGenerator._configGeneratorHelper.getADLSPathAndUri.assert_called_once_with(metadataConfig["sink"])

        self._ingestionConfigGenerator._dataLakeHelper.fileExists.assert_called_once_with("delta/table/path/sourceServiceIdMetadata")

        self._ingestionConfigGenerator._spark.sql.assert_called_once()
        sqlCommand = self._ingestionConfigGenerator._spark.sql.call_args_list[0].args[0]
        self.assertIn("CREATE TABLE IF NOT EXISTS", sqlCommand)
        self.assertIn(f"""{self._ingestionConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.sourceServiceIdMetadata""", sqlCommand)
        self.assertIn("USING PARQUET", sqlCommand)
        self.assertIn("LOCATION 'delta/table/path/sourceServiceIdMetadata'", sqlCommand)

        self._ingestionConfigGenerator._generateIngtOutput.assert_called_with(
            "IngestionConfiguration", "sinkDatabase", "sourceServiceId", "triggerId", "sourceServiceIdMetadata", None, None
        )

        self._ingestionConfigGenerator._generateFwkEntities.assert_called_with(
            "IngestionConfiguration", "sinkDatabase", "sinkServiceId",  None, None, "testSinkPath", "sourceServiceId"
        )

        self.assertEqual(self._ingestionConfigGenerator._dataLakeHelper.writeDataAsParquet.call_count, 2) 

        self._ingestionConfigGenerator._dataLakeHelper.writeDataAsParquet.assert_has_calls([
            [(ingestionDF, f"output/path/{INGT_OUTPUT_FOLDER}/", WRITE_MODE_APPEND),],
            [(fwkEntitiesDF, f"output/path/{FWK_ENTITY_FOLDER}/", WRITE_MODE_APPEND),]
        ])

        self._ingestionConfigGenerator._generateAttributes.assert_called_with(
            "IngestionConfiguration", "sinkDatabase", "sourceServiceId", "sourceServiceIdMetadata"
        )

    def test_generateConfiguration_withoutSourceSystemMetadata(self):
        # arrange
        self._ingestionConfigGenerator._configGeneratorHelper.getADLSPathAndUri = MagicMock(
            return_value=("testSinkPath", "testSinkUri")
        )
        self._ingestionConfigGenerator._dataLakeHelper.fileExists = MagicMock(return_value=False)
        self._ingestionConfigGenerator._dataLakeHelper.writeDataAsParquet = MagicMock()

        ingestionDF = self._spark.createDataFrame([["ingtOutput"],], "Test STRING")
        self._ingestionConfigGenerator._generateIngtOutput = MagicMock(return_value=ingestionDF)

        fwkEntitiesDF = self._spark.createDataFrame([["entities"],], "Test STRING")
        self._ingestionConfigGenerator._generateFwkEntities = MagicMock(return_value=fwkEntitiesDF)

        attributesDF = MagicMock()
        self._ingestionConfigGenerator._generateAttributes = MagicMock(return_value=attributesDF)

        metadataConfig = {
            "FwkTriggerId": "triggerId",
            "source": {
                "FwkLinkedServiceId": "sourceServiceId"
            },
            "sink": {
                "databaseName": "sinkDatabase",
                "FwkLinkedServiceId": "sinkServiceId"
            },
            "metadata": {
                "ingestionFile": "Ingestion_Configuration.json",
                "ingestionTable": "IngestionConfiguration"
            }
        }
        
        # act
        resultDF = self._ingestionConfigGenerator.generateConfiguration(metadataConfig, "output/path", "delta/table/path")
        
        # assert
        self.assertEqual(resultDF, attributesDF)

        self._ingestionConfigGenerator._configGeneratorHelper.getADLSPathAndUri.assert_called_once_with(metadataConfig["sink"])

        self._ingestionConfigGenerator._dataLakeHelper.fileExists.assert_called_once_with("delta/table/path/sourceServiceIdMetadata")

        self._ingestionConfigGenerator._spark.sql.assert_not_called()

        self._ingestionConfigGenerator._generateIngtOutput.assert_called_with(
            "IngestionConfiguration", "sinkDatabase", "sourceServiceId", "triggerId", "sourceServiceIdMetadata", None, None
        )

        self._ingestionConfigGenerator._generateFwkEntities.assert_called_with(
            "IngestionConfiguration", "sinkDatabase", "sinkServiceId",  None, None, "testSinkPath", "sourceServiceId"
        )

        self.assertEqual(self._ingestionConfigGenerator._dataLakeHelper.writeDataAsParquet.call_count, 2) 

        self._ingestionConfigGenerator._dataLakeHelper.writeDataAsParquet.assert_has_calls([
            [(ingestionDF, f"output/path/{INGT_OUTPUT_FOLDER}/", WRITE_MODE_APPEND),],
            [(fwkEntitiesDF, f"output/path/{FWK_ENTITY_FOLDER}/", WRITE_MODE_APPEND),]
        ])

        self._ingestionConfigGenerator._generateAttributes.assert_called_with(
            "IngestionConfiguration", "sinkDatabase", "sourceServiceId", "sourceServiceIdMetadata"
        )
