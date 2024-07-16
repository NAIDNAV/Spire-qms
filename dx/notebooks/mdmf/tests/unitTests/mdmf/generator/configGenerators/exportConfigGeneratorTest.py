# Databricks notebook source
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.generator.configGenerators.exportConfigGenerator import ExportConfigGenerator
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../../generator/configGenerators/exportConfigGenerator

# COMMAND ----------

# MAGIC %run ../../../../../includes/frameworkConfig

# COMMAND ----------

class ExportConfigGeneratorTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()
        self._compartmentConfig = MagicMock()
        self._compartmentConfig.FWK_LINKED_SERVICE_DF = self._spark.createDataFrame([
            ["MyADLSLinkedService", "ADLS", "https://dstgcpbit.dfs.core.windows.net/", None, None, None, None],
            ["MyExportSFTP", "SFTP", "53.44.123.9", None, "sftp_user_name", "sftp_pass", None],
            ["MyExportSQL", "SQL", None, None, None, "sql-client-connectionString", None],
        ], "FwkLinkedServiceId STRING, SourceType STRING, InstanceURL STRING, Port STRING, UserName STRING, SecretName STRING, IPAddress STRING")
        self._compartmentConfig.FWK_METADATA_CONFIG = {
            "metadata": {
                "databaseName": "Application_MEF_Metadata"
            }
        }
        
    def setUp(self):
        self._exportConfigGenerator = ExportConfigGenerator(self._spark, self._compartmentConfig)
        self._exportConfigGenerator._spark = MagicMock()
        self._exportConfigGenerator._getDataType = MagicMock()
        self._exportConfigGenerator._configGeneratorHelper._DeltaTable = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # getEntityName tests

    def test_getEntityName_returnsNameWithoutSlashes(self):
        # arrange
        exportPath = "/path/to/myEntity.csv"
        expected_result = "myEntity"

        # act
        result = self._exportConfigGenerator._getEntityName(exportPath)

        # assert
        self.assertEqual(result, expected_result)

    def test_getEntityName_returnsNameWithoutExtension(self):
        # arrange
        exportPath = "myEntity.csv"
        expected_result = "myEntity"

        # act
        result = self._exportConfigGenerator._getEntityName(exportPath)

        # assert
        self.assertEqual(result, expected_result)

    def test_getEntityName_returnsNameWhenNoExtension(self):
        # arrange
        exportPath = "/path/to/myEntity"
        expected_result = "myEntity"

        # act
        result = self._exportConfigGenerator._getEntityName(exportPath)

        # assert
        self.assertEqual(result, expected_result)


    # getDtExportPathAndName tests

    def test_getDtExportPathAndName_exportAsCSV(self):
        # arrange
        table = { "Path": "/export/files/myEntity.csv", "Entity": None }
        expectedResult = ("/export/files/myEntity.csv", "myEntity")

        # act
        result = self._exportConfigGenerator._getDtExportPathAndName(table)

        # assert
        self.assertEqual(result, expectedResult)

    def test_getDtExportPathAndName_exportToDatabase(self):
        # arrange
        table = { "Path": None, "Entity": "SalesLT.Customer" }
        expectedResult = ("SalesLT.Customer.csv", "SalesLT.Customer")

        # act
        result = self._exportConfigGenerator._getDtExportPathAndName(table)

        # assert
        self.assertEqual(result, expectedResult)


    # generateDtOutput tests

    def test_generateDtOutput_exportAsCSV(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            ["SELECT * FROM Application_MEF_History.Customer", "History_Customer.csv", '{ "header": true, "nullValue": null }', None, None, None, None],
            ["SELECT * FROM Application_MEF_History.CustomerAddress", "History_CustomerAddress.csv", '{ "header": false, "nullValue": null }', None, None, None, None]
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._exportConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedDtOutputDF = self._spark.createDataFrame([
            [
                None,
                "CompartmentADLS.History_Customer",
                '{ "keyColumns": [], "partitionColumns": [], "functions": [{"transformation": "sqlQuery", "params": "SELECT * FROM Application_MEF_History.Customer"}] }',
                WRITE_MODE_OVERWRITE,
                "fwkTriggerId",
                "fwkLayerId",
                1,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                None,
                "CompartmentADLS.History_CustomerAddress",
                '{ "keyColumns": [], "partitionColumns": [], "functions": [{"transformation": "sqlQuery", "params": "SELECT * FROM Application_MEF_History.CustomerAddress"}] }',
                WRITE_MODE_OVERWRITE,
                "fwkTriggerId",
                "fwkLayerId",
                1,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ]
        ], DT_OUTPUT_SCHEMA)

        # act
        dtOutputDF = self._exportConfigGenerator._generateDtOutput("metadataExportTable", "CompartmentADLS", "ADLS", "fwkLayerId", "fwkTriggerId")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")
        
        self.assertEqual(
            dtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedDtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect()
        )

    def test_generateDtOutput_exportToDatabase(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            ["SELECT * FROM Application_MEF_History.Customer", None, None, None, None, "SalesLT.CustomerEXP", "TRUNCATE TABLE SalesLT.CustomerEXP"],
            ["SELECT * FROM Application_MEF_History.CustomerAddress", None,  None, None, None, "SalesLT.CustomerAddressEXP", None]
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._exportConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedDtOutputDF = self._spark.createDataFrame([
            [
                None,
                "CompartmentADLS.SalesLT.CustomerEXP",
                '{ "keyColumns": [], "partitionColumns": [], "functions": [{"transformation": "sqlQuery", "params": "SELECT * FROM Application_MEF_History.Customer"}] }',
                WRITE_MODE_OVERWRITE,
                "fwkTriggerId",
                "fwkLayerId",
                1,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                None,
                "CompartmentADLS.SalesLT.CustomerAddressEXP",
                '{ "keyColumns": [], "partitionColumns": [], "functions": [{"transformation": "sqlQuery", "params": "SELECT * FROM Application_MEF_History.CustomerAddress"}] }',
                WRITE_MODE_OVERWRITE,
                "fwkTriggerId",
                "fwkLayerId",
                1,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ]
        ], DT_OUTPUT_SCHEMA)

        # act
        dtOutputDF = self._exportConfigGenerator._generateDtOutput("metadataExportTable", "CompartmentADLS", "SQL", "fwkLayerId", "fwkTriggerId")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")
        
        self.assertEqual(
            dtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedDtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect()
        )

    def test_generateDtOutput_exportExistingCSVToADLS(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            [None, "Export_History_Customer.csv", None, "SourceADLS", "dldata/Files/Customer.csv", None, None],
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._exportConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedDtOutputDF = self._spark.createDataFrame([
            [
                "SourceADLS.Export_History_Customer",
                "CompartmentADLS.Export_History_Customer",
                '{ "keyColumns": [], "partitionColumns": [], "functions": [] }',
                WRITE_MODE_OVERWRITE,
                "fwkTriggerId",
                "fwkLayerId",
                1,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ]
        ], DT_OUTPUT_SCHEMA)

        # act
        dtOutputDF = self._exportConfigGenerator._generateDtOutput("metadataExportTable", "CompartmentADLS", "ADLS", "fwkLayerId", "fwkTriggerId")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")
        
        self.assertEqual(
            dtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedDtOutputDF.drop("InsertTime", "LastUpdate").rdd.collect()
        )
    
    def test_generateDtOutput_exportExistingCSVToSFTP(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            [None, "Export_History_Customer.csv", None, "SourceADLS", "dldata/Files/Customer.csv", None, None],
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        # act
        dtOutputDF = self._exportConfigGenerator._generateDtOutput("metadataExportTable", "CompartmentADLS", "SFTP", "fwkLayerId", "fwkTriggerId")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")
        
        self.assertEqual(dtOutputDF.count(), 0)

    def test_generateDtOutput_exportExistingCSVToSQL(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            [None, "Export_History_Customer.csv", None, "SourceADLS", "dldata/Files/Customer.csv", None, None],
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)

        # act & assert
        with self.assertRaisesRegex(AssertionError, f"Export of existing CSV file is not supported if sink type is 'SQL'"):
            self._exportConfigGenerator._generateDtOutput("metadataExportTable", "CompartmentADLS", "SQL", "fwkLayerId", "fwkTriggerId")


    # generateFwkEntities tests

    def test_generateFwkEntities_exportAsCSV(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            ["SELECT * FROM Application_MEF_History.Customer", "History_Customer.csv", '{ "header": true, "nullValue": null }', None, None, None, None],
            ["SELECT * FROM Application_MEF_History.CustomerAddress", "History_CustomerAddress.csv", '{ "header": false, "nullValue": null }', None, None, None, None]
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._exportConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "CompartmentADLS.History_Customer",
                "CompartmentADLS",
                "dldata/TempExportSFTP/History_Customer.csv",
                "CSV",
                '{ "header": true, "nullValue": null }',
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "CompartmentADLS.History_CustomerAddress",
                "CompartmentADLS",
                "dldata/TempExportSFTP/History_CustomerAddress.csv",
                "CSV",
                '{ "header": false, "nullValue": null }',
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ]
        ], FWK_ENTITY_SCHEMA)

        # act
        fwkEntitiesDF = self._exportConfigGenerator._generateFwkEntities("metadataExportTable", "CompartmentADLS", "ADLS", "dldata/TempExportSFTP")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")
        
        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").rdd.collect()
        )

    def test_generateFwkEntities_exportToDatabase(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            ["SELECT * FROM Application_MEF_History.Customer", None, None, None, None, "SalesLT.CustomerEXP", "TRUNCATE TABLE SalesLT.CustomerEXP"],
            ["SELECT * FROM Application_MEF_History.CustomerAddress", None,  None, None, None, "SalesLT.CustomerAddressEXP", None]
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._exportConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "CompartmentADLS.SalesLT.CustomerEXP",
                "CompartmentADLS",
                "dldata/TempExportSQL/SalesLT.CustomerEXP.csv",
                "CSV",
                '{"header":true,"nullValue":null}',
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "CompartmentADLS.SalesLT.CustomerAddressEXP",
                "CompartmentADLS",
                "dldata/TempExportSQL/SalesLT.CustomerAddressEXP.csv",
                "CSV",
                '{"header":true,"nullValue":null}',
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ]
        ], FWK_ENTITY_SCHEMA)

        # act
        fwkEntitiesDF = self._exportConfigGenerator._generateFwkEntities("metadataExportTable", "CompartmentADLS", "SQL", "dldata/TempExportSQL")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")
        
        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").rdd.collect()
        )
    
    def test_generateFwkEntities_exportExistingCSVToADLS(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            [None, "Export_History_Customer.csv", None, "SourceADLS", "dldata/Files/Customer.csv", None, None],
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._exportConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "SourceADLS.Export_History_Customer",
                "SourceADLS",
                "dldata/Files/Customer.csv",
                "CSV",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "CompartmentADLS.Export_History_Customer",
                "CompartmentADLS",
                "dldata/TempExportSFTP/Export_History_Customer.csv",
                "CSV",
                '{"header":true,"nullValue":null}',
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ]
        ], FWK_ENTITY_SCHEMA)

        # act
        fwkEntitiesDF = self._exportConfigGenerator._generateFwkEntities("metadataExportTable", "CompartmentADLS", "ADLS", "dldata/TempExportSFTP")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")
        
        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").rdd.collect()
        )
    
    def test_generateFwkEntities_exportExistingCSVToSFTP(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            [None, "Export_History_Customer.csv", None, "SourceADLS", "dldata/Files/Customer.csv", None, None],
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        # act
        fwkEntitiesDF = self._exportConfigGenerator._generateFwkEntities("metadataExportTable", "CompartmentADLS", "SFTP", "dldata/TempExportSFTP")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")
        
        self.assertEqual(fwkEntitiesDF.count(), 0)

    def test_generateFwkEntities_exportExistingCSVToSQL(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            [None, "Export_History_Customer.csv", None, "SourceADLS", "dldata/Files/Customer.csv", None, None],
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)

        # act & assert
        with self.assertRaisesRegex(Exception, f"An error occurred while processing the table 'CompartmentADLS.Export_History_Customer'. Original error: Export of existing CSV file is not supported if sink type is 'SQL'"):
            self._exportConfigGenerator._generateFwkEntities("metadataExportTable", "CompartmentADLS", "SQL", "dldata/TempExportSFTP")


    # generateExpOutput tests

    def test_generateExpOutput_exportAsCSV(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            ["SELECT * FROM Application_MEF_History.Customer", "History_Customer.csv", '{ "header": true, "nullValue": null }', None, None, None, None],
            ["SELECT * FROM Application_MEF_History.CustomerAddress", "History_CustomerAddress.csv", '{ "header": false, "nullValue": null }', None, None, None, None]
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._exportConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")
        
        expectedExpOutputDF = expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "CompartmentADLS.History_Customer",
                "ExportSFTP.History_Customer",
                "fwkTriggerId",
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                "CompartmentADLS.History_CustomerAddress",
                "ExportSFTP.History_CustomerAddress",
                "fwkTriggerId",
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ]
        ], EXP_OUTPUT_SCHEMA)

        # act
        expOutputDF = self._exportConfigGenerator._generateExpOutput("metadataExportTable", "CompartmentADLS", "ExportSFTP", "SFTP", "fwkTriggerId")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")

        self.assertEqual(
            expOutputDF.drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedExpOutputDF.drop("InsertTime", "LastUpdate").rdd.collect()
        )

    def test_generateExpOutput_exportToDatabase(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            ["SELECT * FROM Application_MEF_History.Customer", None, None, None, None, "SalesLT.CustomerEXP", "TRUNCATE TABLE SalesLT.CustomerEXP"],
            ["SELECT * FROM Application_MEF_History.CustomerAddress", None, None, None, None, "SalesLT.CustomerAddressEXP", None]
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._exportConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")
        
        expectedExpOutputDF = expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "CompartmentADLS.SalesLT.CustomerEXP",
                "EXP.SalesLT.CustomerEXP",
                "fwkTriggerId",
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                "CompartmentADLS.SalesLT.CustomerAddressEXP",
                "EXP.SalesLT.CustomerAddressEXP",
                "fwkTriggerId",
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ]
        ], EXP_OUTPUT_SCHEMA)

        # act
        expOutputDF = self._exportConfigGenerator._generateExpOutput("metadataExportTable", "CompartmentADLS", "ExportSQL", "SQL", "fwkTriggerId")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")

        self.assertEqual(
            expOutputDF.drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedExpOutputDF.drop("InsertTime", "LastUpdate").rdd.collect()
        )

    def test_generateExpOutput_exportExistingCSVToSFTP(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            [None, "Export_History_Customer.csv", None, "SourceADLS", "dldata/Files/Customer.csv", None, None],
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._exportConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")
        
        expectedExpOutputDF = expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "SourceADLS.Export_History_Customer",
                "ExportSFTP.Export_History_Customer",
                "fwkTriggerId",
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ]
        ], EXP_OUTPUT_SCHEMA)

        # act
        expOutputDF = self._exportConfigGenerator._generateExpOutput("metadataExportTable", "CompartmentADLS", "ExportSFTP", "SFTP", "fwkTriggerId")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")

        self.assertEqual(
            expOutputDF.drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedExpOutputDF.drop("InsertTime", "LastUpdate").rdd.collect()
        )

    def test_generateExpOutput_exportExistingCSVToSQL(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            [None, "Export_History_Customer.csv", None, "SourceADLS", "dldata/Files/Customer.csv", None, None],
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)  

        # act & assert
        with self.assertRaisesRegex(AssertionError, f"Export of existing CSV file is not supported if sink type is 'SQL'"):
            self._exportConfigGenerator._generateExpOutput("metadataExportTable", "CompartmentADLS", "ExportSQL", "SQL", "fwkTriggerId")


    # generateExpFwkEntities tests

    def test_generateExpFwkEntities_exportAsCSV(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            ["SELECT * FROM Application_MEF_History.Customer", "History_Customer.csv", '{ "header": true, "nullValue": null }', None, None, None, None],
            ["SELECT * FROM Application_MEF_History.CustomerAddress", "History_CustomerAddress.csv", '{ "header": false, "nullValue": null }', None, None, None, None]
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._exportConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "ExportSFTP.History_Customer",
                "ExportSFTP",
                "/to_dir/History_Customer.csv",
                "CSV",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "ExportSFTP.History_CustomerAddress",
                "ExportSFTP",
                "/to_dir/History_CustomerAddress.csv",
                "CSV",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ]
        ], FWK_ENTITY_SCHEMA)

        # act
        fwkEntitiesDF = self._exportConfigGenerator._generateExpFwkEntities("metadataExportTable", "ExportSFTP", "SFTP", "/to_dir")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")

        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").rdd.collect()
        )

    def test_generateExpFwkEntities_exportToDatabase(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            ["SELECT * FROM Application_MEF_History.Customer", None, None, None, None, "SalesLT.CustomerEXP", "TRUNCATE TABLE SalesLT.CustomerEXP"],
            ["SELECT * FROM Application_MEF_History.CustomerAddress", None, None, None, None, "SalesLT.CustomerAddressEXP", None]
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._exportConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "EXP.SalesLT.CustomerEXP",
                "ExportSQL",
                None,
                "Database",
                '{"preCopyScript":"TRUNCATE TABLE SalesLT.CustomerEXP"}',
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "EXP.SalesLT.CustomerAddressEXP",
                "ExportSQL",
                None,
                "Database",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ]
        ], FWK_ENTITY_SCHEMA)

        # act
        fwkEntitiesDF = self._exportConfigGenerator._generateExpFwkEntities("metadataExportTable", "ExportSQL", "SQL", None)

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")

        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").rdd.collect()
        )

    def test_generateExpFwkEntities_exportExistingCSVToSFTP(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            [None, "Export_History_Customer.csv", None, "SourceADLS", "dldata/Files/Customer.csv", None, None],
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)
        self._exportConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._exportConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "SourceADLS.Export_History_Customer",
                "SourceADLS",
                "dldata/Files/Customer.csv",
                "CSV",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "ExportSFTP.Export_History_Customer",
                "ExportSFTP",
                "/to_dir/Export_History_Customer.csv",
                "CSV",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ]
        ], FWK_ENTITY_SCHEMA)

        # act
        fwkEntitiesDF = self._exportConfigGenerator._generateExpFwkEntities("metadataExportTable", "ExportSFTP", "SFTP", "/to_dir")

        # assert
        self._exportConfigGenerator._spark.sql.assert_called_with(f"""SELECT * FROM {self._exportConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.metadataExportTable""")

        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").rdd.collect()
        )
    
    def test_generateExpFwkEntities_exportExistingCSVToSQL(self):
        # arrange
        tablesDF = self._spark.createDataFrame([
            [None, "Export_History_Customer.csv", None, "SourceADLS", "dldata/Files/Customer.csv", None, None],
        ], "Query STRING, Path STRING, Params STRING, FwkLinkedServiceId STRING, SourcePath STRING, Entity STRING, PreCopyScript STRING")
        
        self._exportConfigGenerator._spark.sql = MagicMock(return_value=tablesDF)

        # act & assert
        with self.assertRaisesRegex(AssertionError, f"Export of existing CSV file is not supported if sink type is 'SQL'"):
            self._exportConfigGenerator._generateExpFwkEntities("metadataExportTable", "ExportSQL", "SQL", "/to_dir")


    # generateConfiguration tests

    def test_generateConfiguration_exportToADLS(self):
        # arrange
        self._exportConfigGenerator._configGeneratorHelper.getADLSPathAndUri = MagicMock(
            return_value=("testSinkPath", "testSinkUri")
        )

        dtOutputDF = MagicMock()
        self._exportConfigGenerator._generateDtOutput = MagicMock(return_value=dtOutputDF)
        
        fwkEntitiesDF = MagicMock()
        self._exportConfigGenerator._generateFwkEntities = MagicMock(return_value=fwkEntitiesDF)
        
        self._exportConfigGenerator._generateExpOutput = MagicMock()
        self._exportConfigGenerator._generateExpFwkEntities = MagicMock()

        self._exportConfigGenerator._dataLakeHelper.writeDataAsParquet = MagicMock()

        metadataConfig = {
            "metadata": {"exportTable": "table"},
            "sink": {
                "FwkLinkedServiceId": "MyADLSLinkedService",
                "dataPath": "dataPath",
            },
            "FwkLayerId": "layerId",
            "FwkTriggerId": "triggerId"
        }

        # act
        result = self._exportConfigGenerator.generateConfiguration(metadataConfig, "output/path")

        # assert
        self.assertFalse(result)

        self._exportConfigGenerator._generateDtOutput.assert_called_once_with(
            "table", "MyADLSLinkedService", "ADLS", "layerId", "triggerId")
        
        self._exportConfigGenerator._generateFwkEntities.assert_called_once_with(
            "table", "MyADLSLinkedService", "ADLS", "testSinkPath")
        
        self._exportConfigGenerator._generateExpOutput.assert_not_called()
        self._exportConfigGenerator._generateExpFwkEntities.assert_not_called()

        self.assertEqual(self._exportConfigGenerator._dataLakeHelper.writeDataAsParquet.call_count, 2)

        self._exportConfigGenerator._dataLakeHelper.writeDataAsParquet.assert_has_calls([
            [(dtOutputDF, f"output/path/{DT_OUTPUT_FOLDER}/", WRITE_MODE_APPEND),],
            [(fwkEntitiesDF, f"output/path/{FWK_ENTITY_FOLDER}/", WRITE_MODE_APPEND),]
        ])

    def test_generateConfiguration_exportToSFTP(self):
        # arrange
        self._exportConfigGenerator._configGeneratorHelper.getADLSPathAndUri = MagicMock(
            return_value=("testSinkPath", "testSinkUri")
        )

        dtOutputDF = MagicMock()
        self._exportConfigGenerator._generateDtOutput = MagicMock(return_value=dtOutputDF)
        
        fwkEntitiesDF = MagicMock()
        self._exportConfigGenerator._generateFwkEntities = MagicMock(return_value=fwkEntitiesDF)
        
        expOutputDF = MagicMock()
        self._exportConfigGenerator._generateExpOutput = MagicMock(return_value=expOutputDF)

        expFwkEntitiesDF = MagicMock()
        self._exportConfigGenerator._generateExpFwkEntities = MagicMock(return_value=expFwkEntitiesDF)

        self._exportConfigGenerator._dataLakeHelper.writeDataAsParquet = MagicMock()

        metadataConfig = {
            "metadata": {"exportTable": "table"},
            "sink": {
                "FwkLinkedServiceId": "MyExportSFTP",
                "dataPath": "dataPath",
                "tempSink": {
                    "FwkLinkedServiceId": "MyADLSLinkedService"
                }
            },
            "FwkLayerId": "layerId",
            "FwkTriggerId": "triggerId"
        }

        # act
        result = self._exportConfigGenerator.generateConfiguration(metadataConfig, "output/path")

        # assert
        self.assertTrue(result)

        self._exportConfigGenerator._generateDtOutput.assert_called_once_with(
            "table", "MyADLSLinkedService", "SFTP", "layerId", "triggerId")

        self._exportConfigGenerator._generateFwkEntities.assert_called_once_with(
            "table", "MyADLSLinkedService", "SFTP", "testSinkPath")
        
        self._exportConfigGenerator._generateExpOutput.assert_called_once_with(
            "table", "MyADLSLinkedService", "MyExportSFTP", "SFTP", "triggerId")
        
        self._exportConfigGenerator._generateExpFwkEntities.assert_called_once_with(
            "table", "MyExportSFTP", "SFTP", "dataPath")
        
        self.assertEqual(self._exportConfigGenerator._dataLakeHelper.writeDataAsParquet.call_count, 4)

        self._exportConfigGenerator._dataLakeHelper.writeDataAsParquet.assert_has_calls([
            [(dtOutputDF, f"output/path/{DT_OUTPUT_FOLDER}/", WRITE_MODE_APPEND),],
            [(fwkEntitiesDF, f"output/path/{FWK_ENTITY_FOLDER}/", WRITE_MODE_APPEND),],
            [(expOutputDF, f"output/path/{EXP_OUTPUT_FOLDER}/", WRITE_MODE_APPEND),],
            [(expFwkEntitiesDF, f"output/path/{FWK_ENTITY_FOLDER}/", WRITE_MODE_APPEND),]
        ])

    def test_generateConfiguration_exportToDatabase(self):
        # arrange
        self._exportConfigGenerator._configGeneratorHelper.getADLSPathAndUri = MagicMock(
            return_value=("testSinkPath", "testSinkUri")
        )

        dtOutputDF = MagicMock()
        self._exportConfigGenerator._generateDtOutput = MagicMock(return_value=dtOutputDF)
        
        fwkEntitiesDF = MagicMock()
        self._exportConfigGenerator._generateFwkEntities = MagicMock(return_value=fwkEntitiesDF)
        
        expOutputDF = MagicMock()
        self._exportConfigGenerator._generateExpOutput = MagicMock(return_value=expOutputDF)

        expFwkEntitiesDF = MagicMock()
        self._exportConfigGenerator._generateExpFwkEntities = MagicMock(return_value=expFwkEntitiesDF)

        self._exportConfigGenerator._dataLakeHelper.writeDataAsParquet = MagicMock()

        metadataConfig = {
            "metadata": {"exportTable": "table"},
            "sink": {
                "FwkLinkedServiceId": "MyExportSQL",
                "tempSink": {
                    "FwkLinkedServiceId": "MyADLSLinkedService"
                }
            },
            "FwkLayerId": "layerId",
            "FwkTriggerId": "triggerId"
        }

        # act
        result = self._exportConfigGenerator.generateConfiguration(metadataConfig, "output/path")

        # assert
        self.assertTrue(result)

        self._exportConfigGenerator._generateDtOutput.assert_called_once_with(
            "table", "MyADLSLinkedService", "SQL", "layerId", "triggerId")

        self._exportConfigGenerator._generateFwkEntities.assert_called_once_with(
            "table", "MyADLSLinkedService", "SQL", "testSinkPath")
        
        self._exportConfigGenerator._generateExpOutput.assert_called_once_with(
            "table", "MyADLSLinkedService", "MyExportSQL", "SQL", "triggerId")
        
        self._exportConfigGenerator._generateExpFwkEntities.assert_called_once_with(
            "table", "MyExportSQL", "SQL", None)
        
        self.assertEqual(self._exportConfigGenerator._dataLakeHelper.writeDataAsParquet.call_count, 4)

        self._exportConfigGenerator._dataLakeHelper.writeDataAsParquet.assert_has_calls([
            [(dtOutputDF, f"output/path/{DT_OUTPUT_FOLDER}/", WRITE_MODE_APPEND),],
            [(fwkEntitiesDF, f"output/path/{FWK_ENTITY_FOLDER}/", WRITE_MODE_APPEND),],
            [(expOutputDF, f"output/path/{EXP_OUTPUT_FOLDER}/", WRITE_MODE_APPEND),],
            [(expFwkEntitiesDF, f"output/path/{FWK_ENTITY_FOLDER}/", WRITE_MODE_APPEND),]
        ])
