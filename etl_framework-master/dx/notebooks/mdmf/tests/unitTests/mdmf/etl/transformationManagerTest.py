# Databricks notebook source
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from unittest.mock import MagicMock, ANY

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.etl.transformationManager import TransformationManager
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../../compartment/includes/compartmentConfig

# COMMAND ----------

# MAGIC %run ../../../../etl/transformationManager

# COMMAND ----------

# MAGIC %run ../../../../includes/frameworkConfig

# COMMAND ----------

class TransformationManagerTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()
        self._compartmentConfig = MagicMock()

    def setUp(self):
        self._transformationManager = TransformationManager(self._spark, self._compartmentConfig)
        self._transformationManager._spark = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()

    
    # prepareSinkConfig tests

    def test_prepareSinkConfig(self):
        # arrange
        instructionParameters = {
            "SinkDatabaseName": "Application_MEF_StagingSQL",
            "SinkTableName": "Customer",
            "SinkInstanceURL": "https://ddlasmefdevxx.dfs.core.windows.net/",
            "SinkPath": "dldata/SQL/Customer",
            "SinkWriteMode": "Overwrite",
            "SinkFormat": "Delta",
            "SinkParams": None,
        }

        self._transformationManager._dataLakeHelper.getADLSParams = MagicMock(
            return_value=(
                "ddlasmefdevxx",
                "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SQL/Customer"
            )
        )

        expectedSinkConfig = {
            "databaseName": "Application_MEF_StagingSQL",
            "tableName": "Customer",
            "writeMode": "Overwrite",
            "format": "delta",
            "params": None,
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SQL/Customer"
        }

        # act
        sinkConfig = self._transformationManager._prepareSinkConfig(instructionParameters)

        # assert
        self.assertEqual(sinkConfig, expectedSinkConfig)

        self._transformationManager._dataLakeHelper.getADLSParams.assert_called_once_with(
            "https://ddlasmefdevxx.dfs.core.windows.net/",
            "dldata/SQL/Customer"
        )

    
    # validateSinkConfig tests

    def test_validateSinkConfig_formatNotSupported(self):
        # arrange
        sinkConfig = {
            "databaseName": "Application_MEF_StagingSQL",
            "tableName": "Customer",
            "writeMode": "Overwrite",
            "format": "NOT_SUPPORTED",
            "params": None,
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SQL/Customer"
        }

        # act
        with self.assertRaisesRegex(AssertionError, f"The format of the sink data 'NOT_SUPPORTED' is not supported"):
            self._transformationManager._validateSinkConfig(sinkConfig)

    def test_validateSinkConfig_writeModeForDeltaNotSupported(self):
        # arrange
        sinkConfig = {
            "databaseName": "Application_MEF_StagingSQL",
            "tableName": "Customer",
            "writeMode": "NOT_SUPPORTED",
            "format": "delta",
            "params": None,
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SQL/Customer"
        }

        # act
        with self.assertRaisesRegex(AssertionError, f"The write mode 'NOT_SUPPORTED' is not supported for Delta"):
            self._transformationManager._validateSinkConfig(sinkConfig)

    def test_validateSinkConfig_writeModeForCSVNotSupported(self):
        # arrange
        sinkConfig = {
            "databaseName": "Application_MEF_StagingSQL",
            "tableName": "Customer",
            "writeMode": "NOT_SUPPORTED",
            "format": "csv",
            "params": None,
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SQL/Customer"
        }

        # act
        with self.assertRaisesRegex(AssertionError, f"The write mode 'NOT_SUPPORTED' is not supported for CSV"):
            self._transformationManager._validateSinkConfig(sinkConfig)

    def test_validateSinkConfig_validForDelta(self):
        # arrange
        sinkConfig = {
            "databaseName": "Application_MEF_StagingSQL",
            "tableName": "Customer",
            "writeMode": "Overwrite",
            "format": "delta",
            "params": None,
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SQL/Customer"
        }

        # act
        self._transformationManager._validateSinkConfig(sinkConfig)

    def test_validateSinkConfig_validForCSV(self):
        # arrange
        sinkConfig = {
            "databaseName": None,
            "tableName": None,
            "writeMode": "Overwrite",
            "format": "csv",
            "params": None,
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SQL/Customer"
        }

        # act
        self._transformationManager._validateSinkConfig(sinkConfig)

    
    # prepareSourceConfig test

    def test_prepareSourceConfig_noSource(self):
        # arrange
        instructionParameters = {
            "SourceDatabaseName": None,
            "SourceTableName": None,
            "SourceInstanceURL": None,
            "SourcePath": None,
            "SourceFormat": None
        }

        self._transformationManager._dataLakeHelper.getADLSParams = MagicMock()

        expectedSourceConfig = {
            "databaseName": None,
            "tableName": None
        }

        # act
        sourceConfig = self._transformationManager._prepareSourceConfig(instructionParameters)

        # assert
        self.assertEqual(sourceConfig, expectedSourceConfig)

        self._transformationManager._dataLakeHelper.getADLSParams.assert_not_called()

    def test_prepareSourceConfig_parquetSource(self):
        # arrange
        instructionParameters = {
            "SourceDatabaseName": None,
            "SourceTableName": None,
            "SourceInstanceURL": "https://dlzasmefdevxx.dfs.core.windows.net/",
            "SourcePath": "asmefdevxx/SQL/Customer",
            "SourceFormat": "Parquet",
        }

        self._transformationManager._dataLakeHelper.getADLSParams = MagicMock(
            return_value=(
                "dlzasmefdevxx",
                "abfss://asmefdevxx@dlzasmefdevxx.dfs.core.windows.net/SQL/Customer"
            )
        )

        expectedSourceConfig = {
            "databaseName": None,
            "tableName": None,
            "format": "parquet",
            "adlsName": "dlzasmefdevxx",
            "dataUri": "abfss://asmefdevxx@dlzasmefdevxx.dfs.core.windows.net/SQL/Customer"
        }

        # act
        sourceConfig = self._transformationManager._prepareSourceConfig(instructionParameters)

        # assert
        self.assertEqual(sourceConfig, expectedSourceConfig)

        self._transformationManager._dataLakeHelper.getADLSParams.assert_called_once_with(
            "https://dlzasmefdevxx.dfs.core.windows.net/",
            "asmefdevxx/SQL/Customer"
        )

    def test_prepareSourceConfig_deltaSource(self):
        # arrange
        instructionParameters = {
            "SourceDatabaseName": "Application_MEF_StagingSQL",
            "SourceTableName": "Customer",
            "SourceInstanceURL": "https://ddlasbittestx.dfs.core.windows.net/",
            "SourcePath": "dldata/SQL/Customer",
            "SourceFormat": "Delta",
        }

        self._transformationManager._dataLakeHelper.getADLSParams = MagicMock()

        expectedSourceConfig = {
            "databaseName": "Application_MEF_StagingSQL",
            "tableName": "Customer"
        }

        # act
        sourceConfig = self._transformationManager._prepareSourceConfig(instructionParameters)

        # assert
        self.assertEqual(sourceConfig, expectedSourceConfig)

        self._transformationManager._dataLakeHelper.getADLSParams.assert_not_called()

    
    # validateSourceConfig tests
    
    def test_validateSourceConfig_formatNotSupported(self):
        # arrange
        sourceConfig = {
            "databaseName": None,
            "tableName": None,
            "format": "NOT_SUPPORTED",
            "adlsName": "dlzasmefdevxx",
            "dataUri": "abfss://asmefdevxx@dlzasmefdevxx.dfs.core.windows.net/SQL/Customer"
        }

        sinkConfig = {}

        # act
        with self.assertRaisesRegex(AssertionError, f"The format of the source data 'NOT_SUPPORTED' is not supported"):
            self._transformationManager._validateSourceConfig(sourceConfig, sinkConfig)

    def test_validateSourceConfig_validForParquetSource(self):
        # arrange
        sourceConfig = {
            "databaseName": None,
            "tableName": None,
            "format": "parquet",
            "adlsName": "dlzasmefdevxx",
            "dataUri": "abfss://asmefdevxx@dlzasmefdevxx.dfs.core.windows.net/SQL/Customer"
        }

        sinkConfig = {}

        # act
        self._transformationManager._validateSourceConfig(sourceConfig, sinkConfig)

    def test_validateSourceConfig_validForDeltaSource(self):
        # arrange
        sourceConfig = {
            "databaseName": "Application_MEF_StagingSQL",
            "tableName": "Customer",
            "format": "delta"
        }

        sinkConfig = {}

        # act
        self._transformationManager._validateSourceConfig(sourceConfig, sinkConfig)

    def test_validateSourceConfig_validForNoSource(self):
        # arrange
        sourceConfig = {
            "databaseName": None,
            "tableName": None
        }

        sinkConfig = {}

        # act
        self._transformationManager._validateSourceConfig(sourceConfig, sinkConfig)

    def test_validateSourceConfig_validForCSVSource(self):
        # arrange
        sourceConfig = {
            "databaseName": None,
            "tableName": None,
            "format": "csv",
            "adlsName": "dlzasmefdevxx",
            "dataUri": "abfss://asmefdevxx@dlzasmefdevxx.dfs.core.windows.net/Files/Customer.csv"
        }

        sinkConfig = {
            "databaseName": None,
            "tableName": None,
            "writeMode": "Overwrite",
            "format": "csv",
            "params": None,
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/Export/Customer.csv"
        }

        # act
        self._transformationManager._validateSourceConfig(sourceConfig, sinkConfig)

    def test_validateSourceConfig_invalidForCSVSource_wrongSinkFormat(self):
        # arrange
        sourceConfig = {
            "databaseName": None,
            "tableName": None,
            "format": "csv",
            "adlsName": "dlzasmefdevxx",
            "dataUri": "abfss://asmefdevxx@dlzasmefdevxx.dfs.core.windows.net/Files/Customer.csv"
        }

        sinkConfig = {
            "databaseName": "Application_MEF_StagingSQL",
            "tableName": "Customer",
            "writeMode": "Overwrite",
            "format": "delta",
            "params": None,
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SQL/Customer"
        }

        # act
        with self.assertRaisesRegex(AssertionError, f"The CSV format of the source data is supported only if sink format is also CSV."):
            self._transformationManager._validateSourceConfig(sourceConfig, sinkConfig)


    # readSourceData tests

    def test_readSourceData_hiveSource(self):
        # arrange
        sourceConfig = {
            "databaseName": "Application_MEF_StagingSQL",
            "tableName": "Customer"
        }

        expectedSourceDF = MagicMock()
        self._transformationManager._spark.sql = MagicMock(return_value=expectedSourceDF)

        # act
        sourceDF = self._transformationManager._readSourceData(sourceConfig)

        # assert
        self.assertEqual(sourceDF, expectedSourceDF)

        self._transformationManager._spark.sql.assert_called_once_with(
            "SELECT * FROM Application_MEF_StagingSQL.Customer"
        )

    def test_readSourceData_parquetSource(self):
        # arrange
        sourceConfig = {
            "databaseName": None,
            "tableName": None,
            "format": "parquet",
            "adlsName": "dlzasmefdevxx",
            "dataUri": "abfss://asmefdevxx@dlzasmefdevxx.dfs.core.windows.net/SQL/Customer"
        }

        self._transformationManager._spark.sql = MagicMock()

        expectedSourceDF = MagicMock()

        mock = MagicMock()
        mock.load.return_value = expectedSourceDF
        self._transformationManager._spark.read.format.return_value = mock
        
        # act
        sourceDF = self._transformationManager._readSourceData(sourceConfig)

        # assert
        #self.assertEqual(sourceDF, expectedSourceDF)

        self._transformationManager._spark.sql.assert_not_called()

        self._transformationManager._spark.read.format.assert_called_once_with("parquet")
        mock.load.assert_called_once_with("abfss://asmefdevxx@dlzasmefdevxx.dfs.core.windows.net/SQL/Customer")

    def test_readSourceData_noSource(self):
        # arrange
        sourceConfig = {
            "databaseName": None,
            "tableName": None
        }

        self._transformationManager._spark.sql = MagicMock()
        self._transformationManager._spark.read = MagicMock()

        # act
        sourceDF = self._transformationManager._readSourceData(sourceConfig)

        # assert
        self.assertEqual(sourceDF, None)

        self._transformationManager._spark.sql.assert_not_called()
        self._transformationManager._spark.read.assert_not_called()

    
    # executeTransformationsAndValidation tests

    def test_executeTransformationsAndValidation_oneTransformationAndValidate(self):
        # arrange
        sourceDF = MagicMock()

        sinkConfig = {
            "databaseName": "Application_MEF_SDM",
            "tableName": "Customer",
            "writeMode": "SCDType1",
            "format": "delta",
            "params": None,
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SDM/Customer"
        }

        inputParameters = {
            "keyColumns": [],
            "partitionColumns": [],
            "functions": [
                {"transformation": "select", "params": "CustomerID, FirstName, LastName"}
            ]
        }

        self._transformationManager._validator = MagicMock()

        filteredDF = MagicMock()
        self._transformationManager._validator.filterOutQuarantinedRecords.return_value = filteredDF

        self._transformationManager._validator.hasToBeValidated.return_value = True

        transformedDF = MagicMock()
        self._transformationManager._select = MagicMock()
        self._transformationManager._select.execute.return_value = transformedDF

        self._transformationManager._pseudonymize = MagicMock()
        self._transformationManager._deduplicate = MagicMock()

        validatedDF = MagicMock()
        self._transformationManager._validator.validate.return_value = validatedDF

        # act
        sinkDF = self._transformationManager._executeTransformationsAndValidation(
            sourceDF,
            sinkConfig,
            inputParameters
        )

        # assert
        self._transformationManager._validator.filterOutQuarantinedRecords.assert_called_once_with(sourceDF)
        
        self._transformationManager._validator.hasToBeValidated.assert_called_once_with(
            "Application_MEF_SDM",
            "Customer"
        )

        self._transformationManager._select.execute.assert_called_once_with(
            filteredDF,
            "CustomerID, FirstName, LastName"
        )

        self._transformationManager._validator.validate.assert_called_once_with(
            transformedDF,
            "Application_MEF_SDM",
            "Customer"
        )

        self.assertEqual(sinkDF, validatedDF)

        self._transformationManager._pseudonymize.execute.assert_not_called()
        self._transformationManager._deduplicate.execute.assert_not_called()

    def test_executeTransformationsAndValidation_oneTransformationAndDontValidate(self):
        # arrange
        sourceDF = MagicMock()

        sinkConfig = {
            "databaseName": "Application_MEF_SDM",
            "tableName": "Customer",
            "writeMode": "SCDType1",
            "format": "delta",
            "params": None,
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SDM/Customer"
        }

        inputParameters = {
            "keyColumns": [],
            "partitionColumns": [],
            "functions": [
                {"transformation": "select", "params": "CustomerID, FirstName, LastName"}
            ]
        }

        self._transformationManager._validator = MagicMock()

        filteredDF = MagicMock()
        self._transformationManager._validator.filterOutQuarantinedRecords.return_value = filteredDF

        self._transformationManager._validator.hasToBeValidated.return_value = False

        transformedDF = MagicMock()
        self._transformationManager._select = MagicMock()
        self._transformationManager._select.execute.return_value = transformedDF

        self._transformationManager._pseudonymize = MagicMock()
        self._transformationManager._deduplicate = MagicMock()

        # act
        sinkDF = self._transformationManager._executeTransformationsAndValidation(
            sourceDF,
            sinkConfig,
            inputParameters
        )

        # assert
        self._transformationManager._validator.filterOutQuarantinedRecords.assert_called_once_with(sourceDF)
        
        self._transformationManager._validator.hasToBeValidated.assert_called_once_with(
            "Application_MEF_SDM",
            "Customer"
        )

        self._transformationManager._select.execute.assert_called_once_with(
            filteredDF,
            "CustomerID, FirstName, LastName"
        )

        self._transformationManager._validator.validate.assert_not_called()

        self.assertEqual(sinkDF, transformedDF)

        self._transformationManager._pseudonymize.execute.assert_not_called()
        self._transformationManager._deduplicate.execute.assert_not_called()

    def test_executeTransformationsAndValidation_multipleTransformationsAndValidate(self):
        # arrange
        sourceDF = MagicMock()

        sinkConfig = {
            "databaseName": "Application_MEF_SDM",
            "tableName": "Customer",
            "writeMode": "SCDType1",
            "format": "delta",
            "params": None,
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SDM/Customer"
        }

        inputParameters = {
            "keyColumns": [],
            "partitionColumns": [],
            "functions": [
                {"transformation": "select", "params": "CustomerID, FirstName, LastName"},
                {"transformation": "pseudonymize", "params": ["FirstName", "LastName"]},
                {"transformation": "deduplicate", "params": ["*"]} 
            ]
        }

        self._transformationManager._validator = MagicMock()

        filteredDF = MagicMock()
        self._transformationManager._validator.filterOutQuarantinedRecords.return_value = filteredDF

        self._transformationManager._validator.hasToBeValidated.return_value = True

        selectDF = MagicMock()
        self._transformationManager._select = MagicMock()
        self._transformationManager._select.execute.return_value = selectDF

        pseudonymizedDF = MagicMock()
        self._transformationManager._pseudonymize = MagicMock()
        self._transformationManager._pseudonymize.execute.return_value = pseudonymizedDF

        deduplicateDF = MagicMock()
        self._transformationManager._deduplicate = MagicMock()
        self._transformationManager._deduplicate.execute.return_value = deduplicateDF

        validatedDF = MagicMock()
        self._transformationManager._validator.validate.return_value = validatedDF
    
        # act
        sinkDF = self._transformationManager._executeTransformationsAndValidation(
            sourceDF,
            sinkConfig,
            inputParameters
        )

        # assert
        self._transformationManager._validator.filterOutQuarantinedRecords.assert_called_once_with(sourceDF)
        
        self._transformationManager._validator.hasToBeValidated.assert_called_once_with(
            "Application_MEF_SDM",
            "Customer"
        )

        self._transformationManager._select.execute.assert_called_once_with(
            filteredDF,
            "CustomerID, FirstName, LastName"
        )

        self._transformationManager._pseudonymize.execute.assert_called_once_with(
            selectDF,
            ["FirstName", "LastName"]
        )

        self._transformationManager._deduplicate.execute.assert_called_once_with(
            pseudonymizedDF,
            ["*"]
        )

        self._transformationManager._validator.validate.assert_called_once_with(
            deduplicateDF,
            "Application_MEF_SDM",
            "Customer"
        )

        self.assertEqual(sinkDF, validatedDF)

    
    # writeDataToSink tests

    def test_writeDataToSink_delta(self):
        # arrange
        sinkDF = self._spark.createDataFrame([
            ["value 11", "value 12"],
            ["value 21", "value 22"]
        ], "col1 STRING, col2 STRING")

        sinkConfig = {
            "databaseName": "Application_MEF_SDM",
            "tableName": "Customer",
            "writeMode": "SCDType1",
            "format": "delta",
            "params": None,
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SDM/Customer"
        }

        inputParameters = {}

        entTriggerTime = datetime.now()
        executionStart = datetime.now()

        expectedInputParameters = {
            "entTriggerTime": entTriggerTime,
            "executionStart": executionStart
        }

        expectedSinkDF = (
            sinkDF
            .withColumn(INSERT_TIME_COLUMN_NAME, lit(executionStart))
            .withColumn(UPDATE_TIME_COLUMN_NAME, lit(executionStart))
        )

        expectedStatistics = MagicMock()

        self._transformationManager._dataLakeHelper.writeData = MagicMock(return_value=expectedStatistics)

        self._transformationManager._dataLakeHelper.writeCSV = MagicMock()

        # act
        statistics = self._transformationManager._writeDataToSink(
            sinkDF,
            sinkConfig,
            inputParameters,
            entTriggerTime,
            executionStart
        )

        # assert
        self.assertEqual(statistics, expectedStatistics)

        self._transformationManager._dataLakeHelper.writeData.assert_called_once_with(
            ANY, # will be tested bellow
            "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/SDM/Customer",
            "SCDType1",
            "delta",
            expectedInputParameters,
            "Application_MEF_SDM",
            "Customer"
        )

        actualSinkDF = self._transformationManager._dataLakeHelper.writeData.call_args_list[0].args[0]

        self.assertEqual(
            actualSinkDF.sort("col1").rdd.collect(),
            expectedSinkDF.sort("col1").rdd.collect()
        )

        self._transformationManager._dataLakeHelper.writeCSV.assert_not_called()

    def test_writeDataToSink_csv(self):
        # arrange
        sinkDF = self._spark.createDataFrame([
            ["value 11", "value 12"],
            ["value 21", "value 22"]
        ], "col1 STRING, col2 STRING")

        sinkConfig = {
            "databaseName": None,
            "tableName": None,
            "writeMode": "Overwrite",
            "format": "csv",
            "params": '{ "header": true, "nullValue": null }',
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/CSV/Customer.csv"
        }

        inputParameters = {}

        entTriggerTime = datetime.now()
        executionStart = datetime.now()

        expectedStatistics = MagicMock()

        self._transformationManager._dataLakeHelper.writeData = MagicMock()
        self._transformationManager._dataLakeHelper.writeCSV = MagicMock(return_value=expectedStatistics)

        # act
        statistics = self._transformationManager._writeDataToSink(
            sinkDF,
            sinkConfig,
            inputParameters,
            entTriggerTime,
            executionStart
        )

        # assert
        self.assertEqual(statistics, expectedStatistics)

        self._transformationManager._dataLakeHelper.writeCSV.assert_called_once_with(
            sinkDF,
            "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/CSV/Customer.csv",
            "Overwrite",
            '{ "header": true, "nullValue": null }'
        )

        self._transformationManager._dataLakeHelper.writeData.assert_not_called()

    def test_writeDataToSink_notSupportedFormat(self):
        # arrange
        sinkDF = self._spark.createDataFrame([
            ["value 11", "value 12"],
            ["value 21", "value 22"]
        ], "col1 STRING, col2 STRING")

        sinkConfig = {
            "databaseName": None,
            "tableName": None,
            "writeMode": "Overwrite",
            "format": "NOT_SUPPORTED",
            "params": '{ "header": true, "nullValue": null }',
            "adlsName": "ddlasmefdevxx",
            "dataUri": "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/CSV/Customer.csv"
        }

        inputParameters = {}

        entTriggerTime = datetime.now()
        executionStart = datetime.now()

        expectedStatistics = {}

        self._transformationManager._dataLakeHelper.writeData = MagicMock()
        self._transformationManager._dataLakeHelper.writeCSV = MagicMock()

        # act
        statistics = self._transformationManager._writeDataToSink(
            sinkDF,
            sinkConfig,
            inputParameters,
            entTriggerTime,
            executionStart
        )

        # assert
        self.assertEqual(statistics, expectedStatistics)

        self._transformationManager._dataLakeHelper.writeData.assert_not_called()
        self._transformationManager._dataLakeHelper.writeCSV.assert_not_called()

    
    # createOutput tests

    def test_createOutput_withStatistics(self):
        # arrange
        executionStart = datetime.now()

        statistics = {
            "recordsInserted": 5,
            "recordsUpdated": 10,
            "recordsDeleted": 2
        }

        expectedOutput = {
            "duration": ANY,
            "recordsInserted": 5,
            "recordsUpdated": 10,
            "recordsDeleted": 2
        }

        # act
        output = self._transformationManager._createOutput(executionStart, statistics)

        # assert
        self.assertEqual(output, expectedOutput)

        self.assertIsInstance(output["duration"], int)

    def test_createOutput_withoutStatistics(self):
        # arrange
        executionStart = datetime.now()

        expectedOutput = {
            "duration": ANY,
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }

        # act
        output = self._transformationManager._createOutput(executionStart)

        # assert
        self.assertEqual(output, expectedOutput)

        self.assertIsInstance(output["duration"], int)

    
    # executeInstruction tests

    def test_executeInstruction_regularInstruction(self):
        # arrange
        instructionParameters = {
            "SourceDatabaseName": None,
            "SourceTableName": None,
            "SourceInstanceURL": "https://dlzasmefdevxx.dfs.core.windows.net/",
            "SourcePath": "asmefdevxx/SQL/Customer",
            "SourceFormat": "Parquet",
            "SinkDatabaseName": "Application_MEF_StagingSQL",
            "SinkTableName": "Customer",
            "SinkInstanceURL": "https://ddlasmefdevxx.dfs.core.windows.net/",
            "SinkPath": "dldata/SQL/Customer",
            "SinkWriteMode": "Overwrite",
            "SinkFormat": "Delta",
            "SinkParams": None,
            "InputParameters": """{
                "keyColumns": [],
                "partitionColumns": [],
                "functions": {
                    "pseudonymize": ["FirstName", "MiddleName", "LastName", "EmailAddress", "Phone"]
                }
            }"""
        }

        entRunId = "test run ID"
        entTriggerTime = datetime.now()

        inputParameters = {
            "keyColumns": [],
            "partitionColumns": [],
            "functions": {
                "pseudonymize": ["FirstName", "MiddleName", "LastName", "EmailAddress", "Phone"]
            }
        }

        sinkConfig = MagicMock()
        self._transformationManager._prepareSinkConfig = MagicMock(return_value=sinkConfig)
        self._transformationManager._validateSinkConfig = MagicMock()

        sourceConfig = MagicMock()
        self._transformationManager._prepareSourceConfig = MagicMock(return_value=sourceConfig)
        self._transformationManager._validateSourceConfig = MagicMock()

        self._transformationManager._dbutils = MagicMock()
        self._transformationManager._dbutils.fs = MagicMock()

        sourceDF = MagicMock()
        self._transformationManager._readSourceData = MagicMock(return_value=sourceDF)

        sinkDF = MagicMock()
        self._transformationManager._executeTransformationsAndValidation = MagicMock(return_value=sinkDF)

        statistics = MagicMock()
        self._transformationManager._writeDataToSink = MagicMock(return_value=statistics)

        expectedOutput = MagicMock()
        self._transformationManager._createOutput = MagicMock(return_value=expectedOutput)

        # act
        output = self._transformationManager.executeInstruction(
            instructionParameters,
            entRunId,
            entTriggerTime
        )

        # assert
        self.assertIsNotNone(self._transformationManager._validator)

        self._transformationManager._prepareSinkConfig.assert_called_once_with(instructionParameters)
        self._transformationManager._validateSinkConfig.assert_called_once_with(sinkConfig)

        self._transformationManager._prepareSourceConfig.assert_called_once_with(instructionParameters)
        self._transformationManager._validateSourceConfig.assert_called_once_with(sourceConfig, sinkConfig)

        self._transformationManager._dbutils.fs.cp.assert_not_called()
        self._transformationManager._dbutils.fs.rm.assert_not_called()

        self._transformationManager._readSourceData.assert_called_once_with(sourceConfig)

        self._transformationManager._executeTransformationsAndValidation.assert_called_once_with(
            sourceDF,
            sinkConfig,
            inputParameters
        )

        self._transformationManager._writeDataToSink.assert_called_once_with(
            sinkDF,
            sinkConfig,
            inputParameters,
            entTriggerTime,
            ANY
        )

        self._transformationManager._createOutput(ANY, statistics)

        self.assertEqual(output, expectedOutput)

    def test_executeInstruction_copyCSV(self):
        # arrange
        instructionParameters = {
            "SourceDatabaseName": None,
            "SourceTableName": None,
            "SourceInstanceURL": "https://ddlasmefdevxx.dfs.core.windows.net/",
            "SourcePath": "dldata/Files/Customer.csv",
            "SourceFormat": "CSV",
            "SinkDatabaseName": None,
            "SinkTableName": None,
            "SinkInstanceURL": "https://ddlasmefdevxx.dfs.core.windows.net/",
            "SinkPath": "dldata/Export/Customer.csv",
            "SinkWriteMode": "Overwrite",
            "SinkFormat": "CSV",
            "SinkParams": None,
            "InputParameters": """{
                "keyColumns": [],
                "partitionColumns": [],
                "functions": {}
            }"""
        }

        entRunId = "test run ID"
        entTriggerTime = datetime.now()

        inputParameters = {
            "keyColumns": [],
            "partitionColumns": [],
            "functions": {}
        }

        self._transformationManager._dbutils = MagicMock()
        self._transformationManager._dbutils.fs = MagicMock()

        self._transformationManager._readSourceData = MagicMock()
        self._transformationManager._executeTransformationsAndValidation = MagicMock()
        self._transformationManager._writeDataToSink = MagicMock()

        expectedOutput = MagicMock()
        self._transformationManager._createOutput = MagicMock(return_value=expectedOutput)

        # act
        output = self._transformationManager.executeInstruction(
            instructionParameters,
            entRunId,
            entTriggerTime
        )

        # assert
        self.assertIsNotNone(self._transformationManager._validator)

        self._transformationManager._dbutils.fs.cp.assert_called_once_with(
            "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/Files/Customer.csv",
            "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/Export/Customer.csv"
        )

        self._transformationManager._dbutils.fs.rm.assert_called_once_with(
            "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/Files/Customer.csv"
        )

        self._transformationManager._readSourceData.assert_not_called()
        self._transformationManager._executeTransformationsAndValidation.assert_not_called()
        self._transformationManager._writeDataToSink.assert_not_called()

        self._transformationManager._createOutput(ANY)

        self.assertEqual(output, expectedOutput)
