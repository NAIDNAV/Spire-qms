# Databricks notebook source
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from unittest.mock import MagicMock, ANY

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.includes.dataLakeHelper import DataLakeHelper
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../../compartment/includes/compartmentConfig

# COMMAND ----------

# MAGIC %run ../../../../includes/dataLakeHelper

# COMMAND ----------

# MAGIC %run ../../../../includes/frameworkConfig

# COMMAND ----------

class DataLakeHelperTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

        self._compartmentConfig = MagicMock()
        self._compartmentConfig.FWK_METADATA_CONFIG = {
            "metadata": {
                "databaseName": "Application_MEF_Metadata"
            }
        }
    
    def setUp(self) -> None:
        self._dataLakeHelper = DataLakeHelper(self._spark, self._compartmentConfig)
        self._dataLakeHelper._spark = MagicMock()
        self._dataLakeHelper._sqlContext = MagicMock()
        self._dataLakeHelper._dbutils = MagicMock()
        self._dataLakeHelper._DeltaTable = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # getADLSParams tests

    def test_getADLSParams_withDataPath(self):
        # arrange
        instanceURL = "https://dstgcpbit.dfs.core.windows.net/"
        path = "myContainerName/MySubfolder"

        # act
        (adlsName, dataUri) = self._dataLakeHelper.getADLSParams(instanceURL, path)

        # assert
        self.assertEqual(adlsName, "dstgcpbit")
        self.assertEqual(dataUri, "abfss://myContainerName@dstgcpbit.dfs.core.windows.net/MySubfolder")

    def test_getADLSParams_withoutDataPath(self):
        # arrange
        instanceURL = "https://dstgcpbit.dfs.core.windows.net/"
        path = "myContainerName"

        # act
        (adlsName, dataUri) = self._dataLakeHelper.getADLSParams(instanceURL, path)

        # assert
        self.assertEqual(adlsName, "dstgcpbit")
        self.assertEqual(dataUri, "abfss://myContainerName@dstgcpbit.dfs.core.windows.net")

    def test_getADLSParams_withoutTrailingInstanceUrlSlash(self):
        # arrange
        instanceURL = "https://dstgcpbit.dfs.core.windows.net"
        path = "myContainerName/MySubfolder"

        # act
        (adlsName, dataUri) = self._dataLakeHelper.getADLSParams(instanceURL, path)

        # assert
        self.assertEqual(adlsName, "dstgcpbit")
        self.assertEqual(dataUri, "abfss://myContainerName@dstgcpbit.dfs.core.windows.net/MySubfolder")

    def test_getADLSParams_containerNameInPath(self):
        # arrange
        instanceURL = "https://dstgcpbit.dfs.core.windows.net/"
        path = "history/MySubfolder_history"

        # act
        (adlsName, dataUri) = self._dataLakeHelper.getADLSParams(instanceURL, path)

        # assert
        self.assertEqual(adlsName, "dstgcpbit")
        self.assertEqual(dataUri, "abfss://history@dstgcpbit.dfs.core.windows.net/MySubfolder_history")


    # fileExists tests

    def test_fileExists_exists(self):
        # arrange
        path = "path/to/file.json"

        # act
        exists = self._dataLakeHelper.fileExists(path)

        # assert
        self.assertTrue(exists)
        self._dataLakeHelper._dbutils.fs.ls.assert_called_once_with(path)
    
    def test_fileExists_doesNotExist(self):
        # arrange
        self._dataLakeHelper._dbutils.fs.ls.side_effect = Exception("java.io.FileNotFoundException")
        path = "path/to/nonExistingFile.json"

        # act
        exists = self._dataLakeHelper.fileExists(path)

        # assert
        self.assertFalse(exists)
        self._dataLakeHelper._dbutils.fs.ls.assert_called_once_with(path)

    def test_fileExists_exception(self):
        # arrange
        self._dataLakeHelper._dbutils.fs.ls.side_effect = Exception("UnexpectedException")
        path = "path/to/file.json"

        # act
        with self.assertRaisesRegex(Exception, "UnexpectedException"):
            self._dataLakeHelper.fileExists(path)

        # assert
        self._dataLakeHelper._dbutils.fs.ls.assert_called_once_with(path)


    # allowAlterForTable tests

    def test_allowAlterForTable(self):
        # act
        self._dataLakeHelper.allowAlterForTable("DatabaseName.TableName")
        
        # assert
        self._dataLakeHelper._spark.sql.assert_called_once()
        arg = self._dataLakeHelper._spark.sql.call_args[0][0]
        self.assertIn("ALTER TABLE DatabaseName.TableName", arg)


    # getIdentityColumn tests

    def test_getIdentityColumn(self):
        # arrange
        expectedRow = MagicMock()

        mockSql = MagicMock()
        mockSql.first.return_value = expectedRow
        self._dataLakeHelper._spark.sql.return_value = mockSql
        
        # act
        row = self._dataLakeHelper._getIdentityColumn("DatabaseName", "TableName")

        # assert
        self.assertEqual(row, expectedRow)

        self._dataLakeHelper._spark.sql.assert_called_once()
        
        arg = self._dataLakeHelper._spark.sql.call_args[0][0]
        self.assertIn("SELECT", arg)
        self.assertIn(f"""FROM {self._dataLakeHelper._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}""", arg)
        self.assertIn("DatabaseName = 'DatabaseName'", arg)
        self.assertIn("EntityName = 'TableName'", arg)
        self.assertIn("IsIdentity", arg)

        mockSql.first.assert_called_once()


    # evolveSchema tests

    def test_evolveSchema_newColumn(self):
        # arrange
        sourceDF = self._spark.createDataFrame([
            [1, "value2", "value3"],
        ], "col1 INTEGER, col2 STRING, newcol3 STRING")

        mockSqlTarget = MagicMock()
        mockSqlTarget.limit.return_value.columns =  ["col1", "col2"]
        self._dataLakeHelper._spark.sql.return_value = mockSqlTarget
        self._dataLakeHelper.allowAlterForTable = MagicMock()

        # act
        self._dataLakeHelper._evolveSchema("dbName", "tableName", sourceDF)

        # assert
        self._dataLakeHelper.allowAlterForTable.assert_called_once_with("dbName.tableName")
        self._dataLakeHelper._spark.sql.assert_called_with(f"ALTER TABLE dbName.tableName ADD COLUMNS (`newcol3` string)")

    def test_evolveSchema_newMultipleColumns(self):
        # arrange
        sourceDF = self._spark.createDataFrame([
            [1, "value2", "value3", 4],
        ], "col1 INTEGER, col2 STRING, newcol3 STRING, newcol4 INTEGER")

        mockSqlTarget = MagicMock()
        mockSqlTarget.limit.return_value.columns =  ["col1", "col2"]
        self._dataLakeHelper._spark.sql.return_value = mockSqlTarget
        self._dataLakeHelper.allowAlterForTable = MagicMock()

        # act
        self._dataLakeHelper._evolveSchema("dbName", "tableName", sourceDF)

        # assert
        self._dataLakeHelper.allowAlterForTable.assert_called_once_with("dbName.tableName")
        self._dataLakeHelper._spark.sql.assert_called_with(f"ALTER TABLE dbName.tableName ADD COLUMNS (`newcol3` string, `newcol4` int)")

    def test_evolveSchema_noNewColumn(self):
        # arrange
        sourceDF = self._spark.createDataFrame([
            [1, "value2"],
        ], "col1 INTEGER, col2 STRING")

        mockSqlTarget = MagicMock()
        mockSqlTarget.limit.return_value.columns =  ["col1", "col2"]
        self._dataLakeHelper._spark.sql.return_value = mockSqlTarget
        self._dataLakeHelper.allowAlterForTable = MagicMock()

        # act
        self._dataLakeHelper._evolveSchema("dbName", "tableName", sourceDF)

        # assert
        self._dataLakeHelper.allowAlterForTable.assert_not_called()
        self._dataLakeHelper._spark.sql.assert_called_once_with("SELECT * FROM dbName.tableName")
    

    # _registerTableInHive tests

    def test_registerTableInHive(self):
        # act
        self._dataLakeHelper._registerTableInHive("path/to/data", "dbName", "tableName")

        # assert        
        self._dataLakeHelper._spark.sql.assert_called_once()

        sqlCommand = self._dataLakeHelper._spark.sql.call_args_list[0].args[0]
        self.assertIn("CREATE TABLE IF NOT EXISTS dbName.tableName", sqlCommand)
        self.assertIn("\"path/to/data\"", sqlCommand)

    def test_registerTableInHive_wrongArguments(self):
        # act
        self._dataLakeHelper._registerTableInHive("", "dbName", "tableName")

        # assert
        self._dataLakeHelper._spark.sql.assert_not_called()

        # act
        self._dataLakeHelper._registerTableInHive("path/to/data", "", "tableName")

        # assert
        self._dataLakeHelper._spark.sql.assert_not_called()

        # act
        self._dataLakeHelper._registerTableInHive("path/to/data", "dbName", None)

        # assert
        self._dataLakeHelper._spark.sql.assert_not_called()


    # registerTableWithIdentityColumnInHive tests

    def test_registerTableWithIdentityColumnInHive_withIdentityColumn(self):
        # arrange
        sinkDF = self._spark.createDataFrame([
            [1, "test"],
        ], "id INTEGER, value STRING")
        
        params = {
            "createIdentityColumn": True,
        }

        # act
        tableRegistered = self._dataLakeHelper._registerTableWithIdentityColumnInHive(sinkDF, "path/to/data", "delta", params, "dbName", "tableName")

        # assert
        self.assertTrue(tableRegistered)

        self._dataLakeHelper._spark.sql.assert_called_once()

        sqlCommand = self._dataLakeHelper._spark.sql.call_args_list[0].args[0]
        self.assertIn("CREATE TABLE IF NOT EXISTS dbName.tableName", sqlCommand)
        self.assertIn("AS IDENTITY (START WITH 1 INCREMENT BY 1)", sqlCommand)
        self.assertIn("USING DELTA", sqlCommand)
        self.assertIn("\"path/to/data\"", sqlCommand)

    def test_registerTableWithIdentityColumnInHive_withoutIdentityColumn(self):
        # arrange
        sinkDF = self._spark.createDataFrame([
            [1, "test"],
        ], "id INTEGER, value STRING")
        
        params = {}

        # act
        tableRegistered = self._dataLakeHelper._registerTableWithIdentityColumnInHive(sinkDF, "path/to/data", "delta", params, "dbName", "tableName")

        # assert
        self.assertFalse(tableRegistered)

        self._dataLakeHelper._spark.sql.assert_not_called()


    # validateWriteMode tests

    def test_validateWriteMode_SCDTypes_noDelta(self):
        # arrange
        self._dataLakeHelper._DeltaTable.isDeltaTable.return_value = False

        # act
        newWriteMode = self._dataLakeHelper._validateWriteMode(WRITE_MODE_SCD_TYPE1, "delta", "path/to/data")

        # assert
        self.assertEqual(newWriteMode, WRITE_MODE_OVERWRITE)
        self._dataLakeHelper._DeltaTable.isDeltaTable.assert_called_with(self._dataLakeHelper._spark, "path/to/data")

        # act
        newWriteMode = self._dataLakeHelper._validateWriteMode(WRITE_MODE_SCD_TYPE2, "delta", "path/to/data")

        # assert
        self.assertEqual(newWriteMode, WRITE_MODE_OVERWRITE)
        self._dataLakeHelper._DeltaTable.isDeltaTable.assert_called_with(self._dataLakeHelper._spark, "path/to/data")

    def test_validateWriteMode_SCDTypes_existingDelta(self):
        # arrange
        self._dataLakeHelper._DeltaTable.isDeltaTable.return_value = True

        # act
        newWriteMode = self._dataLakeHelper._validateWriteMode(WRITE_MODE_SCD_TYPE1, "delta", "path/to/data")

        # assert
        self.assertEqual(newWriteMode, WRITE_MODE_SCD_TYPE1)
        self._dataLakeHelper._DeltaTable.isDeltaTable.assert_called_with(self._dataLakeHelper._spark, "path/to/data")

        # act
        newWriteMode = self._dataLakeHelper._validateWriteMode(WRITE_MODE_SCD_TYPE2, "delta", "path/to/data")

        # assert
        self.assertEqual(newWriteMode, WRITE_MODE_SCD_TYPE2)
        self._dataLakeHelper._DeltaTable.isDeltaTable.assert_called_with(self._dataLakeHelper._spark, "path/to/data")

    def test_validateWriteMode_SCDType2Delete_noDelta(self):
        # arrange
        self._dataLakeHelper._DeltaTable.isDeltaTable.return_value = False

        # act
        newWriteMode = self._dataLakeHelper._validateWriteMode(WRITE_MODE_SCD_TYPE2_DELETE, "delta", "path/to/data")

        # assert
        self.assertEqual(newWriteMode, None)
        self._dataLakeHelper._DeltaTable.isDeltaTable.assert_called_with(self._dataLakeHelper._spark, "path/to/data")

    def test_validateWriteMode_SCDType2Delete_existingDelta(self):
        # arrange
        self._dataLakeHelper._DeltaTable.isDeltaTable.return_value = True

        # act
        newWriteMode = self._dataLakeHelper._validateWriteMode(WRITE_MODE_SCD_TYPE2_DELETE, "delta", "path/to/data")

        # assert
        self.assertEqual(newWriteMode, WRITE_MODE_SCD_TYPE2_DELETE)
        self._dataLakeHelper._DeltaTable.isDeltaTable.assert_called_with(self._dataLakeHelper._spark, "path/to/data")

    def test_validateWriteMode_Append(self):
        # act
        newWriteMode = self._dataLakeHelper._validateWriteMode(WRITE_MODE_APPEND, "delta", "path/to/data")

        # assert
        self.assertEqual(newWriteMode, WRITE_MODE_APPEND)

    def test_validateWriteMode_NotDeltaFormat(self):
        # act
        newWriteMode = self._dataLakeHelper._validateWriteMode(WRITE_MODE_APPEND, "parquet", "path/to/data")

        # assert
        self.assertEqual(newWriteMode, WRITE_MODE_APPEND)


    # writeData tests

    def test_writeData_noneSinkDF(self):
        # arrange
        sinkDF = None

        # act
        statistics = self._dataLakeHelper.writeData(sinkDF, "/path/to/data", "writeMode", "delta", {}, "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

    def test_writeData_noneNewWiteMode(self):
        # arrange
        sinkDF = self._spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        writeMode = WRITE_MODE_SCD_TYPE2_DELETE
        dataPath = "/path/to/data"
        format = "delta"

        self._dataLakeHelper._validateWriteMode = MagicMock(return_value=None)
        self._dataLakeHelper._registerTableInHive = MagicMock()

        # act
        statistics = self._dataLakeHelper.writeData(sinkDF, dataPath, writeMode, format, {}, "databaseName", "tableName")
        
        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper._validateWriteMode.assert_called_once_with(writeMode, format, dataPath)
        self._dataLakeHelper._registerTableInHive.assert_not_called()

    def test_writeData_SCDType1(self):
        # arrange
        sinkDF = self._spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        writeMode = WRITE_MODE_SCD_TYPE1

        expectedStatistics = {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }

        self._dataLakeHelper._validateWriteMode = MagicMock(return_value=writeMode)
        self._dataLakeHelper._writeDataSCDType1 = MagicMock(return_value=(expectedStatistics))
        self._dataLakeHelper._registerTableInHive = MagicMock()

        # act
        statistics = self._dataLakeHelper.writeData(sinkDF, "/path/to/data", writeMode, "delta", {}, "databaseName", "tableName")
        
        # assert
        self.assertEqual(statistics, expectedStatistics)

        self._dataLakeHelper._validateWriteMode.assert_called_once()
        self._dataLakeHelper._writeDataSCDType1.assert_called_once()
        self._dataLakeHelper._registerTableInHive.assert_called_once()

    def test_writeData_SCDType2_firstWrite(self):
        # arrange
        sinkDF = self._spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        writeMode = WRITE_MODE_SCD_TYPE2
        newWriteMode = WRITE_MODE_OVERWRITE

        expectedStatistics = {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }

        self._dataLakeHelper._validateWriteMode = MagicMock(return_value=newWriteMode)
        self._dataLakeHelper._prepareDataForSCDType2FirstWrite = MagicMock(return_value=ANY)
        self._dataLakeHelper._writeDataOverwriteOrAppend = MagicMock(return_value=expectedStatistics)
        self._dataLakeHelper._writeDataSCDType2 = MagicMock()
        self._dataLakeHelper._registerTableInHive = MagicMock()

        # act
        statistics = self._dataLakeHelper.writeData(sinkDF, "/path/to/data", writeMode, "delta", {}, "databaseName", "tableName")
        
        # assert
        self.assertEqual(statistics, expectedStatistics)

        self._dataLakeHelper._validateWriteMode.assert_called_once()

        self._dataLakeHelper._prepareDataForSCDType2FirstWrite.assert_called_once()
        self._dataLakeHelper._writeDataOverwriteOrAppend.assert_called_once()
        self._dataLakeHelper._writeDataSCDType2.assert_not_called()

        self._dataLakeHelper._registerTableInHive.assert_called_once()

    def test_writeData_SCDType2_secondWrite(self):
        # arrange
        sinkDF = self._spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        writeMode = WRITE_MODE_SCD_TYPE2

        expectedStatistics = {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }

        self._dataLakeHelper._validateWriteMode = MagicMock(return_value=writeMode)
        self._dataLakeHelper._writeDataSCDType2 = MagicMock(return_value=(expectedStatistics))
        self._dataLakeHelper._prepareDataForSCDType2FirstWrite = MagicMock()
        self._dataLakeHelper._writeDataOverwriteOrAppend = MagicMock()
        self._dataLakeHelper._registerTableInHive = MagicMock()

        # act
        statistics = self._dataLakeHelper.writeData(sinkDF, "/path/to/data", writeMode, "delta", {}, "databaseName", "tableName")
        
        # assert
        self.assertEqual(statistics, expectedStatistics)

        self._dataLakeHelper._validateWriteMode.assert_called_once()

        self._dataLakeHelper._writeDataSCDType2.assert_called_once()
        writeModeArg = self._dataLakeHelper._writeDataSCDType2.call_args_list[0].args[2]
        self.assertEqual(writeModeArg, writeMode)

        self._dataLakeHelper._prepareDataForSCDType2FirstWrite.assert_not_called()
        self._dataLakeHelper._writeDataOverwriteOrAppend.assert_not_called()

        self._dataLakeHelper._registerTableInHive.assert_called_once()

    def test_writeData_SCDType2Delete(self):
        # arrange
        sinkDF = self._spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        writeMode = WRITE_MODE_SCD_TYPE2_DELETE

        expectedStatistics = {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }

        self._dataLakeHelper._validateWriteMode = MagicMock(return_value=writeMode)
        self._dataLakeHelper._writeDataSCDType2Delete = MagicMock(return_value=(expectedStatistics))
        self._dataLakeHelper._registerTableInHive = MagicMock()

        # act
        statistics = self._dataLakeHelper.writeData(sinkDF, "/path/to/data", writeMode, "delta", {}, "databaseName", "tableName")
        
        # assert
        self.assertEqual(statistics, expectedStatistics)

        self._dataLakeHelper._validateWriteMode.assert_called_once()
        self._dataLakeHelper._writeDataSCDType2Delete.assert_called_once()
        self._dataLakeHelper._registerTableInHive.assert_called_once()

    def test_writeData_Snapshot(self):
        # arrange
        sinkDF = self._spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        writeMode = WRITE_MODE_SNAPSHOT

        expectedStatistics = {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }

        self._dataLakeHelper._validateWriteMode = MagicMock(return_value=writeMode)
        self._dataLakeHelper._writeDataSnapshot = MagicMock(return_value=(expectedStatistics))
        self._dataLakeHelper._registerTableInHive = MagicMock()

        # act
        statistics = self._dataLakeHelper.writeData(sinkDF, "/path/to/data", writeMode, "delta", {}, "databaseName", "tableName")
        
        # assert
        self.assertEqual(statistics, expectedStatistics)

        self._dataLakeHelper._validateWriteMode.assert_called_once()
        self._dataLakeHelper._writeDataSnapshot.assert_called_once()
        self._dataLakeHelper._registerTableInHive.assert_called_once()

    def test_writeData_OverwriteOrAppend(self):
        # arrange
        sinkDF = self._spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        writeMode = WRITE_MODE_APPEND

        expectedStatistics = {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }

        self._dataLakeHelper._validateWriteMode = MagicMock(return_value=writeMode)
        self._dataLakeHelper._writeDataOverwriteOrAppend = MagicMock(return_value=(expectedStatistics))
        self._dataLakeHelper._registerTableInHive = MagicMock()

        # act
        statistics = self._dataLakeHelper.writeData(sinkDF, "/path/to/data", writeMode, "delta", {}, "databaseName", "tableName")
        
        # assert
        self.assertEqual(statistics, expectedStatistics)

        self._dataLakeHelper._validateWriteMode.assert_called_once()
        self._dataLakeHelper._writeDataOverwriteOrAppend.assert_called_once()
        self._dataLakeHelper._registerTableInHive.assert_called_once()


    # writeDataSCDType1 tests

    def test_writeDataSCDType1_shouldNotWriteEmptyDF(self):
        # arrange
        sinkDF = self._spark.createDataFrame(self._spark.sparkContext.emptyRDD(), f"col BOOLEAN")
        
        # act
        statistics = self._dataLakeHelper._writeDataSCDType1(sinkDF, "/path/to/data", "params", "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper._DeltaTable.forPath.assert_not_called()

    def test_writeDataSCDType1_shouldEvolveSchemaAndRemoveIdentityColumnAndWriteData(self):
        # arrange
        sinkDF = self._spark.createDataFrame(
            [
                (1, "A", "John", "Doe", datetime.now()),
                (2, "A", "Jane", "Doe", datetime.now()),
                (3, "B", "Bob", "Spark", datetime.now()),
            ],
            ["id", "secondKey", "firstName", "lastName", INSERT_TIME_COLUMN_NAME]
        )

        params = {
            "keyColumns": ["id", "secondKey"],
            "schemaEvolution": True
        }

        self._dataLakeHelper._evolveSchema = MagicMock()

        sqlCommandDF = MagicMock()
        self._dataLakeHelper._spark.sql.return_value = sqlCommandDF
        # table schema in HIVE after schema evolution
        sqlCommandDF.limit.return_value.columns = ["wk_identity", "id", "secondKey", "firstName", "lastName", INSERT_TIME_COLUMN_NAME]
        sqlCommandDF.count.return_value = 1 # inserted count

        self._dataLakeHelper._getIdentityColumn = MagicMock(return_value={ "Attribute": "wk_identity" })

        deltaTableMock = MagicMock()
        self._dataLakeHelper._DeltaTable.forPath.return_value = deltaTableMock
        deltaTableMock.alias.return_value = deltaTableMock
        deltaTableMock.merge.return_value = deltaTableMock
        deltaTableMock.whenMatchedUpdate.return_value = deltaTableMock
        deltaTableMock.whenNotMatchedInsert.return_value = deltaTableMock
        deltaTableMock.execute.return_value = deltaTableMock
        
        expectedMergeStatement = "table.id = updates.id AND table.secondKey = updates.secondKey"

        expectedInsertStatement = {
            "id": "updates.id",
            "secondKey": "updates.secondKey",
            "firstName": "updates.firstName",
            "lastName": "updates.lastName",
            INSERT_TIME_COLUMN_NAME: f"updates.{INSERT_TIME_COLUMN_NAME}"
        }
        expectedUpdateStatement = {
            "id": "updates.id",
            "secondKey": "updates.secondKey",
            "firstName": "updates.firstName",
            "lastName": "updates.lastName"
        }

        # act
        statistics = self._dataLakeHelper._writeDataSCDType1(sinkDF, "/path/to/data", params, "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 1,
            "recordsUpdated": 2,
            "recordsDeleted": 0
        })

        self._dataLakeHelper._evolveSchema.assert_called_once()
        self._dataLakeHelper._getIdentityColumn.assert_called_once()

        self._dataLakeHelper._DeltaTable.forPath.assert_called_once_with(self._dataLakeHelper._spark, "/path/to/data")
        deltaTableMock.alias.assert_called_once_with("table")
        deltaTableMock.merge.assert_called_once_with(ANY, expectedMergeStatement)
        deltaTableMock.whenMatchedUpdate.assert_called_once_with(set = expectedUpdateStatement)
        deltaTableMock.whenNotMatchedInsert.assert_called_once_with(values = expectedInsertStatement)
        deltaTableMock.execute.assert_called_once()

    def test_writeDataSCDType1_shouldNotEvolveSchemaAndRemoveIdentityColumnAndWriteData(self):
        # arrange
        sinkDF = self._spark.createDataFrame(
            [
                (1, "A", "John", "Doe", datetime.now()),
                (2, "A", "Jane", "Doe", datetime.now()),
                (3, "B", "Bob", "Spark", datetime.now()),
            ],
            ["id", "secondKey", "firstName", "lastName", INSERT_TIME_COLUMN_NAME]
        )
        # lastName colum will be ignored

        params = {
            "keyColumns": ["id", "secondKey"],
            "schemaEvolution": False
        }

        self._dataLakeHelper._evolveSchema = MagicMock()

        sqlCommandDF = MagicMock()
        self._dataLakeHelper._spark.sql.return_value = sqlCommandDF
        # table schema in HIVE after schema evolution
        sqlCommandDF.limit.return_value.columns = ["wk_identity", "id", "secondKey", "firstName", INSERT_TIME_COLUMN_NAME]
        sqlCommandDF.count.return_value = 1 # inserted count

        self._dataLakeHelper._getIdentityColumn = MagicMock(return_value={ "Attribute": "wk_identity" })

        deltaTableMock = MagicMock()
        self._dataLakeHelper._DeltaTable.forPath.return_value = deltaTableMock
        deltaTableMock.alias.return_value = deltaTableMock
        deltaTableMock.merge.return_value = deltaTableMock
        deltaTableMock.whenMatchedUpdate.return_value = deltaTableMock
        deltaTableMock.whenNotMatchedInsert.return_value = deltaTableMock
        deltaTableMock.execute.return_value = deltaTableMock
        
        expectedMergeStatement = "table.id = updates.id AND table.secondKey = updates.secondKey"

        expectedInsertStatement = {
            "id": "updates.id",
            "secondKey": "updates.secondKey",
            "firstName": "updates.firstName",
            INSERT_TIME_COLUMN_NAME: f"updates.{INSERT_TIME_COLUMN_NAME}"
        }
        expectedUpdateStatement = {
            "id": "updates.id",
            "secondKey": "updates.secondKey",
            "firstName": "updates.firstName",
        }

        # act
        statistics = self._dataLakeHelper._writeDataSCDType1(sinkDF, "/path/to/data", params, "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 1,
            "recordsUpdated": 2,
            "recordsDeleted": 0
        })

        self._dataLakeHelper._evolveSchema.assert_not_called()
        self._dataLakeHelper._getIdentityColumn.assert_called_once()

        self._dataLakeHelper._DeltaTable.forPath.assert_called_once_with(self._dataLakeHelper._spark, "/path/to/data")
        deltaTableMock.alias.assert_called_once_with("table")
        deltaTableMock.merge.assert_called_once_with(ANY, expectedMergeStatement)
        deltaTableMock.whenMatchedUpdate.assert_called_once_with(set = expectedUpdateStatement)
        deltaTableMock.whenNotMatchedInsert.assert_called_once_with(values = expectedInsertStatement)
        deltaTableMock.execute.assert_called_once()


    # calculateHashesForSCDType2 tests

    def test_calculateHashesForSCDType2(self):
        # arrange
        now = datetime.now()
        sinkDF = self._spark.createDataFrame(
            [
                (1, "A", "John", "Doe", "VALID", now, now),
                (2, "A", "Jane", "Doe", "VALID", now, now),
                (2, "A", "Jane", "Doe", "INVALID", now, now),
                (3, "B", "Bob", "Spark", "INVALID", now, now)
            ],
            ["id", "secondKey", "firstName", "lastName", VALIDATION_STATUS_COLUMN_NAME, INSERT_TIME_COLUMN_NAME, UPDATE_TIME_COLUMN_NAME]
        )

        params = {
            "keyColumns": ["id", "secondKey"]
        }

        # act
        (resultDF, valColumns) = self._dataLakeHelper._calculateHashesForSCDType2(sinkDF, params)

        # assert
        self.assertIn(BUSINESS_KEYS_HASH_COLUMN_NAME, resultDF.columns)
        self.assertIn(VALUE_KEY_HASH_COLUMN_NAME, resultDF.columns)

        result = resultDF.rdd.collect()

        self.assertNotEqual(result[0][BUSINESS_KEYS_HASH_COLUMN_NAME], result[1][BUSINESS_KEYS_HASH_COLUMN_NAME])
        self.assertEqual(result[1][BUSINESS_KEYS_HASH_COLUMN_NAME], result[2][BUSINESS_KEYS_HASH_COLUMN_NAME])
        self.assertNotEqual(result[2][BUSINESS_KEYS_HASH_COLUMN_NAME], result[3][BUSINESS_KEYS_HASH_COLUMN_NAME])

        self.assertNotEqual(result[0][VALUE_KEY_HASH_COLUMN_NAME], result[1][VALUE_KEY_HASH_COLUMN_NAME])
        self.assertEqual(result[1][VALUE_KEY_HASH_COLUMN_NAME], result[2][VALUE_KEY_HASH_COLUMN_NAME])
        self.assertNotEqual(result[2][VALUE_KEY_HASH_COLUMN_NAME], result[3][VALUE_KEY_HASH_COLUMN_NAME])

        self.assertEqual(valColumns, ["firstName", "lastName"])


    # handleWatermarkColumnForSCDType2 tests 

    def test_handleWatermarkColumnForSCDType2_shouldCreateWatermarkColumnIfMissing(self):
        # arrange
        sinkDF = self._spark.createDataFrame(
            [
                (1, "A", "John", "Doe"),
                (2, "A", "Jane", "Doe"),
                (3, "B", "Bob", "Spark")
            ],
            ["id", "secondKey", "firstName", "lastName"]
        )

        expectedWatermarkValue = datetime.now()

        params = {
            "entTriggerTime": expectedWatermarkValue
        }

        # act
        (resultDF, resultParams) = self._dataLakeHelper._handleWatermarkColumnForSCDType2(sinkDF, params, True)

        # assert
        self.assertIn("watermarkColumn", resultParams.keys())
        
        watermarkColumnName = resultParams["watermarkColumn"]
        
        self.assertIn(watermarkColumnName, resultDF.columns)
        self.assertEqual(len(resultDF.columns), 5)

        result = resultDF.rdd.collect()

        self.assertEqual(result[0][watermarkColumnName], expectedWatermarkValue)
        self.assertEqual(result[1][watermarkColumnName], expectedWatermarkValue)
        self.assertEqual(result[2][watermarkColumnName], expectedWatermarkValue)

    def test_handleWatermarkColumnForSCDType2_shouldNotCreateWatermarkColumnIfNotMissing(self):
        # arrange
        sinkDF = self._spark.createDataFrame(
            [
                (1, "A", "John", "Doe", "wmk val A"),
                (2, "A", "Jane", "Doe", "wmk val B"),
                (3, "B", "Bob", "Spark", "wmk val C")
            ],
            ["id", "secondKey", "firstName", "lastName", "wmkColumn"]
        )

        params = {
            "entTriggerTime": datetime.now(),
            "watermarkColumn": "wmkColumn"
        }

        # act
        (resultDF, resultParams) = self._dataLakeHelper._handleWatermarkColumnForSCDType2(sinkDF, params, True)

        # assert
        self.assertIn("watermarkColumn", resultParams.keys())
        
        watermarkColumnName = resultParams["watermarkColumn"]
        self.assertEqual(watermarkColumnName, "wmkColumn")

        self.assertIn(watermarkColumnName, resultDF.columns)
        self.assertEqual(len(resultDF.columns), 5)

        result = resultDF.rdd.collect()

        self.assertEqual(result[0][watermarkColumnName], "wmk val A")
        self.assertEqual(result[1][watermarkColumnName], "wmk val B")
        self.assertEqual(result[2][watermarkColumnName], "wmk val C")

    def test_handleWatermarkColumnForSCDType2_shouldCreateWatermarkColumnIfMissing2(self):
        # arrange
        sinkDF = self._spark.createDataFrame(
            [
                (1, "A", "John", "Doe"),
                (2, "A", "Jane", "Doe"),
                (3, "B", "Bob", "Spark")
            ],
            ["id", "secondKey", "firstName", "lastName"]
        )

        expectedWatermarkValue = datetime.now()

        params = {
            "entTriggerTime": expectedWatermarkValue
        }

        # act
        (resultDF, resultParams) = self._dataLakeHelper._handleWatermarkColumnForSCDType2(sinkDF, params, False)

        # assert
        self.assertIn("watermarkColumn", resultParams.keys())
        
        watermarkColumnName = resultParams["watermarkColumn"]
        
        self.assertIn(watermarkColumnName, resultDF.columns)
        self.assertEqual(len(resultDF.columns), 5)

        result = resultDF.rdd.collect()

        self.assertEqual(result[0][watermarkColumnName], expectedWatermarkValue)
        self.assertEqual(result[1][watermarkColumnName], expectedWatermarkValue)
        self.assertEqual(result[2][watermarkColumnName], expectedWatermarkValue)

    def test_handleWatermarkColumnForSCDType2_shouldReplaceWatermarkColumnValues(self):
        # arrange
        sinkDF = self._spark.createDataFrame(
            [
                (1, "A", "John", "Doe", "wmk val A"),
                (2, "A", "Jane", "Doe", "wmk val B"),
                (3, "B", "Bob", "Spark", "wmk val C")
            ],
            ["id", "secondKey", "firstName", "lastName", "wmkColumn"]
        )

        expectedWatermarkValue = datetime.now()

        params = {
            "entTriggerTime": expectedWatermarkValue,
            "watermarkColumn": "wmkColumn"
        }

        # act
        (resultDF, resultParams) = self._dataLakeHelper._handleWatermarkColumnForSCDType2(sinkDF, params, False)

        # assert
        self.assertIn("watermarkColumn", resultParams.keys())
        
        watermarkColumnName = resultParams["watermarkColumn"]
        self.assertEqual(watermarkColumnName, "wmkColumn")

        self.assertIn(watermarkColumnName, resultDF.columns)
        self.assertEqual(len(resultDF.columns), 5)

        result = resultDF.rdd.collect()

        self.assertEqual(result[0][watermarkColumnName], expectedWatermarkValue)
        self.assertEqual(result[1][watermarkColumnName], expectedWatermarkValue)
        self.assertEqual(result[2][watermarkColumnName], expectedWatermarkValue)


    # prepareDataForSCDType2FirstWrite tests

    def test_prepareDataForSCDType2FirstWrite(self):
        # arrange
        sinkDF = self._spark.createDataFrame(
            [
                (1, "A", "John", "Doe"),
                (2, "A", "Jane", "Doe"),
                (3, "B", "Bob", "Spark")
            ],
            ["id", "secondKey", "firstName", "lastName"]
        )

        entTriggerTime = datetime.now()
        params = {
            "keyColumns": ["id", "secondKey"],
            "entTriggerTime": entTriggerTime
        }

        maxDate = datetime(2999, 12, 31) # on Windows datetime(9999, 12, 31) throws OverflowError
        self._dataLakeHelper._getMaxDate = MagicMock(return_value=maxDate)

        expectedResultDF = self._spark.createDataFrame(
            [
                (1, "A", "John", "Doe", True, False, entTriggerTime, maxDate, ANY, ANY),
                (2, "A", "Jane", "Doe", True, False, entTriggerTime, maxDate, ANY, ANY),
                (3, "B", "Bob", "Spark", True, False, entTriggerTime, maxDate, ANY, ANY)
            ],
            [
                "id",
                "secondKey",
                "firstName",
                "lastName",
                IS_ACTIVE_RECORD_COLUMN_NAME,
                IS_DELETED_RECORD_COLUMN_NAME,
                VALID_FROM_DATETIME_COLUMN_NAME,
                VALID_UNTIL_DATETIME_COLUMN_NAME,
                BUSINESS_KEYS_HASH_COLUMN_NAME,
                VALUE_KEY_HASH_COLUMN_NAME
            ]
        )

        # act
        resultDF = self._dataLakeHelper._prepareDataForSCDType2FirstWrite(sinkDF, params)

        # assert
        self.assertIn(BUSINESS_KEYS_HASH_COLUMN_NAME, resultDF.columns)
        self.assertIn(VALUE_KEY_HASH_COLUMN_NAME, resultDF.columns)

        self.assertEqual(
            resultDF.drop(BUSINESS_KEYS_HASH_COLUMN_NAME, VALUE_KEY_HASH_COLUMN_NAME).rdd.collect(),
            expectedResultDF.drop(BUSINESS_KEYS_HASH_COLUMN_NAME, VALUE_KEY_HASH_COLUMN_NAME).rdd.collect()
        )


    # writeDataSCDType2 tests - will be covered by integration tests
    # writeDataSCDType2Delete tests - will be covered by integration tests 


    # deleteSnapshotData tests

    def test_deleteSnapshotData_shouldNotDeleteDataFromNonExistingTableOrEmptyDF(self):
        # arrange
        sinkDF = self._spark.createDataFrame(
            [
                (1, "Snapshot 1 value 1"),
                (2, "Snapshot 2 value 1")
            ],
            ["id", "value"]
        )
        self._dataLakeHelper._sqlContext.tableNames.return_value = ["secondtable"]
        params = {
            "keyColumns": ["id"]
        }

        # act
        countDeleted = self._dataLakeHelper._deleteSnapshotData(sinkDF, params, "databaseName", "tableName")

        # assert
        self.assertEqual(countDeleted, 0)
        self._dataLakeHelper._spark.sql.assert_not_called()
        
        # arrange
        sinkDF = self._spark.createDataFrame(self._spark.sparkContext.emptyRDD(), f"col BOOLEAN")
        self._dataLakeHelper._sqlContext.tableNames.return_value = ["tablename", "secondtable"]

        # act
        countDeleted = self._dataLakeHelper._deleteSnapshotData(sinkDF, params, "databaseName", "tableName")

        # assert
        self.assertEqual(countDeleted, 0)
        self._dataLakeHelper._spark.sql.assert_not_called()

    def test_deleteSnapshotData_shouldNotDeleteDataIfTheresNoDataForProvidedSnapshotKeys(self):
        # arrange
        sinkDF = self._spark.createDataFrame(
            [
                (1, "Snapshot 1 value 1"),
                (1, "Snapshot 1 value 2"),
                (1, "Snapshot 1 value 3"),
                (2, "Snapshot 2 value 1"),
            ],
            ["id", "value"]
        )
        self._dataLakeHelper._sqlContext.tableNames.return_value = ["tablename", "secondtable"]
        params = {
            "keyColumns": ["id"]
        }

        sqlCommandDF = MagicMock()
        self._dataLakeHelper._spark.sql.return_value = sqlCommandDF
        sqlCommandDF.first.return_value = {
            "countDeleted": 0
        }

        # act
        countDeleted = self._dataLakeHelper._deleteSnapshotData(sinkDF, params, "databaseName", "tableName")

        # assert
        self.assertEqual(countDeleted, 0)
        
        self.assertEqual(self._dataLakeHelper._spark.sql.call_count, 1)

        arguments = self._dataLakeHelper._spark.sql.call_args_list[0].args
        self.assertIn("SELECT", arguments[0])
        self.assertIn("FROM databaseName.tableName", arguments[0])
        self.assertIn("WHERE (id = '1') OR (id = '2')", arguments[0])

    def test_deleteSnapshotData_shouldDeleteDataFromExistingTable(self):
        # arrange
        sinkDF = self._spark.createDataFrame(
            [
                (1, "Snapshot 1 value 1"),
                (1, "Snapshot 1 value 2"),
                (1, "Snapshot 1 value 3"),
                (2, "Snapshot 2 value 1"),
            ],
            ["id", "value"]
        )
        self._dataLakeHelper._sqlContext.tableNames.return_value = ["tablename", "secondtable"]
        params = {
            "keyColumns": ["id"]
        }

        sqlCommandDF = MagicMock()
        self._dataLakeHelper._spark.sql.return_value = sqlCommandDF
        sqlCommandDF.first.return_value = {
            "countDeleted": 1
        }

        expectedFromStatement = "FROM databaseName.tableName"
        expectedWhereStatement = "WHERE (id = '1') OR (id = '2')"

        # act
        countDeleted = self._dataLakeHelper._deleteSnapshotData(sinkDF, params, "databaseName", "tableName")

        # assert
        self.assertEqual(countDeleted, 1)
        
        self.assertEqual(self._dataLakeHelper._spark.sql.call_count, 2)

        arguments = self._dataLakeHelper._spark.sql.call_args_list[0].args
        self.assertIn("SELECT", arguments[0])
        self.assertIn(expectedFromStatement, arguments[0])
        self.assertIn(expectedWhereStatement, arguments[0])

        arguments = self._dataLakeHelper._spark.sql.call_args_list[1].args
        self.assertIn("DELETE", arguments[0])
        self.assertIn(expectedFromStatement, arguments[0])
        self.assertIn(expectedWhereStatement, arguments[0])

    def test_deleteSnapshotData_shouldDeleteDataFromExistingTableByTwoKeys(self):
        # arrange
        sinkDF = self._spark.createDataFrame(
            [
                (1, "A", "Snapshot 1 value 1"),
                (1, "B", "Snapshot 1 value 2"),
                (1, "B", "Snapshot 1 value 3"),
                (2, "A", "Snapshot 2 value 1"),
            ],
            ["id", "secondaryKey", "value"]
        )
        self._dataLakeHelper._sqlContext.tableNames.return_value = ["tablename", "secondtable"]
        params = {
            "keyColumns": ["id", "secondaryKey"]
        }

        sqlCommandDF = MagicMock()
        self._dataLakeHelper._spark.sql.return_value = sqlCommandDF
        sqlCommandDF.first.return_value = {
            "countDeleted": 1
        }

        expectedFromStatement = "FROM databaseName.tableName"
        expectedWhereStatement = "WHERE (id = '1' AND secondaryKey = 'A') OR (id = '1' AND secondaryKey = 'B') OR (id = '2' AND secondaryKey = 'A')"

        # act
        countDeleted = self._dataLakeHelper._deleteSnapshotData(sinkDF, params, "databaseName", "tableName")

        # assert
        self.assertEqual(countDeleted, 1)
        
        self.assertEqual(self._dataLakeHelper._spark.sql.call_count, 2)

        arguments = self._dataLakeHelper._spark.sql.call_args_list[0].args
        self.assertIn("SELECT", arguments[0])
        self.assertIn(expectedFromStatement, arguments[0])
        self.assertIn(expectedWhereStatement, arguments[0])

        arguments = self._dataLakeHelper._spark.sql.call_args_list[1].args
        self.assertIn("DELETE", arguments[0])
        self.assertIn(expectedFromStatement, arguments[0])
        self.assertIn(expectedWhereStatement, arguments[0])


    # writeDataSnapshot tests

    def test_writeDataSnapshot_shouldNotWriteEmptyDF(self):
        # arrange
        sinkDF = MagicMock()
        sinkDF.count.return_value = 0
        sinkDF.write.format.return_value = sinkDF
        sinkDF.mode.return_value = sinkDF
        sinkDF.option.return_value = sinkDF

        # act
        statistics = self._dataLakeHelper._writeDataSnapshot(sinkDF, "/path/to/data", "delta", {}, "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        sinkDF.save.assert_not_called()
    
    def test_writeDataSnapshot_shouldCreateNewSnapshot(self):
        # arrange
        sinkDF = MagicMock()
        sinkDF.count.return_value = 3
        sinkDF.write.format.return_value = sinkDF
        sinkDF.mode.return_value = sinkDF
        sinkDF.option.return_value = sinkDF

        self._dataLakeHelper._deleteSnapshotData = MagicMock(return_value = 0)

        # act
        statistics = self._dataLakeHelper._writeDataSnapshot(sinkDF, "/path/to/data", "delta", {}, "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        sinkDF.write.format.assert_called_once_with("delta")
        sinkDF.mode.assert_called_once_with("append")
        sinkDF.option.assert_called_once_with("mergeSchema", "false")
        sinkDF.save.assert_called_once_with("/path/to/data")

    def test_writeDataSnapshot_shouldReplaceExistingSnapshot(self):
        # arrange
        sinkDF = MagicMock()
        sinkDF.count.return_value = 3
        sinkDF.write.format.return_value = sinkDF
        sinkDF.mode.return_value = sinkDF
        sinkDF.option.return_value = sinkDF

        self._dataLakeHelper._deleteSnapshotData = MagicMock(return_value = 10)

        # act
        statistics = self._dataLakeHelper._writeDataSnapshot(sinkDF, "/path/to/data", "delta", {}, "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 10
        })

        sinkDF.write.format.assert_called_once_with("delta")
        sinkDF.mode.assert_called_once_with("append")
        sinkDF.option.assert_called_once_with("mergeSchema", "false")
        sinkDF.save.assert_called_once_with("/path/to/data")

    def test_writeDataSnapshot_shouldMergeSchema(self):
        # arrange
        sinkDF = MagicMock()
        sinkDF.count.return_value = 3
        sinkDF.write.format.return_value = sinkDF
        sinkDF.mode.return_value = sinkDF
        sinkDF.option.return_value = sinkDF

        params = {
            "schemaEvolution": True
        }

        self._dataLakeHelper._deleteSnapshotData = MagicMock(return_value = 0)

        # act
        self._dataLakeHelper._writeDataSnapshot(sinkDF, "/path/to/data", "delta", params, "databaseName", "tableName")

        # assert
        sinkDF.option.assert_called_once_with("mergeSchema", "true")
        sinkDF.save.assert_called_once_with("/path/to/data")


    # writeDataOverwriteOrAppend tests

    def test_writeDataOverwriteOrAppend_shouldWriteEmptyDF(self):
        # arrange
        sinkDF = MagicMock()
        sinkDF.count.return_value = 0
        sinkDF.write.format.return_value = sinkDF
        sinkDF.mode.return_value = sinkDF

        writeMode = WRITE_MODE_APPEND

        self._dataLakeHelper._registerTableWithIdentityColumnInHive = MagicMock(return_value=False)

        # act
        statistics = self._dataLakeHelper._writeDataOverwriteOrAppend(sinkDF, "/path/to/data", writeMode, "delta", {}, "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper._registerTableWithIdentityColumnInHive.assert_called_once()
        sinkDF.save.assert_called_once_with("/path/to/data")

    def test_writeDataOverwriteOrAppend_append_schemaEvolutionAndPartitioning(self):
        # arrange
        sinkDF = MagicMock()
        sinkDF.count.return_value = 3
        sinkDF.write.format.return_value = sinkDF
        sinkDF.mode.return_value = sinkDF
        sinkDF.option.return_value = sinkDF
        sinkDF.partitionBy.return_value = sinkDF

        writeMode = WRITE_MODE_APPEND
        params = {
            "schemaEvolution": True,
            "partitionColumns": ["col1", "col2"]
        }

        self._dataLakeHelper._registerTableWithIdentityColumnInHive = MagicMock(return_value=False)

        # act
        statistics = self._dataLakeHelper._writeDataOverwriteOrAppend(sinkDF, "/path/to/data", writeMode, "delta", params, "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper._registerTableWithIdentityColumnInHive.assert_called_once()
        
        sinkDF.write.format.assert_called_once_with("delta")
        sinkDF.mode.assert_called_once_with(writeMode)
        sinkDF.option.assert_called_once_with("mergeSchema", "true")
        sinkDF.partitionBy.assert_called_once_with(["col1", "col2"])
        sinkDF.save.assert_called_once_with("/path/to/data")

    def test_writeDataOverwriteOrAppend_append_noSchemaEvolutionAndNoPartitioning(self):
        # arrange
        sinkDF = MagicMock()
        sinkDF.count.return_value = 3
        sinkDF.write.format.return_value = sinkDF
        sinkDF.mode.return_value = sinkDF
        sinkDF.option.return_value = sinkDF
        sinkDF.partitionBy.return_value = sinkDF

        writeMode = WRITE_MODE_APPEND
        params = {}

        self._dataLakeHelper._registerTableWithIdentityColumnInHive = MagicMock(return_value=False)

        # act
        statistics = self._dataLakeHelper._writeDataOverwriteOrAppend(sinkDF, "/path/to/data", writeMode, "delta", params, "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper._registerTableWithIdentityColumnInHive.assert_called_once()
        
        sinkDF.write.format.assert_called_once_with("delta")
        sinkDF.mode.assert_called_once_with(writeMode)
        sinkDF.option.assert_not_called()
        sinkDF.partitionBy.assert_not_called()
        sinkDF.save.assert_called_once_with("/path/to/data")
    
    def test_writeDataOverwriteOrAppend_overwrite_schemaEvolution(self):
        # arrange
        sinkDF = MagicMock()
        sinkDF.count.return_value = 3
        sinkDF.write.format.return_value = sinkDF
        sinkDF.mode.return_value = sinkDF
        sinkDF.option.return_value = sinkDF
        sinkDF.partitionBy.return_value = sinkDF

        writeMode = WRITE_MODE_OVERWRITE
        params = {
            "schemaEvolution": True
        }

        self._dataLakeHelper._registerTableWithIdentityColumnInHive = MagicMock(return_value=False)

        # act
        statistics = self._dataLakeHelper._writeDataOverwriteOrAppend(sinkDF, "/path/to/data", writeMode, "delta", params, "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper._registerTableWithIdentityColumnInHive.assert_called_once()
        
        sinkDF.write.format.assert_called_once_with("delta")
        sinkDF.mode.assert_called_once_with(writeMode)
        sinkDF.option.assert_called_once_with("overwriteSchema", "true")
        sinkDF.partitionBy.assert_not_called()
        sinkDF.save.assert_called_once_with("/path/to/data")

    def test_writeDataOverwriteOrAppend_overwrite_noSchemaEvolution_moreColumns(self):
        # arrange
        sinkDF = MagicMock()
        sinkDF.count.return_value = 3
        sinkDF.write.format.return_value = sinkDF
        sinkDF.mode.return_value = sinkDF
        sinkDF.option.return_value = sinkDF
        sinkDF.partitionBy.return_value = sinkDF
        sinkDF.select.return_value = sinkDF
        sinkDF.columns = ['column1', 'column2', 'column3', 'column4', 'column5']

        writeMode = WRITE_MODE_OVERWRITE
        params = {}

        self._dataLakeHelper._registerTableWithIdentityColumnInHive = MagicMock(return_value=False)

        self._dataLakeHelper._spark.catalog.tableExists = MagicMock(return_value=True)
        sqlMock = MagicMock()
        sqlMock.limit.return_value.columns = ['column1', 'column2', 'column3']
        self._dataLakeHelper._spark.sql.return_value = sqlMock
        
        # act
        statistics = self._dataLakeHelper._writeDataOverwriteOrAppend(sinkDF, "/path/to/data", writeMode, "delta", params, "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper._registerTableWithIdentityColumnInHive.assert_called_once()
        
        sinkDF.write.format.assert_called_once_with("delta")
        sinkDF.mode.assert_called_once_with(writeMode)
        sinkDF.option.assert_not_called()
        sinkDF.partitionBy.assert_not_called()
        sinkDF.save.assert_called_once_with("/path/to/data")

        sinkDF.select.assert_called_once_with(['column1', 'column2', 'column3'])

    def test_writeDataOverwriteOrAppend_overwrite_existingData_lessColumns(self):
        # arrange
        sinkDF = MagicMock()
        sinkDF.count.return_value = 3
        sinkDF.write.format.return_value = sinkDF
        sinkDF.mode.return_value = sinkDF
        sinkDF.select.return_value = sinkDF
        sinkDF.columns = ['column1', 'column2', 'column3']

        writeMode = WRITE_MODE_OVERWRITE
        params = {}

        self._dataLakeHelper._registerTableWithIdentityColumnInHive = MagicMock(return_value=False)
        
        sqlCommandDF = MagicMock()
        sqlCommandDF.count.return_value = 1
        sqlCommandDF.first.return_value = { "countDeleted": 5 }
        self._dataLakeHelper._sqlContext.tableNames.return_value = ["tablename", "secondtable"]

        self._dataLakeHelper._spark.catalog.tableExists = MagicMock(return_value=True)
        sqlMock = MagicMock()
        sqlMock.limit.return_value.columns = ['column1', 'column2', 'column3', 'column4', 'column5']

        def sqlQuery(sqlCommand: str) -> MagicMock:
            if "SELECT * FROM" in sqlCommand:
                return sqlMock
            else:
                return sqlCommandDF
            
        self._dataLakeHelper._spark.sql.side_effect = sqlQuery
        
        # act
        statistics = self._dataLakeHelper._writeDataOverwriteOrAppend(sinkDF, "/path/to/data", writeMode, "delta", params, "databaseName", "tableName")

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 5
        })

        self._dataLakeHelper._registerTableWithIdentityColumnInHive.assert_called_once()
        
        self._dataLakeHelper._sqlContext.tableNames.assert_called_once_with("databaseName")

        sinkDF.write.format.assert_called_once_with("delta")
        sinkDF.mode.assert_called_once_with(writeMode)
        sinkDF.save.assert_called_once_with("/path/to/data")

        sinkDF.select.assert_called_once_with(['column1', 'column2', 'column3'])


    # createSingleCSV tests

    def test_createSingleCSV(self):
        # arrange
        sinkDF = MagicMock()

        tempFile = MagicMock()
        tempFile.name.return_value = 'file.csv.tmp'

        self._dataLakeHelper._dbutils.fs.ls.return_value = [tempFile]

        # act
        self._dataLakeHelper._createSingleCSV(sinkDF, "path/to/file.csv", { "header": True, "nullValue": None })

        # assert
        sinkDF.coalesce.assert_called_once()
        self._dataLakeHelper._dbutils.fs.ls.assert_called_once()
        self._dataLakeHelper._dbutils.fs.cp.assert_called_once()
        self._dataLakeHelper._dbutils.fs.rm.assert_called_once()


    # dropDuplicates tests

    def test_dropDuplicates_firstDuplicateRecordShouldBeKept(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [3, "value C"],
            [4, "value D"],
            [5, "value E"],
            [1, "value A"],
            [3, "value CC"],
        ], "id INTEGER, value STRING")

        expectedDF = self._spark.createDataFrame([
            [1, "value A"],
            [3, "value C"],
            [4, "value D"],
            [5, "value E"],
        ], "id INTEGER, value STRING")

        byColumn = ["id"]
        orderSpec = "value"

        # act
        outputDF = DataLakeHelper._dropDuplicates(dataDF, byColumn, orderSpec)

        # assert
        self.assertEqual(
            outputDF.sort("id").rdd.collect(),
            expectedDF.sort("id").rdd.collect()
        )

    def test_dropDuplicates_lastDuplicateRecordShouldBeKept(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [3, "value CC"],
            [4, "value D"],
            [5, "value E"],
            [1, "value A"],
            [3, "value C"],
        ], "id INTEGER, value STRING")

        expectedDF = self._spark.createDataFrame([
            [1, "value A"],
            [3, "value C"],
            [4, "value D"],
            [5, "value E"],
        ], "id INTEGER, value STRING")

        byColumn = ["id"]
        orderSpec = "value"

        # act
        outputDF = DataLakeHelper._dropDuplicates(dataDF, byColumn, orderSpec)

        # assert
        self.assertEqual(
            outputDF.sort("id").rdd.collect(),
            expectedDF.sort("id").rdd.collect()
        )


    # writeCSV tests

    def test_writeCSV_notSupportedWriteMode(self):
        # act
        with self.assertRaisesRegex(AssertionError, f"write mode '{WRITE_MODE_SCD_TYPE1}' is not supported"):
            self._dataLakeHelper.writeCSV(None, "path/to/file.csv", WRITE_MODE_SCD_TYPE1)

    def test_writeCSV_noData(self):        
        # act
        statistics = self._dataLakeHelper.writeCSV(None, "path/to/file.csv", WRITE_MODE_OVERWRITE)

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

    def test_writeCSV_overwrite(self):
        # arrange
        sinkDF = self._spark.createDataFrame([
            [1, "value A"],
            [2, "value B"],
            [3, "value C"],
        ], "id INTEGER, value STRING")

        self._dataLakeHelper._createSingleCSV = MagicMock()

        # act
        statistics = self._dataLakeHelper.writeCSV(sinkDF, "path/to/file.csv", WRITE_MODE_OVERWRITE)

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })
        
        self._dataLakeHelper._createSingleCSV.assert_called_once()

    def test_writeCSV_append(self):
        # arrange
        sinkDF = self._spark.createDataFrame([
            [3, "value C"],
            [4, "value D"],
            [5, "value E"],
        ], "id INTEGER, value STRING")

        existingFileDataDF = self._spark.createDataFrame([
            [1, "value A"],
            [2, "value B"],
        ], "id INTEGER, value STRING")

        self._dataLakeHelper.fileExists = MagicMock(return_value=True)
        self._dataLakeHelper._readCSV = MagicMock(return_value=existingFileDataDF)
        self._dataLakeHelper._createSingleCSV = MagicMock()

        # act
        statistics = self._dataLakeHelper.writeCSV(sinkDF, "path/to/file.csv", WRITE_MODE_APPEND)

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 5,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper.fileExists.assert_called_once_with("path/to/file.csv")
        self._dataLakeHelper._readCSV.assert_called_once()
        self._dataLakeHelper._createSingleCSV.assert_called_once()

    def test_writeCSV_optionsSpecified(self):
        # arrange
        sinkDF = self._spark.createDataFrame([
            [3, "value C"],
            [4, "value D"],
            [5, "value E"],
        ], "id INTEGER, value STRING")

        existingFileDataDF = self._spark.createDataFrame([
            [1, "value A"],
            [2, "value B"],
        ], "id INTEGER, value STRING")

        self._dataLakeHelper.fileExists = MagicMock(return_value=True)
        self._dataLakeHelper._readCSV = MagicMock(return_value=existingFileDataDF)
        self._dataLakeHelper._createSingleCSV = MagicMock()

        options = '{ "header": true, "nullValue": null, "sep": ";" }'

        expectedOptions = {
            "header": True,
            "nullValue": None,
            "sep": ";"
        }

        # act
        statistics = self._dataLakeHelper.writeCSV(sinkDF, "path/to/file.csv", WRITE_MODE_APPEND, options)

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 5,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper.fileExists.assert_called_once_with("path/to/file.csv")
        self._dataLakeHelper._readCSV.assert_called_once_with("path/to/file.csv", expectedOptions)
        
        arguments = self._dataLakeHelper._createSingleCSV.call_args_list[0].args
        self.assertEqual(arguments[1], "path/to/file.csv")
        self.assertEqual(arguments[2], expectedOptions)

    def test_writeCSV_dropDuplicateByColumn(self):
        # arrange
        sinkDF = MagicMock()

        existingAndSinkUnionDF = MagicMock()

        existingFileDataDF = MagicMock()
        existingFileDataDF.union.return_value = existingAndSinkUnionDF

        expectedDF = MagicMock()
        expectedDF.count.return_value = 4

        self._dataLakeHelper.fileExists = MagicMock(return_value=True)
        self._dataLakeHelper._readCSV = MagicMock(return_value=existingFileDataDF)

        self._dataLakeHelper._dropDuplicates = MagicMock(return_value=expectedDF)

        self._dataLakeHelper._createSingleCSV = MagicMock()

        options = { "header": True, "nullValue": None }

        # act
        statistics = self._dataLakeHelper.writeCSV(
            sinkDF,
            "path/to/file.csv",
            WRITE_MODE_APPEND,
            dropDuplicatesByColumn=["id"],
            dropDuplicatesOrderSpec="value"
        )

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 4,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper.fileExists.assert_called_once_with("path/to/file.csv")
        self._dataLakeHelper._readCSV.assert_called_once_with("path/to/file.csv", options)
        
        existingFileDataDF.union.assert_called_once_with(sinkDF)

        self._dataLakeHelper._dropDuplicates.assert_called_once_with(existingAndSinkUnionDF, ["id"], "value")

        self._dataLakeHelper._createSingleCSV.assert_called_once_with(expectedDF, "path/to/file.csv", options)


    # writeDataAsParquet tests

    def test_writeDataAsParquet_notSupportedWriteMode(self):
        # act
        with self.assertRaisesRegex(AssertionError, f"write mode '{WRITE_MODE_SCD_TYPE1}' is not supported"):
            self._dataLakeHelper.writeDataAsParquet(None, "path/to/folder", WRITE_MODE_SCD_TYPE1)

    def test_writeDataAsParquet_noData(self):        
        # act
        statistics = self._dataLakeHelper.writeDataAsParquet(None, "path/to/folder", WRITE_MODE_OVERWRITE)

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

    def test_writeDataAsParquet_overwrite(self):
        # arrange
        sinkDF = MagicMock()
        sinkDF.count.return_value = 3

        sinkDFModeMock = MagicMock()
        sinkDF.write.mode.return_value = sinkDFModeMock

        # act
        statistics = self._dataLakeHelper.writeDataAsParquet(sinkDF, "path/to/folder", WRITE_MODE_OVERWRITE)

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })
        
        sinkDF.write.mode.assert_called_once_with(WRITE_MODE_OVERWRITE)
        sinkDFModeMock.parquet.assert_called_once_with("path/to/folder")

    def test_writeDataAsParquet_append(self):
        # arrange
        sinkDF = MagicMock()
        sinkDF.count.return_value = 3

        sinkDFModeMock = MagicMock()
        sinkDF.write.mode.return_value = sinkDFModeMock

        # act
        statistics = self._dataLakeHelper.writeDataAsParquet(sinkDF, "path/to/folder", WRITE_MODE_APPEND)

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 3,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })
        
        sinkDF.write.mode.assert_called_once_with(WRITE_MODE_APPEND)
        sinkDFModeMock.parquet.assert_called_once_with("path/to/folder")

    def test_writeDataAsParquet_append_dropDuplicateByColumn_tableDoesNotExist(self):
        # arrange
        sinkDF = MagicMock()

        expectedDF = MagicMock()
        expectedDFModeMock = MagicMock()
        expectedDF.write.mode.return_value = expectedDFModeMock
        expectedDF.count.return_value = 4

        self._dataLakeHelper.fileExists = MagicMock(return_value=False)
        self._dataLakeHelper._dropDuplicates = MagicMock(return_value=expectedDF)

        # act
        statistics = self._dataLakeHelper.writeDataAsParquet(
            sinkDF,
            "path/to/folder",
            WRITE_MODE_APPEND,
            dropDuplicatesByColumn=["id"],
            dropDuplicatesOrderSpec="value"
        )

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 4,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper.fileExists.assert_called_once_with("path/to/folder")
        
        self._dataLakeHelper._spark.read.assert_not_called()

        sinkDF.cache.assert_not_called()
        sinkDF.count.assert_not_called()

        self._dataLakeHelper._dropDuplicates.assert_called_once_with(sinkDF, ["id"], "value")

        expectedDF.write.mode.assert_called_once_with(WRITE_MODE_APPEND)
        expectedDFModeMock.parquet.assert_called_once_with("path/to/folder")
        expectedDF.count.assert_called_once_with()

    def test_writeDataAsParquet_append_dropDuplicateByColumn_tableExists(self):
        # arrange
        sinkDF = MagicMock()

        expectedDF = MagicMock()
        expectedDFModeMock = MagicMock()
        expectedDF.write.mode.return_value = expectedDFModeMock
        expectedDF.count.return_value = 4

        self._dataLakeHelper.fileExists = MagicMock(return_value=True)

        existingAndSinkUnionDF = MagicMock()

        existingDataDF = MagicMock()
        existingDataDF.union.return_value = existingAndSinkUnionDF

        sparkReadFormat = MagicMock()
        sparkReadFormat.load.return_value = existingDataDF
        self._dataLakeHelper._spark.read.format.return_value = sparkReadFormat

        self._dataLakeHelper._dropDuplicates = MagicMock(return_value=expectedDF)

        # act
        statistics = self._dataLakeHelper.writeDataAsParquet(
            sinkDF,
            "path/to/folder",
            WRITE_MODE_APPEND,
            dropDuplicatesByColumn=["id"],
            dropDuplicatesOrderSpec="value"
        )

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 4,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper.fileExists.assert_called_once_with("path/to/folder")
        
        self._dataLakeHelper._spark.read.format.assert_called_once_with("parquet")
        sparkReadFormat.load.assert_called_once_with("path/to/folder")

        existingDataDF.union.assert_called_once_with(sinkDF)

        existingAndSinkUnionDF.cache.assert_called_once()
        existingAndSinkUnionDF.count.assert_called_once()

        self._dataLakeHelper._dropDuplicates.assert_called_once_with(existingAndSinkUnionDF, ["id"], "value")

        expectedDF.write.mode.assert_called_once_with(WRITE_MODE_OVERWRITE)
        expectedDFModeMock.parquet.assert_called_once_with("path/to/folder")
        expectedDF.count.assert_called_once_with()

    def test_writeDataAsParquet_overwrite_dropDuplicateByColumn(self):
        # arrange
        sinkDF = MagicMock()

        expectedDF = MagicMock()
        expectedDFModeMock = MagicMock()
        expectedDF.write.mode.return_value = expectedDFModeMock
        expectedDF.count.return_value = 4

        self._dataLakeHelper.fileExists = MagicMock()
        self._dataLakeHelper._dropDuplicates = MagicMock(return_value=expectedDF)

        # act
        statistics = self._dataLakeHelper.writeDataAsParquet(
            sinkDF,
            "path/to/folder",
            WRITE_MODE_OVERWRITE,
            dropDuplicatesByColumn=["id"],
            dropDuplicatesOrderSpec="value"
        )

        # assert
        self.assertEqual(statistics, {
            "recordsInserted": 4,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        })

        self._dataLakeHelper.fileExists.assert_not_called()
        
        self._dataLakeHelper._spark.read.assert_not_called()

        sinkDF.cache.assert_not_called()
        sinkDF.count.assert_not_called()

        self._dataLakeHelper._dropDuplicates.assert_called_once_with(sinkDF, ["id"], "value")

        expectedDF.write.mode.assert_called_once_with(WRITE_MODE_OVERWRITE)
        expectedDFModeMock.parquet.assert_called_once_with("path/to/folder")
        expectedDF.count.assert_called_once_with()
     
