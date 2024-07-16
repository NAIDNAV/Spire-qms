# Databricks notebook source
import unittest
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import ArrayType
from unittest.mock import MagicMock, ANY

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.generator.configGenerators.modelTransformationsConfigGenerator import ModelTransformationsConfigGenerator
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../../../compartment/includes/compartmentConfig

# COMMAND ----------

# MAGIC %run ../../../../../generator/configGenerators/modelTransformationsConfigGenerator

# COMMAND ----------

# MAGIC %run ../../../../../includes/frameworkConfig

# COMMAND ----------

class ModelTransformationsConfigGeneratorTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()
        self._compartmentConfig = MagicMock()

    def setUp(self):
        self._modelTransformationsConfigGenerator = ModelTransformationsConfigGenerator(self._spark, self._compartmentConfig)
        self._modelTransformationsConfigGenerator._spark = MagicMock()
        self._modelTransformationsConfigGenerator._sqlContext = MagicMock()
    
    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # convertModelToSparkDataType tests

    def test_convertModelToSparkDataType_knowTypes(self):
        # assert
        assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('bigint') == 'BIGINT'
        assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('tinyint') == 'INT'
        assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('int') == 'INT'
        assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('date') == 'DATE'
        assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('datetime') == 'TIMESTAMP'
        assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('varchar(30)') == 'STRING'
        assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('char(1)') == 'STRING'
        assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('decimal') == 'DECIMAL(10,0)'
        assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('numeric') == 'DECIMAL(10,0)'
        assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('decimal(12,2)') == 'DECIMAL(12,2)'
        assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('numeric(12,2)') == 'DECIMAL(12,2)'
        assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('double(10,2)') == 'DOUBLE'

    def test_convertModelToSparkDataType_unknownType(self):
        with self.assertRaisesRegex(AssertionError, f"ModelDataType 'unknown' is not supported"):
            assert self._modelTransformationsConfigGenerator._convertModelToSparkDataType('unknown')


    # getBatchesBasedOnTableRelations tests

    def test_getBatchesBasedOnTableRelations_noCyclicDependency(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["Customer"],
            ["Address"],
            ["CustomerAddress"],
            ["TableWithDependencyOnCustomerAddress"],
        ], "Entity STRING")

        relationsDF = self._spark.createDataFrame([
            ["Customer", "CustomerAddress"],
            ["Address", "CustomerAddress"],
            ["CustomerAddress", "TableWithDependencyOnCustomerAddress"],
        ], "Parent STRING, Child STRING")
        
        def sqlQuery(sqlCommand: str) -> DataFrame:
            if "Entity" in sqlCommand:
                return entitiesDF
            elif "Parent, Child" in sqlCommand:
                return relationsDF

        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        expectedBatches = {
            1: [Row(Entity="Customer"), Row(Entity="Address")],
            2: [Row(Entity="CustomerAddress")],
            3: [Row(Entity="TableWithDependencyOnCustomerAddress")]
        }

        # act
        actualBatches = self._modelTransformationsConfigGenerator._getBatchesBasedOnTableRelations("AttributesTable", "RelationsTable")

        # assert
        self.assertEqual(actualBatches, expectedBatches)

        self.assertEqual(self._modelTransformationsConfigGenerator._spark.sql.call_count, 2)

        sqlCommand = self._modelTransformationsConfigGenerator._spark.sql.call_args_list[0].args[0]
        self.assertIn("Entity", sqlCommand)
        self.assertIn("AttributesTable", sqlCommand)

        sqlCommand = self._modelTransformationsConfigGenerator._spark.sql.call_args_list[1].args[0]
        self.assertIn("Parent, Child", sqlCommand)
        self.assertIn("RelationsTable", sqlCommand)

    def test_getBatchesBasedOnTableRelations_cyclicDependency(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["Customer"],
            ["Address"],
            ["CustomerAddress"],
            ["TableWithDependencyOnCustomerAddress"],
        ], "Entity STRING")

        relationsDF = self._spark.createDataFrame([
            ["Customer", "CustomerAddress"],
            ["Address", "CustomerAddress"],
            ["CustomerAddress", "TableWithDependencyOnCustomerAddress"],
            ["TableWithDependencyOnCustomerAddress", "Customer"]
        ], "Parent STRING, Child STRING")
        
        def sqlQuery(sqlCommand: str) -> DataFrame:
            if "Entity" in sqlCommand:
                return entitiesDF
            elif "Parent, Child" in sqlCommand:
                return relationsDF

        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        # act
        with self.assertRaisesRegex(AssertionError, f"Cyclic dependency found in batch 2"):
            self._modelTransformationsConfigGenerator._getBatchesBasedOnTableRelations("AttributesTable", "RelationsTable")


    # getSQLQueryForTable tests

    def test_getSQLQueryForTable_noParentTables(self):
        # arrange
        self._modelTransformationsConfigGenerator._spark.sql.return_value = self._spark.createDataFrame([], "Parent STRING")

        expectedSqlQuery = "SELECT tv.* FROM testSinkDatabaseName.TV_Address tv"

        # act
        sqlQuery = self._modelTransformationsConfigGenerator._getSQLQueryForTable(
            "testSinkDatabaseName",
            "Address",
            "testMetadataRelationsTable",
            -1
        )

        # assert
        self.assertEqual(sqlQuery, expectedSqlQuery)

    def test_getSQLQueryForTable_parentTableWithoutPIdentifier(self): 
        # arrange
        def sqlQuery(sqlCommand: str) -> DataFrame:
            if "DISTINCT Parent" in sqlCommand:
                return self._spark.createDataFrame([
                    ["AddressParent", None]
                ], "Parent STRING, RolePlayingGroup STRING")
            elif "KeyType = 'PIdentifier'" in sqlCommand and "Parent = 'AddressParent'" in sqlCommand:
                return self._spark.createDataFrame([
                ], "ParentAttribute STRING, ChildAttribute STRING")

        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        expectedSqlQuery = "SELECT tv.* FROM testSinkDatabaseName.TV_Address tv"

        # act
        sqlQuery = self._modelTransformationsConfigGenerator._getSQLQueryForTable(
            "testSinkDatabaseName",
            "Address",
            "testMetadataRelationsTable",
            -1
        )

        # assert
        self.assertEqual(sqlQuery, expectedSqlQuery)

    def test_getSQLQueryForTable_parentTables(self):
        # arrange
        def sqlQuery(sqlCommand: str) -> DataFrame:
            if "DISTINCT Parent" in sqlCommand:
                return self._spark.createDataFrame([
                    ["Customer", None],
                    ["Address", None]
                ], "Parent STRING, RolePlayingGroup STRING")
            elif "KeyType = 'PIdentifier'" in sqlCommand and "Parent = 'Customer'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["WK_Customer_Identifier", "WK_Customer_Identifier_CHILD"],
                ], "ParentAttribute STRING, ChildAttribute STRING")
            elif "KeyType = 'PIdentifier'" in sqlCommand and "Parent = 'Address'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["WK_Address_Identifier", "WK_Address_Identifier_CHILD"],
                ], "ParentAttribute STRING, ChildAttribute STRING")
            elif "KeyType != 'PIdentifier'" in sqlCommand and "Parent = 'Customer'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["CustomerID", "CustomerID_CHILD"],
                ], "ParentAttribute STRING, ChildAttribute STRING")
            elif "KeyType != 'PIdentifier'" in sqlCommand and "Parent = 'Address'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["AddressID", "AddressID_CHILD"],
                ], "ParentAttribute STRING, ChildAttribute STRING")

        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        expectedSqlQuery = "SELECT tv.*, COALESCE(p1.WK_Customer_Identifier, -1) AS WK_Customer_Identifier_CHILD, COALESCE(p2.WK_Address_Identifier, -1) AS WK_Address_Identifier_CHILD FROM testSinkDatabaseName.TV_CustomerAddress tv LEFT JOIN testSinkDatabaseName.Customer p1 ON p1.CustomerID = tv.CustomerID_CHILD LEFT JOIN testSinkDatabaseName.Address p2 ON p2.AddressID = tv.AddressID_CHILD"

        # act
        sqlQuery = self._modelTransformationsConfigGenerator._getSQLQueryForTable(
            "testSinkDatabaseName",
            "CustomerAddress",
            "testMetadataRelationsTable",
            -1
        )

        # assert
        self.maxDiff = None
        self.assertEqual(sqlQuery, expectedSqlQuery)

    def test_getSQLQueryForTable_parentTables_unknownMemberDefaultValueSetAsNone(self):
        # arrange
        def sqlQuery(sqlCommand: str) -> DataFrame:
            if "DISTINCT Parent" in sqlCommand:
                return self._spark.createDataFrame([
                    ["Customer", None],
                    ["Address", None]
                ], "Parent STRING, RolePlayingGroup STRING")
            elif "KeyType = 'PIdentifier'" in sqlCommand and "Parent = 'Customer'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["WK_Customer_Identifier", "WK_Customer_Identifier_CHILD"],
                ], "ParentAttribute STRING, ChildAttribute STRING")
            elif "KeyType = 'PIdentifier'" in sqlCommand and "Parent = 'Address'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["WK_Address_Identifier", "WK_Address_Identifier_CHILD"],
                ], "ParentAttribute STRING, ChildAttribute STRING")
            elif "KeyType != 'PIdentifier'" in sqlCommand and "Parent = 'Customer'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["CustomerID", "CustomerID_CHILD"],
                ], "ParentAttribute STRING, ChildAttribute STRING")
            elif "KeyType != 'PIdentifier'" in sqlCommand and "Parent = 'Address'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["AddressID", "AddressID_CHILD"],
                ], "ParentAttribute STRING, ChildAttribute STRING")

        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        expectedSqlQuery = "SELECT tv.*, p1.WK_Customer_Identifier AS WK_Customer_Identifier_CHILD, p2.WK_Address_Identifier AS WK_Address_Identifier_CHILD FROM testSinkDatabaseName.TV_CustomerAddress tv LEFT JOIN testSinkDatabaseName.Customer p1 ON p1.CustomerID = tv.CustomerID_CHILD LEFT JOIN testSinkDatabaseName.Address p2 ON p2.AddressID = tv.AddressID_CHILD"

        # act
        sqlQuery = self._modelTransformationsConfigGenerator._getSQLQueryForTable(
            "testSinkDatabaseName",
            "CustomerAddress",
            "testMetadataRelationsTable",
            None
        )

        # assert
        self.maxDiff = None
        self.assertEqual(sqlQuery, expectedSqlQuery)

    def test_getSQLQueryForTable_parentTablesWithRolePlayingGroup(self):
        # arrange
        def sqlQuery(sqlCommand: str) -> DataFrame:
            if "DISTINCT Parent" in sqlCommand:
                return self._spark.createDataFrame([
                    ["Customer", "ShipToAddress"],
                    ["Customer", "BillToAddress"],
                    ["Address", None]
                ], "Parent STRING, RolePlayingGroup STRING")
            elif "KeyType = 'PIdentifier'" in sqlCommand and "Parent = 'Customer'" in sqlCommand and "RolePlayingGroup = 'ShipToAddress'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["WK_Customer_Identifier", "WK_Customer_ShipToAddress_Identifier"],
                ], "ParentAttribute STRING, ChildAttribute STRING")
            elif "KeyType = 'PIdentifier'" in sqlCommand and "Parent = 'Customer'" in sqlCommand and "RolePlayingGroup = 'BillToAddress'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["WK_Customer_Identifier", "WK_Customer_BillToAddress_Identifier"],
                ], "ParentAttribute STRING, ChildAttribute STRING")
            elif "KeyType = 'PIdentifier'" in sqlCommand and "Parent = 'Address'" in sqlCommand and "RolePlayingGroup IS NULL" in sqlCommand:
                return self._spark.createDataFrame([
                    ["WK_Address_Identifier", "WK_Address_Identifier_CHILD"],
                ], "ParentAttribute STRING, ChildAttribute STRING")
            elif "KeyType != 'PIdentifier'" in sqlCommand and "Parent = 'Customer'" in sqlCommand and "RolePlayingGroup = 'ShipToAddress'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["CustomerID", "CustomerID_CHILD"],
                ], "ParentAttribute STRING, ChildAttribute STRING")
            elif "KeyType != 'PIdentifier'" in sqlCommand and "Parent = 'Customer'" in sqlCommand and "RolePlayingGroup = 'BillToAddress'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["CustomerID", "CustomerID_CHILD"],
                ], "ParentAttribute STRING, ChildAttribute STRING")
            elif "KeyType != 'PIdentifier'" in sqlCommand and "Parent = 'Address'" in sqlCommand and "RolePlayingGroup IS NULL" in sqlCommand:
                return self._spark.createDataFrame([
                    ["AddressID", "AddressID_CHILD"],
                ], "ParentAttribute STRING, ChildAttribute STRING")

        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        expectedSqlQuery = "SELECT tv.*, COALESCE(p1.WK_Customer_Identifier, -1) AS WK_Customer_ShipToAddress_Identifier, COALESCE(p2.WK_Customer_Identifier, -1) AS WK_Customer_BillToAddress_Identifier, COALESCE(p3.WK_Address_Identifier, -1) AS WK_Address_Identifier_CHILD FROM testSinkDatabaseName.TV_CustomerAddress tv LEFT JOIN testSinkDatabaseName.Customer p1 ON p1.CustomerID = tv.CustomerID_CHILD LEFT JOIN testSinkDatabaseName.Customer p2 ON p2.CustomerID = tv.CustomerID_CHILD LEFT JOIN testSinkDatabaseName.Address p3 ON p3.AddressID = tv.AddressID_CHILD"

        # act
        sqlQuery = self._modelTransformationsConfigGenerator._getSQLQueryForTable(
            "testSinkDatabaseName",
            "CustomerAddress",
            "testMetadataRelationsTable",
            -1
        )

        # assert
        self.maxDiff = None
        self.assertEqual(sqlQuery, expectedSqlQuery)


    # getKeyColumnsForTable tests

    def test_getKeyColumnsForTable_definedInTransformationConfig(self):
        # arrange
        addressTransformationConfig = Row(
            KeyColumns = ["AddressID"]
        )

        expectedKeyColumns = ["AddressID"]

        # act
        keyColumns = self._modelTransformationsConfigGenerator._getKeyColumnsForTable(
            "testSinkDatabaseName",
            "Address",
            addressTransformationConfig,
            "testMetadataKeysTable",
            "testTransformationTable",
            WRITE_MODE_SCD_TYPE1
        )

        # assert
        self.assertEqual(keyColumns, expectedKeyColumns)

    def test_getKeyColumnsForTable_missingKeyColumnsInTransformationConfigForWriteModeSnapshot(self):
        # arrange
        addressTransformationConfig = Row(
            KeyColumns = []
        )

        # act
        with self.assertRaisesRegex(AssertionError, "Table 'testSinkDatabaseName.Address' does not have property 'keyColumns' defined in 'Params' in '.*.testTransformationTable' or it is empty"):
            keyColumns = self._modelTransformationsConfigGenerator._getKeyColumnsForTable(
                "testSinkDatabaseName",
                "Address",
                addressTransformationConfig,
                "testMetadataKeysTable",
                "testTransformationTable",
                WRITE_MODE_SNAPSHOT
            )

    def test_getKeyColumnsForTable_nonPIdentifierKeysUsed(self):
        # arrange
        self._modelTransformationsConfigGenerator._spark.sql.return_value = self._spark.createDataFrame([
            ["NonPIdentifier1"],
            ["NonPIdentifier2"],
        ], "Attribute STRING")

        expectedKeyColumns = ["NonPIdentifier1", "NonPIdentifier2"]

        # act
        keyColumns = self._modelTransformationsConfigGenerator._getKeyColumnsForTable(
            "testSinkDatabaseName",
            "Address",
            None,
            "testMetadataKeysTable",
            "testTransformationTable",
            WRITE_MODE_SCD_TYPE1
        )

        # assert
        self.assertEqual(keyColumns, expectedKeyColumns)

        self._modelTransformationsConfigGenerator._spark.sql.called_once()
        sqlCommand = self._modelTransformationsConfigGenerator._spark.sql.call_args_list[0].args[0]

        self.assertIn("testMetadataKeysTable", sqlCommand)
        self.assertIn("Entity = 'Address'", sqlCommand)
        self.assertIn("KeyType != 'PIdentifier'", sqlCommand)

    def test_getKeyColumnsForTable_PIdentifierKeysUsed(self):
        # arrange
        def sqlQuery(sqlCommand: str) -> DataFrame:
            if "KeyType != 'PIdentifier'" in sqlCommand:
                return self._spark.createDataFrame([
                ], "Attribute STRING")
            elif "KeyType == 'PIdentifier'" in sqlCommand:
                return self._spark.createDataFrame([
                    ["PIdentifier1"],
                    ["PIdentifier2"],
                ], "Attribute STRING")

        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        expectedKeyColumns = ["PIdentifier1", "PIdentifier2"]

        # act
        keyColumns = self._modelTransformationsConfigGenerator._getKeyColumnsForTable(
            "testSinkDatabaseName",
            "Address",
            None,
            "testMetadataKeysTable",
            "testTransformationTable",
            WRITE_MODE_SCD_TYPE1
        )

        # assert
        self.assertEqual(keyColumns, expectedKeyColumns)

        self.assertEqual(self._modelTransformationsConfigGenerator._spark.sql.call_count, 2)
        sqlCommand = self._modelTransformationsConfigGenerator._spark.sql.call_args_list[1].args[0]

        self.assertIn("testMetadataKeysTable", sqlCommand)
        self.assertIn("Entity = 'Address'", sqlCommand)
        self.assertIn("KeyType == 'PIdentifier'", sqlCommand)

    def test_getKeyColumnsForTable_missingKeyColumnsForWriteModeSCDType1(self):
        # arrange
        self._modelTransformationsConfigGenerator._spark.sql.return_value = self._spark.createDataFrame([], "Attribute STRING")

        # act
        with self.assertRaisesRegex(AssertionError, "Table 'testSinkDatabaseName.Address' does not have any primary column defined in '.*.testMetadataKeysTable'"):
            keyColumns = self._modelTransformationsConfigGenerator._getKeyColumnsForTable(
                "testSinkDatabaseName",
                "Address",
                None,
                "testMetadataKeysTable",
                "testTransformationTable",
                WRITE_MODE_SCD_TYPE1
            )

        # assert
        self.assertEqual(self._modelTransformationsConfigGenerator._spark.sql.call_count, 2)
    

    # generateDtOutputInstructionForTable tests

    def test_generateDtOutputInstructionForTable_withoutTransformation(self):
        # arrange
        insertTime = datetime.now()
        batchNumber = 1

        addressTransformationConfig = None

        self._modelTransformationsConfigGenerator._getSQLQueryForTable = MagicMock(return_value="testSQLQuery")
        self._modelTransformationsConfigGenerator._getKeyColumnsForTable = MagicMock(return_value=["keyCol1", "keyCol2"])

        inputParameters = """{ "keyColumns": ["keyCol1", "keyCol2"], "partitionColumns": [], "schemaEvolution": false, "functions": [{"transformation": "sqlQuery", "params": "testSQLQuery"}] }"""

        self._modelTransformationsConfigGenerator._maintenanceManager.arrangeTablePartition = MagicMock()

        self._modelTransformationsConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedDtOutputRecord = [
            None,
            "testSinkDatabaseName.Address",
            inputParameters,
            WRITE_MODE_SCD_TYPE1,
            "testFwkTriggerId",
            "testFwkLayerId",
            batchNumber,
            "Y",
            insertTime,
            insertTime,
            "test_id"
        ]

        runMaintenanceManager = True

        # act
        dtOutputRecord = self._modelTransformationsConfigGenerator._generateDtOutputInstructionForTable(
            "testSinkDatabaseName",
            "Address",
            addressTransformationConfig,
            "testMetadataKeysTable",
            "testMetadataRelationsTable",
            "testTransformationTable",
            WRITE_MODE_SCD_TYPE1,
            "testFwkLayerId",
            "testFwkTriggerId",
            -1,
            batchNumber,
            insertTime,
            runMaintenanceManager
        )

        # assert
        self.assertEqual(expectedDtOutputRecord, dtOutputRecord)

        self._modelTransformationsConfigGenerator._getSQLQueryForTable.assert_called_once_with(
            "testSinkDatabaseName",
            "Address",
            "testMetadataRelationsTable",
            -1
        )

        self._modelTransformationsConfigGenerator._getKeyColumnsForTable.assert_called_once_with(
            "testSinkDatabaseName",
            "Address",
            addressTransformationConfig,
            "testMetadataKeysTable",
            "testTransformationTable",
            WRITE_MODE_SCD_TYPE1
        )

        self._modelTransformationsConfigGenerator._maintenanceManager.arrangeTablePartition.assert_called_once_with(
            "testSinkDatabaseName.Address",
            [],
            WRITE_MODE_SCD_TYPE1
        )

        self._modelTransformationsConfigGenerator._configGeneratorHelper.getDatabricksId.assert_called_once()

    def test_generateDtOutputInstructionForTable_withoutArrangeTablePartition(self):
        # arrange
        insertTime = datetime.now()
        batchNumber = 1

        addressTransformationConfig = None

        self._modelTransformationsConfigGenerator._getSQLQueryForTable = MagicMock(return_value="testSQLQuery")
        self._modelTransformationsConfigGenerator._getKeyColumnsForTable = MagicMock(return_value=["keyCol1", "keyCol2"])

        inputParameters = """{ "keyColumns": ["keyCol1", "keyCol2"], "partitionColumns": [], "schemaEvolution": false, "functions": [{"transformation": "sqlQuery", "params": "testSQLQuery"}] }"""

        self._modelTransformationsConfigGenerator._maintenanceManager.arrangeTablePartition = MagicMock()

        self._modelTransformationsConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedDtOutputRecord = [
            None,
            "testSinkDatabaseName.Address",
            inputParameters,
            WRITE_MODE_SCD_TYPE1,
            "testFwkTriggerId",
            "testFwkLayerId",
            batchNumber,
            "Y",
            insertTime,
            insertTime,
            "test_id"
        ]

        runMaintenanceManager = False

        # act
        dtOutputRecord = self._modelTransformationsConfigGenerator._generateDtOutputInstructionForTable(
            "testSinkDatabaseName",
            "Address",
            addressTransformationConfig,
            "testMetadataKeysTable",
            "testMetadataRelationsTable",
            "testTransformationTable",
            WRITE_MODE_SCD_TYPE1,
            "testFwkLayerId",
            "testFwkTriggerId",
            -1,
            batchNumber,
            insertTime,
            runMaintenanceManager
        )

        # assert
        self.assertEqual(expectedDtOutputRecord, dtOutputRecord)

        self._modelTransformationsConfigGenerator._getSQLQueryForTable.assert_called_once_with(
            "testSinkDatabaseName",
            "Address",
            "testMetadataRelationsTable",
            -1
        )

        self._modelTransformationsConfigGenerator._getKeyColumnsForTable.assert_called_once_with(
            "testSinkDatabaseName",
            "Address",
            addressTransformationConfig,
            "testMetadataKeysTable",
            "testTransformationTable",
            WRITE_MODE_SCD_TYPE1
        )

        self.assertEqual(self._modelTransformationsConfigGenerator._maintenanceManager.arrangeTablePartition.call_count, 0)

        self._modelTransformationsConfigGenerator._configGeneratorHelper.getDatabricksId.assert_called_once()

    def test_generateDtOutputInstructionForTable_withTransformation(self):
        # arrange
        insertTime = datetime.now()
        batchNumber = 1

        addressTransformationConfig = Row(
            EntityName = "Address",
            Key = "testTransformationKey",
            Params = """{ "writeMode": "Snapshot", "keyColumns": ["AddressID"], "partitionColumns": ["Col1", "Col2"], "fwkTriggerId": "Weekly", "batchNumber": 10 }""",
            WriteMode = WRITE_MODE_SNAPSHOT,
            KeyColumns = ["AddressID"],
            PartitionColumns = ["Col1", "Col2"],
            FwkTriggerId = "Weekly",
            BatchNumber = 10
        )

        self._modelTransformationsConfigGenerator._getSQLQueryForTable = MagicMock(return_value="testSQLQuery")
        self._modelTransformationsConfigGenerator._getKeyColumnsForTable = MagicMock(return_value=["keyCol1", "keyCol2"])

        inputParameters = """{ "keyColumns": ["keyCol1", "keyCol2"], "partitionColumns": ["Col1", "Col2"], "schemaEvolution": false, "functions": [{"transformation": "sqlQuery", "params": "testSQLQuery"}] }"""

        self._modelTransformationsConfigGenerator._maintenanceManager.arrangeTablePartition = MagicMock()

        self._modelTransformationsConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedDtOutputRecord = [
            None,
            "testSinkDatabaseName.Address",
            inputParameters,
            WRITE_MODE_SNAPSHOT,
            "Weekly",
            "testFwkLayerId",
            10,
            "Y",
            insertTime,
            insertTime,
            "test_id"
        ]

        runMaintenanceManager = True

        # act
        dtOutputRecord = self._modelTransformationsConfigGenerator._generateDtOutputInstructionForTable(
            "testSinkDatabaseName",
            "Address",
            addressTransformationConfig,
            "testMetadataKeysTable",
            "testMetadataRelationsTable",
            "testTransformationTable",
            WRITE_MODE_SCD_TYPE1,
            "testFwkLayerId",
            "testFwkTriggerId",
            -1,
            batchNumber,
            insertTime,
            runMaintenanceManager
        )

        # assert
        self.assertEqual(expectedDtOutputRecord, dtOutputRecord)

        self._modelTransformationsConfigGenerator._getSQLQueryForTable.assert_called_once_with(
            "testSinkDatabaseName",
            "Address",
            "testMetadataRelationsTable",
            -1
        )

        self._modelTransformationsConfigGenerator._getKeyColumnsForTable.assert_called_once_with(
            "testSinkDatabaseName",
            "Address",
            addressTransformationConfig,
            "testMetadataKeysTable",
            "testTransformationTable",
            WRITE_MODE_SNAPSHOT
        )

        self._modelTransformationsConfigGenerator._maintenanceManager.arrangeTablePartition.assert_called_once_with(
            "testSinkDatabaseName.Address",
            ["Col1", "Col2"],
            WRITE_MODE_SNAPSHOT
        )

        self._modelTransformationsConfigGenerator._configGeneratorHelper.getDatabricksId.assert_called_once()

    def test_generateDtOutputInstructionForTable_notSupportedWriteMode(self):
        # arrange
        insertTime = datetime.now()
        batchNumber = 1

        addressTransformationConfig = Row(
            EntityName = "Address",
            Key = "testTransformationKey",
            Params = """{ "writeMode": "Unsupported_Write_Mode", "keyColumns": ["AddressID"], "fwkTriggerId": "Weekly", "batchNumber": 10 }""",
            WriteMode = "Unsupported_Write_Mode",
            KeyColumns = ["AddressID"],
            FwkTriggerId = "Weekly",
            BatchNumber = 10
        )

        runMaintenanceManager = True

        # act
        with self.assertRaisesRegex(AssertionError, "WriteMode 'Unsupported_Write_Mode' defined in '.*.testTransformationTable' for 'Address' is not supported"):
            self._modelTransformationsConfigGenerator._generateDtOutputInstructionForTable(
                "testSinkDatabaseName",
                "Address",
                addressTransformationConfig,
                "testMetadataKeysTable",
                "testMetadataRelationsTable",
                "testTransformationTable",
                WRITE_MODE_SCD_TYPE1,
                "testFwkLayerId",
                "testFwkTriggerId",
                -1,
                batchNumber,
                insertTime,
                runMaintenanceManager
            )

    def test_generateDtOutputInstructionForTable_notSupportedWriteModeInModelTransformation(self):
        # arrange
        insertTime = datetime.now()
        batchNumber = 1

        runMaintenanceManager = True

        # act
        with self.assertRaisesRegex(AssertionError, "WriteMode 'Unsupported_Write_Mode' defined in '.*.testTransformationTable' for 'Address' is not supported"):
            self._modelTransformationsConfigGenerator._generateDtOutputInstructionForTable(
                "testSinkDatabaseName",
                "Address",
                None,
                "testMetadataKeysTable",
                "testMetadataRelationsTable",
                "testTransformationTable",
                "Unsupported_Write_Mode",
                "testFwkLayerId",
                "testFwkTriggerId",
                -1,
                batchNumber,
                insertTime,
                runMaintenanceManager
            )


    # generateDtOutput tests
    
    def test_generateDtOutput(self):
        # arrange
        self._modelTransformationsConfigGenerator._sqlContext.tableNames.return_value = ["testtransformationtable"]

        self._modelTransformationsConfigGenerator._spark.sql.return_value = self._spark.createDataFrame([
            ["Address", "testTransformationKey", (["AddressID"], ["Col1", "Col2"], "Weekly", "Snapshot", 10)],
        ], """
            EntityName STRING,
            Key STRING,
            Params STRUCT<
                keyColumns ARRAY<STRING>,
                partitionColumns ARRAY<STRING>,
                fwkTriggerId STRING,
                writeMode STRING,
                batchNumber INTEGER
            >
        """)

        addressTransformationConfig = Row(
            EntityName = "Address",
            Key = "testTransformationKey",
            Params = Row(keyColumns=['AddressID'], partitionColumns=["Col1", "Col2"], fwkTriggerId='Weekly', writeMode='Snapshot', batchNumber=10),
            WriteMode = "Snapshot",
            KeyColumns = ["AddressID"],
            PartitionColumns = ["Col1", "Col2"],
            FwkTriggerId = "Weekly",
            BatchNumber = 10
        )

        self._modelTransformationsConfigGenerator._generateDtOutputInstructionForTable = MagicMock(return_value=["Instruction"])

        batches = {
            1: [Row(Entity="Customer"), Row(Entity="Address")],
            2: [Row(Entity="CustomerAddress")]
        }

        expectedDtOutputDF = MagicMock()
        self._modelTransformationsConfigGenerator._spark.createDataFrame.return_value = expectedDtOutputDF

        runMaintenanceManager = True

        # act
        dtOutputDF = self._modelTransformationsConfigGenerator._generateDtOutput(
            "testMetadataKeysTable",
            "testMetadataRelationsTable",
            "testTransformationTable",
            "testTransformationKey",
            "testSinkDatabaseName",
            WRITE_MODE_SCD_TYPE1,
            "testFwkLayerId",
            "testFwkTriggerId",
            -1,
            batches,
            runMaintenanceManager
        )

        # assert
        self.assertEqual(dtOutputDF, expectedDtOutputDF)

        self._modelTransformationsConfigGenerator._spark.createDataFrame.assert_called_once_with([
            ["Instruction"],
            ["Instruction"],
            ["Instruction"],
        ], DT_OUTPUT_SCHEMA)

        self._modelTransformationsConfigGenerator._spark.sql.assert_called_once()

        self.assertEqual(self._modelTransformationsConfigGenerator._generateDtOutputInstructionForTable.call_count, 3)

        self._modelTransformationsConfigGenerator._generateDtOutputInstructionForTable.assert_any_call(
            "testSinkDatabaseName",
            "Customer",
            None,
            "testMetadataKeysTable",
            "testMetadataRelationsTable",
            "testTransformationTable",
            WRITE_MODE_SCD_TYPE1,
            "testFwkLayerId",
            "testFwkTriggerId",
            -1,
            1,
            ANY,
            True
        )

        self._modelTransformationsConfigGenerator._generateDtOutputInstructionForTable.assert_any_call(
            "testSinkDatabaseName",
            "Address",
            addressTransformationConfig,
            "testMetadataKeysTable",
            "testMetadataRelationsTable",
            "testTransformationTable",
            WRITE_MODE_SCD_TYPE1,
            "testFwkLayerId",
            "testFwkTriggerId",
            -1,
            1,
            ANY,
            True
        )

        self._modelTransformationsConfigGenerator._generateDtOutputInstructionForTable.assert_any_call(
            "testSinkDatabaseName",
            "CustomerAddress",
            None,
            "testMetadataKeysTable",
            "testMetadataRelationsTable",
            "testTransformationTable",
            WRITE_MODE_SCD_TYPE1,
            "testFwkLayerId",
            "testFwkTriggerId",
            -1,
            2,
            ANY,
            True
        )


    # generateFwkEntities tests

    def test_generateFwkEntities(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["Customer"],
            ["Address"]
        ], "Entity STRING")

        self._modelTransformationsConfigGenerator._spark.sql.return_value = entitiesDF
        self._modelTransformationsConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        
        self._modelTransformationsConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "StagingSQL.Customer",
                "LinkedServiceId",
                "path/to/data/Customer",
                "Delta",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "StagingSQL.Address",
                "LinkedServiceId",
                "path/to/data/Address",
                "Delta",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
        ], FWK_ENTITY_SCHEMA)

        # act
        fwkEntitiesDF = self._modelTransformationsConfigGenerator._generateFwkEntities("AttributesTable", "StagingSQL", "LinkedServiceId", "path/to/data")

        # assert
        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").rdd.collect()
        )
        
        self.assertEqual(self._modelTransformationsConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 2)


    # generateAttributes tests

    def test_generateAttributes(self):
        # arrange
        metadataKeysTableDF = self._spark.createDataFrame([
            ["Address", "PIdentifier", "WK_Address_Identifier",],
            ["Address", "BusinessIdentifier", "AddressID"],
            ["Customer", "PIdentifier", "WK_Customer_Identifier"],
            ["Customer", "BusinessIdentifier", "CustomerID"],
            ["CustomerNoIdentity", "PIdentifier", "CustomerID"],
            ["CustomerAddress", "PIdentifier", "WK_CustomerAddress_Identifier"],
            ["CustomerAddress", "BusinessIdentifier", "CustomerID"],
            ["CustomerAddress", "BusinessIdentifier", "AddressID"]
        ], METADATA_MODEL_KEYS_SCHEMA)

        metadataAttributesTableDF = self._spark.createDataFrame([
            ["Address", "WK_Address_Identifier", "DataType", True, True],
            ["Address", "AddressID", "DataType", False, False],
            ["Address", "City", "DataType", False, False],
            ["Customer", "WK_Customer_Identifier", "DataType", True, True],
            ["Customer", "CustomerID", "DataType", False, False],
			["Customer", "FirstName", "DataType", False, False],
			["Customer", "LastName", "DataType", False, False],
            ["CustomerNoIdentity", "CustomerID", "DataType", True, False],
			["CustomerNoIdentity", "FirstName", "DataType", False, False],
			["CustomerNoIdentity", "LastName", "DataType", False, False],
            ["CustomerAddress", "WK_CustomerAddress_Identifier", "DataType", True, True],
            ["CustomerAddress", "WK_Customer_Identifier", "DataType", False, False],
            ["CustomerAddress", "CustomerID", "DataType", False, False],
            ["CustomerAddress", "WK_Address_Identifier", "DataType", False, False],
            ["CustomerAddress", "AddressID", "DataType", False, False],
            ["NoPrimaryNoIdentity", "Col1", "DataType", False, False],
            ["NoPrimaryNoIdentity", "Col2", "DataType", False, False],
            ["NoPrimaryNoIdentity", "Col3", "DataType", False, False]
        ], METADATA_MODEL_ATTRIBUTES_SCHEMA)

        allEntitiesDF = self._spark.createDataFrame([
            ["Address"],
            ["Customer"],
            ["CustomerNoIdentity"],
            ["CustomerAddress"],
            ["NoPrimaryNoIdentity"]
        ], "Entity STRING")

        def sqlQuery(sqlCommand: str) -> DataFrame:
            if "WHERE KeyType != 'PIdentifier'" in sqlCommand:
                return metadataKeysTableDF.filter("KeyType != 'PIdentifier'").select("Entity", "Attribute")
            elif "WHERE KeyType == 'PIdentifier'" in sqlCommand:
                return metadataKeysTableDF.filter("KeyType == 'PIdentifier'").select("Entity", "Attribute")
            elif "SELECT Entity, Attribute, IsIdentity" in sqlCommand:
                return metadataAttributesTableDF.select("Entity", "Attribute", "IsIdentity")
            elif "WHERE IsIdentity" in sqlCommand:
                return metadataAttributesTableDF.filter("IsIdentity").selectExpr("Entity", "Attribute", "FALSE AS IsPrimaryKey", "IsIdentity")
            elif "Distinct Entity" in sqlCommand:
                return allEntitiesDF
            
        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        expectedAttributesDF = self._spark.createDataFrame([
            ["SDM", "Address", "WK_Address_Identifier", False, True, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["SDM", "Address", "AddressID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["SDM", "Customer", "WK_Customer_Identifier", False, True, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["SDM", "Customer", "CustomerID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["SDM", "CustomerNoIdentity", "CustomerID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["SDM", "CustomerAddress", "WK_CustomerAddress_Identifier", False, True, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["SDM", "CustomerAddress", "CustomerID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["SDM", "CustomerAddress", "AddressID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["SDM", "NoPrimaryNoIdentity", None, None, None, METADATA_ATTRIBUTES_TRANSFORMATION_KEY]
        ], METADATA_ATTRIBUTES_SCHEMA)

        # act
        attributesDF = self._modelTransformationsConfigGenerator._generateAttributes("AttributesTable", "KeysTable", "SDM")

        # assert
        self.assertEqual(
            attributesDF.sort("DatabaseName", "EntityName", "Attribute").rdd.collect(),
            expectedAttributesDF.sort("DatabaseName", "EntityName", "Attribute").rdd.collect()
        )
    

    # createDeltaTableBasedOnMetadata tests
    
    def test_createDeltaTableBasedOnMetadata_withIdentityColumn(self):
        # arrange
        def sqlQuery(sqlCommand: str) -> DataFrame:
            if "IsIdentity = TRUE" in sqlCommand:
                return self._spark.createDataFrame([
                    ["WK_Customer_Identifier", "bigint"]
                ], "Attribute STRING, DataType STRING")
            else:
                mock_rdd = MagicMock()
                mock_rdd.map = MagicMock(return_value=mock_rdd)
                mock_rdd.take = MagicMock(return_value=["CustomerID BIGINT", "FirstName STRING", "LastName STRING"])
                mock_rdd.count = MagicMock(return_value=3)
                
                mockDF = MagicMock()
                mockDF.rdd = mock_rdd
                return mockDF

        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        # act
        self._modelTransformationsConfigGenerator._createDeltaTableBasedOnMetadata(
            "testDatabaseName",
            "Customer",
            "testDeltaPath",
            None,
            "testMetadataAttributesTable"
        )

        # assert
        self.assertEqual(self._modelTransformationsConfigGenerator._spark.sql.call_count, 3)

        sqlCommand = self._modelTransformationsConfigGenerator._spark.sql.call_args_list[2].args[0]
        self.assertIn("CREATE TABLE IF NOT EXISTS testDatabaseName.Customer", sqlCommand)
        self.assertIn("WK_Customer_Identifier BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)", sqlCommand)
        self.assertIn("CustomerID BIGINT", sqlCommand)
        self.assertIn("FirstName STRING", sqlCommand)
        self.assertIn("LastName STRING", sqlCommand)
        self.assertIn(f"{INSERT_TIME_COLUMN_NAME} TIMESTAMP", sqlCommand)
        self.assertIn(f"{UPDATE_TIME_COLUMN_NAME} TIMESTAMP", sqlCommand)
        self.assertIn("USING DELTA", sqlCommand)
        self.assertIn('LOCATION "testDeltaPath"', sqlCommand)
        self.assertNotIn("PARTITIONED BY", sqlCommand)

    def test_createDeltaTableBasedOnMetadata_withoutIdentityColumn(self):
        # arrange
        def sqlQuery(sqlCommand: str) -> DataFrame:
            if "IsIdentity = TRUE" in sqlCommand:
                return self._spark.createDataFrame([], "Attribute STRING, DataType STRING")
            else:
                mock_rdd = MagicMock()
                mock_rdd.map = MagicMock(return_value=mock_rdd)
                mock_rdd.take = MagicMock(return_value=["CustomerID BIGINT", "FirstName STRING", "LastName STRING"])
                mock_rdd.count = MagicMock(return_value=3)
                
                mockDF = MagicMock()
                mockDF.rdd = mock_rdd
                return mockDF

        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        # act
        self._modelTransformationsConfigGenerator._createDeltaTableBasedOnMetadata(
            "testDatabaseName",
            "Customer",
            "testDeltaPath",
            None,
            "testMetadataAttributesTable"
        )

        # assert
        self.assertEqual(self._modelTransformationsConfigGenerator._spark.sql.call_count, 3)

        sqlCommand = self._modelTransformationsConfigGenerator._spark.sql.call_args_list[2].args[0]
        self.assertIn("CREATE TABLE IF NOT EXISTS testDatabaseName.Customer", sqlCommand)
        self.assertNotIn("WK_Customer_Identifier BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)", sqlCommand)
        self.assertIn("CustomerID BIGINT", sqlCommand)
        self.assertIn("FirstName STRING", sqlCommand)
        self.assertIn("LastName STRING", sqlCommand)
        self.assertIn(f"{INSERT_TIME_COLUMN_NAME} TIMESTAMP", sqlCommand)
        self.assertIn(f"{UPDATE_TIME_COLUMN_NAME} TIMESTAMP", sqlCommand)
        self.assertIn("USING DELTA", sqlCommand)
        self.assertIn('LOCATION "testDeltaPath"', sqlCommand)
        self.assertNotIn("PARTITIONED BY", sqlCommand)
    
    def test_createDeltaTableBasedOnMetadata_withoutPartitionColumns(self):
        # arrange
        def sqlQuery(sqlCommand: str) -> DataFrame:
            if "IsIdentity = TRUE" in sqlCommand:
                return self._spark.createDataFrame([], "Attribute STRING, DataType STRING")
            else:
                mock_rdd = MagicMock()
                mock_rdd.map = MagicMock(return_value=mock_rdd)
                mock_rdd.take = MagicMock(return_value=["CustomerID BIGINT", "FirstName STRING", "LastName STRING"])
                mock_rdd.count = MagicMock(return_value=3)
                
                mockDF = MagicMock()
                mockDF.rdd = mock_rdd
                return mockDF

        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        # act
        self._modelTransformationsConfigGenerator._createDeltaTableBasedOnMetadata(
            "testDatabaseName",
            "Customer",
            "testDeltaPath",
            ["Col1", "Col2"],
            "testMetadataAttributesTable"
        )

        # assert
        self.assertEqual(self._modelTransformationsConfigGenerator._spark.sql.call_count, 3)

        sqlCommand = self._modelTransformationsConfigGenerator._spark.sql.call_args_list[2].args[0]
        self.assertIn("CREATE TABLE IF NOT EXISTS testDatabaseName.Customer", sqlCommand)
        self.assertNotIn("WK_Customer_Identifier BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1)", sqlCommand)
        self.assertIn("CustomerID BIGINT", sqlCommand)
        self.assertIn("FirstName STRING", sqlCommand)
        self.assertIn("LastName STRING", sqlCommand)
        self.assertIn(f"{INSERT_TIME_COLUMN_NAME} TIMESTAMP", sqlCommand)
        self.assertIn(f"{UPDATE_TIME_COLUMN_NAME} TIMESTAMP", sqlCommand)
        self.assertIn("USING DELTA", sqlCommand)
        self.assertIn('LOCATION "testDeltaPath"', sqlCommand)
        self.assertIn("PARTITIONED BY (`Col1`, `Col2`)", sqlCommand)

    # addOrDropColumnsForDeltaTable tests
    
    def test_addOrDropColumnsForDeltaTable(self):
        # arrange
        changesDF = self._spark.createDataFrame([
            ["CustomerID", "bigint", "CustomerID", "bigint"],
            ["FirstName", "varchar(50)", "FirstName", "string"],
            ["LastName", "varchar(55)", "LastName", "string"],
            ["Score", "decimal(10,2)", "Score", "string"],
            ["NewColumnInConfig", "bigint", None, None],
            ["AnotherNewColumnInConfig", "varchar(25)", None, None],
            [None, None, "NewColumnInHive", "integer"],
        ], "Attribute STRING, DataType STRING, CurrentAttribute STRING, CurrentDataType STRING")

        uniqueCL = ['NewColumnInConfig', 'Test', 'AnotherNewColumnInConfig']
        
        def convertModelToSparkDataType(datatype: str) -> str:
            if datatype == "bigint":
                return "BIGINT"
            else:
                return "STRING"

        self._modelTransformationsConfigGenerator._convertModelToSparkDataType = MagicMock(side_effect=convertModelToSparkDataType)
        self._modelTransformationsConfigGenerator._dataLakeHelper.allowAlterForTable = MagicMock()

        # act
        self._modelTransformationsConfigGenerator._addOrDropColumnsForDeltaTable(
            "testDatabaseName",
            "Customer",
            changesDF,
            uniqueCL
        )

        # assert
        self.assertEqual(self._modelTransformationsConfigGenerator._convertModelToSparkDataType.call_count, 2)
        
        self._modelTransformationsConfigGenerator._convertModelToSparkDataType.assert_any_call("bigint")
        self._modelTransformationsConfigGenerator._convertModelToSparkDataType.assert_any_call("varchar(25)")

        self.assertEqual(self._modelTransformationsConfigGenerator._spark.sql.call_count, 3)

        self._modelTransformationsConfigGenerator._spark.sql.assert_any_call(
            "ALTER TABLE testDatabaseName.Customer ADD COLUMN `NewColumnInConfig` BIGINT FIRST" 
        )
        self._modelTransformationsConfigGenerator._spark.sql.assert_any_call(
            "ALTER TABLE testDatabaseName.Customer ADD COLUMN `AnotherNewColumnInConfig` STRING AFTER Test"
        )

        self._modelTransformationsConfigGenerator._spark.sql.assert_any_call(
            "ALTER TABLE testDatabaseName.Customer DROP COLUMN `NewColumnInHive`"
        )

        self.assertEqual(self._modelTransformationsConfigGenerator._dataLakeHelper.allowAlterForTable.call_count, 1)
    

    # changeColumnDataTypesForDeltaTable tests

    def test_changeColumnDataTypesForDeltaTable(self):
        # arrange
        changesDF = self._spark.createDataFrame([
            ["CustomerID", "bigint", "CustomerID", "bigint"],
            ["FirstName", "varchar(50)", "FirstName", "string"],
            ["LastName", "varchar(55)", "LastName", "string"],
            ["Score", "decimal(10,2)", "Score", "string"],
            ["NewColumnInConfig", "bigint", None, None],
            ["AnotherNewColumnInConfig", "varchar(25)", None, None],
            [None, None, "NewColumnInHive", "integer"],
        ], "Attribute STRING, DataType STRING, CurrentAttribute STRING, CurrentDataType STRING")
        
        def convertModelToSparkDataType(datatype: str) -> str:
            if datatype == "bigint":
                return "BIGINT"
            elif datatype == "decimal(10,2)":
                return "DECIMAL(10,2)"
            else:
                return "STRING"

        self._modelTransformationsConfigGenerator._convertModelToSparkDataType = MagicMock(side_effect=convertModelToSparkDataType)
        self._modelTransformationsConfigGenerator._dataLakeHelper.allowAlterForTable = MagicMock()

        # act
        self._modelTransformationsConfigGenerator._changeColumnDataTypesForDeltaTable(
            "testDatabaseName",
            "Customer",
            changesDF
        )

        # assert
        self.assertEqual(self._modelTransformationsConfigGenerator._convertModelToSparkDataType.call_count, 4)
        
        self._modelTransformationsConfigGenerator._convertModelToSparkDataType.assert_any_call("bigint")
        self._modelTransformationsConfigGenerator._convertModelToSparkDataType.assert_any_call("varchar(50)")
        self._modelTransformationsConfigGenerator._convertModelToSparkDataType.assert_any_call("varchar(55)")
        self._modelTransformationsConfigGenerator._convertModelToSparkDataType.assert_any_call("decimal(10,2)")

        self.assertEqual(self._modelTransformationsConfigGenerator._spark.sql.call_count, 4)

        self._modelTransformationsConfigGenerator._spark.sql.assert_any_call(
            "ALTER TABLE testDatabaseName.Customer ADD COLUMN `Score_tempFwk` decimal(10,2) AFTER `Score`"
        )
        self._modelTransformationsConfigGenerator._spark.sql.assert_any_call(
            "UPDATE testDatabaseName.Customer SET `Score_tempFwk` = `Score`"
        )

        self._modelTransformationsConfigGenerator._spark.sql.assert_any_call(
            "ALTER TABLE testDatabaseName.Customer DROP COLUMN IF EXISTS `Score`"
        )

        self._modelTransformationsConfigGenerator._spark.sql.assert_any_call(
            "ALTER TABLE testDatabaseName.Customer RENAME COLUMN `Score_tempFwk` TO `Score`"
        )

        self.assertEqual(self._modelTransformationsConfigGenerator._dataLakeHelper.allowAlterForTable.call_count, 1)


    # alterDeltaTableBasedOnMetadata tests

    def test_alterDeltaTableBasedOnMetadata(self):
        # arrange
        def sqlQuery(sqlCommand: str) -> DataFrame:
            if "SELECT Attribute" in sqlCommand:
                return self._spark.createDataFrame([
                    ["CustomerID", "bigint"],
                    ["FirstName", "varchar(50)"],
                    ["LastName", "varchar(55)"],
                    ["Score", "decimal(10,2)"],
                    ["NewColumnInConfig", "bigint"],
                    ["AnotherNewColumnInConfig", "varchar(25)"],
                ], "Attribute STRING, DataType STRING")
            elif "DESCRIBE TABLE testDatabaseName.Customer" in sqlCommand:
                return self._spark.createDataFrame([
                    ["CustomerID", "bigint"],
                    ["FirstName", "string"],
                    ["LastName", "string"],
                    ["Score", "string"],
                    ["NewColumnInHive", "integer"],
                    ["# Partition Information", None]
                ], "col_name STRING, data_type STRING")

        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        changesDF = self._spark.createDataFrame([
            ["CustomerID", "bigint", "CustomerID", "bigint"],
            ["FirstName", "varchar(50)", "FirstName", "string"],
            ["LastName", "varchar(55)", "LastName", "string"],
            ["Score", "decimal(10,2)", "Score", "string"],
            ["NewColumnInConfig", "bigint", None, None],
            ["AnotherNewColumnInConfig", "varchar(25)", None, None],
            [None, None, "NewColumnInHive", "integer"],
        ], "Attribute STRING, DataType STRING, CurrentAttribute STRING, CurrentDataType STRING")

        self._modelTransformationsConfigGenerator._addOrDropColumnsForDeltaTable = MagicMock(return_value=["Change 1", "Change 2"])
        self._modelTransformationsConfigGenerator._changeColumnDataTypesForDeltaTable = MagicMock(return_value=["Change 3"])

        expectedColumnChangeLogs = ["Change 1", "Change 2", "Change 3"]

        # act
        columnChangeLogs = self._modelTransformationsConfigGenerator._alterDeltaTableBasedOnMetadata(
            "testDatabaseName",
            "Customer",
            "testMetadataAttributesTable"
        )

        # assert
        self.assertEqual(expectedColumnChangeLogs, columnChangeLogs)

        self.assertEqual(self._modelTransformationsConfigGenerator._addOrDropColumnsForDeltaTable.call_count, 1)
        
        args = self._modelTransformationsConfigGenerator._addOrDropColumnsForDeltaTable.call_args_list[0].args
        self.assertEqual(args[0], "testDatabaseName")
        self.assertEqual(args[1], "Customer")
        self.assertEqual(
            args[2].sort("Attribute").rdd.collect(),
            changesDF.sort("Attribute").rdd.collect()
        )
        self.assertEqual(args[3], ['CustomerID', 'FirstName', 'LastName', 'Score', 'NewColumnInConfig', 'AnotherNewColumnInConfig'])

        self.assertEqual(self._modelTransformationsConfigGenerator._changeColumnDataTypesForDeltaTable.call_count, 1)
        
        args = self._modelTransformationsConfigGenerator._changeColumnDataTypesForDeltaTable.call_args_list[0].args
        self.assertEqual(args[0], "testDatabaseName")
        self.assertEqual(args[1], "Customer")
        self.assertEqual(
            args[2].sort("Attribute").rdd.collect(),
            changesDF.sort("Attribute").rdd.collect()
        )
        

    # createOrAlterDeltaTablesBasedOnMetadata tests
    
    def test_createOrAlterDeltaTablesBasedOnMetadata(self):
        # arrange
        metadataConfig = {
            "sink": {"databaseName": "test_db"},
            "metadata": {"attributesTable": "attributes_table"}
        }

        self._modelTransformationsConfigGenerator._configGeneratorHelper.getADLSPathAndUri = MagicMock(
            return_value=("testSinkPath", "testSinkUri")
        )

        attributesDF = self._spark.createDataFrame([
            ["Customer"],
            ["Address"],
            ["CustomerAddress"]
        ], "Entity STRING")

        self._modelTransformationsConfigGenerator._spark.sql.return_value = attributesDF

        self._modelTransformationsConfigGenerator._sqlContext.tableNames.return_value = ["customer", "address"]

        self._modelTransformationsConfigGenerator._createDeltaTableBasedOnMetadata = MagicMock()
        
        def mockedAlterDeltaTableBasedOnMetadata(databaseName: str, tableName: str, metadataAttributesTable: str) -> ArrayType:
            if tableName == "Customer":
                return ["Change 1 for Customer", "Change 2 for Customer"]
            elif tableName == "Address":
                return ["Change 1 for Address"]
            else:
                return []

        self._modelTransformationsConfigGenerator._alterDeltaTableBasedOnMetadata = MagicMock(side_effect=mockedAlterDeltaTableBasedOnMetadata)

        expectedColumnChangeLogs = ["Change 1 for Customer", "Change 2 for Customer", "Change 1 for Address"]

        # act
        columnChangeLogs = self._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata(metadataConfig)

        # assert
        self.assertEqual(expectedColumnChangeLogs, columnChangeLogs)

        self.assertEqual(self._modelTransformationsConfigGenerator._createDeltaTableBasedOnMetadata.call_count, 1)
        self.assertEqual(self._modelTransformationsConfigGenerator._alterDeltaTableBasedOnMetadata.call_count, 2)

        self._modelTransformationsConfigGenerator._createDeltaTableBasedOnMetadata.assert_any_call(
            "test_db", "CustomerAddress", "testSinkUri/CustomerAddress", [], "attributes_table"
        )

        self._modelTransformationsConfigGenerator._alterDeltaTableBasedOnMetadata.assert_any_call(
            "test_db", "Address", "attributes_table"
        )

        self._modelTransformationsConfigGenerator._alterDeltaTableBasedOnMetadata.assert_any_call(
            "test_db", "Customer", "attributes_table"
        )

    def test_createOrAlterDeltaTablesBasedOnMetadata_withPartitionColumns(self):
        # arrange
        metadataConfig = {
            "sink": {"databaseName": "test_db"},
            "metadata": {
                "attributesTable": "attributes_table",
                "transformationTable": "ModelTransformationConfiguration",
                "key": "SDM"
            }
        }

        self._modelTransformationsConfigGenerator._configGeneratorHelper.getADLSPathAndUri = MagicMock(
            return_value=("testSinkPath", "testSinkUri")
        )

        attributesDF = self._spark.createDataFrame([
            ["Customer"],
            ["Address"],
            ["CustomerAddress"]
        ], "Entity STRING")

        modelTransformationConfigurationDF = self._spark.createDataFrame([
            ["CustomerAddress", (None, ["Col1, Col2"], None, None, None)],
        ], """
            EntityName STRING,
            Params STRUCT<
                keyColumns ARRAY<STRING>,
                partitionColumns ARRAY<STRING>,
                fwkTriggerId STRING,
                writeMode STRING,
                batchNumber INTEGER
            >
        """)

        def sqlQuery(sqlCommand: str) -> DataFrame:            
            if "SELECT DISTINCT Entity" in sqlCommand:
                return attributesDF
            elif "SELECT EntityName, Params" in sqlCommand:
                return modelTransformationConfigurationDF

        self._modelTransformationsConfigGenerator._spark.sql.side_effect = sqlQuery

        self._modelTransformationsConfigGenerator._sqlContext.tableNames.return_value = ["customer", "address", "modeltransformationconfiguration"]

        self._modelTransformationsConfigGenerator._createDeltaTableBasedOnMetadata = MagicMock()
        
        def mockedAlterDeltaTableBasedOnMetadata(databaseName: str, tableName: str, metadataAttributesTable: str) -> ArrayType:
            if tableName == "Customer":
                return ["Change 1 for Customer", "Change 2 for Customer"]
            elif tableName == "Address":
                return ["Change 1 for Address"]
            else:
                return []

        self._modelTransformationsConfigGenerator._alterDeltaTableBasedOnMetadata = MagicMock(side_effect=mockedAlterDeltaTableBasedOnMetadata)

        expectedColumnChangeLogs = ["Change 1 for Customer", "Change 2 for Customer", "Change 1 for Address"]

        # act
        columnChangeLogs = self._modelTransformationsConfigGenerator.createOrAlterDeltaTablesBasedOnMetadata(metadataConfig)

        # assert
        self.assertEqual(expectedColumnChangeLogs, columnChangeLogs)

        self.assertEqual(self._modelTransformationsConfigGenerator._createDeltaTableBasedOnMetadata.call_count, 1)
        self.assertEqual(self._modelTransformationsConfigGenerator._alterDeltaTableBasedOnMetadata.call_count, 2)

        self._modelTransformationsConfigGenerator._createDeltaTableBasedOnMetadata.assert_any_call(
            "test_db", "CustomerAddress", "testSinkUri/CustomerAddress", ["Col1, Col2"], "attributes_table"
        )

        self._modelTransformationsConfigGenerator._alterDeltaTableBasedOnMetadata.assert_any_call(
            "test_db", "Address", "attributes_table"
        )

        self._modelTransformationsConfigGenerator._alterDeltaTableBasedOnMetadata.assert_any_call(
            "test_db", "Customer", "attributes_table"
        )
    

    # generateConfiguration tests

    def test_generateConfiguration(self):
        # arrange
        self._modelTransformationsConfigGenerator._getBatchesBasedOnTableRelations = MagicMock()
        self._modelTransformationsConfigGenerator._generateDtOutput = MagicMock()
        self._modelTransformationsConfigGenerator._dataLakeHelper.writeDataAsParquet = MagicMock()
        self._modelTransformationsConfigGenerator._generateFwkEntities = MagicMock()
        self._modelTransformationsConfigGenerator._generateAttributes = MagicMock()

        metadataConfig = {
            "metadata": {
                "attributesTable": "attributes",
                "relationsTable": "relations",
                "keysTable": "keys",
                "unknownMemberDefaultValue": -1
            },
            "sink": {
                "FwkLinkedServiceId": "sinkId",
                "containerName": "myContainerName",
                "databaseName": "sinkDB"
            },
            "FwkLayerId": "layerId",
            "FwkTriggerId": "triggerId"
        }

        runMaintenanceManager = True

        # act
        self._modelTransformationsConfigGenerator.generateConfiguration(
            metadataConfig,
            "testOutputPath",
            runMaintenanceManager
        )

        # assert
        self._modelTransformationsConfigGenerator._getBatchesBasedOnTableRelations.assert_called_once_with(
            "attributes",
            "relations"
        )

        self._modelTransformationsConfigGenerator._generateDtOutput.assert_called_once_with(
            "keys",
            "relations",
            None,
            None,
            "sinkDB",
            WRITE_MODE_OVERWRITE,
            "layerId",
            "triggerId",
            -1,
            self._modelTransformationsConfigGenerator._getBatchesBasedOnTableRelations.return_value,
            True
        )

        self._modelTransformationsConfigGenerator._generateFwkEntities.assert_called_once_with(
            "attributes",
            "sinkDB",
            "sinkId",
            "myContainerName"
        )

        self.assertEqual(self._modelTransformationsConfigGenerator._dataLakeHelper.writeDataAsParquet.call_count, 2) 

        self._modelTransformationsConfigGenerator._dataLakeHelper.writeDataAsParquet.assert_any_call(
            self._modelTransformationsConfigGenerator._generateDtOutput.return_value,
            f"testOutputPath/{DT_OUTPUT_FOLDER}/", WRITE_MODE_APPEND
        )

        self._modelTransformationsConfigGenerator._dataLakeHelper.writeDataAsParquet.assert_any_call(
            self._modelTransformationsConfigGenerator._generateFwkEntities.return_value,
            f"testOutputPath/{FWK_ENTITY_FOLDER}/", WRITE_MODE_APPEND
        )

        self._modelTransformationsConfigGenerator._generateAttributes.assert_called_once_with(
            "attributes",
            "keys",
            "sinkDB"
        )

        self._modelTransformationsConfigGenerator._generateAttributes.assert_called_once_with(
            "attributes",
            "keys",
            "sinkDB"
        )

        self._modelTransformationsConfigGenerator._generateAttributes.assert_called_once_with(
            "attributes",
            "keys",
            "sinkDB"
        )
