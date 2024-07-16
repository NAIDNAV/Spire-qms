# Databricks notebook source
import unittest
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, Row
from unittest.mock import MagicMock, ANY

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.generator.configGenerators.transformationsConfigGenerator import TransformationsConfigGenerator
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../../../compartment/includes/compartmentConfig 

# COMMAND ----------

# MAGIC %run ../../../../../generator/configGenerators/transformationsConfigGenerator

# COMMAND ----------

# MAGIC %run ../../../../../includes/frameworkConfig

# COMMAND ----------

class TransformationsConfigGeneratorTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()
        self._compartmentConfig = MagicMock()

    def setUp(self):
        self._transformationsConfigGenerator = TransformationsConfigGenerator(self._spark, self._compartmentConfig)
        self._transformationsConfigGenerator._spark = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # getDistinctDatabaseNames tests

    def test_getDistinctDatabaseNames(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer"],
            ["StagingSQL", "Address"],
            ["StagingOracle", "Customer"],
            ["StagingOracle", "Address"],
        ], "DatabaseName STRING, EntityName STRING")

        expectedDatabaseNames = ["StagingSQL", "StagingOracle"]

        # act
        databaseNames = self._transformationsConfigGenerator._getDistinctDatabaseNames(entitiesDF)

        # assert
        self.assertEqual(databaseNames.sort(), expectedDatabaseNames.sort())


    # getAttributesForEntitiesDatabaseNames tests

    def test_getAttributesForEntitiesDatabaseNames(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer"],
            ["StagingSQL", "Address"],
            ["StagingOracle", "Customer"],
            ["StagingOracle", "Address"],
        ], "DatabaseName STRING, EntityName STRING")

        expectedResultDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer", "CustomerID", True, False, "Transformation_Configuration"],
            ["StagingSQL", "Customer", "FirstName", False, False, "Transformation_Configuration"],
            ["StagingSQL", "Customer", "LastName", False, False, "Transformation_Configuration"],
            ["StagingSQL", "Address", "AddressID", True, False, "Transformation_Configuration"],
            ["StagingSQL", "Address", "AddressLine1", False, False, "Transformation_Configuration"],
            ["StagingSQL", "Address", "AddressLine2", False, False, "Transformation_Configuration"],
            ["StagingSQL", "AnotherTable", "Col1", True, False, "Transformation_Configuration"],
            ["StagingSQL", "AnotherTable", "Col2", True, False, "Transformation_Configuration"],
            ["StagingOracle", "Customer", "CustomerID", True, False, "Transformation_Configuration"],
            ["StagingOracle", "Customer", "FirstName", False, False, "Transformation_Configuration"],
            ["StagingOracle", "Customer", "LastName", False, False, "Transformation_Configuration"],
            ["StagingOracle", "Address", "AddressID", True, False, "Transformation_Configuration"],
            ["StagingOracle", "Address", "AddressLine1", False, False, "Transformation_Configuration"],
            ["StagingOracle", "Address", "AddressLine2", False, False, "Transformation_Configuration"],
        ], METADATA_ATTRIBUTES_SCHEMA)

        self._transformationsConfigGenerator._spark.sql.return_value = expectedResultDF

        # act
        resultDF = self._transformationsConfigGenerator._getAttributesForEntitiesDatabaseNames(entitiesDF)

        # assert
        self.assertEqual(resultDF.rdd.collect(), expectedResultDF.rdd.collect())

        self._transformationsConfigGenerator._spark.sql.assert_called_once()

        sqlCommand = self._transformationsConfigGenerator._spark.sql.call_args_list[0].args[0]

        self.assertIn("SELECT *", sqlCommand)
        self.assertIn(f"""{self._transformationsConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}""", sqlCommand)
        self.assertIn(f"DatabaseName IN ('StagingSQL', 'StagingOracle')", sqlCommand)


    # getAllEntitiesForDatabaseNames tests

    def test_getAllEntitiesForDatabaseNames(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer"],
            ["StagingSQL", "Address"],
            ["StagingOracle", "Customer"],
            ["StagingOracle", "Address"],
        ], "DatabaseName STRING, EntityName STRING")

        expectedResultDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer"],
            ["StagingSQL", "Address"],
            ["StagingSQL", "AnotherTable"],
            ["StagingOracle", "Customer"],
            ["StagingOracle", "Address"],
        ], "DatabaseName STRING, EntityName STRING")

        self._transformationsConfigGenerator._spark.sql.return_value = expectedResultDF

        # act
        resultDF = self._transformationsConfigGenerator._getAllEntitiesForDatabaseNames(entitiesDF)

        # assert
        self.assertEqual(resultDF.rdd.collect(), expectedResultDF.rdd.collect())

        self._transformationsConfigGenerator._spark.sql.assert_called_once()

        sqlCommand = self._transformationsConfigGenerator._spark.sql.call_args_list[0].args[0]

        self.assertIn("SELECT DISTINCT DatabaseName, EntityName", sqlCommand)
        self.assertIn(f"""{self._transformationsConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}""", sqlCommand)
        self.assertIn(f"DatabaseName IN", sqlCommand)
        self.assertIn(f"StagingSQL", sqlCommand)
        self.assertIn(f"StagingOracle", sqlCommand)


    # getEntitiesFromTransformationTableByKey tests

    def test_getEntitiesFromTransformationTableByKey(self):
        # arrange
        self._transformationsConfigGenerator._spark.sql.return_value = MagicMock(spec=DataFrame)

        # act
        resultDF = self._transformationsConfigGenerator._getEntitiesFromTransformationTableByKey("Transformation_Configuration", "LandingPseudonymization")

        # assert
        self.assertIsInstance(resultDF, DataFrame)

        self._transformationsConfigGenerator._spark.sql.assert_called_once()

        sqlCommand = self._transformationsConfigGenerator._spark.sql.call_args_list[0].args[0]

        self.assertIn("SELECT *", sqlCommand)
        self.assertIn(f"""{self._transformationsConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.Transformation_Configuration""", sqlCommand)
        self.assertIn(f"WHERE Key = 'LandingPseudonymization'", sqlCommand)


    # getWildcardEntitiesReplacedByRealEntities tests

    def test_getWildcardEntitiesReplacedByRealEntities_shouldReturnSameDataFrameWhenThereAreNoWildcardEntities(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer"],
            ["StagingSQL", "Address"],
            ["StagingOracle", "Customer"],
            ["StagingOracle", "Address"],
        ], "DatabaseName STRING, EntityName STRING")

        # act
        resultDF = self._transformationsConfigGenerator._getWildcardEntitiesReplacedByRealEntities(entitiesDF)

        # assert
        self.assertEqual(resultDF.rdd.collect(), entitiesDF.rdd.collect())

    def test_getWildcardEntitiesReplacedByRealEntities_shouldReplaceWildcardStarEntitiesWithMatchingTableNames(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["k1", "StagingSQL", "Cu[*]er", "wildcard val1", ""],
            ["k", "StagingSQL", "Address", "transformations array", ""],
            ["k2", "StagingSQL", "[*]Table", "wildcard val2", ""],
            ["k", "StagingOracle", "Customer", "transformations array", ""],
            ["k3", "StagingOracle", "Addre[*]", "wildcard val3", ""],
        ], "Key STRING, DatabaseName STRING, EntityName STRING, Transformations STRING, Params STRING")

        expectedResultDF = self._spark.createDataFrame([
            ["k1", "StagingSQL", "Customer", "wildcard val1", ""],
            ["k", "StagingSQL", "Address", "transformations array", ""],
            ["k2", "StagingSQL", "ThirdTable", "wildcard val2", ""],
            ["k2", "StagingSQL", "AnotherTable", "wildcard val2", ""],
            ["k", "StagingOracle", "Customer", "transformations array", ""],
            ["k3", "StagingOracle", "Address", "wildcard val3", ""],
        ], "Key STRING, DatabaseName STRING, EntityName STRING, Transformations STRING, Params STRING")

        allEntitiesForDatabaseNamesDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer"],
            ["StagingSQL", "Address"],
            ["StagingSQL", "ThirdTable"],
            ["StagingSQL", "AnotherTable"],
            ["StagingOracle", "Customer"],
            ["StagingOracle", "Address"],
            ["StagingOracle", "NotPresentInEntitiesDF"],
        ], "DatabaseName STRING, EntityName STRING")

        self._transformationsConfigGenerator._getAllEntitiesForDatabaseNames = MagicMock(return_value=allEntitiesForDatabaseNamesDF)
        self._transformationsConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        self._transformationsConfigGenerator._isInDataFrame = MagicMock(return_value=False)

        # act
        resultDF = self._transformationsConfigGenerator._getWildcardEntitiesReplacedByRealEntities(entitiesDF)

        # assert
        self.assertEqual(resultDF.count(), expectedResultDF.count())
        
        self.assertEqual(
            resultDF.sort("DatabaseName", "EntityName").rdd.collect(),
            expectedResultDF.sort("DatabaseName", "EntityName").rdd.collect()
        )

    def test_getWildcardEntitiesReplacedByRealEntities_shouldReplaceWildcardArrayEntitiesWithMatchingTableNames(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["k", "StagingSQL", "Customer", "transformations array", ""],
            ["k", "StagingSQL", "Address", "transformations array", ""],
            ["k1", "StagingSQL", "[Third,Another]Table", "wildcard val1", ""],
            ["k2", "StagingOracle", "Cu[sto,somethingElse]mer", "wildcard val2", ""],
            ["k3", "StagingOracle", "Add[ress]", "wildcard val3", ""],
        ], "Key STRING, DatabaseName STRING, EntityName STRING, Transformations STRING, Params STRING")

        expectedResultDF = self._spark.createDataFrame([
            ["k", "StagingSQL", "Customer", "transformations array", ""],
            ["k", "StagingSQL", "Address", "transformations array", ""],
            ["k1", "StagingSQL", "ThirdTable", "wildcard val1", ""],
            ["k1", "StagingSQL", "AnotherTable", "wildcard val1", ""],
            ["k2", "StagingOracle", "Customer", "wildcard val2", ""],
            ["k3", "StagingOracle", "Address", "wildcard val3", ""],
        ], "Key STRING, DatabaseName STRING, EntityName STRING, Transformations STRING, Params STRING")

        allEntitiesForDatabaseNamesDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer"],
            ["StagingSQL", "Address"],
            ["StagingSQL", "ThirdTable"],
            ["StagingSQL", "AnotherTable"],
            ["StagingSQL", "NotMatchingTable"],
            ["StagingOracle", "Customer"],
            ["StagingOracle", "Address"],
            ["StagingOracle", "NotPresentInEntitiesDF"],
        ], "DatabaseName STRING, EntityName STRING")

        self._transformationsConfigGenerator._getAllEntitiesForDatabaseNames = MagicMock(return_value=allEntitiesForDatabaseNamesDF)
        self._transformationsConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        self._transformationsConfigGenerator._isInDataFrame = MagicMock(return_value=False)

        # act
        resultDF = self._transformationsConfigGenerator._getWildcardEntitiesReplacedByRealEntities(entitiesDF)

        # assert
        self.assertEqual(resultDF.count(), expectedResultDF.count())

        self.assertEqual(
            resultDF.sort("DatabaseName", "EntityName").rdd.collect(),
            expectedResultDF.sort("DatabaseName", "EntityName").rdd.collect()
        )

    def test_getWildcardEntitiesReplacedByRealEntities_shouldRemoveWildcardEntitiesWhenThereAreNoMatchingTables(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["StagingSQL", "Cu[*]er", "wildcard val1", "k1", ""],
            ["StagingSQL", "Address", "transformations array", "k", ""],
            ["StagingSQL", "[Third,Another]Table", "wildcard val2", "k2", ""],
            ["StagingOracle", "Customer", "transformations array", "k", ""],
            ["StagingOracle", "Addre[*]", "wildcard val3", "k3", ""],
        ], "DatabaseName STRING, EntityName STRING, Transformations STRING, Key STRING, Params STRING")

        expectedResultDF = self._spark.createDataFrame([
            ["StagingSQL", "Address", "transformations array", "k", ""],
            ["StagingOracle", "Customer", "transformations array", "k", ""],
        ], "DatabaseName STRING, EntityName STRING, Transformations STRING, Key STRING, Params STRING")

        allEntitiesForDatabaseNamesDF = self._spark.createDataFrame([
            ["StagingSQL", "CustomerPostfix"],
            ["StagingSQL", "Address"],
            ["StagingSQL", "ThirdTablePostfix"],
            ["StagingSQL", "AnotherTablePostfix"],
            ["StagingOracle", "Customer"],
            ["StagingOracle", "PrefixAddress"],
            ["StagingOracle", "NotPresentInEntitiesDF"],
        ], "DatabaseName STRING, EntityName STRING")

        self._transformationsConfigGenerator._getAllEntitiesForDatabaseNames = MagicMock(return_value=allEntitiesForDatabaseNamesDF)
        self._transformationsConfigGenerator._spark.createDataFrame = self._spark.createDataFrame
        self._transformationsConfigGenerator._isInDataFrame = MagicMock(return_value=False)

        # act
        resultDF = self._transformationsConfigGenerator._getWildcardEntitiesReplacedByRealEntities(entitiesDF)

        # assert
        self.assertEqual(resultDF.count(), expectedResultDF.count())
        
        self.assertEqual(
            resultDF.sort("DatabaseName", "EntityName").rdd.collect(),
            expectedResultDF.sort("DatabaseName", "EntityName").rdd.collect()
        )


    # appendRemainingEntities tests

    def test_appendRemainingEntities_shouldNotAppendRemainingEntitiesIfCopyRestIsNotTrue(self):
        # arrange
        metadataConfig = {
            "metadata": {
                "copyRest": False
            }
        }
        entitiesDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer", "transformations array", "k", ""],
            ["StagingSQL", "Address", "transformations array", "k", ""],
        ], "DatabaseName STRING, EntityName STRING, Transformations STRING, Key STRING, Params STRING")

        # act
        resultDF = self._transformationsConfigGenerator._appendRemainingEntities(entitiesDF, metadataConfig)

        # assert
        self.assertEqual(resultDF.rdd.collect(), entitiesDF.rdd.collect())

    def test_appendRemainingEntities_shouldAppendRemainingEntities(self):
        # arrange
        metadataConfig = {
            "metadata": {
                "copyRest": True
            }
        }
        entitiesDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer", "transformations array", "k", ""],
            ["StagingSQL", "Address", "transformations array", "k", ""],
        ], "DatabaseName STRING, EntityName STRING, Transformations STRING, Key STRING, Params STRING")

        expectedResultDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer", "transformations array", "k", ""],
            ["StagingSQL", "Address", "transformations array", "k", ""],
            ["StagingSQL", "AnotherTable", None, None, None],
            ["StagingOracle", "Customer", None, None, None],
            ["StagingOracle", "Address", None, None, None],
        ], "DatabaseName STRING, EntityName STRING, Transformations STRING, Key STRING, Params STRING")

        allEntitiesDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer"],
            ["StagingSQL", "Address"],
            ["StagingSQL", "AnotherTable"],
            ["StagingOracle", "Customer"],
            ["StagingOracle", "Address"],
        ], "DatabaseName STRING, EntityName STRING")

        self._transformationsConfigGenerator._getAllEntitiesForDatabaseNames = MagicMock(return_value=allEntitiesDF)

        # act
        resultDF = self._transformationsConfigGenerator._appendRemainingEntities(entitiesDF, metadataConfig)

        # assert
        self.assertEqual(resultDF.count(), expectedResultDF.count())
        
        self.assertEqual(
            resultDF.sort("DatabaseName", "EntityName").rdd.collect(),
            expectedResultDF.sort("DatabaseName", "EntityName").rdd.collect()
        )


    # getEntities tests

    def test_getEntities(self):
        # arrange
        metadataConfig = {
            "metadata": {
                "transformationTable": "tableName",
                "key": "testKey"
            }
        }

        entitiesFromTransTableDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer", "transformations array", "testKey", ""],
            ["StagingSQL", "Address", "transformations array", "testKey", ""],
        ], "DatabaseName STRING, EntityName STRING, Transformations STRING, Key STRING, Params STRING")

        self._transformationsConfigGenerator._getEntitiesFromTransformationTableByKey = MagicMock(return_value=entitiesFromTransTableDF)
        self._transformationsConfigGenerator._getWildcardEntitiesReplacedByRealEntities = MagicMock(return_value=entitiesFromTransTableDF)
        self._transformationsConfigGenerator._appendRemainingEntities = MagicMock(return_value=entitiesFromTransTableDF)

        # act
        resultDF = self._transformationsConfigGenerator._getEntities(metadataConfig)

        # assert
        self.assertEqual(resultDF.rdd.collect(), entitiesFromTransTableDF.rdd.collect())

        self._transformationsConfigGenerator._getEntitiesFromTransformationTableByKey.assert_called_once_with("tableName", "testKey")
        self._transformationsConfigGenerator._getWildcardEntitiesReplacedByRealEntities.assert_called_once_with(entitiesFromTransTableDF)
        self._transformationsConfigGenerator._appendRemainingEntities.assert_called_once_with(entitiesFromTransTableDF, metadataConfig)


    # _identifyAndValidateBusinessKeys tests

    def test_identifyAndValidateBusinessKeys_shouldReturnUserDefinedKeys(self):
        # arrange
        table = Row(
            KeyColumns = ["Col1", "Col2"],
            WriteMode = WRITE_MODE_SCD_TYPE1
        )

        # act
        bkColumns = self._transformationsConfigGenerator._identifyAndValidateBusinessKeys(table, "databaseName", "entityName")

        # assert
        self.assertEqual(bkColumns, ["Col1", "Col2"])

    def test_identifyAndValidateBusinessKeys_shouldIdentifyKeysAutomaticallyFromMetadata(self):
        # arrange
        table = Row(
            KeyColumns = None,
            WriteMode = WRITE_MODE_SCD_TYPE1
        )

        self._transformationsConfigGenerator._spark.sql.return_value = self._spark.createDataFrame([
            ["Col1"],
            ["Col2"]
        ], "Attribute STRING")

        # act
        bkColumns = self._transformationsConfigGenerator._identifyAndValidateBusinessKeys(table, "databaseName", "entityName")

        # assert
        self.assertEqual(bkColumns, ["Col1", "Col2"])

        self._transformationsConfigGenerator._spark.sql.assert_called_once()

        sqlCommand = self._transformationsConfigGenerator._spark.sql.call_args_list[0].args[0]

        self.assertIn("SELECT Attribute", sqlCommand)
        self.assertIn(f"""{self._transformationsConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}""", sqlCommand)
        self.assertIn(f"DatabaseName = 'databaseName'", sqlCommand)
        self.assertIn(f"EntityName = 'entityName'", sqlCommand)
        self.assertIn(f"IsPrimaryKey", sqlCommand)

    def test_identifyAndValidateBusinessKeys_shouldAssertWhenKeysCantBeFoundAutomaticallyFromMetadata(self):
        # arrange
        table = Row(
            KeyColumns = None,
            WriteMode = WRITE_MODE_SCD_TYPE1
        )

        self._transformationsConfigGenerator._spark.sql.return_value = self._spark.createDataFrame(
            self._spark.sparkContext.emptyRDD(), 
            "Attribute STRING"
        )

        # act
        with self.assertRaisesRegex(AssertionError, f"Table 'databaseName.entityName' does not have"):
            self._transformationsConfigGenerator._identifyAndValidateBusinessKeys(table, "databaseName", "entityName")

        # assert
        self._transformationsConfigGenerator._spark.sql.assert_called_once()

        sqlCommand = self._transformationsConfigGenerator._spark.sql.call_args_list[0].args[0]

        self.assertIn("SELECT Attribute", sqlCommand)
        self.assertIn(f"""{self._transformationsConfigGenerator._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}""", sqlCommand)
        self.assertIn(f"DatabaseName = 'databaseName'", sqlCommand)
        self.assertIn(f"EntityName = 'entityName'", sqlCommand)
        self.assertIn(f"IsPrimaryKey", sqlCommand)

    def test_identifyAndValidateBusinessKeys_shouldAssertWhenThereAreNotUserDefinedKeysForSnapshot(self):
        # arrange
        table = Row(
            KeyColumns = None,
            WriteMode = WRITE_MODE_SNAPSHOT
        )

        # act
        with self.assertRaisesRegex(AssertionError, f"Table 'databaseName.entityName' does not have property 'keyColumns' defined in 'Params'"):
            self._transformationsConfigGenerator._identifyAndValidateBusinessKeys(table, "databaseName", "entityName")

        # assert
        self._transformationsConfigGenerator._spark.sql.assert_not_called()
    

    # generateDtOutput tests

    def test_generateDtOutput_shouldGenerateOneBatch(self):        
        # arrange
        entitiesDF = self._spark.createDataFrame([
            [
                "LandingSQL",
                "Customer",
                [
                    ("Filter", (None, "CustomerID > 0", None)),
                    ("Select", (["CustomerID", "FirstName", "LastName"], None, None))
                ],
                "testKey",
                (["CustomerID"], [IS_ACTIVE_RECORD_COLUMN_NAME, IS_DELETED_RECORD_COLUMN_NAME], "CustomerOverride", "TriggerIDOverride", WRITE_MODE_SCD_TYPE2, None),
                "CustomerOverride",
                WRITE_MODE_SCD_TYPE2
            ],
            [
                "LandingSQL",
                "Address",
                None,
                "testKey",
                None,
                "Address",
                WRITE_MODE_SCD_TYPE1
            ],
        ], """
            DatabaseName STRING,
            EntityName STRING,
            Transformations ARRAY<
                STRUCT<
                    TransformationType STRING,
                    Params STRUCT<
                        columns ARRAY<STRING>,
                        condition STRING,
                        replaceSpec STRING
                    >
                >
            >,
            Key STRING,
            Params STRUCT<
                keyColumns ARRAY<STRING>,
                partitionColumns ARRAY<STRING>,
                sinkEntityName STRING,
                fwkTriggerId STRING,
                writeMode STRING,
                batchNumber INTEGER
            >,
            SinkEntityName STRING,
            WriteMode STRING
        """)

        self._transformationsConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        def identifyAndValidateBusinessKeys(table: Row, sourceDatabaseName: str, sourceTableName: str):
            if sourceTableName == "Address":
                return ["AddressID"]
            elif sourceTableName == "Customer":
                return ["CustomerID"]

        self._transformationsConfigGenerator._identifyAndValidateBusinessKeys = MagicMock(side_effect=identifyAndValidateBusinessKeys)

        expectedCustomerFunctions = '[{"transformation": "filter", "params": "CustomerID > 0"}, {"transformation": "select", "params": ["CustomerID", "FirstName", "LastName"]}]'

        self._transformationsConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        self._transformationsConfigGenerator._maintenanceManager.arrangeTablePartition = MagicMock()

        expectedDtOutputDF = self._spark.createDataFrame([
            [
                "LandingSQL.Address",
                "StagingSQL.Address",
                """{ "keyColumns": ["AddressID"], "partitionColumns": [], "schemaEvolution": true, "functions": [] }""",
                WRITE_MODE_SCD_TYPE1,
                "TriggerID",
                "LayerID",
                1,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                "LandingSQL.Customer",
                "StagingSQL.CustomerOverride",
                f"""{{ "keyColumns": ["CustomerID"], "partitionColumns": ["{IS_ACTIVE_RECORD_COLUMN_NAME}", "{IS_DELETED_RECORD_COLUMN_NAME}"], "schemaEvolution": true, "functions": {expectedCustomerFunctions} }}""",
                WRITE_MODE_SCD_TYPE2,
                "TriggerIDOverride",
                "LayerID",
                1,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
        ], DT_OUTPUT_SCHEMA)

        runMaintenanceManager = True

        # act
        dtOutputDF = self._transformationsConfigGenerator._generateDtOutput(entitiesDF, "StagingSQL", "LayerID", "TriggerID", runMaintenanceManager)

        # assert
        self.assertEqual(
            dtOutputDF.sort("FwkSourceEntityId").drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedDtOutputDF.sort("FwkSourceEntityId").drop("InsertTime", "LastUpdate").rdd.collect()
        )

        self.assertEqual(self._transformationsConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 2)

        self.assertEqual(self._transformationsConfigGenerator._maintenanceManager.arrangeTablePartition.call_count, 2)
        self._transformationsConfigGenerator._maintenanceManager.arrangeTablePartition.assert_any_call(
            "StagingSQL.Address",
            None,
            WRITE_MODE_SCD_TYPE1
        )
        self._transformationsConfigGenerator._maintenanceManager.arrangeTablePartition.assert_any_call(
            "StagingSQL.CustomerOverride",
            [IS_ACTIVE_RECORD_COLUMN_NAME, IS_DELETED_RECORD_COLUMN_NAME],
            WRITE_MODE_SCD_TYPE2
        )

    def test_generateDtOutput_withoutArrangeTableManager(self):        
        # arrange
        entitiesDF = self._spark.createDataFrame([
            [
                "LandingSQL",
                "Customer",
                [
                    ("Filter", (None, "CustomerID > 0", None)),
                    ("Select", (["CustomerID", "FirstName", "LastName"], None, None))
                ],
                "testKey",
                (["CustomerID"], [IS_ACTIVE_RECORD_COLUMN_NAME, IS_DELETED_RECORD_COLUMN_NAME], "CustomerOverride", "TriggerIDOverride", WRITE_MODE_SCD_TYPE2, None),
                "CustomerOverride",
                WRITE_MODE_SCD_TYPE2
            ],
            [
                "LandingSQL",
                "Address",
                None,
                "testKey",
                None,
                "Address",
                WRITE_MODE_SCD_TYPE1
            ],
        ], """
            DatabaseName STRING,
            EntityName STRING,
            Transformations ARRAY<
                STRUCT<
                    TransformationType STRING,
                    Params STRUCT<
                        columns ARRAY<STRING>,
                        condition STRING,
                        replaceSpec STRING
                    >
                >
            >,
            Key STRING,
            Params STRUCT<
                keyColumns ARRAY<STRING>,
                partitionColumns ARRAY<STRING>,
                sinkEntityName STRING,
                fwkTriggerId STRING,
                writeMode STRING,
                batchNumber INTEGER
            >,
            SinkEntityName STRING,
            WriteMode STRING
        """)

        self._transformationsConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        def identifyAndValidateBusinessKeys(table: Row, sourceDatabaseName: str, sourceTableName: str):
            if sourceTableName == "Address":
                return ["AddressID"]
            elif sourceTableName == "Customer":
                return ["CustomerID"]

        self._transformationsConfigGenerator._identifyAndValidateBusinessKeys = MagicMock(side_effect=identifyAndValidateBusinessKeys)

        expectedCustomerFunctions = '[{"transformation": "filter", "params": "CustomerID > 0"}, {"transformation": "select", "params": ["CustomerID", "FirstName", "LastName"]}]'

        self._transformationsConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        self._transformationsConfigGenerator._maintenanceManager.arrangeTablePartition = MagicMock()

        expectedDtOutputDF = self._spark.createDataFrame([
            [
                "LandingSQL.Address",
                "StagingSQL.Address",
                """{ "keyColumns": ["AddressID"], "partitionColumns": [], "schemaEvolution": true, "functions": [] }""",
                WRITE_MODE_SCD_TYPE1,
                "TriggerID",
                "LayerID",
                1,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                "LandingSQL.Customer",
                "StagingSQL.CustomerOverride",
                f"""{{ "keyColumns": ["CustomerID"], "partitionColumns": ["{IS_ACTIVE_RECORD_COLUMN_NAME}", "{IS_DELETED_RECORD_COLUMN_NAME}"], "schemaEvolution": true, "functions": {expectedCustomerFunctions} }}""",
                WRITE_MODE_SCD_TYPE2,
                "TriggerIDOverride",
                "LayerID",
                1,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
        ], DT_OUTPUT_SCHEMA)

        runMaintenanceManager = False

        # act
        dtOutputDF = self._transformationsConfigGenerator._generateDtOutput(entitiesDF, "StagingSQL", "LayerID", "TriggerID", runMaintenanceManager)

        # assert
        self.assertEqual(
            dtOutputDF.sort("FwkSourceEntityId").drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedDtOutputDF.sort("FwkSourceEntityId").drop("InsertTime", "LastUpdate").rdd.collect()
        )

        self.assertEqual(self._transformationsConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 2)

        self.assertEqual(self._transformationsConfigGenerator._maintenanceManager.arrangeTablePartition.call_count, 0)

    def test_generateDtOutput_shouldGenerateMultipleBatches(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            [
                "LandingSQL",
                "CustomerB",
                [
                    ("Filter", (None, "CustomerID > 2", None)),
                    ("Select", (["CustomerID", "FirstName", "LastName"], None, None))
                ],
                "testKey",
                (["CustomerID"], [IS_ACTIVE_RECORD_COLUMN_NAME, IS_DELETED_RECORD_COLUMN_NAME], "CustomerOverride", "TriggerIDOverride", WRITE_MODE_SCD_TYPE2, 2),
                "CustomerOverride",
                WRITE_MODE_SCD_TYPE2
            ],
            [
                "LandingSQL",
                "CustomerA",
                [
                    ("Filter", (None, "CustomerID > 1", None)),
                    ("Select", (["CustomerID", "FirstName", "LastName"], None, None))
                ],
                "testKey",
                (["CustomerID"], [IS_ACTIVE_RECORD_COLUMN_NAME, IS_DELETED_RECORD_COLUMN_NAME], "CustomerOverride", "TriggerIDOverride", WRITE_MODE_SCD_TYPE2, None),
                "CustomerOverride",
                WRITE_MODE_SCD_TYPE2
            ],
            [
                "LandingSQL",
                "CustomerC",
                [
                    ("Filter", (None, "CustomerID > 3", None)),
                    ("Select", (["CustomerID", "FirstName", "LastName"], None, None))
                ],
                "testKey",
                (["CustomerID"], [IS_ACTIVE_RECORD_COLUMN_NAME, IS_DELETED_RECORD_COLUMN_NAME], "CustomerOverride", "TriggerIDOverride", WRITE_MODE_SCD_TYPE2, 3),
                "CustomerOverride",
                WRITE_MODE_SCD_TYPE2
            ],
            [
                "LandingSQL",
                "Address",
                None,
                "testKey",
                None,
                "Address",
                WRITE_MODE_SCD_TYPE1
            ],
        ], """
            DatabaseName STRING,
            EntityName STRING,
            Transformations ARRAY<
                STRUCT<
                    TransformationType STRING,
                    Params STRUCT<
                        columns ARRAY<STRING>,
                        condition STRING,
                        replaceSpec STRING
                    >
                >
            >,
            Key STRING,
            Params STRUCT<
                keyColumns ARRAY<STRING>,
                partitionColumns ARRAY<STRING>,
                sinkEntityName STRING,
                fwkTriggerId STRING,
                writeMode STRING,
                batchNumber INTEGER
            >,
            SinkEntityName STRING,
            WriteMode STRING
        """)

        self._transformationsConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        def identifyAndValidateBusinessKeys(table: Row, sourceDatabaseName: str, sourceTableName: str):
            if sourceTableName == "Address":
                return ["AddressID"]
            elif "Customer" in sourceTableName:
                return ["CustomerID"]

        self._transformationsConfigGenerator._identifyAndValidateBusinessKeys = MagicMock(side_effect=identifyAndValidateBusinessKeys)

        expectedCustomerAFunctions = '[{"transformation": "filter", "params": "CustomerID > 1"}, {"transformation": "select", "params": ["CustomerID", "FirstName", "LastName"]}]'
        expectedCustomerBFunctions = '[{"transformation": "filter", "params": "CustomerID > 2"}, {"transformation": "select", "params": ["CustomerID", "FirstName", "LastName"]}]'
        expectedCustomerCFunctions = '[{"transformation": "filter", "params": "CustomerID > 3"}, {"transformation": "select", "params": ["CustomerID", "FirstName", "LastName"]}]'

        self._transformationsConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")

        self._transformationsConfigGenerator._maintenanceManager.arrangeTablePartition = MagicMock()

        expectedDtOutputDF = self._spark.createDataFrame([
            [
                "LandingSQL.Address",
                "StagingSQL.Address",
                """{ "keyColumns": ["AddressID"], "partitionColumns": [], "schemaEvolution": true, "functions": [] }""",
                WRITE_MODE_SCD_TYPE1,
                "TriggerID",
                "LayerID",
                1,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                "LandingSQL.CustomerA",
                "StagingSQL.CustomerOverride",
                f"""{{ "keyColumns": ["CustomerID"], "partitionColumns": ["{IS_ACTIVE_RECORD_COLUMN_NAME}", "{IS_DELETED_RECORD_COLUMN_NAME}"], "schemaEvolution": true, "functions": {expectedCustomerAFunctions} }}""",
                WRITE_MODE_SCD_TYPE2,
                "TriggerIDOverride",
                "LayerID",
                1,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                "LandingSQL.CustomerB",
                "StagingSQL.CustomerOverride",
                f"""{{ "keyColumns": ["CustomerID"], "partitionColumns": ["{IS_ACTIVE_RECORD_COLUMN_NAME}", "{IS_DELETED_RECORD_COLUMN_NAME}"], "schemaEvolution": true, "functions": {expectedCustomerBFunctions} }}""",
                WRITE_MODE_SCD_TYPE2,
                "TriggerIDOverride",
                "LayerID",
                2,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
            [
                "LandingSQL.CustomerC",
                "StagingSQL.CustomerOverride",
                f"""{{ "keyColumns": ["CustomerID"], "partitionColumns": ["{IS_ACTIVE_RECORD_COLUMN_NAME}", "{IS_DELETED_RECORD_COLUMN_NAME}"], "schemaEvolution": true, "functions": {expectedCustomerCFunctions} }}""",
                WRITE_MODE_SCD_TYPE2,
                "TriggerIDOverride",
                "LayerID",
                3,
                "Y",
                datetime.now(),
                datetime.now(),
                "test_id"
            ],
        ], DT_OUTPUT_SCHEMA)

        runMaintenanceManager = True

        # act
        dtOutputDF = self._transformationsConfigGenerator._generateDtOutput(entitiesDF, "StagingSQL", "LayerID", "TriggerID", runMaintenanceManager)

        # assert
        self.assertEqual(
            dtOutputDF.sort("FwkSourceEntityId").drop("InsertTime", "LastUpdate").rdd.collect(),
            expectedDtOutputDF.sort("FwkSourceEntityId").drop("InsertTime", "LastUpdate").rdd.collect()
        )

        self.assertEqual(self._transformationsConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 4)

        self.assertEqual(self._transformationsConfigGenerator._maintenanceManager.arrangeTablePartition.call_count, 4)
        self._transformationsConfigGenerator._maintenanceManager.arrangeTablePartition.assert_any_call(
            "StagingSQL.Address",
            None,
            WRITE_MODE_SCD_TYPE1
        )
        self._transformationsConfigGenerator._maintenanceManager.arrangeTablePartition.assert_any_call(
            "StagingSQL.CustomerOverride",
            [IS_ACTIVE_RECORD_COLUMN_NAME, IS_DELETED_RECORD_COLUMN_NAME],
            WRITE_MODE_SCD_TYPE2
        )
        self._transformationsConfigGenerator._maintenanceManager.arrangeTablePartition.assert_any_call(
            "StagingSQL.CustomerOverride",
            [IS_ACTIVE_RECORD_COLUMN_NAME, IS_DELETED_RECORD_COLUMN_NAME],
            WRITE_MODE_SCD_TYPE2
        )
        self._transformationsConfigGenerator._maintenanceManager.arrangeTablePartition.assert_any_call(
            "StagingSQL.CustomerOverride",
            [IS_ACTIVE_RECORD_COLUMN_NAME, IS_DELETED_RECORD_COLUMN_NAME],
            WRITE_MODE_SCD_TYPE2
        )


    # generateFwkEntities tests

    def test_generateFwkEntities_firstLayerDoesNotGenerateSourceEntities(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["Customer", "CustomerOverride", "Landing"],
            ["Address", "Address", "Landing"]
        ], "EntityName STRING, SinkEntityName STRING, DatabaseName STRING")

        self._transformationsConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        self._transformationsConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")
        self._transformationsConfigGenerator._minDtOrderId = 1

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "StagingSQL.CustomerOverride",
                "LinkedServiceId",
                "path/to/data/CustomerOverride",
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
        fwkEntitiesDF = self._transformationsConfigGenerator._generateFwkEntities(entitiesDF, "StagingSQL", 1, "LinkedServiceId", "path/to/data")

        # asset
        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").rdd.collect()
        )
        
        self.assertEqual(self._transformationsConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 2)

    def test_generateFwkEntities_secondLayerGeneratesSourceEntities(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["Customer", "CustomerOverride", "Staging"],
            ["Address", "Address", "Staging"],
            ["V_Address", "V_Address", "Staging"]
        ], "EntityName STRING, SinkEntityName STRING, DatabaseName STRING")

        self._transformationsConfigGenerator._spark.createDataFrame = self._spark.createDataFrame

        self._transformationsConfigGenerator._configGeneratorHelper.getDatabricksId = MagicMock(return_value="test_id")
        self._transformationsConfigGenerator._minDtOrderId = 1

        expectedFwkEntitiesDF = self._spark.createDataFrame([
            [
                "Staging.Customer",
                "na",
                None,
                "Delta",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "History.CustomerOverride",
                "LinkedServiceId",
                "path/to/data/CustomerOverride",
                "Delta",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "Staging.Address",
                "na",
                None,
                "Delta",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "History.Address",
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
            [
                "Staging.V_Address",
                "na",
                None,
                "Delta",
                None,
                None,
                None,
                None,
                datetime.now(),
                "test_id"
            ],
            [
                "History.V_Address",
                "LinkedServiceId",
                "path/to/data/V_Address",
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
        fwkEntitiesDF = self._transformationsConfigGenerator._generateFwkEntities(entitiesDF, "History", 2, "LinkedServiceId", "path/to/data")

        # asset
        self.assertEqual(
            fwkEntitiesDF.drop("LastUpdate").orderBy("FwkEntityId").rdd.collect(),
            expectedFwkEntitiesDF.drop("LastUpdate").orderBy("FwkEntityId").rdd.collect()
        )
        
        self.assertEqual(self._transformationsConfigGenerator._configGeneratorHelper.getDatabricksId.call_count, 6)


    # selectAttributesFromPreviousLayer tests

    def test_selectAttributesFromPreviousLayer(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer", "CustomerOverride"],
            ["StagingSQL", "Address", None],
            ["StagingSQL", "NewTable", None],
        ], "DatabaseName STRING, EntityName STRING, SinkEntityName STRING")

        attributesFromPrevLayerDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer", "CustomerID", True, False, "something"],
            ["StagingSQL", "Customer", "FirstName", False, False, "something"],
            ["StagingSQL", "Customer", "LastName", False, False, "something"],
            ["StagingSQL", "Address", "AddressID", True, False, "something"],
            ["StagingSQL", "Address", "AddressLine1", False, False, "something"],
            ["StagingSQL", "Address", "AddressLine2", False, False, "something"],
        ], METADATA_ATTRIBUTES_SCHEMA)

        self._transformationsConfigGenerator._getAttributesForEntitiesDatabaseNames = MagicMock(return_value=attributesFromPrevLayerDF)

        expectedAttributesDF = self._spark.createDataFrame([
            ["CompartmentADLS", "CustomerOverride", "CustomerID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "Address", "AddressID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "NewTable", None, None, None, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
        ], METADATA_ATTRIBUTES_SCHEMA)

        # act
        attributesDF = self._transformationsConfigGenerator._selectAttributesFromPreviousLayer(entitiesDF, "CompartmentADLS")

        # assert
        self.assertEqual(
            attributesDF.sort("DatabaseName", "EntityName", "Attribute").rdd.collect(),
            expectedAttributesDF.sort("DatabaseName", "EntityName", "Attribute").rdd.collect()
        )

        self._transformationsConfigGenerator._getAttributesForEntitiesDatabaseNames.assert_called_once_with(entitiesDF)


    # generateAttributes tests
    
    def test_generateAttributes(self):
        # arrange
        entitiesDF = self._spark.createDataFrame([
            ["StagingSQL", "Customer", None, "CustomerOverride", WRITE_MODE_SCD_TYPE2_DELTA],
            ["StagingSQL", "Customer", (["CustomerNumber", "Market"], None), "CustomerWithCustomKeyColumns", WRITE_MODE_SCD_TYPE2_DELTA],
            ["StagingSQL", "CustomerFull", None, None, WRITE_MODE_SCD_TYPE2],
            ["StagingSQL", "Address", None, None, WRITE_MODE_SCD_TYPE1],
            ["StagingSQL", "NewTableSCD1", None, None, WRITE_MODE_SCD_TYPE1],
            ["StagingSQL", "NewTableSCD2", (["Col1", "Col2"], None), None, WRITE_MODE_SCD_TYPE2],
        ], "DatabaseName STRING, EntityName STRING, Params STRUCT<keyColumns ARRAY<STRING>, partitionColumns ARRAY<STRING>>, SinkEntityName STRING, WriteMode STRING")

        attributesFromPrevLayerDF = self._spark.createDataFrame([
            ["CompartmentADLS", "CustomerOverride", "CustomerID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "CustomerWithCustomKeyColumns", "CustomerID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "CustomerFull", "CustomerID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "Address", "AddressID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "NewTableSCD1", None, None, None, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "NewTableSCD2", None, None, None, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
        ], METADATA_ATTRIBUTES_SCHEMA)

        expectedAttributesDF = self._spark.createDataFrame([
            ["CompartmentADLS", "CustomerOverride", "CustomerID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "CustomerOverride", VALID_FROM_DATETIME_COLUMN_NAME, True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "CustomerWithCustomKeyColumns", "CustomerNumber", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "CustomerWithCustomKeyColumns", "Market", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "CustomerWithCustomKeyColumns", VALID_FROM_DATETIME_COLUMN_NAME, True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "CustomerFull", "CustomerID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "CustomerFull", VALID_FROM_DATETIME_COLUMN_NAME, True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "Address", "AddressID", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "NewTableSCD1", None, None, None, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "NewTableSCD2", "Col1", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "NewTableSCD2", "Col2", True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
            ["CompartmentADLS", "NewTableSCD2", VALID_FROM_DATETIME_COLUMN_NAME, True, False, METADATA_ATTRIBUTES_TRANSFORMATION_KEY],
        ], METADATA_ATTRIBUTES_SCHEMA)

        self._transformationsConfigGenerator._selectAttributesFromPreviousLayer = MagicMock(return_value=attributesFromPrevLayerDF)

        # act
        attributesDF = self._transformationsConfigGenerator._generateAttributes(entitiesDF, "CompartmentADLS")

        # assert
        self.assertEqual(
            attributesDF.sort("DatabaseName", "EntityName", "Attribute").rdd.collect(),
            expectedAttributesDF.sort("DatabaseName", "EntityName", "Attribute").rdd.collect()
        )

        self._transformationsConfigGenerator._selectAttributesFromPreviousLayer.assert_called_once_with(entitiesDF, "CompartmentADLS")

    
    # generateConfiguration tests

    def test_generateConfiguration(self):
        # arrange
        metadataConfig = {
            "FwkLayerId": "HistorizationLayer",
            "FwkTriggerId": "Daily",
            "sink": {
                "FwkLinkedServiceId": "CompartmentADLS",
                "databaseName": "History"
            },
            "metadata": {
                "transformationTable": "TransformationConfiguration",
                "key": "StagingHistorization"
            },
            "DtOrder": 2
        }

        self._transformationsConfigGenerator._configGeneratorHelper.getADLSPathAndUri = MagicMock(
            return_value=("testSinkPath", "testSinkUri")
        )

        getEntitiesDF = MagicMock()
        self._transformationsConfigGenerator._getEntities = MagicMock(return_value=getEntitiesDF)

        postProcessedEntitiesDF = MagicMock()
        getEntitiesDF.withColumn.return_value = postProcessedEntitiesDF
        postProcessedEntitiesDF.withColumn.return_value = postProcessedEntitiesDF

        dtOutputDF = MagicMock()
        self._transformationsConfigGenerator._generateDtOutput = MagicMock(return_value=dtOutputDF)

        fwkEntitiesDF = MagicMock()
        self._transformationsConfigGenerator._generateFwkEntities = MagicMock(return_value=fwkEntitiesDF)

        expectedAttributesDF = MagicMock()
        self._transformationsConfigGenerator._generateAttributes = MagicMock(return_value=expectedAttributesDF)

        self._transformationsConfigGenerator._dataLakeHelper.writeDataAsParquet = MagicMock()

        runMaintenanceManager = True

        # act
        attributesDF = self._transformationsConfigGenerator.generateConfiguration(metadataConfig, "testConfigOutputPath", runMaintenanceManager)

        # assert
        self.assertEqual(attributesDF, expectedAttributesDF)

        self._transformationsConfigGenerator._configGeneratorHelper.getADLSPathAndUri.assert_called_once_with(metadataConfig["sink"])

        self._transformationsConfigGenerator._getEntities.assert_called_once_with(metadataConfig)

        self.assertEqual(getEntitiesDF.withColumn.call_count, 1)
        self.assertEqual(postProcessedEntitiesDF.withColumn.call_count, 1)

        self._transformationsConfigGenerator._generateDtOutput.assert_called_once_with(
            postProcessedEntitiesDF,
            metadataConfig["sink"]["databaseName"],
            metadataConfig["FwkLayerId"],
            metadataConfig["FwkTriggerId"],
            runMaintenanceManager
        )

        self._transformationsConfigGenerator._generateFwkEntities.assert_called_once_with(
            postProcessedEntitiesDF,
            metadataConfig["sink"]["databaseName"],
            metadataConfig["DtOrder"],
            metadataConfig["sink"]["FwkLinkedServiceId"],
            "testSinkPath"
        )

        self._transformationsConfigGenerator._generateAttributes.assert_called_once_with(
            postProcessedEntitiesDF,
            metadataConfig["sink"]["databaseName"]
        )

        self.assertEqual(self._transformationsConfigGenerator._dataLakeHelper.writeDataAsParquet.call_count, 2) 

        self._transformationsConfigGenerator._dataLakeHelper.writeDataAsParquet.assert_any_call(
            dtOutputDF, 
            f"testConfigOutputPath/{DT_OUTPUT_FOLDER}/", 
            WRITE_MODE_APPEND
        )

        self._transformationsConfigGenerator._dataLakeHelper.writeDataAsParquet.assert_any_call(
            fwkEntitiesDF, 
            f"testConfigOutputPath/{FWK_ENTITY_FOLDER}/", 
            WRITE_MODE_APPEND, 
            dropDuplicatesByColumn=["FwkEntityId"],
            dropDuplicatesOrderSpec=ANY
        )

        self.assertEqual(
            str(self._transformationsConfigGenerator._dataLakeHelper.writeDataAsParquet.call_args_list[1].kwargs["dropDuplicatesOrderSpec"]),
            "Column<'CASE WHEN `=`(FwkLinkedServiceId, na) THEN 1 ELSE 0 END'>"
        )
