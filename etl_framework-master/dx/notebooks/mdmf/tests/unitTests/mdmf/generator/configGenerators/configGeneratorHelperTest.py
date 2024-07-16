# Databricks notebook source
import unittest
from datetime import datetime
from json import JSONDecodeError
from pyspark.sql import SparkSession, DataFrame
from unittest.mock import MagicMock

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.generator.configGenerators.configGeneratorHelper import ConfigGeneratorHelper
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../../generator/configGenerators/configGeneratorHelper

# COMMAND ----------

# MAGIC %run ../../../../../includes/frameworkConfig

# COMMAND ----------

class ConfigGeneratorHelperTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

        self._compartmentConfig = MagicMock()
        self._compartmentConfig.FWK_LINKED_SERVICE_DF = self._spark.createDataFrame([
            ["MyLinkedService", "ADLS", "https://dstgcpbit.dfs.core.windows.net/", None, None, None, None],
        ], "FwkLinkedServiceId STRING, SourceType STRING, InstanceURL STRING, Port STRING, UserName STRING, SecretName STRING, IPAddress STRING")
        self._compartmentConfig.FWK_METADATA_CONFIG = {
            "metadata": {
                "databaseName": "Application_MEF_Metadata"
            }
        }

    def setUp(self):
        self._configGeneratorHelper = ConfigGeneratorHelper(self._spark, self._compartmentConfig)
        self._configGeneratorHelper._spark = MagicMock()
        self._configGeneratorHelper._DeltaTable = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # getDatabricksId tests

    def test_getDatabricksId(self):
        # arrange
        self._configGeneratorHelper._spark.conf.get = MagicMock(return_value = "12345")

        # act
        databricksId = self._configGeneratorHelper.getDatabricksId()

        # assert
        self.assertEqual(databricksId, "Databricks 12345")

        self._configGeneratorHelper._spark.conf.get.assert_called_once_with("spark.databricks.clusterUsageTags.clusterOwnerOrgId")


    # getADLSPathAndUri tests

    def test_getADLSPathAndUri_withDataPath(self):
        # arrange
        metadataADLSConfig = {
            "FwkLinkedServiceId": "MyLinkedService",
            "containerName": "myContainerName",
            "dataPath": "/MySubfolder"
        }

        # act
        (path, uri) = self._configGeneratorHelper.getADLSPathAndUri(metadataADLSConfig)

        # assert
        self.assertEqual(path, "myContainerName/MySubfolder")
        self.assertEqual(uri, "abfss://myContainerName@dstgcpbit.dfs.core.windows.net/MySubfolder")

    def test_getADLSPathAndUri_withoutDataPath(self):
        # arrange
        metadataADLSConfig = {
            "FwkLinkedServiceId": "MyLinkedService",
            "containerName": "myContainerName"
        }

        # act
        (path, uri) = self._configGeneratorHelper.getADLSPathAndUri(metadataADLSConfig)

        # assert
        self.assertEqual(path, "myContainerName")
        self.assertEqual(uri, "abfss://myContainerName@dstgcpbit.dfs.core.windows.net")

    def test_getADLSPathAndUri_notExistingFwkLinkedService(self):
        # arrange
        metadataADLSConfig = {
            "FwkLinkedServiceId": "NotExistingLinkedService"
        }

        # act
        with self.assertRaisesRegex(AssertionError, "FwkLinkedServiceId .* was not found"):
            self._configGeneratorHelper.getADLSPathAndUri(metadataADLSConfig)


    # stringifyArray tests

    def test_stringifyArray_multipleValues(self):
        # act
        result = ConfigGeneratorHelper.stringifyArray(["One", "Two", "Three"])

        # assert
        self.assertEqual(result, '["One", "Two", "Three"]')

    def test_stringifyArray_oneValue(self):
        # act
        result = ConfigGeneratorHelper.stringifyArray(["One"])

        # assert
        self.assertEqual(result, '["One"]')

    def test_stringifyArray_noValue(self):
        # act
        result = ConfigGeneratorHelper.stringifyArray([])

        # assert
        self.assertEqual(result, '[]')

    def test_stringifyArray_null(self):
        # act
        result = ConfigGeneratorHelper.stringifyArray(None)

        # assert
        self.assertEqual(result, '[]')


    # convertAndValidateTransformationsToFunctions tests

    def test_convertAndValidateTransformationsToFunctions_covertsSuccessfully(self):
        # arrange
        transformations = [
            {
                "TransformationType": "Aggregate",
                "Params": {
                    "transformationSpecs": {
                        "groupBy": ["DueDate", "Status"],
                        "aggregations": [
                            {
                                "operation": "sum",
                                "column": "TotalDue",
                                "alias": "SumTotalDue"
                            }
                        ]
                    }
                }
            },
            {
                "TransformationType": "Cast",
                "Params": {
                    "transformationSpecs": [
                        {
                            "column": ["AddressLine2"],
                            "dataType": "string"
                        },
                        {
                            "column": ["InsertTime"],
                            "dataType": "timestamp"
                        }
                    ]
                }
            },
            {
                "TransformationType": "Select",
                "Params": {
                    "columns": [
                        "AddressID",
                        "AddressLine1",
                        "PostalCode",
                        "City"
                    ]
                }
            },
            {
                "TransformationType": "SelectExpression",
                "Params": {
                    "expressions": [
                        "AddressID as AddressIdentifier",
                        "AddressLine1 as FirstAddress"
                    ]
                }
            },
            {
                "TransformationType": "Filter",
                "Params": {
                    "condition": "AddressID < 15000"
                }
            },
            {
                "TransformationType": "Deduplicate",
                "Params": {
                    "columns": [
                        "*"
                    ]
                }
            },
            {
                "TransformationType": "DeriveColumn",
                "Params": {
                    "transformationSpecs": [
                        {
                            "column": "CurrentDate",
                            "value": "currentDate()"
                        }
                    ]
                }
            },
            {
                "TransformationType": "RenameColumn",
                "Params": {
                    "transformationSpecs": {
                        "column": ["AddressID", "AddressLine1"],
                        "value": ["AddressIdentifier", "FirstAddress"]
                    }
                }
            },
            {
                "TransformationType": "ReplaceNull",
                "Params": {
                    "transformationSpecs": [
                        {
                            "column": ["AddressLine2"],
                            "value": "n/a"
                        },
                        {
                            "column": ["AddressID"],
                            "value": -1
                        }
                    ]
                }
            },
            {
                "TransformationType": "Join",
                "Params": {
                    "transformationSpecs": {
                        "joinTable": "Application_MEF_StagingSQL.CustomerAddress",
                        "joinType": "LEFT",
                        "joinColumns": "AddressID,AddressID",
                        "selectColumns": "table.*, AddressType"
                    }
                }
            },
            {
                "TransformationType": "Pseudonymization",
                "Params": {
                    "columns": [
                        "AddressLine1",
                        "City"
                    ]
                }
            }
        ]

        expectedFunctions = [
            '{"transformation": "aggregate", "params": {\'groupBy\': [\'DueDate\', \'Status\'], \'aggregations\': [{\'operation\': \'sum\', \'column\': \'TotalDue\', \'alias\': \'SumTotalDue\'}]}}',
            '{"transformation": "cast", "params": [{\'column\': [\'AddressLine2\'], \'dataType\': \'string\'}, {\'column\': [\'InsertTime\'], \'dataType\': \'timestamp\'}]}',
            '{"transformation": "select", "params": ["AddressID", "AddressLine1", "PostalCode", "City"]}',
            '{"transformation": "selectExpression", "params": ["AddressID as AddressIdentifier", "AddressLine1 as FirstAddress"]}',
            '{"transformation": "filter", "params": "AddressID < 15000"}',
            '{"transformation": "deduplicate", "params": ["*"]}',
            '{"transformation": "deriveColumn", "params": [{\'column\': \'CurrentDate\', \'value\': \'currentDate()\'}]}',
            '{"transformation": "renameColumn", "params": {\'column\': [\'AddressID\', \'AddressLine1\'], \'value\': [\'AddressIdentifier\', \'FirstAddress\']}}',
            '{"transformation": "replaceNull", "params": [{\'column\': [\'AddressLine2\'], \'value\': \'n/a\'}, {\'column\': [\'AddressID\'], \'value\': -1}]}',
            '{"transformation": "join", "params": {\'joinTable\': \'Application_MEF_StagingSQL.CustomerAddress\', \'joinType\': \'LEFT\', \'joinColumns\': \'AddressID,AddressID\', \'selectColumns\': \'table.*, AddressType\'}}',
            '{"transformation": "pseudonymize", "params": ["AddressLine1", "City"]}',
        ]

        # act
        functions = ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

        # assert
        self.assertEqual(functions, expectedFunctions)

    def test_convertAndValidateTransformationsToFunctions_unknownTransformation(self):
        # arrange
        transformations = [
            {
                "TransformationType": "UnknownTransformation",
                "Params": {}
            }
        ]

        # act & assert
        with self.assertRaisesRegex(NotImplementedError, "TransformationType 'UnknownTransformation' is not implemented"):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

    def test_convertAndValidateTransformationsToFunctions_missingColumnsParam(self):
        # arrange
        transformations = [
            {
                "TransformationType": "Deduplicate",
                "Params": {}
            }
        ]

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Parameter 'columns' is mandatory for TransformationType 'Deduplicate'"):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

        # arrange
        transformations = [
            {
                "TransformationType": "Pseudonymization",
                "Params": {}
            }
        ]

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Parameter 'columns' is mandatory for TransformationType 'Pseudonymization'"):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

        # arrange
        transformations = [
            {
                "TransformationType": "Select",
                "Params": {}
            }
        ]

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Parameter 'columns' is mandatory for TransformationType 'Select'"):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

    def test_convertAndValidateTransformationsToFunctions_missingExpressionsParam(self):
        # arrange
        transformations = [
            {
                "TransformationType": "SelectExpression",
                "Params": {}
            }
        ]

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Parameter 'expressions' is mandatory for TransformationType 'SelectExpression'"):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)
    
    def test_convertAndValidateTransformationsToFunctions_missingConditionParam(self):
        # arrange
        transformations = [
            {
                "TransformationType": "Filter",
                "Params": {}
            }
        ]

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Parameter 'condition' is mandatory for TransformationType 'Filter'"):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

    def test_convertAndValidateTransformationsToFunctions_missingTransformationSpecsParam(self):
        # arrange
        transformations = [
            {
                "TransformationType": "Aggregate",
                "Params": {}
            }
        ]

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Parameter 'transformationSpecs' is mandatory for TransformationType 'Aggregate'"):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

        # arrange
        transformations = [
            {
                "TransformationType": "Cast",
                "Params": {}
            }
        ]

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Parameter 'transformationSpecs' is mandatory for TransformationType 'Cast'"):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

        # arrange
        transformations = [
            {
                "TransformationType": "DeriveColumn",
                "Params": {}
            }
        ]

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Parameter 'transformationSpecs' is mandatory for TransformationType 'DeriveColumn'"):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

        # arrange
        transformations = [
            {
                "TransformationType": "RenameColumn",
                "Params": {}
            }
        ]

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Parameter 'transformationSpecs' is mandatory for TransformationType 'RenameColumn'"):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)
        
        # arrange
        transformations = [
            {
                "TransformationType": "ReplaceNull",
                "Params": {}
            }
        ]

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Parameter 'transformationSpecs' is mandatory for TransformationType 'ReplaceNull'"):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

        # arrange
        transformations = [
            {
                "TransformationType": "Join",
                "Params": {}
            }
        ]

        # act & assert
        with self.assertRaisesRegex(AssertionError, "Parameter 'transformationSpecs' is mandatory for TransformationType 'Join'"):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

    def test_convertAndValidateTransformationsToFunctions_withoutAggregationInAggregate(self):
        # arrange
        transformations = [
            {
                "TransformationType": "Aggregate",
                "Params": {
                    "transformationSpecs": {
                        "groupBy": ["DueDate", "Status"]
                    }
                }
            }
        ]

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("Attribute 'aggregations' of 'transformationSpecs' parameter is mandatory and can't be empty for TransformationType 'Aggregate'")):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

    def test_convertAndValidateTransformationsToFunctions_withoutOperationColumnAliasKeysInAggregate(self):
        # arrange
        transformations = [
            {
                "TransformationType": "Aggregate",
                "Params": {
                    "transformationSpecs": {
                        "groupBy": ["DueDate", "Status"],
                        "aggregations": [
                            {
                                "column": "TotalDue",
                                "alias": "SumTotalDue"
                            },
                            {
                                "operation": "avg",
                                "alias": "AvgTotalDue"
                            },
                            {
                                "operation": "min",
                                "column": "SubTotal"
                            }
                        ]
                    }
                }
            }
        ]

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("Attributes 'operation', 'column' and 'alias' of 'transformationSpecs.aggregations' object are mandatory for TransformationType 'Aggregate'")):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)
    
    def test_convertAndValidateTransformationsToFunctions_mismatchLengthOfRenameColumnSpecs(self):
        # arrange
        transformations = [
            {
                "TransformationType": "RenameColumn",
                "Params": {
                    "transformationSpecs": {
                        "column": ["AddressID", "AddressLine1"],
                        "value": ["AddressIdentifier", "FirstLine", "AddressLineOne"]
                    }
                }
            }
        ]

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("Length of the column and value mismatch in transformationSpecs of RenameColumn transformation")):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

    def test_convertAndValidateTransformationsToFunctions_mismatchLengthOfRenameColumnStringSpecs(self):
        # arrange
        transformations = [
            {
                "TransformationType": "RenameColumn",
                "Params": {
                    "transformationSpecs": """{
                        "column": ["AddressID", "AddressLine1"],
                        "value": ["AddressIdentifier", "FirstLine", "AddressLineOne"]
                    }"""
                }
            }
        ]

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("Length of the column and value mismatch in transformationSpecs of RenameColumn transformation")):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

    def test_convertAndValidateTransformationsToFunctions_missingJoinProperties(self):
        # arrange
        transformations = [
            {
                "TransformationType": "Join",
                "Params": {
                    "transformationSpecs": {
                        "joinType": "LEFT",
                        "joinColumns": "AddressID,AddressID",
                        "selectColumns": "table.*, AddressType"
                    }
                }
            }
        ]

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("Attributes 'joinTable', 'joinType', 'joinColumns' and 'selectColumns' of 'transformationSpecs' parameter are mandatory for TransformationType 'Join'")):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

        # arrange
        transformations = [
            {
                "TransformationType": "Join",
                "Params": {
                    "transformationSpecs": {
                        "joinTable": "Application_MEF_StagingSQL.CustomerAddress",
                        "joinColumns": "AddressID,AddressID",
                        "selectColumns": "table.*, AddressType"
                    }
                }
            }
        ]

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("Attributes 'joinTable', 'joinType', 'joinColumns' and 'selectColumns' of 'transformationSpecs' parameter are mandatory for TransformationType 'Join'")):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

        # arrange
        transformations = [
            {
                "TransformationType": "Join",
                "Params": {
                    "transformationSpecs": {
                        "joinTable": "Application_MEF_StagingSQL.CustomerAddress",
                        "joinType": "LEFT",
                        "selectColumns": "table.*, AddressType"
                    }
                }
            }
        ]

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("Attributes 'joinTable', 'joinType', 'joinColumns' and 'selectColumns' of 'transformationSpecs' parameter are mandatory for TransformationType 'Join'")):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)

        # arrange
        transformations = [
            {
                "TransformationType": "Join",
                "Params": {
                    "transformationSpecs": {
                        "joinTable": "Application_MEF_StagingSQL.CustomerAddress",
                        "joinType": "LEFT",
                        "joinColumns": "AddressID,AddressID",
                    }
                }
            }
        ]

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("Attributes 'joinTable', 'joinType', 'joinColumns' and 'selectColumns' of 'transformationSpecs' parameter are mandatory for TransformationType 'Join'")):
            ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(transformations)


    # isContentOfDataframesSame tests

    def test_isContentOfDataframesSame_isSame(self):
        # arrange       
        firstDF = self._spark.createDataFrame([
            [1, "Value A"],
            [2, "Value B"],
        ], "ID INTEGER, ColumnName STRING")
        
        secondDF = self._spark.createDataFrame([
            [1, "Value A"],
            [2, "Value B"],
        ], "ID INTEGER, ColumnName STRING")
        
        # act
        isSame = self._configGeneratorHelper._isContentOfDataframesSame(firstDF, secondDF)

        # assert
        self.assertTrue(isSame)

    def test_isContentOfDataframesSame_differentSchema(self):
        # arrange        
        firstDF = self._spark.createDataFrame([
            [1, "Value A"],
            [2, "Value B"],
        ], "ID INTEGER, ColumnName STRING")
        
        secondDF = self._spark.createDataFrame([
            [1, "Value A"],
            [2, "Value B"],
        ], "ID INTEGER, DifferentColumnName STRING")
        
        # act
        isSame = self._configGeneratorHelper._isContentOfDataframesSame(firstDF, secondDF)

        # assert
        self.assertFalse(isSame)

    def test_isContentOfDataframesSame_differentContent(self):
        # arrange        
        firstDF = self._spark.createDataFrame([
            [1, "Value A"],
            [2, "Value B"],
        ], "ID INTEGER, ColumnName STRING")
        
        secondDF = self._spark.createDataFrame([
            [1, "Different Value"],
            [2, "Value B"],
        ], "ID INTEGER, ColumnName STRING")
        
        # act
        isSame = self._configGeneratorHelper._isContentOfDataframesSame(firstDF, secondDF)

        # assert
        self.assertFalse(isSame)
    
    
    # isDeltaTableChanged tests

    def test_isDeltaTableChanged_noDeltaTable(self):
        # arrange
        self._configGeneratorHelper._DeltaTable.isDeltaTable.return_value = False

        # act
        result = self._configGeneratorHelper.isDeltaTableChanged(
            "metadataDeltaTablePath",
            "TransformationConfiguration",
            "'Key=LandingSQLPseudonymization'",
            ["Params.partitionColumns"]
        )

        # assert
        self.assertFalse(result)

    def test_isDeltaTableChanged_oneVersionDeltaTable(self):
        # arrange
        transformationConfigurationDF = self._spark.createDataFrame([
            [0, "WRITE"]
        ], "version int, col2 STRING")
        self._configGeneratorHelper._spark.sql.return_value = transformationConfigurationDF
        
        self._configGeneratorHelper._DeltaTable.isDeltaTable.return_value = True

        # act
        result = self._configGeneratorHelper.isDeltaTableChanged(
            "metadataDeltaTablePath",
            "TransformationConfiguration",
            "'Key=LandingSQLPseudonymization'",
            ["Params.partitionColumns"]
        )

        # assert
        self.assertFalse(result)

        sqlCommand = self._configGeneratorHelper._spark.sql.call_args_list[0].args[0]
        self.assertIn("TransformationConfiguration", sqlCommand)

    def test_isDeltaTableChanged_multiVersionUnchangedDeltaTable(self):
        # arrange
        transformationConfigurationDF = self._spark.createDataFrame([
            [1, "WRITE"]
        ], "version int, col2 STRING")
        self._configGeneratorHelper._spark.sql.return_value = transformationConfigurationDF

        self._configGeneratorHelper._DeltaTable.isDeltaTable.return_value = True
        
        currentVersionDF = self._spark.createDataFrame([
            ["Database1", "Customer1", "A"],
            ["Database1", "Customer2", "B"],
            ["Database1", "Customer3", "C"],
            ["Database2", "Customer4", "D"],
            ["Database3", "Customer5", "E"]
        ], "DatabaseName STRING, TableName STRING, IgnoredColumn STRING")

        previousVersionDF = self._spark.createDataFrame([
            ["Database1", "Customer1", "1"],
            ["Database1", "Customer2", "2"],
            ["Database1", "Customer3", "3"],
            ["Database4", "Customer4", "4"]
        ], "DatabaseName STRING, TableName STRING, IgnoredColumn STRING")

        def readTable(sqlCommand: str) -> DataFrame:
            if "@v0" in sqlCommand:
                return previousVersionDF
            elif "@v1" in sqlCommand:
                return currentVersionDF
            
        self._configGeneratorHelper._spark.read.table.side_effect = readTable

        # act
        result = self._configGeneratorHelper.isDeltaTableChanged(
            "metadataDeltaTablePath",
            "TransformationConfiguration",
            "DatabaseName='Database1'",
            ["TableName"]
        )

        # assert
        self.assertFalse(result)

        sqlCommand = self._configGeneratorHelper._spark.sql.call_args_list[0].args[0]
        self.assertIn("TransformationConfiguration", sqlCommand)

        self.assertEqual(self._configGeneratorHelper._spark.read.table.call_count, 2)

    def test_isDeltaTableChanged_multiVersionChangedDeltaTable(self):
        # arrange
        transformationConfigurationDF = self._spark.createDataFrame([
            [1, "WRITE"]
        ], "version int, col2 STRING")
        self._configGeneratorHelper._spark.sql.return_value = transformationConfigurationDF

        self._configGeneratorHelper._DeltaTable.isDeltaTable.return_value = True
        
        currentVersionDF = self._spark.createDataFrame([
            ["Database1", "Customer1_Changed", "A"],
            ["Database1", "Customer2", "B"],
            ["Database1", "Customer3_Changed", "C"],
            ["Database2", "Customer4", "D"],
            ["Database3", "Customer5", "E"]
        ], "DatabaseName STRING, TableName STRING, IgnoredColumn STRING")

        previousVersionDF = self._spark.createDataFrame([
            ["Database1", "Customer1", "1"],
            ["Database1", "Customer2", "2"],
            ["Database1", "Customer3", "3"],
            ["Database4", "Customer4", "4"]
        ], "DatabaseName STRING, TableName STRING, IgnoredColumn STRING")

        def readTable(sqlCommand: str) -> DataFrame:
            if "@v0" in sqlCommand:
                return previousVersionDF
            elif "@v1" in sqlCommand:
                return currentVersionDF
            
        self._configGeneratorHelper._spark.read.table.side_effect = readTable

        # act
        result = self._configGeneratorHelper.isDeltaTableChanged(
            "metadataDeltaTablePath",
            "TransformationConfiguration",
            "DatabaseName='Database1'",
            ["TableName"]
        )

        # assert
        self.assertTrue(result)

        sqlCommand = self._configGeneratorHelper._spark.sql.call_args_list[0].args[0]
        self.assertIn("TransformationConfiguration", sqlCommand)

        self.assertEqual(self._configGeneratorHelper._spark.read.table.call_count, 2)

    
    # readMetadataConfig tests

    def test_readMetadataConfig_fileDoesNotExist(self):
        # arrange
        self._configGeneratorHelper._dataLakeHelper.fileExists = MagicMock(return_value=False)

        emptyDF = self._spark.createDataFrame(self._spark.sparkContext.emptyRDD(), f"col BOOLEAN")
        self._configGeneratorHelper._spark.createDataFrame.return_value = emptyDF

        # act
        configDF = self._configGeneratorHelper.readMetadataConfig(
            "config.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )

        # assert
        self.assertEqual(configDF, emptyDF)
        self._configGeneratorHelper._dataLakeHelper.fileExists.assert_called_once_with(f"{METADATA_INPUT_PATH}/config.json")
        self._configGeneratorHelper._spark.createDataFrame.assert_called_once()
        self._configGeneratorHelper._spark.read.schema.assert_not_called()

    def test_readMetadataConfig_invalidJSON(self):
        # arrange
        self._configGeneratorHelper._dataLakeHelper.fileExists = MagicMock(return_value=True)

        self._configGeneratorHelper._dbutils = MagicMock()
        self._configGeneratorHelper._dbutils.fs.head.return_value = 'not valid JSON'

        # act
        with self.assertRaises(JSONDecodeError) as context:
            self._configGeneratorHelper.readMetadataConfig(
                "config.json",
                "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
            )

        # assert
        self.assertEqual(str(context.exception), "Expecting value: line 1 column 1 (char 0)")

        self._configGeneratorHelper._dataLakeHelper.fileExists.assert_called_once_with(f"{METADATA_INPUT_PATH}/config.json")
        self._configGeneratorHelper._dbutils.fs.head.assert_called_once_with(f"{METADATA_INPUT_PATH}/config.json", 1000000000)

        self._configGeneratorHelper._spark.read.assert_not_called()

    def test_readMetadataConfig_flatConfig(self):
        # arrange
        self._configGeneratorHelper._dataLakeHelper.fileExists = MagicMock(return_value=True)

        self._configGeneratorHelper._dbutils = MagicMock()
        self._configGeneratorHelper._dbutils.fs.head.return_value = '{ "valid": "JSON" }'

        configDF = self._spark.createDataFrame(
            [
                ["Val1", True, 1],
                ["Val2", False, 2],
                ["Val3", True, 3],
                ["Val4", False, 4],
            ],
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )

        mock = MagicMock()
        mock.option.return_value = mock
        mock.json.return_value = configDF
        self._configGeneratorHelper._spark.read.schema.return_value = mock

        # act
        resultDF = self._configGeneratorHelper.readMetadataConfig(
            "config.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )

        # assert
        self.assertEqual(resultDF, configDF)

        self._configGeneratorHelper._dataLakeHelper.fileExists.assert_called_once_with(f"{METADATA_INPUT_PATH}/config.json")
        self._configGeneratorHelper._dbutils.fs.head.assert_called_once_with(f"{METADATA_INPUT_PATH}/config.json", 1000000000)

        self._configGeneratorHelper._spark.read.schema.assert_called_once_with("Col1 STRING, Col2 BOOLEAN, Col3 INTEGER")
        mock.json.assert_called_once_with(f"{METADATA_INPUT_PATH}/config.json")

    def test_readMetadataConfig_groupedConfig(self):
        # arrange
        self._configGeneratorHelper._dataLakeHelper.fileExists = MagicMock(return_value=True)

        self._configGeneratorHelper._dbutils = MagicMock()
        self._configGeneratorHelper._dbutils.fs.head.return_value = '{ "valid": "JSON" }'

        schema = """
            Key STRING,
            Entities ARRAY<
                STRUCT<
                    Col1 STRING,
                    Col2 BOOLEAN,
                    Col3 INTEGER
                >
            >
        """

        configDF = self._spark.createDataFrame(
            [
                ["key1", [("Val1", True, 1), ("Val2", False, 2)]],
                ["key2", [("Val3", True, 3)]],
                ["key3", [("Val4", False, 4)]]
            ],
            schema
        )

        expectedResultDF = self._spark.createDataFrame(
            [
                ["key1", "Val1", True, 1],
                ["key1", "Val2", False, 2],
                ["key2", "Val3", True, 3],
                ["key3", "Val4", False, 4],
            ],
            "Key STRING, Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )

        mock = MagicMock()
        mock.option.return_value = mock
        mock.json.return_value = configDF
        self._configGeneratorHelper._spark.read.schema.return_value = mock

        # act
        resultDF = self._configGeneratorHelper.readMetadataConfig(
            "config.json",
            schema
        )

        # assert
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedResultDF.rdd.collect()
        )

        self._configGeneratorHelper._dataLakeHelper.fileExists.assert_called_once_with(f"{METADATA_INPUT_PATH}/config.json")
        self._configGeneratorHelper._dbutils.fs.head.assert_called_once_with(f"{METADATA_INPUT_PATH}/config.json", 1000000000)

        self._configGeneratorHelper._spark.read.schema.assert_called_once_with(schema)
        mock.json.assert_called_once_with(f"{METADATA_INPUT_PATH}/config.json")

    def test_readMetadataConfig_groupedTwiceInDQConfig(self):
        # arrange
        self._configGeneratorHelper._dataLakeHelper.fileExists = MagicMock(return_value=True)

        self._configGeneratorHelper._dbutils = MagicMock()
        self._configGeneratorHelper._dbutils.fs.head.return_value = '{ "valid": "JSON" }'

        schema = """
            DatabaseName STRING,
            Entities ARRAY<
                STRUCT<
                    EntityName STRING,
                    Expectations ARRAY<
                        STRUCT<
                            Description STRING,
                            ExpectationType STRING,
                            KwArgs STRING,
                            Quarantine BOOLEAN,
                            DQLogOutput STRING
                        >
                    >
                >
            >
        """

        configDF = self._spark.createDataFrame(
            [
                ["db1", [
                    ("tableA", [("descA1", "expA1", "kwargsA1", True, "outputA1"), ("descA2", "expA2", "kwargsA2", False, "outputA2")]),
                    ("tableB", [("descB", "expB", "kwargsB", False, "outputB")])
                ]],
                ["db2", [("table2", [("desc2", "exp2", "kwargs2", True, "output2")])]],
                ["db3", [("table3", [("desc3", "exp3", "kwargs3", False, "output3")])]]
            ],
            schema
        )

        expectedResultDF = self._spark.createDataFrame(
            [
                ["db1", "tableA", "descA1", "expA1", "kwargsA1", True, "outputA1"],
                ["db1", "tableA", "descA2", "expA2", "kwargsA2", False, "outputA2"],
                ["db1", "tableB", "descB", "expB", "kwargsB", False, "outputB"],
                ["db2", "table2", "desc2", "exp2", "kwargs2", True, "output2"],
                ["db3", "table3", "desc3", "exp3", "kwargs3", False, "output3"]
            ],
            "DatabaseName STRING, EntityName STRING, Description STRING, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, DQLogOutput STRING"
        )

        mock = MagicMock()
        mock.option.return_value = mock
        mock.json.return_value = configDF
        self._configGeneratorHelper._spark.read.schema.return_value = mock

        # act
        resultDF = self._configGeneratorHelper.readMetadataConfig(
            "config.json",
            schema
        )

        # assert
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedResultDF.rdd.collect()
        )

        self._configGeneratorHelper._dataLakeHelper.fileExists.assert_called_once_with(f"{METADATA_INPUT_PATH}/config.json")
        self._configGeneratorHelper._dbutils.fs.head.assert_called_once_with(f"{METADATA_INPUT_PATH}/config.json", 1000000000)

        self._configGeneratorHelper._spark.read.schema.assert_called_once_with(schema)
        mock.json.assert_called_once_with(f"{METADATA_INPUT_PATH}/config.json")


    # saveMetadataConfigAsNewDeltaVersion tests

    def test_saveMetadataConfigAsNewDeltaVersion_noDeltaTable(self):
        # arrange
        self._configGeneratorHelper._DeltaTable.isDeltaTable.return_value = False
        self._configGeneratorHelper._dataLakeHelper.writeData = MagicMock()
        configDF = MagicMock(spec=DataFrame)
        self._configGeneratorHelper.readMetadataConfig = MagicMock(return_value=configDF)

        # act
        result = self._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER",
            "tableDeltaPath",
            "tableName"
        )

        # assert
        self.assertTrue(result)
        self._configGeneratorHelper._DeltaTable.isDeltaTable.assert_called_once_with(
            self._configGeneratorHelper._spark,
            "tableDeltaPath/tableName"
        )
        self._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )

    def test_saveMetadataConfigAsNewDeltaVersion_withDeltaTable_sameDataFrames(self):
        # arrange
        self._configGeneratorHelper._DeltaTable.isDeltaTable.return_value = True
        configFromAdslDF = MagicMock(spec=DataFrame)
        self._configGeneratorHelper.readMetadataConfig = MagicMock(return_value=configFromAdslDF)
        configFromHiveDF = MagicMock(spec=DataFrame)
        self._configGeneratorHelper._spark.sql = MagicMock(return_value=configFromHiveDF)
        self._configGeneratorHelper._dataLakeHelper.writeData = MagicMock()
        self._configGeneratorHelper._isContentOfDataframesSame = MagicMock(return_value=True)

        # act
        result = self._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER",
            "tableDeltaPath",
            "tableName"
        )

        # assert
        self.assertFalse(result)
        self._configGeneratorHelper._DeltaTable.isDeltaTable.assert_called_once_with(
            self._configGeneratorHelper._spark,
            "tableDeltaPath/tableName"
        )
        self._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )
        self._configGeneratorHelper._isContentOfDataframesSame.assert_called_once_with(configFromAdslDF, configFromHiveDF)

    def test_saveMetadataConfigAsNewDeltaVersion_withDeltaTable_differentDataFrames(self):
        # arrange
        self._configGeneratorHelper._DeltaTable.isDeltaTable.return_value = True
        configFromAdslDF = MagicMock(spec=DataFrame)
        self._configGeneratorHelper.readMetadataConfig = MagicMock(return_value=configFromAdslDF)
        configFromHiveDF = MagicMock(spec=DataFrame)
        self._configGeneratorHelper._spark.sql = MagicMock(return_value=configFromHiveDF)
        self._configGeneratorHelper._dataLakeHelper.writeData = MagicMock()
        self._configGeneratorHelper._isContentOfDataframesSame = MagicMock(return_value=False)

        # act
        result = self._configGeneratorHelper.saveMetadataConfigAsNewDeltaVersion(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER",
            "tableDeltaPath",
            "tableName"
        )

        # assert
        self.assertTrue(result)
        self._configGeneratorHelper._DeltaTable.isDeltaTable.assert_called_once_with(
            self._configGeneratorHelper._spark,
            "tableDeltaPath/tableName"
        )
        self._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )
        self._configGeneratorHelper._isContentOfDataframesSame.assert_called_once_with(configFromAdslDF, configFromHiveDF)


    # saveMetadataConfigAsSCDType2 tests

    def test_saveMetadataConfigAsSCDType2_configPathAsString(self):
        # arrange
        self._configGeneratorHelper.readMetadataConfig = MagicMock()

        statistics = {
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }
        self._configGeneratorHelper._dataLakeHelper.writeData = MagicMock(return_value=statistics)

        # act
        self._configGeneratorHelper.saveMetadataConfigAsSCDType2(
            "DQConfig.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER",
            "tableDeltaPath",
            "tableName",
            []
        )

        # assert
        self._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "DQConfig.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )

        self._configGeneratorHelper._spark.createDataFrame.assert_not_called()
    
    def test_saveMetadataConfigAsSCDType2_configPathAsList(self):
        # arrange
        configDFMock = MagicMock()
        configDFMockUinonedOnce = MagicMock()
        configDFMock.unionByName.return_value = configDFMockUinonedOnce
        self._configGeneratorHelper._spark.createDataFrame.return_value = configDFMock
        self._configGeneratorHelper.readMetadataConfig = MagicMock()

        statistics = {
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }
        self._configGeneratorHelper._dataLakeHelper.writeData = MagicMock(return_value=statistics)

        # act
        self._configGeneratorHelper.saveMetadataConfigAsSCDType2(
            ["DQConfig.json","DQConfig1.json"],
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER",
            "tableDeltaPath",
            "tableName",
            []
        )

        # assert
        self.assertEqual(self._configGeneratorHelper.readMetadataConfig.call_count, 2)
        self._configGeneratorHelper.readMetadataConfig.assert_any_call(
            "DQConfig.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )
        self._configGeneratorHelper.readMetadataConfig.assert_any_call(
            "DQConfig1.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )

        configDFMock.unionByName.assert_called_once()
        configDFMockUinonedOnce.unionByName.assert_called_once()

    def test_saveMetadataConfigAsSCDType2_configDidNotChange(self):
        # arrange
        configDF = self._spark.createDataFrame([
            [1, "Value A"]
        ], "ID INTEGER, ColumnName STRING")
        self._configGeneratorHelper.readMetadataConfig = MagicMock(return_value=configDF)
        
        statistics = {
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }
        self._configGeneratorHelper._dataLakeHelper.writeData = MagicMock(return_value=statistics)

        # act
        configHasChanged = self._configGeneratorHelper.saveMetadataConfigAsSCDType2(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER",
            "tableDeltaPath",
            "tableName",
            []
        )

        # assert
        self.assertEqual(configHasChanged, False)

        self._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )
        self._configGeneratorHelper._dataLakeHelper.writeData.assert_called_once()

    def test_saveMetadataConfigAsSCDType2_configHasChanged(self):
        # arrange
        configDF = self._spark.createDataFrame([
            [1, "Value A"]
        ], "ID INTEGER, ColumnName STRING")
        self._configGeneratorHelper.readMetadataConfig = MagicMock(return_value=configDF)
        
        statistics = {
            "recordsInserted": 1,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }
        self._configGeneratorHelper._dataLakeHelper.writeData = MagicMock(return_value=statistics)

        # act
        configHasChanged = self._configGeneratorHelper.saveMetadataConfigAsSCDType2(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER",
            "tableDeltaPath",
            "tableName",
            []
        )

        # assert
        self.assertEqual(configHasChanged, True)

        self._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )
        self._configGeneratorHelper._dataLakeHelper.writeData.assert_called_once()

    def test_saveMetadataConfigAsSCDType2_withValidateDataFunction_noException(self):
        # arrange
        configDF = self._spark.createDataFrame([
            [1, "Value A"]
        ], "ID INTEGER, ColumnName STRING")
        self._configGeneratorHelper.readMetadataConfig = MagicMock(return_value=configDF)
        validateDataFunction = MagicMock()
        
        statistics = {
            "recordsInserted": 0,
            "recordsUpdated": 1,
            "recordsDeleted": 0
        }
        self._configGeneratorHelper._dataLakeHelper.writeData = MagicMock(return_value=statistics)

        # act
        configHasChanged = self._configGeneratorHelper.saveMetadataConfigAsSCDType2(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER",
            "tableDeltaPath",
            "tableName",
            [],
            validateDataFunction
        )

        # assert
        self.assertEqual(configHasChanged, True)

        self._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )
        validateDataFunction.assert_called_once_with(configDF)
        self._configGeneratorHelper._dataLakeHelper.writeData.assert_called_once()

    def test_saveMetadataConfigAsSCDType2_withValidateDataFunction_emptyDataFrame(self):
        # arrange
        emptyDF = self._spark.createDataFrame(self._spark.sparkContext.emptyRDD(), f"col BOOLEAN")
        self._configGeneratorHelper.readMetadataConfig = MagicMock(return_value=emptyDF)
        validateDataFunction = MagicMock()
        self._configGeneratorHelper._dataLakeHelper.writeData = MagicMock()

        # act
        configHasChanged = self._configGeneratorHelper.saveMetadataConfigAsSCDType2(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER",
            "tableDeltaPath",
            "tableName",
            [],
            validateDataFunction
        )

        # assert
        self.assertEqual(configHasChanged, False)

        self._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )
        validateDataFunction.assert_not_called()
        self._configGeneratorHelper._dataLakeHelper.writeData.assert_not_called()

    def test_saveMetadataConfigAsSCDType2_withValidateDataFunction_exception(self):
        # arrange
        configDF = self._spark.createDataFrame([
            [1, "Value A"]
        ], "ID INTEGER, ColumnName STRING")
        self._configGeneratorHelper.readMetadataConfig = MagicMock(return_value=configDF)
        self._configGeneratorHelper._dataLakeHelper.writeData = MagicMock()
        validateDataFunction = MagicMock(side_effect=Exception("Test exception"))

        # act
        with self.assertRaises(Exception) as context:
            self._configGeneratorHelper.saveMetadataConfigAsSCDType2(
                "path/to/file.json",
                "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER",
                "tableDeltaPath",
                "tableName",
                [],
                validateDataFunction
            )

        # assert
        self.assertEqual(str(context.exception), "Test exception")
        
        self._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "path/to/file.json",
            "Col1 STRING, Col2 BOOLEAN, Col3 INTEGER"
        )
        validateDataFunction.assert_called_once_with(configDF)
        self._configGeneratorHelper._dataLakeHelper.writeData.assert_not_called()


    # createFwkFlagsTable tests

    def test_createFwkFlagsTable(self):
        # act
        self._configGeneratorHelper.createFwkFlagsTable("test_data_path")

        # assert
        sqlCommand = self._configGeneratorHelper._spark.sql.call_args_list[0].args[0]

        self.assertIn("CREATE TABLE", sqlCommand)
        self.assertIn(f"""{self._configGeneratorHelper._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_FWK_FLAGS}""", sqlCommand)
        self.assertIn(f"LOCATION \"test_data_path/{METADATA_FWK_FLAGS}\"", sqlCommand)


    # getFwkFlag tests

    def test_getFwkFlag_noResult(self):
        # arrange
        flagDF = self._spark.createDataFrame([], "key STRING, value STRING")
        self._configGeneratorHelper._spark.sql.return_value = flagDF

        # act
        result = self._configGeneratorHelper.getFwkFlag("nonExistingKey")
        
        # assert
        self.assertIsNone(result)
        
        sqlCommand = self._configGeneratorHelper._spark.sql.call_args_list[0].args[0]

        self.assertIn(f"""{self._configGeneratorHelper._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_FWK_FLAGS}""", sqlCommand)
        self.assertIn("WHERE key = 'nonExistingKey'", sqlCommand)


    def test_getFwkFlag_withResult(self):
        # arrange
        flagDF = self._spark.createDataFrame([
            ["testKey", "test value"],
            ["anotherKey", "another value"]
        ], "key STRING, value STRING")
        self._configGeneratorHelper._spark.sql.return_value = flagDF

        # act
        result = self._configGeneratorHelper.getFwkFlag("testKey")
        
        # assert
        self.assertEqual(result, "test value")
        
        sqlCommand = self._configGeneratorHelper._spark.sql.call_args_list[0].args[0]

        self.assertIn(f"""{self._configGeneratorHelper._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_FWK_FLAGS}""", sqlCommand)
        self.assertIn("WHERE key = 'testKey'", sqlCommand)


    # setFwkFlag tests

    def test_setFwkFlag_insert(self):
        # arrange
        self._configGeneratorHelper._spark.sql = MagicMock()
        self._configGeneratorHelper.getFwkFlag = MagicMock(return_value=None)
        
        # act
        self._configGeneratorHelper.setFwkFlag("testKey", "testValue")

        # assert
        self._configGeneratorHelper.getFwkFlag.assert_called_once_with("testKey")
        
        sqlCommand = self._configGeneratorHelper._spark.sql.call_args_list[0].args[0]

        self.assertIn("INSERT INTO", sqlCommand)
        self.assertIn(f"""{self._configGeneratorHelper._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_FWK_FLAGS}""", sqlCommand)
        self.assertIn("testKey", sqlCommand)
        self.assertIn("testValue", sqlCommand)

    def test_setFwkFlag_update(self):
        # arrange
        self._configGeneratorHelper._spark.sql = MagicMock()
        self._configGeneratorHelper.getFwkFlag = MagicMock(return_value="oldValue")

        # act
        self._configGeneratorHelper.setFwkFlag("testKey", "testValue")

        # assert
        self._configGeneratorHelper.getFwkFlag.assert_called_once_with("testKey")
        
        sqlCommand = self._configGeneratorHelper._spark.sql.call_args_list[0].args[0]

        self.assertIn("UPDATE", sqlCommand)
        self.assertIn(f"""{self._configGeneratorHelper._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_FWK_FLAGS}""", sqlCommand)
        self.assertIn("testKey", sqlCommand)
        self.assertIn("testValue", sqlCommand)


    # moveUnprocessedFilesToArchive tests

    def test_moveUnprocessedFilesToArchive(self):
        # arrange
        self._configGeneratorHelper._dataLakeHelper.fileExists = MagicMock(return_value=True)
        self._configGeneratorHelper._dbutils = MagicMock()
        
        # act
        executionStart = datetime.now()
        self._configGeneratorHelper.moveUnprocessedFilesToArchive("testPath", executionStart)

        # assert
        unprocessedConfigFiles = [
            FWK_LAYER_FILE,
            FWK_TRIGGER_FILE,
            FWK_LINKED_SERVICE_FILE,
            FWK_ENTITY_FOLDER,
            INGT_OUTPUT_FOLDER,
            DT_OUTPUT_FOLDER,
            EXP_OUTPUT_FOLDER
        ]
        for unprocessedFile in unprocessedConfigFiles:
            if unprocessedFile in [FWK_ENTITY_FOLDER, INGT_OUTPUT_FOLDER, DT_OUTPUT_FOLDER, EXP_OUTPUT_FOLDER]:
                unprocessedFile = f"{unprocessedFile}/"

            self._configGeneratorHelper._dataLakeHelper.fileExists.assert_any_call(f"testPath/{unprocessedFile}")
            self._configGeneratorHelper._dbutils.fs.mv.assert_any_call(
                f"testPath/{unprocessedFile}",
                f"testPath/Archive/{executionStart.strftime('%Y%m%d%H%M%S')}_unprocessed_{unprocessedFile}",
                True
            )
