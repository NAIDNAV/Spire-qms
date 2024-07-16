# Databricks notebook source
import numpy as np
import unittest
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, StructType
from typing import Tuple
from unittest.mock import MagicMock, ANY

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.etl.validator import Validator
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../../compartment/includes/compartmentConfig

# COMMAND ----------

# MAGIC %run ../../../../etl/validator

# COMMAND ----------

# MAGIC %run ../../../../includes/frameworkConfig

# COMMAND ----------

class ValidatorTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

        self._compartmentConfig = MagicMock()
        self._compartmentConfig.METADATA_DQ_CONFIGURATION_FILE = "DQ_Configuration.json"
        self._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE = "DQ_Reference_Values.json"
        self._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE = "DQ_Reference_Pair_Values.json"
        self._compartmentConfig.METADATA_CONFIGURATION_FILE_VALIDATION_FILE = "Configuration_File_Validation.json"
        self._compartmentConfig.FWK_METADATA_CONFIG = {
            "metadata": {
                "databaseName": "Application_MEF_Metadata"
            }
        }

        self._entRunId = "test_run"
        self._entTriggerTime = datetime.now()

    def setUp(self):
        self._validator = Validator(self._spark, self._compartmentConfig, self._entRunId, self._entTriggerTime)
        self._validator._spark = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # constructor tests

    def test_constructor_etlValidator(self):
        # act
        validator = Validator(self._spark, self._compartmentConfig, self._entRunId, self._entTriggerTime)

        # assert
        self.assertEqual(validator._entRunId, self._entRunId)
        self.assertEqual(validator._entRunDatetime, self._entTriggerTime)
        self.assertEqual(validator._validationConfigurationTable, Validator.METADATA_DQ_CONFIGURATION_TABLE)
        self.assertEqual(validator._logToDatabase, True)

    def test_constructor_fileValidator(self):
        # arrange
        validatorParams = {
            "validationConfigurationTable": Validator.METADATA_CONFIGURATION_FILE_VALIDATION_TABLE,
            "logToDatabase": False,
        }

        # act
        validator = Validator(self._spark, self._compartmentConfig, self._entRunId, self._entTriggerTime, validatorParams)

        # assert
        self.assertEqual(validator._entRunId, self._entRunId)
        self.assertEqual(validator._entRunDatetime, self._entTriggerTime)
        self.assertEqual(validator._validationConfigurationTable, Validator.METADATA_CONFIGURATION_FILE_VALIDATION_TABLE)
        self.assertEqual(validator._logToDatabase, False)

    
    # validateReferenceValuesConfig tests

    def test_validateReferenceValuesConfig_invalid(self):
        # arrange
        configDF = self._spark.createDataFrame([
            ["CountryRegion", "Value 1", "Desc 1"],
            ["CountryRegion", "Value 2", "Desc 2"],
            ["CountryRegion", None, "Desc 3"],
        ], METADATA_DQ_REFERENCE_VALUES_SCHEMA)

        # act
        with self.assertRaisesRegex(AssertionError, f"Null can't be present in Value column in {self._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE} config"):
            self._validator._validateReferenceValuesConfig(configDF)

    def test_validateReferenceValuesConfig_valid(self):
        # arrange
        configDF = self._spark.createDataFrame([
            ["CountryRegion", "Value 1", "Desc 1"],
            ["CountryRegion", "Value 2", "Desc 2"],
            ["CountryRegion", "Value 3", "Desc 3"],
        ], METADATA_DQ_REFERENCE_VALUES_SCHEMA)

        # act
        self._validator._validateReferenceValuesConfig(configDF)


    # validateReferencePairValuesConfig tests

    def test_validateReferencePairValuesConfig_invalidValue1(self):
        # arrange
        configDF = self._spark.createDataFrame([
            ["CountryRegion", "Value 1", "Desc 1", "State", "Value 4", "Desc 4"],
            ["CountryRegion", "Value 2", "Desc 2", "State", "Value 5", "Desc 5"],
            ["CountryRegion", None, "Desc 3", "State", "Value 6", "Desc 6"],
        ], METADATA_DQ_REFERENCE_PAIR_VALUES_SCHEMA)

        # act
        with self.assertRaisesRegex(AssertionError, f"Null can't be present neither in Value1 nor Value2 column in {self._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE} config"):
            self._validator._validateReferencePairValuesConfig(configDF)

    def test_validateReferencePairValuesConfig_invalidValue2(self):
        # arrange
        configDF = self._spark.createDataFrame([
            ["CountryRegion", "Value 1", "Desc 1", "State", "Value 4", "Desc 4"],
            ["CountryRegion", "Value 2", "Desc 2", "State", "Value 5", "Desc 5"],
            ["CountryRegion", "Value 3", "Desc 3", "State", None, "Desc 6"],
        ], METADATA_DQ_REFERENCE_PAIR_VALUES_SCHEMA)

        # act
        with self.assertRaisesRegex(AssertionError, f"Null can't be present neither in Value1 nor Value2 column in {self._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE} config"):
            self._validator._validateReferencePairValuesConfig(configDF)

    def test_validateReferencePairValuesConfig_valid(self):
        # arrange
        configDF = self._spark.createDataFrame([
            ["CountryRegion", "Value 1", "Desc 1", "State", "Value 4", "Desc 4"],
            ["CountryRegion", "Value 2", "Desc 2", "State", "Value 5", "Desc 5"],
            ["CountryRegion", "Value 3", "Desc 3", "State", "Value 6", "Desc 6"],
        ], METADATA_DQ_REFERENCE_PAIR_VALUES_SCHEMA)

        # act
        self._validator._validateReferencePairValuesConfig(configDF)

    
    # saveMetadataConfig tests

    def test_saveDqMetadataConfig_dqConfiguration_configHasChanged(self):
        # arrange
        configPath = self._compartmentConfig.METADATA_DQ_CONFIGURATION_FILE
        dataPath = "test/dataPath"

        self._validator._configGeneratorHelper.saveMetadataConfigAsSCDType2 = MagicMock(return_value=True)

        # act
        configHasChanged = self._validator.saveDqMetadataConfig(configPath, dataPath)

        # assert
        self.assertEqual(configHasChanged, True)

        self._validator._configGeneratorHelper.saveMetadataConfigAsSCDType2.assert_called_once_with(
            configPath,
            METADATA_DQ_CONFIGURATION_SCHEMA,
            dataPath,
            self._validator.METADATA_DQ_CONFIGURATION_TABLE,
            ["DatabaseName", "EntityName", "ExpectationType", "KwArgs"]
        )

    def test_saveDqMetadataConfig_dqConfiguration_configDidNotChange(self):
        # arrange
        configPath = self._compartmentConfig.METADATA_DQ_CONFIGURATION_FILE
        dataPath = "test/dataPath"

        self._validator._configGeneratorHelper.saveMetadataConfigAsSCDType2 = MagicMock(return_value=False)

        # act
        configHasChanged = self._validator.saveDqMetadataConfig(configPath, dataPath)

        # assert
        self.assertEqual(configHasChanged, False)

        self._validator._configGeneratorHelper.saveMetadataConfigAsSCDType2.assert_called_once_with(
            configPath,
            METADATA_DQ_CONFIGURATION_SCHEMA,
            dataPath,
            self._validator.METADATA_DQ_CONFIGURATION_TABLE,
            ["DatabaseName", "EntityName", "ExpectationType", "KwArgs"]
        )

    def test_saveDqMetadataConfig_dqConfiguration_configIsNone(self):
        # arrange
        configPath = None
        dataPath = "test/dataPath"

        self._validator._configGeneratorHelper.saveMetadataConfigAsSCDType2 = MagicMock()

        # act
        configHasChanged = self._validator.saveDqMetadataConfig(configPath, dataPath)

        # assert
        self.assertEqual(configHasChanged, False)

        self._validator._configGeneratorHelper.saveMetadataConfigAsSCDType2.assert_not_called()

    def test_saveDqRefMetadataConfig_referenceValues(self):
        # arrange
        configPath = self._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE
        dataPath = "test/dataPath"

        self._validator._configGeneratorHelper.saveMetadataConfigAsSCDType2 = MagicMock(return_value=True)

        # act
        configHasChanged = self._validator.saveDqRefMetadataConfig(configPath, dataPath)

        # assert
        self.assertEqual(configHasChanged, True)

        self._validator._configGeneratorHelper.saveMetadataConfigAsSCDType2.assert_called_once_with(
            configPath,
            METADATA_DQ_REFERENCE_VALUES_SCHEMA,
            dataPath,
            self._validator.METADATA_DQ_REFERENCE_VALUES_TABLE,
            ["ReferenceKey", "Value"],
            validateDataFunction = self._validator._validateReferenceValuesConfig
        )

    def test_saveDqRefPairMetadataConfig_referencePairValues(self):
        # arrange
        configPath = self._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE
        dataPath = "test/dataPath"

        self._validator._configGeneratorHelper.saveMetadataConfigAsSCDType2 = MagicMock(return_value=True)

        # act
        configHasChanged = self._validator.saveDqRefPairMetadataConfig(configPath, dataPath)

        # assert
        self.assertEqual(configHasChanged, True)

        self._validator._configGeneratorHelper.saveMetadataConfigAsSCDType2.assert_called_once_with(
            configPath,
            METADATA_DQ_REFERENCE_PAIR_VALUES_SCHEMA,
            dataPath,
            self._validator.METADATA_DQ_REFERENCE_PAIR_VALUES_TABLE,
            ["ReferenceKey1", "Value1", "ReferenceKey2", "Value2"],
            validateDataFunction = self._validator._validateReferencePairValuesConfig
        )

    def test_saveConfigFileMetadataConfig_configurationValidation(self):
        # arrange
        configPath = self._compartmentConfig.METADATA_CONFIGURATION_FILE_VALIDATION_FILE
        dataPath = "test/dataPath"

        self._validator._configGeneratorHelper.saveMetadataConfigAsSCDType2 = MagicMock(return_value=True)

        # act
        configHasChanged = self._validator.saveConfigFileMetadataConfig(configPath, dataPath)

        # assert
        self.assertEqual(configHasChanged, True)

        self._validator._configGeneratorHelper.saveMetadataConfigAsSCDType2.assert_called_once_with(
            configPath,
            METADATA_DQ_CONFIGURATION_SCHEMA,
            dataPath,
            self._validator.METADATA_CONFIGURATION_FILE_VALIDATION_TABLE,
            ["DatabaseName", "EntityName", "ExpectationType", "KwArgs"]
        )
    
    
    # getTableNamesMatchingWildcard tests

    def test_getTableNamesMatchingWildcard_starEnd(self):
        # arrange
        wildcard = "Addre[*]"
        tableNames = ["Address", "prefix_Address", "address_postfix", "ADDRESS_1", "test"]

        expectedResult = ["Address", "address_postfix", "ADDRESS_1"]

        # act
        result = self._validator._getTableNamesMatchingWildcard(wildcard, tableNames)

        # assert
        self.assertEqual(result, expectedResult)

    def test_getTableNamesMatchingWildcard_starStart(self):
        # arrange
        wildcard = "[*]ress"
        tableNames = ["Address", "prefix_ADDRESS", "Address_postfix", "1_ADDRESS", "test"]

        expectedResult = ["Address", "prefix_ADDRESS", "1_ADDRESS"]

        # act
        result = self._validator._getTableNamesMatchingWildcard(wildcard, tableNames)

        # assert
        self.assertEqual(result, expectedResult)

    def test_getTableNamesMatchingWildcard_starMiddle(self):
        # arrange
        wildcard = "Addre[*]s"
        tableNames = ["Address", "prefix_Address", "Address_postfix", "ADDRE_S", "test"]

        expectedResult = ["Address", "ADDRE_S"]

        # act
        result = self._validator._getTableNamesMatchingWildcard(wildcard, tableNames)

        # assert
        self.assertEqual(result, expectedResult)

    def test_getTableNamesMatchingWildcard_listEnd(self):
        # arrange
        wildcard = "Addre[ss, ss_postfix]"
        tableNames = ["Address", "prefix_Address", "address_postfix", "addre_ss_postfix"]

        expectedResult = ["Address", "address_postfix"]

        # act
        result = self._validator._getTableNamesMatchingWildcard(wildcard, tableNames)

        # assert
        self.assertEqual(result, expectedResult)

    def test_getTableNamesMatchingWildcard_listStart(self):
        # arrange
        wildcard = "[add, prefix_Add]ress"
        tableNames = ["Address", "prefix_address", "prefix_Add_ress", "Address_postfix"]

        expectedResult = ["Address", "prefix_address"]

        # act
        result = self._validator._getTableNamesMatchingWildcard(wildcard, tableNames)

        # assert
        self.assertEqual(result, expectedResult)

    def test_getTableNamesMatchingWildcard_listMiddle(self):
        # arrange
        wildcard = "addre[s]s"
        tableNames = ["Address", "prefix_Address", "Address_postfix"]

        expectedResult = ["Address"]

        # act
        result = self._validator._getTableNamesMatchingWildcard(wildcard, tableNames)

        # assert
        self.assertEqual(result, expectedResult)

    def test_getTableNamesMatchingWildcard_noMatch(self):
        # act & assert
        result = self._validator._getTableNamesMatchingWildcard("Addre[*]", ["prefix_Address"])
        self.assertEqual(result, [])

        # act & assert
        result = self._validator._getTableNamesMatchingWildcard("[*]ress", ["Address_postfix"])
        self.assertEqual(result, [])

        # act & assert
        result = self._validator._getTableNamesMatchingWildcard("Addre[*]s", ["Address_postfix", "prefix_Address"])
        self.assertEqual(result, [])

        # act & assert
        result = self._validator._getTableNamesMatchingWildcard("Addre[ss, ss_postfix]", ["prefix_Address"])
        self.assertEqual(result, [])

        # act & assert
        result = self._validator._getTableNamesMatchingWildcard("[Add, prefix_Add]ress", ["Address_postfix"])
        self.assertEqual(result, [])

        # act & assert
        result = self._validator._getTableNamesMatchingWildcard("Addre[s]s", ["Address_postfix", "prefix_Address"])
        self.assertEqual(result, [])


    # isTableNameMatchingWildcard tests

    def test_isTableNameMatchingWildcard_starStart(self):
        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[*]", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[*]", "address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("addre[*]", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[*]", "Prefix_Address")
        self.assertEqual(result, False)

    def test_isTableNameMatchingWildcard_starEnd(self):
        # act & assert
        result = self._validator._isTableNameMatchingWildcard("[*]ress", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("[*]Ress", "address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("[*]ress", "AddRess")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("[*]ress", "Address_postfix")
        self.assertEqual(result, False)

    def test_isTableNameMatchingWildcard_starMiddle(self):
        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[*]s", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[*]", "address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("addre[*]s", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[*]s", "Prefix_Address")
        self.assertEqual(result, False)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[*]s", "Address_postfix")
        self.assertEqual(result, False)

    def test_isTableNameMatchingWildcard_listEnd(self):
        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[ss, ss_postfix]", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[ss, ss_postfix]", "address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("addre[ss, ss_postfix]", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[SS, ss_postfix]", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("addre[SS, ss_postfix]", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[ss, ss_postfix]", "Address_postfix")
        self.assertEqual(result, True)
        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[ss, ss_postfix]", "address_postfix")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[ss, ss_postfix]", "Prefix_Address")
        self.assertEqual(result, False)

    def test_isTableNameMatchingWildcard_listStart(self):
        # act & assert
        result = self._validator._isTableNameMatchingWildcard("[Add, prefix_Add]ress", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("[Add, prefix_Add]ress", "address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("[add, prefix_add]ress", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("[ADD, prefix_ADD]ress", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("[add, prefix_add]ress", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("[Add, prefix_Add]ress", "prefix_Address")
        self.assertEqual(result, True)
        # act & assert
        result = self._validator._isTableNameMatchingWildcard("[Add, prefix_Add]ress", "prefix_address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[ss, ss_postfix]", "Prefix_Address")
        self.assertEqual(result, False)

    def test_isTableNameMatchingWildcard_listMiddle(self):
        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[s]s", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[s]s", "address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("addre[s]s", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[S]s", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("addre[S]s", "Address")
        self.assertEqual(result, True)

        # act & assert
        result = self._validator._isTableNameMatchingWildcard("Addre[s]s", "Prefix_Address")
        self.assertEqual(result, False)

    
    # addValidationStatusForTablesInHive tests

    def test_addValidationStatusForTablesInHive_noDQConfig(self):
        # arrange
        self._validator._isDQConfigurationDefined = MagicMock(return_value=False)
        self._validator._dataLakeHelper.allowAlterForTable = MagicMock()

        # act
        self._validator.addValidationStatusForTablesInHive()

        # assert
        self._validator._spark.sql.assert_not_called()
        self._validator._dataLakeHelper.allowAlterForTable.assert_not_called()

    def test_addValidationStatusForTablesInHive_alterTables(self):
        # arrange
        self._validator._isDQConfigurationDefined = MagicMock(return_value=True)
        self._validator._dataLakeHelper.allowAlterForTable = MagicMock()

        def getTableNames(databaseName: str) -> ArrayType:
            if databaseName == "Database1":
                return ["customer", "address", "address_postfix", "orders", "vendors"]
            elif databaseName == "Database2":
                return ["customerhistory", "addresshistory", "address_postfixhistory", "ordershistory", "vendorshistory"]

        self._validator._sqlContext = MagicMock()
        self._validator._sqlContext.tableNames.side_effect = getTableNames

        distinctDqExpectationsDF = self._spark.createDataFrame([
            ["Database1", "Customer"],
            ["Database1", "Addre[ss, ss_postfix]"],
            ["Database2", "Addre[*]"],
        ], "DatabaseName STRING, EntityName STRING")

        databasesInHiveDF = self._spark.createDataFrame([
            ["database1"],
            ["database2"],
        ], "databaseName STRING")

        tablesInDatabase1DF = self._spark.createDataFrame([
            ["Customer"],
            ["Address"],
            ["Address_postfix"],
            ["Orders"],
            ["Vendors"],
        ], "EntityName STRING")

        tablesInDatabase2DF = self._spark.createDataFrame([
            ["CustomerHistory"],
            ["AddressHistory"],
            ["Address_postfixHistory"],
            ["OrdersHistory"],
            ["VendorsHistory"],
        ], "EntityName STRING")

        noValidationColumnDF = self._spark.createDataFrame([
        ], "col_name STRING")

        validationColumnDF = self._spark.createDataFrame([
            [VALIDATION_STATUS_COLUMN_NAME],
        ], "col_name STRING")

        def sqlQuery(sqlCommand: str) -> DataFrame:            
            if ("SELECT DISTINCT DatabaseName, EntityName" in sqlCommand
                and self._validator.METADATA_DQ_CONFIGURATION_TABLE in sqlCommand
            ):
                return distinctDqExpectationsDF
            elif "SHOW DATABASES" in sqlCommand:
                return databasesInHiveDF
            elif ("SELECT DISTINCT EntityName" in sqlCommand
                  and METADATA_ATTRIBUTES in sqlCommand
                  and "Database1" in sqlCommand
            ):
                return tablesInDatabase1DF
            elif ("SELECT DISTINCT EntityName" in sqlCommand
                  and METADATA_ATTRIBUTES in sqlCommand
                  and "Database2" in sqlCommand
            ):
                return tablesInDatabase2DF
            elif "DESC FORMATTED Database1.Customer" in sqlCommand:
                return noValidationColumnDF
            elif "DESC FORMATTED Database1.Address_postfix" in sqlCommand:
                return validationColumnDF
            elif "DESC FORMATTED Database1.Address" in sqlCommand:
                return noValidationColumnDF
            elif "DESC FORMATTED Database2.AddressHistory" in sqlCommand:
                return noValidationColumnDF
            elif "DESC FORMATTED Database2.Address_postfixHistory" in sqlCommand:
                return validationColumnDF

        self._validator._spark.sql.side_effect = sqlQuery

        # act
        self._validator.addValidationStatusForTablesInHive()

        # assert
        self.assertEqual(self._validator._dataLakeHelper.allowAlterForTable.call_count, 3)
        self._validator._dataLakeHelper.allowAlterForTable.assert_any_call("Database1.Customer")
        self._validator._dataLakeHelper.allowAlterForTable.assert_any_call("Database1.Address")
        self._validator._dataLakeHelper.allowAlterForTable.assert_any_call("Database2.AddressHistory")

        self._validator._spark.sql.assert_any_call(f"""ALTER TABLE Database1.Customer ADD COLUMN {VALIDATION_STATUS_COLUMN_NAME} STRING""")
        self._validator._spark.sql.assert_any_call(f"""ALTER TABLE Database1.Address ADD COLUMN {VALIDATION_STATUS_COLUMN_NAME} STRING""")
        self._validator._spark.sql.assert_any_call(f"""ALTER TABLE Database2.AddressHistory ADD COLUMN {VALIDATION_STATUS_COLUMN_NAME} STRING""")

    def test_addValidationStatusForTablesInHive_databaseNotInHIVE(self):
        # arrange
        self._validator._isDQConfigurationDefined = MagicMock(return_value=True)
        self._validator._dataLakeHelper.allowAlterForTable = MagicMock()

        distinctDqExpectationsDF = self._spark.createDataFrame([
            ["Database1", "Customer"],
            ["Database1", "Addre[ss, ss_postfix]"],
            ["Database1", "Addre[*]"],
        ], "DatabaseName STRING, EntityName STRING")

        databasesInHiveDF = self._spark.createDataFrame([
            ["database2"],
        ], "databaseName STRING")

        def sqlQuery(sqlCommand: str) -> DataFrame:            
            if ("SELECT DISTINCT DatabaseName, EntityName" in sqlCommand
                and self._validator.METADATA_DQ_CONFIGURATION_TABLE in sqlCommand
            ):
                return distinctDqExpectationsDF
            elif "SHOW DATABASES" in sqlCommand:
                return databasesInHiveDF

        self._validator._spark.sql.side_effect = sqlQuery

        # act
        self._validator.addValidationStatusForTablesInHive()

        # assert
        self._validator._dataLakeHelper.allowAlterForTable.assert_not_called()

    def test_addValidationStatusForTablesInHive_tableNotInHIVE(self):
        # arrange
        self._validator._isDQConfigurationDefined = MagicMock(return_value=True)
        self._validator._dataLakeHelper.allowAlterForTable = MagicMock()

        def getTableNames(databaseName: str) -> ArrayType:
            if databaseName == "Database1":
                return ["orders", "vendors"]

        self._validator._sqlContext = MagicMock()
        self._validator._sqlContext.tableNames.side_effect = getTableNames

        distinctDqExpectationsDF = self._spark.createDataFrame([
            ["Database1", "Customer"],
            ["Database1", "Addre[ss]"],
        ], "DatabaseName STRING, EntityName STRING")

        databasesInHiveDF = self._spark.createDataFrame([
            ["database1"],
            ["database2"],
        ], "databaseName STRING")

        tablesInDatabase1DF = self._spark.createDataFrame([
            ["Customer"],
            ["Address"],
            ["Orders"],
            ["Vendors"],
        ], "EntityName STRING")

        def sqlQuery(sqlCommand: str) -> DataFrame:            
            if ("SELECT DISTINCT DatabaseName, EntityName" in sqlCommand
                and self._validator.METADATA_DQ_CONFIGURATION_TABLE in sqlCommand
            ):
                return distinctDqExpectationsDF
            elif "SHOW DATABASES" in sqlCommand:
                return databasesInHiveDF
            elif ("SELECT DISTINCT EntityName" in sqlCommand
                  and METADATA_ATTRIBUTES in sqlCommand
                  and "Database1" in sqlCommand
            ):
                return tablesInDatabase1DF

        self._validator._spark.sql.side_effect = sqlQuery

        # act
        self._validator.addValidationStatusForTablesInHive()

        # assert
        self.assertEqual(self._validator._dataLakeHelper.allowAlterForTable.call_count, 0)


    # filterOutQuarantinedRecords tests

    def test_filterOutQuarantinedRecords_fewQuarantined(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, "value 1", Validator.VALIDATION_STATUS_VALID],
            [2, "value 2", Validator.VALIDATION_STATUS_INVALID],
            [3, "value 3", Validator.VALIDATION_STATUS_QUARANTINED],
            [4, "value 4", ""],
            [5, "value 5", None],
            [6, "value 6", Validator.VALIDATION_STATUS_QUARANTINED],
        ], f"Id INTEGER, Value STRING, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        expectedResultDF = self._spark.createDataFrame([
            [1, "value 1"],
            [2, "value 2"],
            [4, "value 4"],
            [5, "value 5"],
        ], "Id INTEGER, Value STRING")

        # act
        resultDF = self._validator.filterOutQuarantinedRecords(dataDF)

        # assert
        self.assertEqual(
            resultDF.sort("Id").rdd.collect(),
            expectedResultDF.sort("Id").rdd.collect()
        )

    def test_filterOutQuarantinedRecords_noQuarantined(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, "value 1", Validator.VALIDATION_STATUS_VALID],
            [2, "value 2", Validator.VALIDATION_STATUS_INVALID],
            [3, "value 3", Validator.VALIDATION_STATUS_VALID],
            [4, "value 4", ""],
            [5, "value 5", None],
            [6, "value 6", Validator.VALIDATION_STATUS_INVALID],
        ], f"Id INTEGER, Value STRING, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        expectedResultDF = self._spark.createDataFrame([
            [1, "value 1"],
            [2, "value 2"],
            [3, "value 3"],
            [4, "value 4"],
            [5, "value 5"],
            [6, "value 6"],
        ], "Id INTEGER, Value STRING")

        # act
        resultDF = self._validator.filterOutQuarantinedRecords(dataDF)

        # assert
        self.assertEqual(
            resultDF.sort("Id").rdd.collect(),
            expectedResultDF.sort("Id").rdd.collect()
        )
    
    def test_filterOutQuarantinedRecords_noValidationStatusColumn(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, "value 1"],
            [2, "value 2"],
            [3, "value 3"],
        ], "Id INTEGER, Value STRING")

        # act
        resultDF = self._validator.filterOutQuarantinedRecords(dataDF)

        # assert
        self.assertEqual(resultDF, dataDF)

    def test_filterOutQuarantinedRecords_empty(self):
        # arrange
        dataDF = self._spark.createDataFrame([
        ], f"Id INTEGER, Value STRING, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        expectedResultDF = self._spark.createDataFrame([
        ], f"Id INTEGER, Value STRING")

        # act
        resultDF = self._validator.filterOutQuarantinedRecords(dataDF)

        # assert
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedResultDF.rdd.collect()
        )
    
    def test_filterOutQuarantinedRecords_none(self):
        # arrange
        dataDF = None

        # act
        resultDF = self._validator.filterOutQuarantinedRecords(dataDF)

        # assert
        self.assertEqual(resultDF, dataDF)

    
    # isDQConfigurationDefined tests

    def test_isDQConfigurationDefined_true(self):
        # arrange
        validationTableName = self._validator._validationConfigurationTable
        self._validator._sqlContext = MagicMock()
        self._validator._sqlContext.tableNames.return_value = [validationTableName.lower()]
        
        # act
        result = self._validator._isDQConfigurationDefined()

        # assert
        self.assertEqual(result, True)
    
    def test_isDQConfigurationDefined_false(self):
        # arrange
        self._validator._sqlContext = MagicMock()
        self._validator._sqlContext.tableNames.return_value = ["no_validation_table"]
        
        # act
        result = self._validator._isDQConfigurationDefined()

        # assert
        self.assertEqual(result, False)


    # getDQConfiguration tests

    def test_getDQConfiguration_exactRules(self):
        # arrange
        wholeDQConfigurationDF = self._spark.createDataFrame([
            [1, "expect_column_values_to_be_null", "KwArgs1", False, "Description1", "DQLogOutput1", "testTableName", 2],
            [2, "expect_column_value_lengths_to_equal", "KwArgs2", False, "Description2", "DQLogOutput2", "testtablename", 2], # lowercase
            [3, "expect_table_columns_to_match_set", "KwArgs3", False, "Description3", "DQLogOutput3", "testTableName", 1],
            [4, "expect_table_columns_to_match_ordered_list", "KwArgs4", True, "Description3", "DQLogOutput3", "testTableName", 1],
            [5, "expect_column_values_to_be_of_type", "KwArgs5", False, "Description4", "DQLogOutput4", "testTableName", 1],
            [6, "expect_column_values_to_be_null", "KwArgs6", False, "Description6", "DQLogOutput6", "anotherTableName", 2],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING, EntityName STRING, OrderPrioritized INTEGER")

        self._validator._spark.sql.return_value = wholeDQConfigurationDF
        self._validator._spark.createDataFrame = self._spark.createDataFrame

        expectedDQConfigurationDF = self._spark.createDataFrame([
            [3, "expect_table_columns_to_match_set", "KwArgs3", False, "Description3", "DQLogOutput3"],
            [4, "expect_table_columns_to_match_ordered_list", "KwArgs4", True, "Description3", "DQLogOutput3"],
            [5, "expect_column_values_to_be_of_type", "KwArgs5", False, "Description4", "DQLogOutput4"],
            [1, "expect_column_values_to_be_null", "KwArgs1", False, "Description1", "DQLogOutput1"],
            [2, "expect_column_value_lengths_to_equal", "KwArgs2", False, "Description2", "DQLogOutput2"],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING")

        # act
        dqConfigurationDF = self._validator._getDQConfiguration("testDatabaseName", "testTableName")

        # assert
        self.assertEqual(self._validator._spark.sql.call_count, 1)

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]

        self.assertIn(f"""FROM Application_MEF_Metadata.{self._validator._validationConfigurationTable}""", sqlCommand)
        self.assertIn("testDatabaseName", sqlCommand)
        self.assertIn(f"""AND {IS_ACTIVE_RECORD_COLUMN_NAME} AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE""", sqlCommand)

        self.assertEqual(
            dqConfigurationDF.rdd.collect(),
            expectedDQConfigurationDF.rdd.collect()
        )

    def test_getDQConfiguration_wildcardStartRules(self):
        # arrange
        wholeDQConfigurationDF = self._spark.createDataFrame([
            [1, "expect_column_values_to_be_null", "KwArgs1", False, "Description1", "DQLogOutput1", "test[*]Name", 2],
            [2, "expect_column_value_lengths_to_equal", "KwArgs2", False, "Description2", "DQLogOutput2", "test[*]name", 2], # lowercase
            [3, "expect_table_columns_to_match_set", "KwArgs3", False, "Description3", "DQLogOutput3", "test[*]Name", 1],
            [4, "expect_table_columns_to_match_ordered_list", "KwArgs4", True, "Description3", "DQLogOutput3", "test[*]Name", 1],
            [5, "expect_column_values_to_be_of_type", "KwArgs5", False, "Description4", "DQLogOutput4", "test[*]Name", 1],
            [6, "expect_column_values_to_be_null", "KwArgs6", False, "Description6", "DQLogOutput6", "another[*]Name", 2],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING, EntityName STRING, OrderPrioritized INTEGER")

        self._validator._spark.sql.return_value = wholeDQConfigurationDF
        self._validator._spark.createDataFrame = self._spark.createDataFrame

        expectedDQConfigurationDF = self._spark.createDataFrame([
            [3, "expect_table_columns_to_match_set", "KwArgs3", False, "Description3", "DQLogOutput3"],
            [4, "expect_table_columns_to_match_ordered_list", "KwArgs4", True, "Description3", "DQLogOutput3"],
            [5, "expect_column_values_to_be_of_type", "KwArgs5", False, "Description4", "DQLogOutput4"],
            [1, "expect_column_values_to_be_null", "KwArgs1", False, "Description1", "DQLogOutput1"],
            [2, "expect_column_value_lengths_to_equal", "KwArgs2", False, "Description2", "DQLogOutput2"],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING")

        # act
        dqConfigurationDF = self._validator._getDQConfiguration("testDatabaseName", "testTableName")

        # assert
        self.assertEqual(self._validator._spark.sql.call_count, 1)

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]

        self.assertIn(f"""FROM Application_MEF_Metadata.{self._validator._validationConfigurationTable}""", sqlCommand)
        self.assertIn("testDatabaseName", sqlCommand)
        self.assertIn(f"""AND {IS_ACTIVE_RECORD_COLUMN_NAME} AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE""", sqlCommand)

        self.assertEqual(
            dqConfigurationDF.rdd.collect(),
            expectedDQConfigurationDF.rdd.collect()
        )

    def test_getDQConfiguration_wildcardListRules(self):
        # arrange
        wholeDQConfigurationDF = self._spark.createDataFrame([
            [1, "expect_column_values_to_be_null", "KwArgs1", False, "Description1", "DQLogOutput1", "test[Table, Something]Name", 2],
            [2, "expect_column_value_lengths_to_equal", "KwArgs2", False, "Description2", "DQLogOutput2", "Test[table, something]name", 2], # uppercase + lowercase
            [3, "expect_table_columns_to_match_set", "KwArgs3", False, "Description3", "DQLogOutput3", "test[Table, Something]Name", 1],
            [4, "expect_table_columns_to_match_ordered_list", "KwArgs4", True, "Description3", "DQLogOutput3", "test[Table, Something]Name", 1],
            [5, "expect_column_values_to_be_of_type", "KwArgs5", False, "Description4", "DQLogOutput4", "test[Table, Something]Name", 1],
            [6, "expect_column_values_to_be_null", "KwArgs6", False, "Description6", "DQLogOutput6", "another[Table, Something]Name", 2],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING, EntityName STRING, OrderPrioritized INTEGER")

        self._validator._spark.sql.return_value = wholeDQConfigurationDF
        self._validator._spark.createDataFrame = self._spark.createDataFrame

        expectedDQConfigurationDF = self._spark.createDataFrame([
            [3, "expect_table_columns_to_match_set", "KwArgs3", False, "Description3", "DQLogOutput3"],
            [4, "expect_table_columns_to_match_ordered_list", "KwArgs4", True, "Description3", "DQLogOutput3"],
            [5, "expect_column_values_to_be_of_type", "KwArgs5", False, "Description4", "DQLogOutput4"],
            [1, "expect_column_values_to_be_null", "KwArgs1", False, "Description1", "DQLogOutput1"],
            [2, "expect_column_value_lengths_to_equal", "KwArgs2", False, "Description2", "DQLogOutput2"],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING")

        # act
        dqConfigurationDF = self._validator._getDQConfiguration("testDatabaseName", "testTableName")

        # assert
        self.assertEqual(self._validator._spark.sql.call_count, 1)

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]

        self.assertIn(f"""FROM Application_MEF_Metadata.{self._validator._validationConfigurationTable}""", sqlCommand)
        self.assertIn("testDatabaseName", sqlCommand)
        self.assertIn(f"""AND {IS_ACTIVE_RECORD_COLUMN_NAME} AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE""", sqlCommand)

        self.assertEqual(
            dqConfigurationDF.rdd.collect(),
            expectedDQConfigurationDF.rdd.collect()
        )

    def test_getDQConfiguration_mixedRules(self):
        # arrange
        wholeDQConfigurationDF = self._spark.createDataFrame([
            [1, "expect_column_values_to_be_null", "KwArgs1", False, "Description1", "DQLogOutput1", "testTableName", 2],
            [2, "expect_column_value_lengths_to_equal", "KwArgs2", False, "Description2", "DQLogOutput2", "test[*]Name", 2],
            [3, "expect_table_columns_to_match_set", "KwArgs3", False, "Description3", "DQLogOutput3", "test[Table, Something]Name", 1],
            [4, "expect_table_columns_to_match_ordered_list", "KwArgs4", True, "Description3", "DQLogOutput3", "Test[table, something]Name", 1],
            [5, "expect_column_values_to_be_of_type", "KwArgs5", False, "Description4", "DQLogOutput4", "test[*]Name", 1],
            [6, "expect_column_values_to_be_null", "KwArgs6", False, "Description6", "DQLogOutput6", "another[Table, Something]Name", 2],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING, EntityName STRING, OrderPrioritized INTEGER")

        self._validator._spark.sql.return_value = wholeDQConfigurationDF
        self._validator._spark.createDataFrame = self._spark.createDataFrame

        expectedDQConfigurationDF = self._spark.createDataFrame([
            [3, "expect_table_columns_to_match_set", "KwArgs3", False, "Description3", "DQLogOutput3"],
            [4, "expect_table_columns_to_match_ordered_list", "KwArgs4", True, "Description3", "DQLogOutput3"],
            [5, "expect_column_values_to_be_of_type", "KwArgs5", False, "Description4", "DQLogOutput4"],
            [1, "expect_column_values_to_be_null", "KwArgs1", False, "Description1", "DQLogOutput1"],
            [2, "expect_column_value_lengths_to_equal", "KwArgs2", False, "Description2", "DQLogOutput2"],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING")

        # act
        dqConfigurationDF = self._validator._getDQConfiguration("testDatabaseName", "testTableName")

        # assert
        self.assertEqual(self._validator._spark.sql.call_count, 1)

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]

        self.assertIn(f"""FROM Application_MEF_Metadata.{self._validator._validationConfigurationTable}""", sqlCommand)
        self.assertIn("testDatabaseName", sqlCommand)
        self.assertIn(f"""AND {IS_ACTIVE_RECORD_COLUMN_NAME} AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE""", sqlCommand)

        self.assertEqual(
            dqConfigurationDF.rdd.collect(),
            expectedDQConfigurationDF.rdd.collect()
        )

    def test_getDQConfiguration_noMatchingRules(self):
        # arrange
        wholeDQConfigurationDF = self._spark.createDataFrame([
            [1, "expect_column_values_to_be_null", "KwArgs1", False, "Description1", "DQLogOutput1", "testTableName", 2],
            [2, "expect_column_value_lengths_to_equal", "KwArgs2", False, "Description2", "DQLogOutput2", "test[*]Name", 2],
            [3, "expect_table_columns_to_match_set", "KwArgs3", False, "Description3", "DQLogOutput3", "test[Table, Something]Name", 1],
            [4, "expect_table_columns_to_match_ordered_list", "KwArgs4", True, "Description3", "DQLogOutput3", "Test[table, something]Name", 1],
            [5, "expect_column_values_to_be_of_type", "KwArgs5", False, "Description4", "DQLogOutput4", "test[*]Name", 1],
            [6, "expect_column_values_to_be_null", "KwArgs6", False, "Description6", "DQLogOutput6", "another[Table, Something]Name", 2],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING, EntityName STRING, OrderPrioritized INTEGER")

        self._validator._spark.sql.return_value = wholeDQConfigurationDF
        self._validator._spark.createDataFrame = self._spark.createDataFrame

        # act
        dqConfigurationDF = self._validator._getDQConfiguration("testDatabaseName", "noMatchingTableName")

        # assert
        self.assertEqual(self._validator._spark.sql.call_count, 1)

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]

        self.assertIn(f"""FROM Application_MEF_Metadata.{self._validator._validationConfigurationTable}""", sqlCommand)
        self.assertIn("testDatabaseName", sqlCommand)
        self.assertIn(f"""AND {IS_ACTIVE_RECORD_COLUMN_NAME} AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE""", sqlCommand)

        self.assertEqual(dqConfigurationDF.count(), 0)

    
    # hasToBeValidated tests

    def test_hasToBeValidated_dqConfigurationNotDefined(self):
        # arrange
        self._validator._isDQConfigurationDefined = MagicMock(return_value=False)

        # act
        result = self._validator.hasToBeValidated("testDatabaseName", "testTableName")

        # assert
        self.assertEqual(result, False)

    def test_hasToBeValidated_noDQConfigurationForTable(self):
        # arrange
        self._validator._isDQConfigurationDefined = MagicMock(return_value=True)

        tableDQConfigurationDF = self._spark.createDataFrame(
            self._spark.sparkContext.emptyRDD(),
            "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING"
        )

        self._validator._getDQConfiguration = MagicMock(return_value=tableDQConfigurationDF)

        # act
        result = self._validator.hasToBeValidated("testDatabaseName", "testTableName")

        # assert
        self.assertEqual(result, False)

    def test_hasToBeValidated_dqConfigurationForTable(self):
        # arrange
        self._validator._isDQConfigurationDefined = MagicMock(return_value=True)

        tableDQConfigurationDF = self._spark.createDataFrame([
            [1, "expect_table_columns_to_match_set", "KwArgs1", False, "Description1", "DQLogOutput1"],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING")

        self._validator._getDQConfiguration = MagicMock(return_value=tableDQConfigurationDF)

        # act
        result = self._validator.hasToBeValidated("testDatabaseName", "testTableName")

        # assert
        self.assertEqual(result, True)

    
    # getDQReferenceValues tests

    def test_getDQReferenceValues_matchingKey(self):
        # arrange
        self._validator._spark.sql.return_value = self._spark.createDataFrame([
            ["value 1"],
            ["value 2"],
            ["value 3"],
        ], "Value STRING")

        expectedResult = ["value 1", "value 2", "value 3"]

        # act
        result = self._validator._getDQReferenceValues("#testKey")

        # assert
        self.assertEqual(result, expectedResult)

        self.assertEqual(self._validator._spark.sql.call_count, 1)

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]

        self.assertIn(f"""FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._validator.METADATA_DQ_REFERENCE_VALUES_TABLE}""", sqlCommand)
        self.assertIn("lower(ReferenceKey) = lower('testKey')", sqlCommand)
        self.assertIn(f"""AND {IS_ACTIVE_RECORD_COLUMN_NAME}""", sqlCommand)
        self.assertIn(f"""AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE""", sqlCommand)

    def test_getDQReferenceValues_noMatchingKey(self):
        # arrange
        self._validator._spark.sql.return_value = self._spark.createDataFrame([
        ], "Value STRING")

        expectedResult = "#testKey"

        # act
        result = self._validator._getDQReferenceValues("#testKey")

        # assert
        self.assertEqual(result, expectedResult)

        self.assertEqual(self._validator._spark.sql.call_count, 1)

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]

        self.assertIn(f"""FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._validator.METADATA_DQ_REFERENCE_VALUES_TABLE}""", sqlCommand)
        self.assertIn("lower(ReferenceKey) = lower('testKey')", sqlCommand)
        self.assertIn(f"""AND {IS_ACTIVE_RECORD_COLUMN_NAME}""", sqlCommand)
        self.assertIn(f"""AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE""", sqlCommand)

    
    # getDQReferencePairValues tests

    def test_getDQReferencePairValues_matchingKeys(self):
        # arrange
        self._validator._spark.sql.return_value = self._spark.createDataFrame([
            ["value 1", "value A"],
            ["value 2", "value A"],
            ["value 3", "value B"],
        ], "Value1 STRING, Value2 STRING")

        expectedResult = [
            ("value 1", "value A"),
            ("value 2", "value A"),
            ("value 3", "value B")
        ]

        # act
        result = self._validator._getDQReferencePairValues("#testKey1#testKey2")

        # assert
        self.assertEqual(result, expectedResult)

        self.assertEqual(self._validator._spark.sql.call_count, 1)

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]

        self.assertIn(f"""FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._validator.METADATA_DQ_REFERENCE_PAIR_VALUES_TABLE}""", sqlCommand)
        self.assertIn("lower(ReferenceKey1) = lower('testKey1')", sqlCommand)
        self.assertIn("lower(ReferenceKey2) = lower('testKey2')", sqlCommand)
        self.assertIn("UNION ALL", sqlCommand)
        self.assertIn("lower(ReferenceKey2) = lower('testKey1')", sqlCommand)
        self.assertIn("lower(ReferenceKey1) = lower('testKey2')", sqlCommand)
        self.assertIn(f"""AND {IS_ACTIVE_RECORD_COLUMN_NAME}""", sqlCommand)
        self.assertIn(f"""AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE""", sqlCommand)

    def test_getDQReferencePairValues_noMatchingKeys(self):
        # arrange
        self._validator._spark.sql.return_value = self._spark.createDataFrame([
        ], "Value1 STRING, Value2 STRING")

        expectedResult = "#testKey1#testKey2"

        # act
        result = self._validator._getDQReferencePairValues("#testKey1#testKey2")

        # assert
        self.assertEqual(result, expectedResult)

        self.assertEqual(self._validator._spark.sql.call_count, 1)

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]

        self.assertIn(f"""FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._validator.METADATA_DQ_REFERENCE_PAIR_VALUES_TABLE}""", sqlCommand)
        self.assertIn("lower(ReferenceKey1) = lower('testKey1')", sqlCommand)
        self.assertIn("lower(ReferenceKey2) = lower('testKey2')", sqlCommand)
        self.assertIn("UNION ALL", sqlCommand)
        self.assertIn("lower(ReferenceKey2) = lower('testKey1')", sqlCommand)
        self.assertIn("lower(ReferenceKey1) = lower('testKey2')", sqlCommand)
        self.assertIn(f"""AND {IS_ACTIVE_RECORD_COLUMN_NAME}""", sqlCommand)
        self.assertIn(f"""AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE""", sqlCommand)

    
    # getExpectationKwArgs tests

    def test_getExpectationKwArgs_valueSetWithHashtag(self):
        # arrange
        kwArgsString = """
            {
                "column": "AddressType",
                "value_set": "#AddressType"
            }
        """

        self._validator._getDQReferenceValues = MagicMock(return_value=["value 1", "value 2", "value 3"])

        expectedResult = {
            "column": "AddressType",
            "value_set": ["value 1", "value 2", "value 3"]
        }

        # act
        result = self._validator._getExpectationKwArgs(kwArgsString)

        # assert
        self.assertEqual(result, expectedResult)

        self._validator._getDQReferenceValues.assert_called_once_with("#AddressType")

    def test_getExpectationKwArgs_valueSetWithoutHashtag(self):
        # arrange
        kwArgsString = """
            {
                "column": "AddressType",
                "value_set": [0, 1]
            }
        """

        self._validator._getDQReferenceValues = MagicMock()

        expectedResult = {
            "column": "AddressType",
            "value_set": [0, 1]
        }

        # act
        result = self._validator._getExpectationKwArgs(kwArgsString)

        # assert
        self.assertEqual(result, expectedResult)

        self._validator._getDQReferenceValues.assert_not_called()

    def test_getExpectationKwArgs_valuePairsSetWithHashtag(self):
        # arrange
        kwArgsString = """
            {
                "column_A": "RevisionNumber",
                "column_B": "Status",
                "value_pairs_set": "#RevisionNumber#Status"
            }
        """

        self._validator._getDQReferencePairValues = MagicMock(return_value=[(2,5), (3,4)])

        expectedResult = {
            "column_A": "RevisionNumber",
            "column_B": "Status",
            "value_pairs_set": [(2,5), (3,4)]
        }

        # act
        result = self._validator._getExpectationKwArgs(kwArgsString)

        # assert
        self.assertEqual(result, expectedResult)

        self._validator._getDQReferencePairValues.assert_called_once_with("#RevisionNumber#Status")

    def test_getExpectationKwArgs_valuePairsSetAsArrayOfArrays(self):
        # arrange
        kwArgsString = """
            {
                "column_A": "RevisionNumber",
                "column_B": "Status",
                "value_pairs_set": [[2,5], [3,4]]
            }
        """

        self._validator._getDQReferencePairValues = MagicMock()

        expectedResult = {
            "column_A": "RevisionNumber",
            "column_B": "Status",
            "value_pairs_set": [(2,5), (3,4)]
        }

        # act
        result = self._validator._getExpectationKwArgs(kwArgsString)

        # assert
        self.assertEqual(result, expectedResult)

        self._validator._getDQReferencePairValues.assert_not_called()

    def test_getExpectationKwArgs_conditionWithHashtag(self):
        # arrange
        kwArgsString = """
            {
                "condition": "AddressType NOT IN (#AddressType)"
            }
        """

        self._validator._getDQReferenceValues = MagicMock(return_value=["value 1", "value 2", "value 3"])

        expectedResult = {
            "condition": "AddressType NOT IN ('value 1', 'value 2', 'value 3')"
        }

        # act
        result = self._validator._getExpectationKwArgs(kwArgsString)

        # assert
        self.assertEqual(result, expectedResult)

        self._validator._getDQReferenceValues.assert_called_once_with("#AddressType")

    def test_getExpectationKwArgs_conditionWithMultipleHashtags(self):
        # arrange
        kwArgsString = """
            {
                "condition": "AddressType NOT IN (#AddressType) AND Country NOT IN (#Country)"
            }
        """

        def getDQReferenceValues(referenceKey: str) -> ArrayType:
            if referenceKey == "#AddressType":
                return ["value 1", "value 2", "value 3"]
            elif referenceKey == "#Country":
                return ["value A", "value B"]

        self._validator._getDQReferenceValues = MagicMock(side_effect=getDQReferenceValues)

        expectedResult = {
            "condition": "AddressType NOT IN ('value 1', 'value 2', 'value 3') AND Country NOT IN ('value A', 'value B')"
        }

        # act
        result = self._validator._getExpectationKwArgs(kwArgsString)

        # assert
        self.assertEqual(result, expectedResult)

        self._validator._getDQReferenceValues.assert_any_call("#AddressType")
        self._validator._getDQReferenceValues.assert_any_call("#Country")

    def test_getExpectationKwArgs_conditionWithoutHashtag(self):
        # arrange
        kwArgsString = """
            {
                "condition": "AddressType NOT IN ('value 1', 'value 2', 'value 3')"
            }
        """

        self._validator._getDQReferenceValues = MagicMock()

        expectedResult = {
            "condition": "AddressType NOT IN ('value 1', 'value 2', 'value 3')"
        }

        # act
        result = self._validator._getExpectationKwArgs(kwArgsString)

        # assert
        self.assertEqual(result, expectedResult)

        self._validator._getDQReferenceValues.assert_not_called()

    
    # getSuiteConfig tests

    def test_getSuiteConfig(self):
        # arrange
        dqConfigurationDF = self._spark.createDataFrame([
            [1, "expect_table_columns_to_match_set", '{"column_set": ["col1", "col2"]}', False, "Description1", "DQLogOutput1"],
            [2, "expect_table_columns_to_match_ordered_list", '{"column_list": ["col1", "col2"]}', True, "Description2", "DQLogOutput2"],
            [3, "expect_column_values_to_not_match_condition", '{"condition": "col1 > 100"}', False, "Description3", "DQLogOutput3"],
            [4, "expect_column_values_to_be_null", '{"column": "col1"}', False, "Description4", "DQLogOutput4"],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING")

        self._validator._getDQConfiguration = MagicMock(return_value=dqConfigurationDF)

        expectedGeSuiteConfig = {
            "expectation_suite_name": "greatExpectationsSuite",
            "expectations": [
                {
                    "expectation_type": "expect_table_columns_to_match_set",
                    "kwargs": {
                        "column_set": ["col1", "col2"]
                    },
                    "meta": {
                        "WK_ID": 1,
                        "Quarantine": False,
                        "Description": "Description1",
                        "DQLogOutput": "DQLogOutput1",
                    }
                },
                {
                    "expectation_type": "expect_table_columns_to_match_ordered_list",
                    "kwargs": {
                        "column_list": ["col1", "col2"]
                    },
                    "meta": {
                        "WK_ID": 2,
                        "Quarantine": True,
                        "Description": "Description2",
                        "DQLogOutput": "DQLogOutput2",
                    }
                },
                {
                    "expectation_type": "expect_column_values_to_be_null",
                    "kwargs": {
                        "column": "col1"
                    },
                    "meta": {
                        "WK_ID": 4,
                        "Quarantine": False,
                        "Description": "Description4",
                        "DQLogOutput": "DQLogOutput4",
                    }
                }
            ]
        }

        expectedCustomSuiteConfig = {
            "expectation_suite_name": "customRulesSuite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_match_condition",
                    "kwargs": {
                        "condition": "col1 > 100"
                    },
                    "meta": {
                        "WK_ID": 3,
                        "Quarantine": False,
                        "Description": "Description3",
                        "DQLogOutput": "DQLogOutput3",
                    }
                }
            ]
        }

        # act
        (geSuiteConfig, customSuiteConfig) = self._validator._getSuiteConfig("testDatabaseName", "testTableName")

        # assert
        self.assertEqual(geSuiteConfig, expectedGeSuiteConfig)
        self.assertEqual(customSuiteConfig, expectedCustomSuiteConfig)

        self._validator._getDQConfiguration.assert_called_once_with("testDatabaseName", "testTableName")


    # findJoinTableColumns tests

    def test_findJoinTableColumns_matchesFound(self):
        # arrange
        dqLogOutputColumns = "CustomerID, Application_MEF_SDM.SalesOrderHeader.SalesOrderID, Application_MEF_SDM.SalesOrderHeader.SubTotal"
        joinTable = "Application_MEF_SDM.SalesOrderHeader"

        expectedJoinTableColumns = ["SalesOrderID", "SubTotal"]

        # act
        joinTableColumns = self._validator._findJoinTableColumns(dqLogOutputColumns, joinTable)

        # assert
        self.assertEqual(joinTableColumns, expectedJoinTableColumns)

    def test_findJoinTableColumns_noMatchesFound(self):
        # arrange
        dqLogOutputColumns = "CustomerID"
        joinTable = "Application_MEF_SDM.SalesOrderHeader"

        expectedJoinTableColumns = []

        # act
        joinTableColumns = self._validator._findJoinTableColumns(dqLogOutputColumns, joinTable)

        # assert
        self.assertEqual(joinTableColumns, expectedJoinTableColumns)


    # getAdditionalColumns tests

    def test_getAdditionalColumns_shouldAcceptColumnPresentInTheGroupByConditionAndIgnoreSalesOrderIDAsItsNotPresent(self):
        # arrange
        dqLogOutputColumns = "CustomerID, Application_MEF_SDM.SalesOrderHeader.ShipMethod, Application_MEF_SDM.SalesOrderHeader.SalesOrderID"
        joinTable = "Application_MEF_SDM.SalesOrderHeader"
        condition = "GROUP BY table.CustomerID, Application_MEF_SDM.SalesOrderHeader.ShipMethod HAVING SUM(Application_MEF_SDM.SalesOrderHeader.SubTotal) > 70000"

        expectedAdditionalColumns = ", Application_MEF_SDM.SalesOrderHeader.ShipMethod as joinTable_ShipMethod"

        # act
        additionalColumns = self._validator._getAdditionalColumns(dqLogOutputColumns, joinTable, condition)

        # assert
        self.assertEqual(additionalColumns, expectedAdditionalColumns)

    def test_getAdditionalColumns_shouldIgnoreAllColumnsAsTheyAreNotPresentInTheGroupByCondition(self):
        # arrange
        dqLogOutputColumns = "CustomerID, Application_MEF_SDM.SalesOrderHeader.ShipMethod, Application_MEF_SDM.SalesOrderHeader.SalesOrderID"
        joinTable = "Application_MEF_SDM.SalesOrderHeader"
        condition = "GROUP BY table.CustomerID, Application_MEF_SDM.SalesOrderHeader.Total HAVING count(*) > 1"

        expectedAdditionalColumns = ""

        # act
        additionalColumns = self._validator._getAdditionalColumns(dqLogOutputColumns, joinTable, condition)

        # assert
        self.assertEqual(additionalColumns, expectedAdditionalColumns)

    def test_getAdditionalColumns_shouldAcceptAllColumnsAsGroupByIsNotPresentInCondition(self):
        # arrange
        dqLogOutputColumns = "CustomerID, Application_MEF_SDM.SalesOrderHeader.ShipMethod, Application_MEF_SDM.SalesOrderHeader.SalesOrderID"
        joinTable = "Application_MEF_SDM.SalesOrderHeader"
        condition = "Application_MEF_SDM.SalesOrderHeader.SubTotal > 70000"

        expectedAdditionalColumns = ", Application_MEF_SDM.SalesOrderHeader.ShipMethod as joinTable_ShipMethod, Application_MEF_SDM.SalesOrderHeader.SalesOrderID as joinTable_SalesOrderID"
        expectedAdditionalColumns = f"{expectedAdditionalColumns}, {self._validator.MGT_TEMP_ID_COLUMN_NAME}"

        # act
        additionalColumns = self._validator._getAdditionalColumns(dqLogOutputColumns, joinTable, condition)

        # assert
        self.assertEqual(additionalColumns, expectedAdditionalColumns)


    # getAdditionalData tests

    def test_getAdditionalData_groupByInCondition(self):
        # arrange
        invalidDF = self._spark.createDataFrame([
            [200, "Method A", "2024-02-05"],
            [400, "Method B", "2024-02-24"],
        ], "CustomerID INTEGER, joinTable_ShipMethod STRING, joinTable_OrderDate STRING")

        dataDF = self._spark.createDataFrame([
            [100, "A", "aa", 1],
            [200, "B", "bb", 2],
            [300, "C", "cc", 3],
            [400, "D", "dd", 4],
            [500, "E", "ee", 5],
        ], "CustomerID INTEGER, Col1 STRING, Col2 STRING, MGT_TEMP_id INTEGER")

        condition = "GROUP BY table.CustomerID, Application_MEF_SDM.SalesOrderHeader.ShipMethod, Application_MEF_SDM.SalesOrderHeader.OrderDate HAVING SUM(Application_MEF_SDM.SalesOrderHeader.SubTotal) > 70000"
        column = "CustomerID"

        expectedAdditionalData = [
            {
                "joinTable_ShipMethod": "Method A",
                "joinTable_OrderDate": "2024-02-05",
                "MGT_TEMP_id": 2
            }, 
            {
                "joinTable_ShipMethod": "Method B",
                "joinTable_OrderDate": "2024-02-24",
                "MGT_TEMP_id": 4
            }
        ]

        # act
        additionalData = self._validator._getAdditionalData(invalidDF, dataDF, condition, column)

        # assert
        self.assertEqual(additionalData, expectedAdditionalData)

    def test_getAdditionalData_groupByNotInCondition(self):
        # arrange
        invalidDF = self._spark.createDataFrame([
            [200, "Method A", "2024-02-05", 2],
            [400, "Method B", "2024-02-24", 4],
        ], "CustomerID INTEGER, joinTable_ShipMethod STRING, joinTable_OrderDate STRING, MGT_TEMP_id INTEGER")

        dataDF = self._spark.createDataFrame([
            [100, "A", "aa", 1],
            [200, "B", "bb", 2],
            [300, "C", "cc", 3],
            [400, "D", "dd", 4],
            [500, "E", "ee", 5],
        ], "CustomerID INTEGER, Col1 STRING, Col2 STRING, MGT_TEMP_id INTEGER")

        condition = "table.CustomerID > 0"
        column = "CustomerID"

        expectedAdditionalData = [
            {
                "joinTable_ShipMethod": "Method A",
                "joinTable_OrderDate": "2024-02-05",
                "MGT_TEMP_id": 2
            }, 
            {
                "joinTable_ShipMethod": "Method B",
                "joinTable_OrderDate": "2024-02-24",
                "MGT_TEMP_id": 4
            }
        ]

        # act
        additionalData = self._validator._getAdditionalData(invalidDF, dataDF, condition, column)

        # assert
        self.assertEqual(additionalData, expectedAdditionalData)


    # getDqLogOutputColumns tests

    def test_getDqLogOutputColumns_ifJoinTableNotPresent(self):
        # arrange
        invalidInputDF = self._spark.createDataFrame([
                [200, "CompanyName A", "2024-02-05", 2],
                [400, "CompanyName B", "2024-02-24", 4],
            ], "CustomerID INTEGER, CompanyName STRING, OrderDate STRING, MGT_TEMP_id INTEGER")
            
        result = MagicMock()
        result.expectation_config = MagicMock()
        result.expectation_config.kwargs = {
            "column_A": "col1",
            "column_B": "col2"
        }
        result.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "CustomerID,CompanyName,OrderDate"
        }

        expectedDqLogOutputColumns = ["CustomerID", "CompanyName", "OrderDate"]

        # act
        dqLogOutputColumns, invalidDF = self._validator._getDqLogOutputColumns(result, invalidInputDF)

        # assert
        self.assertEqual(dqLogOutputColumns, expectedDqLogOutputColumns)
        self.assertEqual(invalidDF, invalidInputDF)

    def test_getDqLogOutputColumns_ifJoinTablePresent_dontLogColumnsFromJoinTable(self):
        # arrange
        invalidInputDF = self._spark.createDataFrame([
                [200, "CompanyName A", "2024-02-05", 2],
                [400, "CompanyName B", "2024-02-24", 4],
            ], "CustomerID INTEGER, CompanyName STRING, OrderDate STRING, MGT_TEMP_id INTEGER")
            
        result = MagicMock()
        result.expectation_config = MagicMock()
        result.expectation_config.kwargs = {
            "column": "CustomerID",
            "joinTable": "Application_MEF_SDM.SalesOrderHeader",
            "joinType": "LEFT",
            "joinColumns": "CustomerID,CustomerID"
        }
        result.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "CustomerID,CompanyName,OrderDate"
        }

        expectedDqLogOutputColumns = ["CustomerID", "CompanyName", "OrderDate"]

        # act
        dqLogOutputColumns, invalidDF = self._validator._getDqLogOutputColumns(result, invalidInputDF)

        # assert
        self.assertEqual(dqLogOutputColumns, expectedDqLogOutputColumns)
        self.assertEqual(invalidDF, invalidInputDF)

    def test_getDqLogOutputColumns_ifJoinTablePresent_logColumnsFromJoinTable(self):
        # arrange
        invalidInputDF = self._spark.createDataFrame([
                [200, "CompanyName A", "2024-02-05", 2],
                [400, "CompanyName B", "2024-02-24", 4],
            ], "CustomerID INTEGER, CompanyName STRING, OrderDate STRING, MGT_TEMP_id INTEGER")
            
        result = MagicMock()
        result.expectation_config = MagicMock()
        result.expectation_config.kwargs = {
            "column": "CustomerID",
            "joinTable": "Application_MEF_SDM.SalesOrderHeader",
            "joinType": "LEFT",
            "joinColumns": "CustomerID,CustomerID"
        }
        result.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "CustomerID, Application_MEF_SDM.SalesOrderHeader.ShipMethod, Application_MEF_SDM.SalesOrderHeader.SalesOrderID"
        }
        result.result = {
            "additional_data": [
                {
                    "joinTable_ShipMethod": "Method A",
                    "joinTable_SalesOrderID": 10,
                    "MGT_TEMP_id": 2
                },
                {
                    "joinTable_ShipMethod": "Method B",
                    "joinTable_SalesOrderID": 11,
                    "MGT_TEMP_id": 4
                }
            ]
        }

        self._validator._spark.sql.return_value = self._spark.createDataFrame([
                ["Method A", 10, "value 1", 123],
            ], f"ShipMethod STRING, SalesOrderID INT, col1 STRING, col2 INT")

        self._validator._spark.createDataFrame = self._spark.createDataFrame

        expectedDqLogOutputColumns = ["CustomerID", "joinTable_ShipMethod", "joinTable_SalesOrderID"]

        expectedInvalidDF = self._spark.createDataFrame([
                [200, "CompanyName A", "2024-02-05", 2, "Method A", 10, 2],
                [400, "CompanyName B", "2024-02-24", 4, "Method B", 11, 4],
            ], f"CustomerID INTEGER, CompanyName STRING, OrderDate STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, joinTable_ShipMethod STRING, joinTable_SalesOrderID INT, joinTable_{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT")

        # act
        dqLogOutputColumns, invalidDF = self._validator._getDqLogOutputColumns(result, invalidInputDF)

        # assert
        self.assertEqual(dqLogOutputColumns, expectedDqLogOutputColumns)
        self.assertEqual(
            invalidDF.rdd.collect(),
            expectedInvalidDF.rdd.collect()
        )


    # getAdditionalDataCompareDataset tests

    def test_getAdditionalDataCompareDataset_shouldContainBothMissingAndUnexpectedRowsIfFullMatch(self):
        # arrange
        invalidDF =  self._spark.createDataFrame([
                [None, 5],
                [6, None],
            ], "ContractNumber INTEGER, joinTable_ContractNumber INTEGER")

        comparedColumn = ['ContractNumber,ContractNumber']
        expectationMatch = "fullMatch"

        expectedAdditionalData = [
            {
                "missingRows": [["ContractNumber", "joinTable_ContractNumber"],[None, 5]], 
                "unexpectedRows": [["ContractNumber", "joinTable_ContractNumber"], [6, None]]
            }
        ]
        
        # act
        additionalData = self._validator._getAdditionalDataCompareDataset(invalidDF, comparedColumn, expectationMatch)

        # assert
        self.assertEqual(additionalData, expectedAdditionalData)


    def test_getAdditionalDataCompareDataset_shouldContainMissingRowsOnlyIfNoMissing(self):
        # arrange
        invalidDF =  self._spark.createDataFrame([
                [None, 5],
                [6, None],
            ], "ContractNumber INTEGER, joinTable_ContractNumber INTEGER")

        comparedColumn = ['ContractNumber,ContractNumber']
        expectationMatch = "noMissing"

        expectedAdditionalData = [
            {
                "missingRows": [["ContractNumber", "joinTable_ContractNumber"],[None, 5]]
            }
        ]
        
        # act
        additionalData = self._validator._getAdditionalDataCompareDataset(invalidDF, comparedColumn, expectationMatch)

        # assert
        self.assertEqual(additionalData, expectedAdditionalData)


    def test_getAdditionalDataCompareDataset_shouldContainMissingRowsOnlyIfNoAddition(self):
        # arrange
        invalidDF =  self._spark.createDataFrame([
                [None, 5],
                [6, None],
            ], "ContractNumber INTEGER, joinTable_ContractNumber INTEGER")

        comparedColumn = ['ContractNumber,ContractNumber']
        expectationMatch = "noAddition"

        expectedAdditionalData = [
            {
                "unexpectedRows": [["ContractNumber", "joinTable_ContractNumber"],[6, None]]
            }
        ]
        
        # act
        additionalData = self._validator._getAdditionalDataCompareDataset(invalidDF, comparedColumn, expectationMatch)

        # assert
        self.assertEqual(additionalData, expectedAdditionalData)

     
    # getInvalidDFCompareDataset tests

    def test_getInvalidDFCompareDataset_with2DifferentDatasets(self):
        #arrange
        joinTableDF = self._spark.createDataFrame([
            ["PTL", 1, datetime(2024, 1, 1), "Active"],
            ["PTL", 4, datetime(2024, 1, 1), "Active"],
            ["PTL", 5, datetime(2024, 1, 1), "Active"],
            ["GER", 8, datetime(2024, 2, 1), "Active"],
        ], "Market STRING, ContractNumber INT, ReportDate Timestamp, ContractStatus STRING")

        tableDF = self._spark.createDataFrame([
            ["PTL", 1, datetime(2024, 2, 1), "Active"],
            ["PTL", 6, datetime(2024, 2, 1), "Active"],
            ["PTL", 7, datetime(2024, 2, 1), "Active"],
            ["IND", 1, datetime(2024, 2, 1), "Active"] 
        ], "Market STRING, ContractNumber INT, ReportDate Timestamp, ContractStatus STRING")

        self._validator._spark.sql = self._spark.sql

        joinTableDF.createOrReplaceTempView("joinTable")
        tableDF.createOrReplaceTempView("table")

        columns = "table.ContractNumber,joinTable.ContractNumber as joinTable_ContractNumber,joinTable.Market as joinTable_Market"
        joinTable = "joinTable"
        condition = "table.Market = joinTable.Market and DATEADD(month,-1,table.ReportDate) = joinTable.ReportDate"
        joinOnColumns = "table.ContractNumber = joinTable.ContractNumber"
        nullCheckCondition = "table.ContractNumber IS NULL OR joinTable.ContractNumber IS NULL"

        expectedInvalidDF = self._spark.createDataFrame([
            [None, 4, "PTL"],
            [None, 5, "PTL"],
            [6, None,  None],
            [7, None,  None],
        ], "ContractNumber INT, joinTable_ContractNumber INT, joinTable_Market STRING")
        
        #act
        invalidDF = self._validator._getInvalidDFCompareDataset(columns, joinTable, condition, joinOnColumns, nullCheckCondition)

        #assert
        self.assertEqual(invalidDF.sort("ContractNumber").rdd.collect(), expectedInvalidDF.sort("ContractNumber").rdd.collect())

    def test_getInvalidDFCompareDataset_withSameDatasets(self):
        #arrange
        joinTableDF = self._spark.createDataFrame([
            ["PTL", 1, datetime(2024, 1, 1), "Active"],
            ["PTL", 2, datetime(2024, 1, 1), "Active"],
            ["PTL", 3, datetime(2024, 1, 1), "Active"],
            ["GER", 4, datetime(2024, 1, 1), "Active"],
        ], "Market STRING, ContractNumber INT, ReportDate Timestamp, ContractStatus STRING")

        tableDF = self._spark.createDataFrame([
            ["PTL", 1, datetime(2024, 2, 1), "Active"],
            ["PTL", 2, datetime(2024, 2, 1), "Active"],
            ["PTL", 3, datetime(2024, 2, 1), "Active"],
            ["GER", 4, datetime(2024, 2, 1), "Active"] 
        ], "Market STRING, ContractNumber INT, ReportDate Timestamp, ContractStatus STRING")

        self._validator._spark.sql = self._spark.sql

        joinTableDF.createOrReplaceTempView("joinTable")
        tableDF.createOrReplaceTempView("table")

        columns = "table.ContractNumber,joinTable.ContractNumber as joinTable_ContractNumber,joinTable.Market as joinTable_Market"
        joinTable = "joinTable"
        condition = "table.Market = joinTable.Market and DATEADD(month,-1,table.ReportDate) = joinTable.ReportDate"
        joinOnColumns = "table.ContractNumber = joinTable.ContractNumber"
        nullCheckCondition = "table.ContractNumber IS NULL OR joinTable.ContractNumber IS NULL"

        expectedInvalidDF = self._spark.createDataFrame([], StructType([]))
        
        #act
        invalidDF = self._validator._getInvalidDFCompareDataset(columns, joinTable, condition, joinOnColumns, nullCheckCondition)

        #assert
        self.assertEqual(invalidDF.rdd.collect(), expectedInvalidDF.rdd.collect())

    def test_getInvalidDFCompareDataset_withMultipleScenarios(self):
        #arrange
        joinTableDF = self._spark.createDataFrame([
            ["PTL", 1, datetime(2024, 1, 1), "Active"],
            ["PTL", 2, datetime(2024, 1, 1), "Active"],
            ["PTL", 3, datetime(2024, 1, 1), "Active"],
            ["PTL", 3, datetime(2024, 1, 1), "Active"],
            ["PTL", 4, datetime(2024, 1, 1), "Active"],
            [None, 5, datetime(2024, 2, 1), "Active"],
        ], "Market STRING, ContractNumber INT, ReportDate Timestamp, ContractStatus STRING")

        tableDF = self._spark.createDataFrame([
            ["PTL", 1, datetime(2024, 2, 1), "Active"],
            ["PTL", 2, datetime(2024, 2, 1), "Active"],
            ["PTL", 4, datetime(2024, 2, 1), "Active"],
            ["PTL", 4, datetime(2024, 2, 1), "Active"],
            ["PTL", 5, datetime(2024, 2, 1), "Active"],
            ["PTL", 5, datetime(2024, 2, 1), "Active"]
        ], "Market STRING, ContractNumber INT, ReportDate Timestamp, ContractStatus STRING")

        self._validator._spark.sql = self._spark.sql

        joinTableDF.createOrReplaceTempView("joinTable")
        tableDF.createOrReplaceTempView("table")

        columns = "table.ContractNumber,joinTable.ContractNumber as joinTable_ContractNumber,joinTable.Market as joinTable_Market"
        joinTable = "joinTable"
        condition = "table.Market = joinTable.Market and DATEADD(month,-1,table.ReportDate) = joinTable.ReportDate"
        joinOnColumns = "table.ContractNumber = joinTable.ContractNumber"
        nullCheckCondition = "table.ContractNumber IS NULL OR joinTable.ContractNumber IS NULL"

        expectedInvalidDF = self._spark.createDataFrame([
            [None, 3, "PTL"],
            [None, 3, "PTL"],
            [5, None,  None],
            [5, None,  None],
        ], "ContractNumber INT, joinTable_ContractNumber INT, joinTable_Market STRING")
        
        #act
        invalidDF = self._validator._getInvalidDFCompareDataset(columns, joinTable, condition, joinOnColumns, nullCheckCondition)

        #assert
        self.assertEqual(invalidDF.sort("ContractNumber").rdd.collect(), expectedInvalidDF.sort("ContractNumber").rdd.collect())


    # processCompareDatasetRule tests

    def test_processCompareDatasetRule_successfulExpectation(self):
        # arrange
        expectation = {
            "kwargs": {
                "compareAgainst": "Application_MEF_History.ContractHistory",
                "comparedColumn": "ContractNumber,ContractNumber",
                "expectation": "fullMatch",
                "condition": "condition"
            },
            "meta": {
                "DQLogOutput": "ContractNumber, Application_MEF_History.ContractHistory.Col1"
            }
        }

        joinTableColumns = ["Col1"]
        self._validator._findJoinTableColumns = MagicMock(return_value = joinTableColumns)

        invalidDF = self._spark.createDataFrame([], StructType([]))
        self._validator._getInvalidDFCompareDataset = MagicMock(return_value = invalidDF)

        self._validator._getAdditionalDataCompareDataset = MagicMock()
        expectedUnexpectedList = None
        expectedAdditionalData = None

        # act
        unexpectedList, additionalData = self._validator._processCompareDatasetRule(expectation)

        # assert
        self._validator._findJoinTableColumns.assert_called_with('Application_MEF_History.ContractHistory.Col1', 'Application_MEF_History.ContractHistory')
        self.assertEqual(self._validator._findJoinTableColumns.call_count, 2)

        self._validator._getInvalidDFCompareDataset.assert_called_once_with(
            "table.ContractNumber,joinTable.ContractNumber as joinTable_ContractNumber,joinTable.Col1 as joinTable_Col1",
            "Application_MEF_History.ContractHistory",
            "condition",
            "table.ContractNumber = joinTable.ContractNumber",
            "table.ContractNumber IS NULL OR joinTable.ContractNumber IS NULL"
        )
        self._validator._getAdditionalDataCompareDataset.assert_not_called()

        self.assertEqual(unexpectedList, expectedUnexpectedList)
        self.assertEqual(additionalData, expectedAdditionalData)
    
    def test_processCompareDatasetRule_emptyJoinTable(self):
        # arrange
        expectation = {
            "kwargs": {
                "compareAgainst": "Application_MEF_History.ContractHistory",
                "comparedColumn": "ContractNumber,ContractNumber",
                "expectation": "fullMatch",
                "condition": "condition"
            },
            "meta": {
                "DQLogOutput": "ContractNumber, Application_MEF_History.ContractHistory.Col1"
            }
        }

        joinTableColumns = ["Col1"]
        self._validator._findJoinTableColumns = MagicMock(return_value = joinTableColumns)
        df = self._spark.createDataFrame([[0]], "rowCount INT")
        self._validator._spark.sql = MagicMock(return_value=df)

        self._validator._getInvalidDFCompareDataset = MagicMock()
        self._validator._getAdditionalDataCompareDataset = MagicMock()
        expectedUnexpectedList = None
        expectedAdditionalData = "There is no data existing in compared table that matches the condition"

        # act
        unexpectedList, additionalData = self._validator._processCompareDatasetRule(expectation)

        # assert
        self._validator._findJoinTableColumns.assert_called_with('Application_MEF_History.ContractHistory.Col1', 'Application_MEF_History.ContractHistory')
        self.assertEqual(self._validator._findJoinTableColumns.call_count, 2)

        self._validator._getInvalidDFCompareDataset.assert_not_called()
        self._validator._getAdditionalDataCompareDataset.assert_not_called()

        self.assertEqual(unexpectedList, expectedUnexpectedList)
        self.assertEqual(additionalData, expectedAdditionalData)

    def test_processCompareDatasetRule_unSuccessfulExpectation(self):
        # arrange
        expectation = {
            "kwargs": {
                "compareAgainst": "Application_MEF_History.ContractHistory",
                "comparedColumn": "ContractNumber,ContractNumber",
                "expectation": "fullMatch",
                "condition": "condition"
            },
            "meta": {
                "DQLogOutput": "ContractNumber, Application_MEF_History.ContractHistory.Col1"
            }
        }

        joinTableColumns = ["Col1"]
        self._validator._findJoinTableColumns = MagicMock(return_value = joinTableColumns)

        invalidDF =  self._spark.createDataFrame([
            [None, 5, 1],
            [6, None, 2],
        ], "ContractNumber INTEGER, joinTable_ContractNumber INTEGER, joinTable_Col1 INTEGER")
        self._validator._getInvalidDFCompareDataset = MagicMock(return_value = invalidDF)

        expectedUnexpectedList = None
        expectedAdditionalData = [
            {
                "missingRows": [["ContractNumber", "joinTable_ContractNumber", "joinTable_Col1"],[None, 5, 1]], 
                "unexpectedRows": [["ContractNumber", "joinTable_ContractNumber", "joinTable_Col1"], [6, None, 2]]
            }
        ]
        self._validator._getAdditionalDataCompareDataset = MagicMock(return_value=expectedAdditionalData)

        # act
        unexpectedList, additionalData = self._validator._processCompareDatasetRule(expectation)

        # assert
        self._validator._findJoinTableColumns.assert_called_with('Application_MEF_History.ContractHistory.Col1', 'Application_MEF_History.ContractHistory')
        self.assertEqual(self._validator._findJoinTableColumns.call_count, 2)

        self._validator._getInvalidDFCompareDataset.assert_called_once_with(
            "table.ContractNumber,joinTable.ContractNumber as joinTable_ContractNumber,joinTable.Col1 as joinTable_Col1",
            "Application_MEF_History.ContractHistory",
            "condition",
            "table.ContractNumber = joinTable.ContractNumber",
            "table.ContractNumber IS NULL OR joinTable.ContractNumber IS NULL"
        )

        self._validator._getAdditionalDataCompareDataset.assert_called_once_with(
            invalidDF,
            ['ContractNumber,ContractNumber'],
            "fullMatch"
        )

        self.assertEqual(unexpectedList, expectedUnexpectedList)
        self.assertEqual(additionalData, expectedAdditionalData)


    # processCustomExpectationRule tests

    def test_processCustomExpectationRule_withoutColumnParam(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        expectation = {
            "kwargs": {
                "condition": "col1 > 100"
            },
            "meta": {
                "WK_ID": 1,
                "Quarantine": False,
                "Description": "Description1",
                "DQLogOutput": "DQLogOutput1",
            }
        }

        self._validator._spark.sql.return_value = self._spark.createDataFrame([
            [2],
            [3],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT")

        # act
        unexpectedList, additionalData = self._validator._processCustomExpectationRule(expectation, dataDF)

        # assert
        self.assertEqual(unexpectedList, [2, 3])
        self.assertEqual(additionalData, [])
        self._validator._spark.sql.assert_called_once()

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"SELECT {self._validator.MGT_TEMP_ID_COLUMN_NAME}", sqlCommand)
        self.assertIn("FROM table", sqlCommand)
        self.assertIn("WHERE col1 > 100", sqlCommand)

    def test_processCustomExpectationRule_withColumnParam(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        expectation = {
            "kwargs": {
                "column": "col1",
                "condition": "col1 > 100"
            },
            "meta": {
                "WK_ID": 1,
                "Quarantine": False,
                "Description": "Description1",
                "DQLogOutput": "DQLogOutput1",
            }
        }

        self._validator._spark.sql.return_value = self._spark.createDataFrame([
            [120],
            [105],
        ], "col1 INT")
        
        # act
        unexpectedList, additionalData = self._validator._processCustomExpectationRule(expectation, dataDF)

        # assert
        self.assertEqual(unexpectedList, [120, 105])
        self.assertEqual(additionalData, [])
        self._validator._spark.sql.assert_called_once()

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"SELECT col1", sqlCommand)
        self.assertIn("FROM table", sqlCommand)
        self.assertIn("WHERE col1 > 100", sqlCommand)
    
    def test_processCustomExpectationRule_successfulExpectation(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        expectation = {
            "kwargs": {
                "column": "col1",
                "condition": "col1 > 200"
            },
            "meta": {
                "WK_ID": 1,
                "Quarantine": False,
                "Description": "Description1",
                "DQLogOutput": "DQLogOutput1",
            }
        }

        self._validator._spark.sql.return_value = self._spark.createDataFrame([], "col1 INT")
        
        # act
        unexpectedList, additionalData = self._validator._processCustomExpectationRule(expectation, dataDF)

        # assert
        self.assertEqual(unexpectedList, [])
        self.assertEqual(additionalData, [])
        self._validator._spark.sql.assert_called_once()

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"SELECT col1", sqlCommand)
        self.assertIn("FROM table", sqlCommand)
        self.assertIn("WHERE col1 > 200", sqlCommand)

    def test_processCustomExpectationRule_joinTableCondition_dontLogColumnsFromJoinedTable(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        expectation = {
            "kwargs": {
                "column": "col1",
                "joinTable": "Application_MEF_History.Customer",
                "joinType": "LEFT",
                "joinColumns": "col1,CustomerID",
                "condition": "Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1"
            },
            "meta": {
                "WK_ID": 1,
                "Quarantine": False,
                "Description": "Description1",
                "DQLogOutput": "DQLogOutput1",
            }        
        }

        self._validator._spark.sql.return_value = self._spark.createDataFrame([
            [100],
            [105]
        ], "col1 INT")
        
        # act
        unexpectedList, additionalData = self._validator._processCustomExpectationRule(expectation, dataDF)

        # assert
        self.assertEqual(unexpectedList, [100, 105])
        self.assertEqual(additionalData, [])
        self._validator._spark.sql.assert_called_once()

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"SELECT table.col1", sqlCommand)
        self.assertIn("FROM table", sqlCommand)
        self.assertIn("LEFT JOIN Application_MEF_History.Customer ON table.col1 = Application_MEF_History.Customer.CustomerID", sqlCommand)
        self.assertIn("WHERE Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1", sqlCommand)

    def test_processCustomExpectationRule_joinTableCondition_LogColumnsFromJoinedTable(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        expectation = {
            "kwargs": {
                "column": "col1",
                "joinTable": "Application_MEF_History.Customer",
                "joinType": "LEFT",
                "joinColumns": "col1,CustomerID",
                "condition": "Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1"
            },
            "meta": {
                "WK_ID": 1,
                "Quarantine": False,
                "Description": "Description1",
                "DQLogOutput": "DQLogOutput1, Application_MEF_History.Customer.CustomerID",
            }        
        }

        invalidDF = self._spark.createDataFrame([
                [100],
                [105]
            ], "col1 INT")

        self._validator._spark.sql.return_value = invalidDF

        additionalColumns = ", Application_MEF_History.Customer.CustomerID as joinTable_CustomerID"
        self._validator._getAdditionalColumns = MagicMock(return_value=additionalColumns)

        additionalData = MagicMock()
        self._validator._getAdditionalData = MagicMock(return_value=additionalData)
        
        # act
        unexpectedList, additionalData = self._validator._processCustomExpectationRule(expectation, dataDF)

        # assert
        self.assertEqual(unexpectedList, [100, 105])
        self.assertEqual(additionalData, additionalData)
        self._validator._spark.sql.assert_called_once()

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"SELECT table.col1{additionalColumns}", sqlCommand)
        self.assertIn("FROM table", sqlCommand)
        self.assertIn("LEFT JOIN Application_MEF_History.Customer ON table.col1 = Application_MEF_History.Customer.CustomerID", sqlCommand)
        self.assertIn("WHERE Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1", sqlCommand)

        self._validator._getAdditionalColumns.assert_called_once_with(
            "DQLogOutput1, Application_MEF_History.Customer.CustomerID",
            "Application_MEF_History.Customer",
            "Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1"
        )

        self._validator._getAdditionalData.assert_called_once_with(
            invalidDF,
            dataDF,
            "Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1",
            "col1"
        )

    def test_processCustomExpectationRule_joinTableOnMultipleColumns(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        expectation = {
            "kwargs": {
                "column": "col1",
                "joinTable": "Application_MEF_History.Customer",
                "joinType": "LEFT",
                "joinColumns": "col1,CustomerID| col2 , CustomerCol2",
                "condition": "Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1"
            },
            "meta": {
                "WK_ID": 1,
                "Quarantine": False,
                "Description": "Description1",
                "DQLogOutput": "DQLogOutput1",
            }        
        }

        invalidDF = self._spark.createDataFrame([
                [100],
                [105]
            ], "col1 INT")

        self._validator._spark.sql.return_value = invalidDF

        # act
        unexpectedList, additionalData = self._validator._processCustomExpectationRule(expectation, dataDF)

        # assert
        self.assertEqual(unexpectedList, [100, 105])
        self.assertEqual(additionalData, [])
        self._validator._spark.sql.assert_called_once()

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]
        self.assertIn("SELECT table.col1", sqlCommand)
        self.assertIn("FROM table", sqlCommand)
        self.assertIn("LEFT JOIN Application_MEF_History.Customer ON table.col1 = Application_MEF_History.Customer.CustomerID AND table.col2 = Application_MEF_History.Customer.CustomerCol2", sqlCommand)
        self.assertIn("WHERE Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1", sqlCommand)

    def test_processCustomExpectationRule_joinTableMissingWhenJoin(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        expectation = {
            "kwargs": {
                "column": "col1",
                "joinType": "LEFT",
                "joinColumns": "col1,CustomerID| col2 , CustomerCol2",
                "condition": "Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1"
            },
            "meta": {
                "WK_ID": 1,
                "Quarantine": False,
                "Description": "Description1",
                "DQLogOutput": "DQLogOutput1",
            }        
        }

        # act
        with self.assertRaises(NotImplementedError):
            self._validator._processCustomExpectationRule(expectation, dataDF)

        # assert
        self._validator._spark.sql.assert_not_called()

    def test_processCustomExpectationRule_joinColumnsMissingWhenJoin(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        expectation = {
            "kwargs": {
                "column": "col1",
                "joinTable": "Application_MEF_History.Customer",
                "joinType": "LEFT",
                "condition": "Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1"
            },
            "meta": {
                "WK_ID": 1,
                "Quarantine": False,
                "Description": "Description1",
                "DQLogOutput": "DQLogOutput1",
            }        
        }

        # act
        with self.assertRaises(NotImplementedError):
            self._validator._processCustomExpectationRule(expectation, dataDF)

        # assert
        self._validator._spark.sql.assert_not_called()


    # validateCustomRules tests

    def test_validateCustomRules_successfulCustomExpectation(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        dataDF.createOrReplaceTempView("table")

        customSuiteConfig = {
            "expectation_suite_name": "customRulesSuite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_match_condition",
                    "kwargs": {
                        "column": "col1",
                        "condition": "col1 > 200"
                    },
                    "meta": {
                        "WK_ID": 1,
                        "Quarantine": False,
                        "Description": "Description1",
                        "DQLogOutput": "DQLogOutput1",
                    }
                }
            ]
        }

        unexpectedList = []
        additionalData = []
        self._validator._processCustomExpectationRule = MagicMock(return_value=(unexpectedList, additionalData))
        
        geOutput = MagicMock()
        geOutput.success = True
        geOutput.results = []

        # act
        geOutput = self._validator._validateCustomRules(dataDF, customSuiteConfig, geOutput)

        # assert
        self.assertEqual(geOutput.success, True)
        self.assertEqual(len(geOutput.results), 1)

        self.assertEqual(geOutput.results[0].success, True)
        self.assertEqual(geOutput.results[0].result["unexpected_list"], [])
        self.assertEqual(geOutput.results[0].result["additional_data"], [])
        self.assertEqual(geOutput.results[0].expectation_config.expectation_type, "expect_column_values_to_not_match_condition")
        self.assertEqual(geOutput.results[0].expectation_config.kwargs, { "column": "col1", "condition": "col1 > 200" })
        self.assertEqual(geOutput.results[0].expectation_config.meta, {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "DQLogOutput1",
        })

        self._validator._processCustomExpectationRule.assert_called_once_with(
            {
                'expectation_type': 'expect_column_values_to_not_match_condition', 
                'kwargs': {'column': 'col1', 'condition': 'col1 > 200'}, 
                'meta': {'WK_ID': 1, 'Quarantine': False, 'Description': 'Description1', 'DQLogOutput': 'DQLogOutput1'}
            }, dataDF)
        
    def test_validateCustomRules_unSuccessfulCustomExpectation(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        dataDF.createOrReplaceTempView("table")

        customSuiteConfig = {
            "expectation_suite_name": "customRulesSuite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_match_condition",
                    "kwargs": {
                        "column": "col1",
                        "joinTable": "Application_MEF_History.Customer",
                        "joinType": "LEFT",
                        "joinColumns": "col1,CustomerID",
                        "condition": "Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1"
                    },
                    "meta": {
                        "WK_ID": 1,
                        "Quarantine": False,
                        "Description": "Description1",
                        "DQLogOutput": "DQLogOutput1, Application_MEF_History.Customer.CustomerID",
                    }
                }
            ]
        }

        unexpectedList = [100, 105]
        additionalData = MagicMock()
        self._validator._processCustomExpectationRule = MagicMock(return_value=(unexpectedList, additionalData))
        self._validator._processCompareDatasetRule = MagicMock()
        
        geOutput = MagicMock()
        geOutput.success = True
        geOutput.results = []

        # act
        geOutput = self._validator._validateCustomRules(dataDF, customSuiteConfig, geOutput)

        # assert
        self.assertEqual(geOutput.success, False)
        self.assertEqual(len(geOutput.results), 1)

        self.assertEqual(geOutput.results[0].success, False)
        self.assertEqual(geOutput.results[0].result["unexpected_list"], [100, 105])
        self.assertEqual(geOutput.results[0].result["additional_data"], additionalData)
        self.assertEqual(geOutput.results[0].expectation_config.expectation_type, "expect_column_values_to_not_match_condition")
        self.assertEqual(geOutput.results[0].expectation_config.kwargs, {
            "column": "col1",
            "joinTable": "Application_MEF_History.Customer",
            "joinType": "LEFT",
            "joinColumns": "col1,CustomerID",
            "condition": "Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1"
        })
        self.assertEqual(geOutput.results[0].expectation_config.meta, {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "DQLogOutput1, Application_MEF_History.Customer.CustomerID",
        })

        self._validator._processCustomExpectationRule.assert_called_once_with(
            {
                "expectation_type": "expect_column_values_to_not_match_condition",
                "kwargs": {
                    "column": "col1",
                    "joinTable": "Application_MEF_History.Customer",
                    "joinType": "LEFT",
                    "joinColumns": "col1,CustomerID",
                    "condition": "Application_MEF_History.Customer.CustomerID IS NULL GROUP BY table.col1"
                },
                "meta": {
                    "WK_ID": 1,
                    "Quarantine": False,
                    "Description": "Description1",
                    "DQLogOutput": "DQLogOutput1, Application_MEF_History.Customer.CustomerID",
                }
            }, dataDF)

        self._validator._processCompareDatasetRule.assert_not_called()
        
    def test_validateCustomRules_CompareDatasetRule_successfulExpectation(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        dataDF.createOrReplaceTempView("table")

        customSuiteConfig = {
            "expectation_suite_name": "customRulesSuite",
            "expectations": [
                {
                    "expectation_type": "compare_datasets",
                    "kwargs": {
                        "compareAgainst": "Application_MEF_History.ContractHistory",
                        "comparedColumn": "ContractNumber,ContractNumber",
                        "expectation": "fullMatch",
                        "condition": "condition"
                    },
                    "meta": {
                        "WK_ID": 1,
                        "Quarantine": False,
                        "Description": "Description1",
                        "DQLogOutput": "ContractNumber, Application_MEF_History.ContractHistory.Col1",
                    }
                }
            ]
        }

        unexpectedList = None
        additionalData = None
        self._validator._processCompareDatasetRule = MagicMock(return_value=(unexpectedList, additionalData))
        self._validator._processCustomExpectationRule = MagicMock()
        
        geOutput = MagicMock()
        geOutput.success = True
        geOutput.results = []

        # act
        geOutput = self._validator._validateCustomRules(dataDF, customSuiteConfig, geOutput)

        # assert
        self.assertEqual(geOutput.success, True)
        self.assertEqual(len(geOutput.results), 1)

        self.assertEqual(geOutput.results[0].success, True)
        self.assertNotIn('result', geOutput.results)
        self.assertEqual(geOutput.results[0].expectation_config.expectation_type, "compare_datasets")
        self.assertEqual(geOutput.results[0].expectation_config.kwargs, {
            "compareAgainst": "Application_MEF_History.ContractHistory",
            "comparedColumn": "ContractNumber,ContractNumber",
            "expectation": "fullMatch",
            "condition": "condition"
        })
        self.assertEqual(geOutput.results[0].expectation_config.meta, {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "ContractNumber, Application_MEF_History.ContractHistory.Col1",
        })

        self._validator._processCompareDatasetRule.assert_called_once_with(
            {
                'expectation_type': 'compare_datasets', 
                'kwargs': {
                    "compareAgainst": "Application_MEF_History.ContractHistory",
                    "comparedColumn": "ContractNumber,ContractNumber",
                    "expectation": "fullMatch",
                    "condition": "condition"
                }, 
                'meta': {
                    "WK_ID": 1,
                    "Quarantine": False,
                    "Description": "Description1",
                    "DQLogOutput": "ContractNumber, Application_MEF_History.ContractHistory.Col1",
                }
            }
            )

        self._validator._processCustomExpectationRule.assert_not_called()
    
    def test_validateCustomRules_CompareDatasetRule_unSuccessfulExpectation(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        dataDF.createOrReplaceTempView("table")

        customSuiteConfig = {
            "expectation_suite_name": "customRulesSuite",
            "expectations": [
                {
                    "expectation_type": "compare_datasets",
                    "kwargs": {
                        "compareAgainst": "Application_MEF_History.ContractHistory",
                        "comparedColumn": "ContractNumber,ContractNumber",
                        "expectation": "fullMatch",
                        "condition": "condition"
                    },
                    "meta": {
                        "WK_ID": 1,
                        "Quarantine": False,
                        "Description": "Description1",
                        "DQLogOutput": "ContractNumber, Application_MEF_History.ContractHistory.Col1",
                    }
                }
            ]
        }

        unexpectedList = None
        additionalData = [
            {
                "missingRows": [["ContractNumber", "joinTable_ContractNumber", "joinTable_Col1"],[None, 5, 1]], 
                "unexpectedRows": [["ContractNumber", "joinTable_ContractNumber", "joinTable_Col1"], [6, None, 2]]
            }
        ]
        self._validator._processCompareDatasetRule = MagicMock(return_value=(unexpectedList, additionalData))
        self._validator._processCustomExpectationRule = MagicMock()
        
        geOutput = MagicMock()
        geOutput.success = True
        geOutput.results = []

        # act
        geOutput = self._validator._validateCustomRules(dataDF, customSuiteConfig, geOutput)

        # assert
        self.assertEqual(geOutput.success, False)
        self.assertEqual(len(geOutput.results), 1)

        self.assertEqual(geOutput.results[0].success, False)
        self.assertNotIn('unexpected_list', geOutput.results[0].result)
        self.assertEqual(geOutput.results[0].result["details"], {"details": additionalData})
        self.assertEqual(geOutput.results[0].expectation_config.expectation_type, "compare_datasets")
        self.assertEqual(geOutput.results[0].expectation_config.kwargs, {
            "compareAgainst": "Application_MEF_History.ContractHistory",
            "comparedColumn": "ContractNumber,ContractNumber",
            "expectation": "fullMatch",
            "condition": "condition"
        })
        self.assertEqual(geOutput.results[0].expectation_config.meta, {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "ContractNumber, Application_MEF_History.ContractHistory.Col1",
        })

        self._validator._processCompareDatasetRule.assert_called_once_with(
            {
                'expectation_type': 'compare_datasets', 
                'kwargs': {
                    "compareAgainst": "Application_MEF_History.ContractHistory",
                    "comparedColumn": "ContractNumber,ContractNumber",
                    "expectation": "fullMatch",
                    "condition": "condition"
                }, 
                'meta': {
                    "WK_ID": 1,
                    "Quarantine": False,
                    "Description": "Description1",
                    "DQLogOutput": "ContractNumber, Application_MEF_History.ContractHistory.Col1",
                }
            }
        )

        self._validator._processCustomExpectationRule.assert_not_called()

    def test_validateCustomRules_multipleExpectations(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        customSuiteConfig = {
            "expectation_suite_name": "customRulesSuite",
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_match_condition",
                    "kwargs": {
                        "column": "col1",
                        "condition": "col1 > 100"
                    },
                    "meta": {
                        "WK_ID": 1,
                        "Quarantine": False,
                        "Description": "Description1",
                        "DQLogOutput": "DQLogOutput1",
                    }
                },
                {
                    "expectation_type": "expect_column_values_to_not_match_condition",
                    "kwargs": {
                        "column": "col1",
                        "condition": "col1 < 0"
                    },
                    "meta": {
                        "WK_ID": 2,
                        "Quarantine": False,
                        "Description": "Description2",
                        "DQLogOutput": "DQLogOutput2",
                    }
                },
                {
                    "expectation_type": "expect_column_values_to_not_match_condition",
                    "kwargs": {
                        "column": "col2",
                        "condition": "col2 = 'value 4'"
                    },
                    "meta": {
                        "WK_ID": 3,
                        "Quarantine": True,
                        "Description": "Description3",
                        "DQLogOutput": "DQLogOutput3",
                    }
                }
            ]
        }

        def getUnexpectedListAndAdditionalData(expectation: dict, dataDF: DataFrame) -> Tuple[list, list]:
            condition = expectation["kwargs"]["condition"]
            if condition == "col1 > 100":
                return [120, 105], []
            elif condition == "col1 < 0":
                return [], []
            elif condition == "col2 = 'value 4'":
                return ["value 4"], []
            else:
                return [], []
            
        self._validator._processCustomExpectationRule = MagicMock(side_effect = getUnexpectedListAndAdditionalData)

        geOutput = MagicMock()
        geOutput.success = True
        geOutput.results = []

        # act
        geOutput = self._validator._validateCustomRules(dataDF, customSuiteConfig, geOutput)

        # assert
        self.assertEqual(geOutput.success, False)
        self.assertEqual(len(geOutput.results), 3)

        self.assertEqual(geOutput.results[0].success, False)
        self.assertEqual(geOutput.results[0].result["unexpected_list"], [120, 105])
        self.assertEqual(geOutput.results[0].expectation_config.expectation_type, "expect_column_values_to_not_match_condition")
        self.assertEqual(geOutput.results[0].expectation_config.kwargs, { "column": "col1", "condition": "col1 > 100" })
        self.assertEqual(geOutput.results[0].expectation_config.meta, {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "DQLogOutput1",
        })

        self.assertEqual(geOutput.results[1].success, True)
        self.assertEqual(geOutput.results[1].result["unexpected_list"], [])
        self.assertEqual(geOutput.results[1].expectation_config.expectation_type, "expect_column_values_to_not_match_condition")
        self.assertEqual(geOutput.results[1].expectation_config.kwargs, { "column": "col1", "condition": "col1 < 0" })
        self.assertEqual(geOutput.results[1].expectation_config.meta, {
            "WK_ID": 2,
            "Quarantine": False,
            "Description": "Description2",
            "DQLogOutput": "DQLogOutput2",
        })

        self.assertEqual(geOutput.results[2].success, False)
        self.assertEqual(geOutput.results[2].result["unexpected_list"], ["value 4"])
        self.assertEqual(geOutput.results[2].expectation_config.expectation_type, "expect_column_values_to_not_match_condition")
        self.assertEqual(geOutput.results[2].expectation_config.kwargs, { "column": "col2", "condition": "col2 = 'value 4'" })
        self.assertEqual(geOutput.results[2].expectation_config.meta, {
            "WK_ID": 3,
            "Quarantine": True,
            "Description": "Description3",
            "DQLogOutput": "DQLogOutput3",
        })

    def test_validateCustomRules_notSupportedExpectation(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        customSuiteConfig = {
            "expectation_suite_name": "customRulesSuite",
            "expectations": [
                {
                    "expectation_type": "not_supported",
                    "kwargs": {
                        "condition": "col1 > 100",
                    },
                    "meta": {
                        "WK_ID": 1,
                        "Quarantine": False,
                        "Description": "Description1",
                        "DQLogOutput": "DQLogOutput1",
                    }
                }
            ]
        }

        geOutput = MagicMock()
        geOutput.success = True
        geOutput.results = []

        # act
        with self.assertRaises(NotImplementedError):
            self._validator._validateCustomRules(dataDF, customSuiteConfig, geOutput)

        # assert        
        self._validator._spark.sql.assert_not_called()

    
    # isResultSuccessful tests

    def test_isResultSuccessful_success(self):
        # arrange
        result = MagicMock()
        result.success = True

        # act
        isSuccessful = self._validator._isResultSuccessful(result)

        # assert
        self.assertEqual(isSuccessful, True)

    def test_isResultSuccessful_failed(self):
        # arrange
        result = MagicMock()
        result.success = False

        # act
        isSuccessful = self._validator._isResultSuccessful(result)

        # assert
        self.assertEqual(isSuccessful, False)

    def test_isResultSuccessful_columnsSet_success(self):
        # arrange
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_table_columns_to_match_set"

        result.result = {
            "details": {
                "mismatched": {
                    "unexpected": ["__eval_col_column1", "__eval_col_column2"]
                }
            }
        }

        # act
        isSuccessful = self._validator._isResultSuccessful(result)

        # assert
        self.assertEqual(isSuccessful, True)
    
    def test_isResultSuccessful_columnsSet_failed_unexpectedColumn(self):
        # arrange
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_table_columns_to_match_set"

        result.result = {
            "details": {
                "mismatched": {
                    "unexpected": ["__eval_col_column1", "__eval_col_column2", "column3"]
                }
            }
        }

        # act
        isSuccessful = self._validator._isResultSuccessful(result)

        # assert
        self.assertEqual(isSuccessful, False)
    
    def test_isResultSuccessful_columnsSet_failed_missingColumn(self):
        # arrange
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_table_columns_to_match_set"

        result.result = {
            "details": {
                "mismatched": {
                    "unexpected": ["__eval_col_column1", "__eval_col_column2"],
                    "missing": ["column3"]
                }
            }
        }

        # act
        isSuccessful = self._validator._isResultSuccessful(result)

        # assert
        self.assertEqual(isSuccessful, False)

    def test_isResultSuccessful_columnsList_success(self):
        # arrange
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_table_columns_to_match_ordered_list"

        result.result = {
            "details": {
                "mismatched": [
                    {
                        "Expected": None,
                        "Found": "__eval_col_column1"
                    },
                    {
                        "Expected": None,
                        "Found": "__eval_col_column2"
                    }
                ]
            }
        }

        # act
        isSuccessful = self._validator._isResultSuccessful(result)

        # assert
        self.assertEqual(isSuccessful, True)
    
    def test_isResultSuccessful_columnsList_failed_unexpectedColumn(self):
        # arrange
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_table_columns_to_match_ordered_list"

        result.result = {
            "details": {
                "mismatched": [
                    {
                        "Expected": None,
                        "Found": "__eval_col_column1"
                    },
                    {
                        "Expected": None,
                        "Found": "__eval_col_column2"
                    },
                    {
                        "Expected": None,
                        "Found": "column3"
                    }
                ]
            }
        }

        # act
        isSuccessful = self._validator._isResultSuccessful(result)

        # assert
        self.assertEqual(isSuccessful, False)
    
    def test_isResultSuccessful_columnsList_failed_missingColumn(self):
        # arrange
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_table_columns_to_match_ordered_list"

        result.result = {
            "details": {
                "mismatched": [
                    {
                        "Expected": None,
                        "Found": "__eval_col_column1"
                    },
                    {
                        "Expected": None,
                        "Found": "__eval_col_column2"
                    },
                    {
                        "Expected": "column3",
                        "Found": None
                    }
                ]
            }
        }

        # act
        isSuccessful = self._validator._isResultSuccessful(result)

        # assert
        self.assertEqual(isSuccessful, False)
    
    def test_isResultSuccessful_columnsList_failed_wrongOrderOfColumns(self):
        # arrange
        result = MagicMock()
        result.success = False
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_table_columns_to_match_ordered_list"

        result.result = {
            "details": {
                "mismatched": [
                    {
                        "Expected": None,
                        "Found": "__eval_col_column1"
                    },
                    {
                        "Expected": None,
                        "Found": "__eval_col_column2"
                    },
                    {
                        "Expected": "column1",
                        "Found": "column2"
                    },
                    {
                        "Expected": "column2",
                        "Found": "column1"
                    }
                ]
            }
        }

        # act
        isSuccessful = self._validator._isResultSuccessful(result)

        # assert
        self.assertEqual(isSuccessful, False)

    
    # getInvalidData tests

    def test_getInvalidData_dataFrameBasedExpectation(self):
        # arrange
        dataDF = MagicMock()

        result = MagicMock()
        result.result = {}

        self._validator._spark.createDataFrame = self._spark.createDataFrame

        # act
        invalidDataDF = self._validator._getInvalidData(dataDF, result)

        # assert
        self.assertEqual(invalidDataDF, dataDF)

    def test_getInvalidData_columPairExpectation(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        result = MagicMock()
        result.result = {
            "unexpected_list": [
                [100, "value 1"],
                [105, "value 3"]
            ]
        }
        result.expectation_config = MagicMock()
        result.expectation_config.kwargs = {
            "column_A": "col1",
            "column_B": "col2"
        }

        expectedInvalidDataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [3, 105, "value 3"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        self._validator._spark.createDataFrame = self._spark.createDataFrame

        # act
        invalidDataDF = self._validator._getInvalidData(dataDF, result)

        # assert
        self.assertEqual(
            invalidDataDF.rdd.collect(),
            expectedInvalidDataDF.rdd.collect()
        )
    
    def test_getInvalidData_columListExpectation(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        result = MagicMock()
        result.result = {
            "unexpected_list": [
                {
                    "col1": 100,
                    "col2": "value 1"
                },
                {
                    "col1": 105,
                    "col2": "value 3"
                }
            ]
        }
        result.expectation_config = MagicMock()
        result.expectation_config.kwargs = {
            "column_list": "col1"
        }

        expectedInvalidDataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [3, 105, "value 3"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        self._validator._spark.createDataFrame = self._spark.createDataFrame

        # act
        invalidDataDF = self._validator._getInvalidData(dataDF, result)

        # assert
        self.assertEqual(
            invalidDataDF.rdd.collect(),
            expectedInvalidDataDF.rdd.collect()
        )
    
    def test_getInvalidData_columNotNullExpectation(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, None, "value 1"],
            [2, 120, "value 2"],
            [3, None, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        result = MagicMock()
        result.result = {
            "unexpected_list": [
                None,
                None
            ]
        }
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_column_values_to_not_be_null"
        result.expectation_config.kwargs = {
            "column": "col1"
        }

        expectedInvalidDataDF = self._spark.createDataFrame([
            [1, None, "value 1"],
            [3, None, "value 3"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        self._validator._spark.createDataFrame = self._spark.createDataFrame

        # act
        invalidDataDF = self._validator._getInvalidData(dataDF, result)

        # assert
        self.assertEqual(
            invalidDataDF.rdd.collect(),
            expectedInvalidDataDF.rdd.collect()
        )
    
    def test_getInvalidData_columExpectation(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        result = MagicMock()
        result.result = {
            "unexpected_list": [
                100,
                105
            ]
        }
        result.expectation_config = MagicMock()
        result.expectation_config.kwargs = {
            "column": "col1"
        }

        expectedInvalidDataDF = self._spark.createDataFrame([
            [100, 1, "value 1"],
            [105, 3, "value 3"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        self._validator._spark.createDataFrame = self._spark.createDataFrame

        # act
        invalidDataDF = self._validator._getInvalidData(dataDF, result)

        # assert
        self.assertEqual(
            invalidDataDF.rdd.collect(),
            expectedInvalidDataDF.rdd.collect()
        )

    def test_getInvalidData_columExpectationWithoutColumnName(self):
        # arrange
        dataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [2, 120, "value 2"],
            [3, 105, "value 3"],
            [4, 99, "value 4"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        result = MagicMock()
        result.result = {
            "unexpected_list": [
                1,
                3
            ]
        }
        result.expectation_config = MagicMock()
        result.expectation_config.kwargs = {}

        expectedInvalidDataDF = self._spark.createDataFrame([
            [1, 100, "value 1"],
            [3, 105, "value 3"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING")

        self._validator._spark.createDataFrame = self._spark.createDataFrame

        # act
        invalidDataDF = self._validator._getInvalidData(dataDF, result)

        # assert
        self.assertEqual(
            invalidDataDF.rdd.collect(),
            expectedInvalidDataDF.rdd.collect()
        )

    
    # createDQLogTable tests

    def test_createDQLogTable_noDQConfig(self):
        # arrange
        self._validator._isDQConfigurationDefined = MagicMock(return_value=False)

        # act
        self._validator.createDQLogTable("testDataPath")

        # assert
        self._validator._spark.sql.assert_not_called()

    def test_createDQLogTable_createTable(self):
        # arrange
        self._validator._isDQConfigurationDefined = MagicMock(return_value=True)

        # act
        self._validator.createDQLogTable("testDataPath")

        # assert
        self.assertEqual(self._validator._spark.sql.call_count, 1)

        sqlCommand = self._validator._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"""CREATE TABLE IF NOT EXISTS {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._validator.METADATA_DQ_LOG_TABLE}""", sqlCommand)
        self.assertIn("USING DELTA", sqlCommand)
        self.assertIn(f'LOCATION "testDataPath/{self._validator.METADATA_DQ_LOG_TABLE}"', sqlCommand)

    
    # writeDQLogData tests

    def test_writeDQLogData_logToDatabase_firstRun(self):
        # arrange
        self._validator._logToDatabase = True
        self._validator.METADATA_DQ_LOG_TABLE_PATH = None
        self._validator._dataLakeHelper.writeData = MagicMock()
        logTablePath = "abfss://metadata@ddlasmefdevxx.dfs.core.windows.net/Delta/DQLog"
        
        self._validator._spark.sql.return_value = self._spark.createDataFrame([
            ["Location", logTablePath],
        ], "col_name STRING, data_type STRING")

        dqLogDF = MagicMock()

        # act
        self._validator._writeDQLogData(dqLogDF)

        # assert
        self._validator._dataLakeHelper.writeData.assert_called_once_with(
            dqLogDF,
            self._validator.METADATA_DQ_LOG_TABLE_PATH,
            WRITE_MODE_APPEND,
            "delta",
            {},
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"],
            self._validator.METADATA_DQ_LOG_TABLE
        )

        self._validator._spark.sql.assert_called_once_with(f"""DESC FORMATTED {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._validator.METADATA_DQ_LOG_TABLE}""")

        self.assertEqual(self._validator.METADATA_DQ_LOG_TABLE_PATH, logTablePath)

    def test_writeDQLogData_logToDatabase_secondRun(self):
        # arrange
        self._validator._logToDatabase = True
        self._validator.METADATA_DQ_LOG_TABLE_PATH = "abfss://metadata@ddlasmefdevxx.dfs.core.windows.net/Delta/DQLog"
        self._validator._dataLakeHelper.writeData = MagicMock()

        dqLogDF = MagicMock()

        # act
        self._validator._writeDQLogData(dqLogDF)

        # assert
        self._validator._dataLakeHelper.writeData.assert_called_once_with(
            dqLogDF,
            self._validator.METADATA_DQ_LOG_TABLE_PATH,
            WRITE_MODE_APPEND,
            "delta",
            {},
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"],
            self._validator.METADATA_DQ_LOG_TABLE
        )

        self._validator._spark.sql.assert_not_called()

    def test_writeDQLogData_display_firstRun(self):
        # arrange
        self._validator._logToDatabase = False
        self._validator._dataLakeHelper.writeData = MagicMock()
        self._validator._dqLogToDisplayDF = None

        dqLogDF = MagicMock()

        # act
        self._validator._writeDQLogData(dqLogDF)

        # assert
        self.assertEqual(self._validator._dqLogToDisplayDF, dqLogDF)

        self._validator._dataLakeHelper.writeData.assert_not_called()
        self._validator._spark.sql.assert_not_called()

    def test_writeDQLogData_display_secondRun(self):
        # arrange
        self._validator._logToDatabase = False
        self._validator._dataLakeHelper.writeData = MagicMock()
        
        combinedDQLogToDisplayDF = MagicMock()
        self._validator._dqLogToDisplayDF = MagicMock()
        self._validator._dqLogToDisplayDF.union.return_value = combinedDQLogToDisplayDF

        dqLogDF = MagicMock()

        # act
        self._validator._writeDQLogData(dqLogDF)

        # assert
        self.assertEqual(self._validator._dqLogToDisplayDF, combinedDQLogToDisplayDF)

        self._validator._dataLakeHelper.writeData.assert_not_called()


    # logValidationResult tests

    def test_logValidationResult_rowBasedExpectation(self):
        # arrange
        result = MagicMock()
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_column_values_to_not_match_condition"
        result.expectation_config.kwargs = {
            "column": "col1"
        }
        result.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "col1, col2",
        }
        result.result = {
            "unexpected_list": [
                [100],
                [99]
            ]
        }

        invalidDF = self._spark.createDataFrame([
            [1, 100, "value 1", "other data 1"],
            [4, 99, "value 4", "other data 2"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING, col3 STRING")

        self._validator._entRunId = "testEntRunId"
        self._validator._entRunDatetime = datetime.now()

        expectedDqLogDF = self._spark.createDataFrame([
            [
                "testDatabaseName.testTableName",
                "expect_column_values_to_not_match_condition",
                '{"column": "col1"}',
                "Description1",
                '{"col1":100,"col2":"value 1"}',
                False,
                datetime.now(),
                self._validator._entRunId,
                self._validator._entRunDatetime
            ],
            [
                "testDatabaseName.testTableName",
                "expect_column_values_to_not_match_condition",
                '{"column": "col1"}',
                "Description1",
                '{"col1":99,"col2":"value 4"}',
                False,
                datetime.now(),
                self._validator._entRunId,
                self._validator._entRunDatetime
            ],
        ], "FwkEntityId STRING, ExpectationType STRING, KwArgs STRING, Description STRING, Output STRING, Quarantine BOOLEAN, ValidationDatetime TIMESTAMP, EntRunId STRING, EntRunDatetime TIMESTAMP")

        self._validator._writeDQLogData = MagicMock()

        # act
        self._validator._logValidationResult(result, invalidDF, "testDatabaseName", "testTableName")

        # assert
        self.assertEqual(self._validator._writeDQLogData.call_count, 1)
        dqLogDF = self._validator._writeDQLogData.call_args_list[0].args[0]

        dqLogDF = dqLogDF.drop("ValidationDatetime")
        expectedDqLogDF = expectedDqLogDF.drop("ValidationDatetime")

        self.assertEqual(
            dqLogDF.rdd.collect(),
            expectedDqLogDF.rdd.collect()
        )

    def test_logValidationResult_rowBasedExpectation_mockGetDqLogOutputColumns(self):
        # arrange
        result = MagicMock()
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_column_values_to_not_match_condition"
        result.expectation_config.kwargs = {
            "column": "col1"
        }
        result.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "col1, col2",
        }
        result.result = {
            "unexpected_list": [
                [100],
                [99]
            ]
        }

        invalidDF = self._spark.createDataFrame([
            [1, 100, "value 1", "other data 1"],
            [4, 99, "value 4", "other data 2"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING, col3 STRING")

        self._validator._entRunId = "testEntRunId"
        self._validator._entRunDatetime = datetime.now()

        extendedInvalidDF = self._spark.createDataFrame([
            [1, 100, "value 1", "other data 1", "col1 value 1 from table B"],
            [4, 99, "value 4", "other data 2", "col1 value 4 from table B"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING, col3 STRING, joinTable_col1 STRING")

        dqLogOutputColumns = ["col1", "col2", "joinTable_col1"]

        self._validator._getDqLogOutputColumns = MagicMock(return_value=(dqLogOutputColumns, extendedInvalidDF))

        expectedDqLogDF = self._spark.createDataFrame([
            [
                "testDatabaseName.testTableName",
                "expect_column_values_to_not_match_condition",
                '{"column": "col1"}',
                "Description1",
                '{"col1":100,"col2":"value 1","joinTable_col1":"col1 value 1 from table B"}',
                False,
                datetime.now(),
                self._validator._entRunId,
                self._validator._entRunDatetime
            ],
            [
                "testDatabaseName.testTableName",
                "expect_column_values_to_not_match_condition",
                '{"column": "col1"}',
                "Description1",
                '{"col1":99,"col2":"value 4","joinTable_col1":"col1 value 4 from table B"}',
                False,
                datetime.now(),
                self._validator._entRunId,
                self._validator._entRunDatetime
            ],
        ], "FwkEntityId STRING, ExpectationType STRING, KwArgs STRING, Description STRING, Output STRING, Quarantine BOOLEAN, ValidationDatetime TIMESTAMP, EntRunId STRING, EntRunDatetime TIMESTAMP")

        self._validator._writeDQLogData = MagicMock()

        # act
        self._validator._logValidationResult(result, invalidDF, "testDatabaseName", "testTableName")

        # assert
        self.assertEqual(self._validator._writeDQLogData.call_count, 1)
        dqLogDF = self._validator._writeDQLogData.call_args_list[0].args[0]

        dqLogDF = dqLogDF.drop("ValidationDatetime")
        expectedDqLogDF = expectedDqLogDF.drop("ValidationDatetime")

        self.assertEqual(
            dqLogDF.rdd.collect(),
            expectedDqLogDF.rdd.collect()
        )

        self._validator._getDqLogOutputColumns.assert_called_once_with(result, invalidDF)

    def test_logValidationResult_dataFrameBasedExpectation(self):
        # arrange
        result = MagicMock()
        result.expectation_config = MagicMock()
        result.expectation_config.expectation_type = "expect_table_columns_to_match_set"
        result.expectation_config.kwargs = {
            "column_set": ["col1", "col2"]
        }
        result.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": None,
        }
        result.result = {
            "details": {
                "mismatched": {
                    "unexpected": ["__eval_col_column1", "__eval_col_column2"],
                    "missing": ["column3"]
                }
            }
        }

        invalidDF = self._spark.createDataFrame([
            [1, 100, "value 1", "other data 1"],
            [4, 99, "value 4", "other data 2"],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING, col3 STRING")

        self._validator._entRunId = "testEntRunId"
        self._validator._entRunDatetime = datetime.now()

        expectedDqLogDF = self._spark.createDataFrame([
            [
                "testDatabaseName.testTableName",
                "expect_table_columns_to_match_set",
                '{"column_set": ["col1", "col2"]}',
                "Description1",
                '{"mismatched": {"unexpected": ["__eval_col_column1", "__eval_col_column2"], "missing": ["column3"]}, "RowCount": 2}',
                False,
                datetime.now(),
                self._validator._entRunId,
                self._validator._entRunDatetime
            ],
        ], "FwkEntityId STRING, ExpectationType STRING, KwArgs STRING, Description STRING, Output STRING, Quarantine BOOLEAN, ValidationDatetime TIMESTAMP, EntRunId STRING, EntRunDatetime TIMESTAMP")

        self._validator._writeDQLogData = MagicMock()

        # act
        self._validator._logValidationResult(result, invalidDF, "testDatabaseName", "testTableName")

        # assert
        self.assertEqual(self._validator._writeDQLogData.call_count, 1)
        dqLogDF = self._validator._writeDQLogData.call_args_list[0].args[0]

        dqLogDF = dqLogDF.drop("ValidationDatetime")
        expectedDqLogDF = expectedDqLogDF.drop("ValidationDatetime")

        self.assertEqual(
            dqLogDF.rdd.collect(),
            expectedDqLogDF.rdd.collect()
        )

    
    # logValidationSummary tests

    def test_logValidationSummary(self):
        # arrange
        validationDF = self._spark.createDataFrame([
            [1, 100, "value 1", "other data 1", Validator.VALIDATION_STATUS_INVALID],
            [2, 105, "value 2", "other data 2", Validator.VALIDATION_STATUS_VALID],
            [3, 120, "value 3", "other data 3", Validator.VALIDATION_STATUS_VALID],
            [4, 99, "value 4", "other data 4", Validator.VALIDATION_STATUS_QUARANTINED],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING, col3 STRING, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        self._validator._entRunId = "testEntRunId"
        self._validator._entRunDatetime = datetime.now()

        expectedDqLogDF = self._spark.createDataFrame([
            [
                "testDatabaseName.testTableName",
                None,
                '{}',
                "Summary",
                '{"TotalRowCount": 4, "ValidRowCount": 2, "InvalidRowCount": 1, "QuarantinedRowCount": 1}',
                None,
                datetime.now(),
                self._validator._entRunId,
                self._validator._entRunDatetime
            ]
        ], "FwkEntityId STRING, ExpectationType STRING, KwArgs STRING, Description STRING, Output STRING, Quarantine BOOLEAN, ValidationDatetime TIMESTAMP, EntRunId STRING, EntRunDatetime TIMESTAMP")

        self._validator._writeDQLogData = MagicMock()

        # act
        self._validator._logValidationSummary(validationDF, "testDatabaseName", "testTableName")

        # assert
        self.assertEqual(self._validator._writeDQLogData.call_count, 1)
        dqLogDF = self._validator._writeDQLogData.call_args_list[0].args[0]

        dqLogDF = dqLogDF.drop("ValidationDatetime")
        expectedDqLogDF = expectedDqLogDF.drop("ValidationDatetime")

        self.assertEqual(
            dqLogDF.rdd.collect(),
            expectedDqLogDF.rdd.collect()
        )

    
    # getValidationResult tests

    def test_getValidationResult(self):
        # arrange
        validationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1"],
            [105, "value 2", "other data 2"],
            [120, "value 3", "other data 3"],
            [99, "value 4", "other data 4"],
        ], "col1 INT, col2 STRING, col3 STRING")

        geSuiteConfig = {
            "expectation_suite_name": "greatExpectationsSuite",
            "expectations": []
        }
        customSuiteConfig = {
            "expectation_suite_name": "customRulesSuite",
            "expectations": []
        }
        expectedGeOutput = MagicMock()

        self._validator._getSuiteConfig = MagicMock(return_value=(geSuiteConfig, customSuiteConfig))
        self._validator._validateCustomRules = MagicMock(return_value=expectedGeOutput)

        expectedValidationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1],
            [105, "value 2", "other data 2", 2],
            [120, "value 3", "other data 3", 3],
            [99, "value 4", "other data 4", 4],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT")

        # act
        (geOutput, validationDF) = self._validator._getValidationResult(validationDF, "testDatabaseName", "testTableName")

        # assert
        self.assertEqual(geOutput, expectedGeOutput)
        self.assertEqual(
            validationDF.drop(self._validator.MGT_TEMP_ID_COLUMN_NAME).rdd.collect(),
            expectedValidationDF.drop(self._validator.MGT_TEMP_ID_COLUMN_NAME).rdd.collect()
        )

        self._validator._getSuiteConfig.assert_called_once_with("testDatabaseName", "testTableName")
        
        self._validator._validateCustomRules.assert_called_once()
        args = self._validator._validateCustomRules.call_args_list[0].args
        self.assertEqual(
            args[0].drop(self._validator.MGT_TEMP_ID_COLUMN_NAME).rdd.collect(),
            expectedValidationDF.drop(self._validator.MGT_TEMP_ID_COLUMN_NAME).rdd.collect()
        )
        self.assertEqual(customSuiteConfig, args[1])


    # setValidationStatus tests

    def test_setValidationStatus_notIDs(self):
        # arrange
        validationDF = MagicMock()
        rowIDs = np.array([])
        validationStatus = "INVALID"

        # act
        resultDF = self._validator._setValidationStatus(validationDF, rowIDs, validationStatus)

        # assert
        self.assertEqual(resultDF, validationDF)

    def test_setValidationStatus_allIDs(self):
        # arrange
        validationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1, Validator.VALIDATION_STATUS_VALID],
            [105, "value 2", "other data 2", 2, Validator.VALIDATION_STATUS_VALID],
            [120, "value 3", "other data 3", 3, Validator.VALIDATION_STATUS_VALID],
            [99, "value 4", "other data 4", 4, Validator.VALIDATION_STATUS_VALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, {VALIDATION_STATUS_COLUMN_NAME} STRING")
        
        rowIDs = np.array([1, 2, 3, 4])
        validationStatus = Validator.VALIDATION_STATUS_INVALID

        expectedValidationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1, Validator.VALIDATION_STATUS_INVALID],
            [105, "value 2", "other data 2", 2, Validator.VALIDATION_STATUS_INVALID],
            [120, "value 3", "other data 3", 3, Validator.VALIDATION_STATUS_INVALID],
            [99, "value 4", "other data 4", 4, Validator.VALIDATION_STATUS_INVALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        # act
        resultDF = self._validator._setValidationStatus(validationDF, rowIDs, validationStatus)

        # assert
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedValidationDF.rdd.collect()
        )

        self._validator._spark.createDataFrame.assert_not_called()

    def test_setValidationStatus_fewIDs(self):
        # arrange
        validationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1, Validator.VALIDATION_STATUS_VALID],
            [105, "value 2", "other data 2", 2, Validator.VALIDATION_STATUS_VALID],
            [120, "value 3", "other data 3", 3, Validator.VALIDATION_STATUS_VALID],
            [99, "value 4", "other data 4", 4, Validator.VALIDATION_STATUS_VALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, {VALIDATION_STATUS_COLUMN_NAME} STRING")
        
        rowIDs = np.array([1, 4])
        validationStatus = Validator.VALIDATION_STATUS_INVALID

        expectedValidationDF = self._spark.createDataFrame([
            [1, 100, "value 1", "other data 1", Validator.VALIDATION_STATUS_INVALID],
            [2, 105, "value 2", "other data 2", Validator.VALIDATION_STATUS_VALID],
            [3, 120, "value 3", "other data 3", Validator.VALIDATION_STATUS_VALID],
            [4, 99, "value 4", "other data 4", Validator.VALIDATION_STATUS_INVALID],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING, col3 STRING, {VALIDATION_STATUS_COLUMN_NAME} STRING")
    
        self._validator._spark.createDataFrame = self._spark.createDataFrame

        # act
        resultDF = self._validator._setValidationStatus(validationDF, rowIDs, validationStatus)

        # assert
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedValidationDF.rdd.collect()
        )

    
    # processNonValidRecords tests

    def test_processNonValidRecords_oneInvalid_oneQuarantined(self):
        # arrange
        validationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1, Validator.VALIDATION_STATUS_VALID],
            [105, "value 2", "other data 2", 2, Validator.VALIDATION_STATUS_VALID],
            [120, "value 3", "other data 3", 3, Validator.VALIDATION_STATUS_VALID],
            [99, "value 4", "other data 4", 4, Validator.VALIDATION_STATUS_VALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        result1 = MagicMock()
        result1.success = False
        result1.expectation_config = MagicMock()
        result1.expectation_config.expectation_type = "expect_column_values_to_not_match_condition"
        result1.expectation_config.kwargs = {
            "column": "col1"
        }
        result1.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "col1, col2",
        }
        result1.result = {
            "unexpected_list": [100]
        }

        result2 = MagicMock()
        result2.success = False
        result2.expectation_config = MagicMock()
        result2.expectation_config.expectation_type = "expect_column_values_to_not_match_condition"
        result2.expectation_config.kwargs = {
            "column": "col1"
        }
        result2.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": True,
            "Description": "Description2",
            "DQLogOutput": "col1, col2",
        }
        result2.result = {
            "unexpected_list": [99]
        }

        geOutput = MagicMock()
        geOutput.success = False
        geOutput.results = [result1, result2]

        self._validator._logValidationResult = MagicMock()
        self._validator._spark.createDataFrame = self._spark.createDataFrame

        expectedValidationDF = self._spark.createDataFrame([
            [1, 100, "value 1", "other data 1", Validator.VALIDATION_STATUS_INVALID],
            [2, 105, "value 2", "other data 2", Validator.VALIDATION_STATUS_VALID],
            [3, 120, "value 3", "other data 3", Validator.VALIDATION_STATUS_VALID],
            [4, 99, "value 4", "other data 4", Validator.VALIDATION_STATUS_QUARANTINED],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING, col3 STRING, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        # act
        resultDF = self._validator._processNonValidRecords(validationDF, "testDatabaseName", "testTableName", geOutput)

        # assert
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedValidationDF.rdd.collect()
        )

        self.assertEqual(self._validator._logValidationResult.call_count, 2)

    def test_processNonValidRecords_fourInvalid_fourQuarantined(self):
        # arrange
        validationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1, Validator.VALIDATION_STATUS_VALID],
            [105, "value 2", "other data 2", 2, Validator.VALIDATION_STATUS_VALID],
            [120, "value 3", "other data 3", 3, Validator.VALIDATION_STATUS_VALID],
            [99, "value 4", "other data 4", 4, Validator.VALIDATION_STATUS_VALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        result1 = MagicMock()
        result1.success = False
        result1.expectation_config = MagicMock()
        result1.expectation_config.expectation_type = "expect_column_values_to_not_match_condition"
        result1.expectation_config.kwargs = {
            "column": "col1"
        }
        result1.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "col1, col2",
        }
        result1.result = {
            "unexpected_list": [100, 105, 120, 99]
        }

        result2 = MagicMock()
        result2.success = False
        result2.expectation_config = MagicMock()
        result2.expectation_config.expectation_type = "expect_column_values_to_not_match_condition"
        result2.expectation_config.kwargs = {
            "column": "col1"
        }
        result2.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": True,
            "Description": "Description2",
            "DQLogOutput": "col1, col2",
        }
        result2.result = {
            "unexpected_list": [100, 105, 120, 99]
        }

        geOutput = MagicMock()
        geOutput.success = False
        geOutput.results = [result1, result2]

        self._validator._logValidationResult = MagicMock()
        self._validator._spark.createDataFrame = self._spark.createDataFrame

        expectedValidationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1, Validator.VALIDATION_STATUS_QUARANTINED],
            [105, "value 2", "other data 2", 2, Validator.VALIDATION_STATUS_QUARANTINED],
            [120, "value 3", "other data 3", 3, Validator.VALIDATION_STATUS_QUARANTINED],
            [99, "value 4", "other data 4", 4, Validator.VALIDATION_STATUS_QUARANTINED],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        # act
        resultDF = self._validator._processNonValidRecords(validationDF, "testDatabaseName", "testTableName", geOutput)

        # assert
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedValidationDF.rdd.collect()
        )

        self.assertEqual(self._validator._logValidationResult.call_count, 2)
    
    def test_processNonValidRecords_fourInvalid_threeQuarantined(self):
        # arrange
        validationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1, Validator.VALIDATION_STATUS_VALID],
            [105, "value 2", "other data 2", 2, Validator.VALIDATION_STATUS_VALID],
            [120, "value 3", "other data 3", 3, Validator.VALIDATION_STATUS_VALID],
            [99, "value 4", "other data 4", 4, Validator.VALIDATION_STATUS_VALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        result1 = MagicMock()
        result1.success = False
        result1.expectation_config = MagicMock()
        result1.expectation_config.expectation_type = "expect_column_values_to_not_match_condition"
        result1.expectation_config.kwargs = {
            "column": "col1"
        }
        result1.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "col1, col2",
        }
        result1.result = {
            "unexpected_list": [100, 105, 120, 99]
        }

        result2 = MagicMock()
        result2.success = False
        result2.expectation_config = MagicMock()
        result2.expectation_config.expectation_type = "expect_column_values_to_not_match_condition"
        result2.expectation_config.kwargs = {
            "column": "col1"
        }
        result2.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": True,
            "Description": "Description2",
            "DQLogOutput": "col1, col2",
        }
        result2.result = {
            "unexpected_list": [100, 105, 99]
        }

        geOutput = MagicMock()
        geOutput.success = False
        geOutput.results = [result1, result2]

        self._validator._logValidationResult = MagicMock()
        self._validator._spark.createDataFrame = self._spark.createDataFrame

        expectedValidationDF = self._spark.createDataFrame([
            [1, 100, "value 1", "other data 1", Validator.VALIDATION_STATUS_QUARANTINED],
            [2, 105, "value 2", "other data 2", Validator.VALIDATION_STATUS_QUARANTINED],
            [3, 120, "value 3", "other data 3", Validator.VALIDATION_STATUS_INVALID],
            [4, 99, "value 4", "other data 4", Validator.VALIDATION_STATUS_QUARANTINED],
        ], f"{self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, col1 INT, col2 STRING, col3 STRING, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        # act
        resultDF = self._validator._processNonValidRecords(validationDF, "testDatabaseName", "testTableName", geOutput)

        # assert
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedValidationDF.rdd.collect()
        )

        self.assertEqual(self._validator._logValidationResult.call_count, 2)

    def test_processNonValidRecords_zeroInvalid_zeroQuarantined(self):
        # arrange
        validationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1, Validator.VALIDATION_STATUS_VALID],
            [105, "value 2", "other data 2", 2, Validator.VALIDATION_STATUS_VALID],
            [120, "value 3", "other data 3", 3, Validator.VALIDATION_STATUS_VALID],
            [99, "value 4", "other data 4", 4, Validator.VALIDATION_STATUS_VALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        result1 = MagicMock()
        result1.success = True
        result1.expectation_config = MagicMock()
        result1.expectation_config.expectation_type = "expect_column_values_to_not_match_condition"
        result1.expectation_config.kwargs = {
            "column": "col1"
        }
        result1.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": False,
            "Description": "Description1",
            "DQLogOutput": "col1, col2",
        }
        result1.result = {
            "unexpected_list": []
        }

        result2 = MagicMock()
        result2.success = True
        result2.expectation_config = MagicMock()
        result2.expectation_config.expectation_type = "expect_column_values_to_not_match_condition"
        result2.expectation_config.kwargs = {
            "column": "col1"
        }
        result2.expectation_config.meta = {
            "WK_ID": 1,
            "Quarantine": True,
            "Description": "Description2",
            "DQLogOutput": "col1, col2",
        }
        result2.result = {
            "unexpected_list": []
        }

        geOutput = MagicMock()
        geOutput.success = True
        geOutput.results = [result1, result2]

        self._validator._logValidationResult = MagicMock()
        self._validator._spark.createDataFrame = self._spark.createDataFrame

        expectedValidationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1, Validator.VALIDATION_STATUS_VALID],
            [105, "value 2", "other data 2", 2, Validator.VALIDATION_STATUS_VALID],
            [120, "value 3", "other data 3", 3, Validator.VALIDATION_STATUS_VALID],
            [99, "value 4", "other data 4", 4, Validator.VALIDATION_STATUS_VALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        # act
        resultDF = self._validator._processNonValidRecords(validationDF, "testDatabaseName", "testTableName", geOutput)

        # assert
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedValidationDF.rdd.collect()
        )

        self.assertEqual(self._validator._logValidationResult.call_count, 0)


    # handleEmptyDFForCompareDataset tests

    def test_handleEmptyDFForCompareDataset_with_none_no_compare_dataset(self):
        # arrange
        validationDF = None
        dqConfigurationDF = self._spark.createDataFrame([
            [3, "expect_table_columns_to_match_set", "KwArgs3", False, "Description3", "DQLogOutput3"],
            [4, "expect_table_columns_to_match_ordered_list", "KwArgs4", True, "Description3", "DQLogOutput3"],
            [5, "expect_column_values_to_be_of_type", "KwArgs5", False, "Description4", "DQLogOutput4"],
            [1, "expect_column_values_to_be_null", "KwArgs1", False, "Description1", "DQLogOutput1"],
            [2, "expect_column_value_lengths_to_equal", "KwArgs2", False, "Description2", "DQLogOutput2"],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING")

        self._validator._getDQConfiguration = MagicMock(return_value=dqConfigurationDF)
        self._validator._writeDQLogData = MagicMock()

        # act
        self._validator._handleEmptyDFForCompareDataset(validationDF, "testDatabaseName", "testTableName")

        # assert
        self._validator._getDQConfiguration.assert_called_once_with("testDatabaseName", "testTableName")
        self._validator._writeDQLogData.assert_not_called()

    def test_handleEmptyDFForCompareDataset_emptyDF_with_compare_dataset(self):
        # arrange
        validationDF = self._spark.createDataFrame(
            self._spark.sparkContext.emptyRDD(),
            "col1 STRING"
        )

        dqConfigurationDF = self._spark.createDataFrame([
            [3, "compare_datasets", '{"expectation": "noMissing"}', False, "Description3", "DQLogOutput3"],
            [4, "expect_table_columns_to_match_ordered_list", "KwArgs4", True, "Description3", "DQLogOutput3"],
            [5, "expect_column_values_to_be_of_type", "KwArgs5", False, "Description4", "DQLogOutput4"],
            [1, "expect_column_values_to_be_null", "KwArgs1", False, "Description1", "DQLogOutput1"],
            [2, "expect_column_value_lengths_to_equal", "KwArgs2", False, "Description2", "DQLogOutput2"],
        ], "WK_ID BIGINT, ExpectationType STRING, KwArgs STRING, Quarantine BOOLEAN, Description STRING, DQLogOutput STRING")

        self._validator._getDQConfiguration = MagicMock(return_value = dqConfigurationDF) 
        self._validator._writeDQLogData = MagicMock()
    
        # act
        self._validator._handleEmptyDFForCompareDataset(validationDF, "testDatabaseName", "testTableName")
        
        # assert        
        self._validator._getDQConfiguration.assert_called_once_with("testDatabaseName", "testTableName")                   
        self._validator._writeDQLogData.assert_called_once()


    # validate tests

    def test_validate_none(self):
        # arrange
        validationDF = None
        self._validator._getValidationResult = MagicMock()
        self._validator._handleEmptyDFForCompareDataset = MagicMock()

        # act
        resultDF = self._validator.validate(validationDF, "testDatabaseName", "testTableName")

        # assert
        self.assertEqual(resultDF, validationDF)
        self._validator._handleEmptyDFForCompareDataset.assert_called_once_with(validationDF, "testDatabaseName", "testTableName")
        self._validator._getValidationResult.assert_not_called()
    
    def test_validate_emptyDF(self):
        # arrange
        validationDF = self._spark.createDataFrame(
            self._spark.sparkContext.emptyRDD(),
            "col1 STRING"
        )
        
        expectedValidationDF = self._spark.createDataFrame(
            self._spark.sparkContext.emptyRDD(),
            f"col1 STRING, {VALIDATION_STATUS_COLUMN_NAME} STRING"
        )   
             
        self._validator._getValidationResult = MagicMock()
        self._validator._handleEmptyDFForCompareDataset = MagicMock()
        
        # act
        resultDF = self._validator.validate(validationDF, "testDatabaseName", "testTableName")
        
        # assert        
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedValidationDF.rdd.collect()
        )
        
        self.assertEqual(expectedValidationDF.count(), 0)
        self._validator._handleEmptyDFForCompareDataset.assert_called_once_with(validationDF, "testDatabaseName", "testTableName")                   
        self._validator._getValidationResult.assert_not_called()

    def test_validate_validData(self):
        # arrange
        validationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1"],
            [105, "value 2", "other data 2"],
            [120, "value 3", "other data 3"],
            [99, "value 4", "other data 4"],
        ], f"col1 INT, col2 STRING, col3 STRING")

        geOutput = MagicMock()
        geOutput.success = True

        enrichedValidationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1],
            [105, "value 2", "other data 2", 2],
            [120, "value 3", "other data 3", 3],
            [99, "value 4", "other data 4", 4],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT")

        self._validator._handleEmptyDFForCompareDataset = MagicMock()
        self._validator._getValidationResult = MagicMock(return_value=(geOutput, enrichedValidationDF))

        self._validator._processNonValidRecords = MagicMock()
        self._validator._logValidationSummary = MagicMock()
        self._validator._display = MagicMock()

        expectedResultDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", Validator.VALIDATION_STATUS_VALID],
            [105, "value 2", "other data 2", Validator.VALIDATION_STATUS_VALID],
            [120, "value 3", "other data 3", Validator.VALIDATION_STATUS_VALID],
            [99, "value 4", "other data 4", Validator.VALIDATION_STATUS_VALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        # act
        resultDF = self._validator.validate(validationDF, "testDatabaseName", "testTableName")

        # assert
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedResultDF.rdd.collect()
        )

        self._validator._handleEmptyDFForCompareDataset.assert_called_once_with(validationDF, "testDatabaseName", "testTableName")
        self._validator._getValidationResult.assert_called_once_with(validationDF, "testDatabaseName", "testTableName")
        self._validator._processNonValidRecords.assert_not_called()
        self._validator._logValidationSummary.assert_called_once_with(ANY, "testDatabaseName", "testTableName")
        self._validator._display.assert_not_called()

    def test_validate_invalidData(self):
        # arrange
        validationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1"],
            [105, "value 2", "other data 2"],
            [120, "value 3", "other data 3"],
            [99, "value 4", "other data 4"],
        ], f"col1 INT, col2 STRING, col3 STRING")

        geOutput = MagicMock()
        geOutput.success = False

        enrichedValidationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1],
            [105, "value 2", "other data 2", 2],
            [120, "value 3", "other data 3", 3],
            [99, "value 4", "other data 4", 4],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT")

        self._validator._handleEmptyDFForCompareDataset = MagicMock()
        self._validator._getValidationResult = MagicMock(return_value=(geOutput, enrichedValidationDF))

        markedInvalidValidationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1, Validator.VALIDATION_STATUS_INVALID],
            [105, "value 2", "other data 2", 2, Validator.VALIDATION_STATUS_VALID],
            [120, "value 3", "other data 3", 3, Validator.VALIDATION_STATUS_QUARANTINED],
            [99, "value 4", "other data 4", 4, Validator.VALIDATION_STATUS_VALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        self._validator._processNonValidRecords = MagicMock(return_value=markedInvalidValidationDF)
        self._validator._logValidationSummary = MagicMock()
        self._validator._display = MagicMock()

        expectedResultDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", Validator.VALIDATION_STATUS_INVALID],
            [105, "value 2", "other data 2", Validator.VALIDATION_STATUS_VALID],
            [120, "value 3", "other data 3", Validator.VALIDATION_STATUS_QUARANTINED],
            [99, "value 4", "other data 4", Validator.VALIDATION_STATUS_VALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        # act
        resultDF = self._validator.validate(validationDF, "testDatabaseName", "testTableName")

        # assert
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedResultDF.rdd.collect()
        )

        self._validator._handleEmptyDFForCompareDataset.assert_called_once_with(validationDF, "testDatabaseName", "testTableName")
        self._validator._getValidationResult.assert_called_once_with(validationDF, "testDatabaseName", "testTableName")
        self._validator._processNonValidRecords.assert_called_once_with(ANY, "testDatabaseName", "testTableName", geOutput)
        self._validator._logValidationSummary.assert_called_once_with(ANY, "testDatabaseName", "testTableName")
        self._validator._display.assert_not_called()

    def test_validate_display_invalidData(self):
        # arrange
        validationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1"],
            [105, "value 2", "other data 2"],
            [120, "value 3", "other data 3"],
            [99, "value 4", "other data 4"],
        ], f"col1 INT, col2 STRING, col3 STRING")

        geOutput = MagicMock()
        geOutput.success = False

        enrichedValidationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1],
            [105, "value 2", "other data 2", 2],
            [120, "value 3", "other data 3", 3],
            [99, "value 4", "other data 4", 4],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT")

        self._validator._handleEmptyDFForCompareDataset = MagicMock()
        self._validator._getValidationResult = MagicMock(return_value=(geOutput, enrichedValidationDF))

        markedInvalidValidationDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", 1, Validator.VALIDATION_STATUS_INVALID],
            [105, "value 2", "other data 2", 2, Validator.VALIDATION_STATUS_VALID],
            [120, "value 3", "other data 3", 3, Validator.VALIDATION_STATUS_QUARANTINED],
            [99, "value 4", "other data 4", 4, Validator.VALIDATION_STATUS_VALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {self._validator.MGT_TEMP_ID_COLUMN_NAME} INT, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        self._validator._processNonValidRecords = MagicMock(return_value=markedInvalidValidationDF)
        
        def setValidatorState(validationDF: DataFrame, databaseName: str, tableName: str):
            self._validator._dqLogToDisplayDF = MagicMock()
        
        self._validator._logValidationSummary = MagicMock(side_effect=setValidatorState)
        self._validator._display = MagicMock()
        self._validator._logToDatabase = False

        expectedResultDF = self._spark.createDataFrame([
            [100, "value 1", "other data 1", Validator.VALIDATION_STATUS_INVALID],
            [105, "value 2", "other data 2", Validator.VALIDATION_STATUS_VALID],
            [120, "value 3", "other data 3", Validator.VALIDATION_STATUS_QUARANTINED],
            [99, "value 4", "other data 4", Validator.VALIDATION_STATUS_VALID],
        ], f"col1 INT, col2 STRING, col3 STRING, {VALIDATION_STATUS_COLUMN_NAME} STRING")

        # act
        resultDF = self._validator.validate(validationDF, "testDatabaseName", "testTableName")

        # assert
        self.assertEqual(
            resultDF.rdd.collect(),
            expectedResultDF.rdd.collect()
        )

        self._validator._handleEmptyDFForCompareDataset.assert_called_once_with(validationDF, "testDatabaseName", "testTableName")
        self._validator._getValidationResult.assert_called_once_with(validationDF, "testDatabaseName", "testTableName")
        self._validator._processNonValidRecords.assert_called_once_with(ANY, "testDatabaseName", "testTableName", geOutput)
        self._validator._logValidationSummary.assert_called_once_with(ANY, "testDatabaseName", "testTableName")
        self._validator._display.assert_called_once_with(self._validator._dqLogToDisplayDF)
