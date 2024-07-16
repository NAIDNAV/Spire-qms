# Databricks notebook source
import json
import numpy as np
import re
import decimal
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, row_number, struct, to_json, when, monotonically_increasing_id
from pyspark.sql.types import ArrayType, MapType, LongType, StructType, StructField
from typing import Tuple, Type, Union
datetimeNow = datetime.now()

import great_expectations

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.generator.configGenerators.configGeneratorHelper import ConfigGeneratorHelper
    from notebooks.mdmf.includes.dataLakeHelper import DataLakeHelper
    from notebooks.mdmf.includes.frameworkConfig import *


class Validator:
    """The purpose of the data validation module is to validate the data and
    to create data quality log.

    Validation status can have these values: valid, invalid and quarantined.
    Valid and invalid data are allowed to move to the next layer. However
    quarantined data should not be processed further and will stay stored
    on the current layer.

    Public methods:
        saveDqMetadataConfig
        saveDqRefMetadataConfig
        saveDqRefPairMetadataConfig
        saveConfigFileMetadataConfig
        createDQLogTable
        addValidationStatusForTablesInHive
        filterOutQuarantinedRecords
        hasToBeValidated
        validate
    """
    
    VALIDATION_STATUS_VALID = "VALID"
    VALIDATION_STATUS_INVALID = "INVALID"
    VALIDATION_STATUS_QUARANTINED = "QUARANTINED"
    
    METADATA_DQ_CONFIGURATION_TABLE = "DQConfiguration"
    METADATA_DQ_REFERENCE_VALUES_TABLE = "DQReferenceValues"
    METADATA_DQ_REFERENCE_PAIR_VALUES_TABLE = "DQReferencePairValues"
    METADATA_CONFIGURATION_FILE_VALIDATION_TABLE = "ConfigurationFileValidation"
    
    METADATA_DQ_LOG_TABLE = "DQLog"
    METADATA_DQ_LOG_TABLE_PATH = None
    METADATA_DQ_LOG_TABLE_COLUMNS = [
        "FwkEntityId",
        "ExpectationType",
        "KwArgs",
        "Description",
        "Output",
        "Quarantine",
        "ValidationDatetime",
        "EntRunId",
        "EntRunDatetime"
    ]
    
    MGT_TEMP_ID_COLUMN_NAME = "MGT_TEMP_id"
    
    def __init__(self, spark: SparkSession, compartmentConfig: MapType, entRunId: str, entTriggerTime: datetime, params: MapType = None):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._sqlContext = globals()["sqlContext"] if "sqlContext" in globals() else None
        self._display = globals()["display"] if "display" in globals() else None
        
        self._dataLakeHelper = DataLakeHelper(spark, compartmentConfig)
        self._configGeneratorHelper = ConfigGeneratorHelper(spark, compartmentConfig)
        
        self._entRunId = entRunId
        self._entRunDatetime = entTriggerTime
        
        # set validation configuration table name
        if params and "validationConfigurationTable" in params.keys():
            self._validationConfigurationTable = params["validationConfigurationTable"]
        else:
            self._validationConfigurationTable = self.METADATA_DQ_CONFIGURATION_TABLE
        
        # set flag whether to log to database or not
        if params and "logToDatabase" in params.keys():
            self._logToDatabase = params["logToDatabase"]
        else:
            self._logToDatabase = True
    
    def _validateReferenceValuesConfig(self, configDF: DataFrame):
        assert configDF.filter("Value IS NULL").count() == 0, (
            f"Null can't be present in Value column in {self._compartmentConfig.METADATA_DQ_REFERENCE_VALUES_FILE} config")

    def _validateReferencePairValuesConfig(self, configDF: DataFrame):
        assert configDF.filter("Value1 IS NULL OR Value2 IS NULL").count() == 0, (
            f"Null can't be present neither in Value1 nor Value2 column in {self._compartmentConfig.METADATA_DQ_REFERENCE_PAIR_VALUES_FILE} config")
        
    def saveDqMetadataConfig(self, configPath: Union[str, list], dataPath: str) -> bool:
        """Saves and validates DQ metadata config.

        Returns:
            Flag whether the content of the config has changed.
        """
        if configPath is None:
            return False
        
        return self._configGeneratorHelper.saveMetadataConfigAsSCDType2(
            configPath,
            METADATA_DQ_CONFIGURATION_SCHEMA,
            dataPath,
            self.METADATA_DQ_CONFIGURATION_TABLE,
            ["DatabaseName", "EntityName", "ExpectationType", "KwArgs"]
        )
    
    def saveDqRefMetadataConfig(self, configPath: Union[str, list], dataPath: str) -> bool:
        """Saves and validates DQ Reference metadata config.

        Returns:
            Flag whether the content of the config has changed.
        """
        if configPath is None:
            return False
        
        return self._configGeneratorHelper.saveMetadataConfigAsSCDType2(
            configPath,
            METADATA_DQ_REFERENCE_VALUES_SCHEMA,
            dataPath,
            self.METADATA_DQ_REFERENCE_VALUES_TABLE,
            ["ReferenceKey", "Value"],
            validateDataFunction = self._validateReferenceValuesConfig
        )

    def saveDqRefPairMetadataConfig(self, configPath: Union[str, list], dataPath: str) -> bool:
        """Saves and validates DQ Reference Pair Values metadata config.

        Returns:
            Flag whether the content of the config has changed.
        """
        if configPath is None:
            return False
        
        return self._configGeneratorHelper.saveMetadataConfigAsSCDType2(
            configPath,
            METADATA_DQ_REFERENCE_PAIR_VALUES_SCHEMA,
            dataPath,
            self.METADATA_DQ_REFERENCE_PAIR_VALUES_TABLE,
            ["ReferenceKey1", "Value1", "ReferenceKey2", "Value2"],
            validateDataFunction = self._validateReferencePairValuesConfig
        )
    
    def saveConfigFileMetadataConfig(self, configPath: Union[str, list], dataPath: str) -> bool:
        
        if configPath is None:
            return False
        
        return self._configGeneratorHelper.saveMetadataConfigAsSCDType2(
                    configPath,
            METADATA_DQ_CONFIGURATION_SCHEMA,
            dataPath,
            self.METADATA_CONFIGURATION_FILE_VALIDATION_TABLE,
            ["DatabaseName", "EntityName", "ExpectationType", "KwArgs"]
        )

    @staticmethod
    def _getTableNamesMatchingWildcard(wildcard: str, tableNames: ArrayType) -> ArrayType:
        result = []

        wildcard = wildcard.replace(" ", "").lower()

        try:
            leftPart = wildcard.split("[")[0]
            rightPart = wildcard.split("]")[1]
            leftAndRightPartLength = len(leftPart) + len(rightPart)
            middlePart = wildcard.split("[")[1].split("]")[0]
            middlePartOptions = middlePart.split(",")
        except:
            print(f"""Exception details
                Wildcard: '{wildcard}'
                Wildcard expected format: 'Starts[*]Ends' or 'Starts[A,B,C]Ends' where Starts and Ends are optional
            """)
            raise

        for tableName in tableNames:
            tableNameLower = tableName.lower()

            if middlePart == "*":
                if (tableNameLower.startswith(leftPart)
                    and tableNameLower.endswith(rightPart)
                    and len(tableNameLower) >= leftAndRightPartLength
                ):
                    result.append(tableName)
            else:
                for middlePartOption in middlePartOptions:
                    if tableNameLower == f"{leftPart}{middlePartOption}{rightPart}":
                        result.append(tableName)

        return result

    @staticmethod
    def _isTableNameMatchingWildcard(wildcard: str, tableName: str) -> bool:
        tableNamesMatching = Validator._getTableNamesMatchingWildcard(
            wildcard,
            [tableName]
        )

        return len(tableNamesMatching) == 1 and tableNamesMatching[0] == tableName

    def addValidationStatusForTablesInHive(self):
        """Adds validation status column name to all tables that should be validated."""

        if not self._isDQConfigurationDefined():
            return
        
        entitiesWithActiveExpectations = self._spark.sql(f"""
            SELECT DISTINCT DatabaseName, EntityName
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._validationConfigurationTable}
            WHERE {IS_ACTIVE_RECORD_COLUMN_NAME} AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE
        """).rdd.collect()
        
        tableNamesPerDatabase = {}

        for table in entitiesWithActiveExpectations:
            try:
                databaseName = table["DatabaseName"]
                entityName = table["EntityName"]
                tableName = None
                sqlCommand = None
                
                # check if database already exists in HIVE
                if self._spark.sql("SHOW DATABASES").filter(f"databaseName == '{databaseName.lower()}'").count() == 1:
                    if "[" in entityName and "]" in entityName:
                        # if entity name is a wildcard, get matching table names
                        if databaseName not in tableNamesPerDatabase.keys():
                            tableNamesInDatabase = (
                                self._spark.sql(f"""
                                    SELECT DISTINCT EntityName 
                                    FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}
                                    WHERE DatabaseName = '{databaseName}'
                                """)
                                .rdd.map(lambda x: x[0])
                            )
                            tableNamesInDatabase = tableNamesInDatabase.take(tableNamesInDatabase.count())
                            tableNamesPerDatabase[databaseName] = tableNamesInDatabase
                        
                        tableNames = Validator._getTableNamesMatchingWildcard(
                            entityName,
                            tableNamesPerDatabase[databaseName]
                        )
                    else:
                        # if entity name is not a wildcard, it's an exact table name
                        tableNames = [entityName]

                    for tableName in tableNames:
                        # check if table exists in HIVE
                        if tableName.lower() not in self._sqlContext.tableNames(databaseName):
                            continue

                        # check if validation column does not exist
                        validationColumnPresent = (
                            self._spark.sql(f"DESC FORMATTED {databaseName}.{tableName}")
                            .filter(f"col_name == '{VALIDATION_STATUS_COLUMN_NAME}'")
                            .count() == 1
                        )
                        
                        if not validationColumnPresent:
                            sqlCommand = f"""ALTER TABLE {databaseName}.{tableName} ADD COLUMN {VALIDATION_STATUS_COLUMN_NAME} STRING"""

                            self._dataLakeHelper.allowAlterForTable(f"{databaseName}.{tableName}")
                            self._spark.sql(sqlCommand)
            except:
                print(f"""Exception details
                    Entity name: {entityName}
                    HIVE table: {databaseName}.{tableName}
                    SQL command: {sqlCommand}
                """)
                raise
    
    def filterOutQuarantinedRecords(self, dataDF) -> DataFrame:
        """Filters out quarantined records from a DataFrame.

        If DataFrame does not contain validation status column, all records are returned.
        
        Returns:
            DataFrame containing records with validation status other then quarantined.
        """

        if (dataDF is not None
            and VALIDATION_STATUS_COLUMN_NAME in dataDF.schema.fieldNames()
        ):
            return (
                dataDF
                .filter(f"""
                    {VALIDATION_STATUS_COLUMN_NAME} IS NULL
                    OR {VALIDATION_STATUS_COLUMN_NAME} != '{self.VALIDATION_STATUS_QUARANTINED}'
                """)
                .drop(VALIDATION_STATUS_COLUMN_NAME)
            )
        else:
            return dataDF
    
    def _isDQConfigurationDefined(self) -> bool:
        return self._validationConfigurationTable.lower() in self._sqlContext.tableNames(self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"])
    
    def _getDQConfiguration(self, databaseName: str, tableName: str) -> DataFrame:
        prioritizedExpectations = """(
            'expect_table_columns_to_match_set',
            'expect_table_columns_to_match_ordered_list',
            'expect_column_values_to_be_of_type'
        )"""
        
        dqConfigurationDF = self._spark.sql(f"""
            SELECT WK_ID, ExpectationType, KwArgs, Quarantine, Description, DQLogOutput, EntityName,
                CASE WHEN ExpectationType IN {prioritizedExpectations} THEN 1 
                     ELSE 2 END AS OrderPrioritized        
                FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._validationConfigurationTable}
            WHERE lower(DatabaseName) = lower('{databaseName}')
                AND {IS_ACTIVE_RECORD_COLUMN_NAME} AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE
        """)
        
        #1 - exact match
        matchingDQConfigurationDF = dqConfigurationDF.filter(f"lower(EntityName) = lower('{tableName}')")
        
        #2 - wildcard match
        wildcardDQConfigurationDF = dqConfigurationDF.filter(("EntityName like '%[%'") and ("EntityName like '%]%'"))

        if wildcardDQConfigurationDF.count() > 0:
            # collect WK_IDs of matching wildcard rules
            matchingWildcardDQConfigurationIDs = []

            for wildcardDQConfiguration in wildcardDQConfigurationDF.rdd.collect():
                if Validator._isTableNameMatchingWildcard(wildcardDQConfiguration["EntityName"], tableName):
                    matchingWildcardDQConfigurationIDs.append(wildcardDQConfiguration["WK_ID"])
            
            # collect matching wildcard rules
            matchingWildcardDQConfigurationDF = wildcardDQConfigurationDF.join(
                self._spark.createDataFrame(
                    [[value] for value in matchingWildcardDQConfigurationIDs],
                    f"""WK_ID {dict(wildcardDQConfigurationDF.dtypes)["WK_ID"]}"""
                ).distinct(),
                on="WK_ID"
            )

            matchingDQConfigurationDF = (
                matchingDQConfigurationDF
                .union(
                    matchingWildcardDQConfigurationDF
                    .select([
                        "WK_ID",
                        "ExpectationType",
                        "KwArgs",
                        "Quarantine",
                        "Description",
                        "DQLogOutput",
                        "EntityName",
                        "OrderPrioritized"
                    ])
                )
            )
        
        return (
            matchingDQConfigurationDF
            .select("WK_ID", "ExpectationType", "KwArgs", "Quarantine", "Description", "DQLogOutput")
            .sort("OrderPrioritized")
        )
    
    def hasToBeValidated(self, databaseName: str, tableName: str) -> bool:
        """Returns flag whether a table has to be validated.
        
        Returns:
            True if table is supposed to be validated.
        """

        return (
            self._isDQConfigurationDefined()
            and self._getDQConfiguration(databaseName, tableName).limit(1).count() == 1
        )
    
    def _getDQReferenceValues(self, referenceKey: str) -> ArrayType:
        values = self._spark.sql(f"""
            SELECT Value
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self.METADATA_DQ_REFERENCE_VALUES_TABLE}
            WHERE lower(ReferenceKey) = lower('{referenceKey.replace("#", "")}')
                AND {IS_ACTIVE_RECORD_COLUMN_NAME}
                AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE
        """).rdd.map(lambda x: x[0])
        
        return (values.take(values.count()) if values.count() > 0 else referenceKey)
    
    def _getDQReferencePairValues(self, referenceKeyPair: str) -> ArrayType:
        referenceKeys = referenceKeyPair[1:].split('#')
        
        values = self._spark.sql(f"""
            SELECT Value1, Value2
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self.METADATA_DQ_REFERENCE_PAIR_VALUES_TABLE}
            WHERE lower(ReferenceKey1) = lower('{referenceKeys[0]}')
                AND lower(ReferenceKey2) = lower('{referenceKeys[1]}')
                AND {IS_ACTIVE_RECORD_COLUMN_NAME}
                AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE
            UNION ALL
            SELECT Value2, Value1
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self.METADATA_DQ_REFERENCE_PAIR_VALUES_TABLE}
            WHERE lower(ReferenceKey2) = lower('{referenceKeys[0]}')
                AND lower(ReferenceKey1) = lower('{referenceKeys[1]}')
                AND {IS_ACTIVE_RECORD_COLUMN_NAME}
                AND {IS_DELETED_RECORD_COLUMN_NAME} = FALSE
        """).rdd.map(lambda x: (x[0], x[1]))
        
        return (values.take(values.count()) if values.count() > 0 else referenceKeyPair)
    
    def _getExpectationKwArgs(self, kwArgsString: str) -> MapType:
        kwArgs = json.loads(kwArgsString)
        
        if ("value_set" in kwArgs.keys()
            and str(type(kwArgs["value_set"])) == "<class 'str'>"
            and kwArgs["value_set"].startswith("#")
           ):
            # replace reference key with values in value_set argument
            kwArgs["value_set"] = self._getDQReferenceValues(kwArgs["value_set"])
        elif ("value_pairs_set" in kwArgs.keys()
              and str(type(kwArgs["value_pairs_set"])) == "<class 'str'>"
              and kwArgs["value_pairs_set"].startswith("#")
             ):
            # replace reference key pair with pair values in value_pairs_set argument
            kwArgs["value_pairs_set"] = self._getDQReferencePairValues(kwArgs["value_pairs_set"])
        elif ("value_pairs_set" in kwArgs.keys()
              and str(type(kwArgs["value_pairs_set"])) == "<class 'list'>"
              and str(type(kwArgs["value_pairs_set"][0])) == "<class 'list'>"
             ):
            # remap list of lists to list of tuples
            remappedList = []
            for item in kwArgs["value_pairs_set"]:
                remappedList.append((item[0], item[1]))
            kwArgs["value_pairs_set"] = remappedList
        elif "condition" in kwArgs.keys() and "#" in kwArgs["condition"]:
            # replace all reference keys with values in condition argument
            referenceKeys = re.findall(r"#\w+", kwArgs["condition"])
            for referenceKey in referenceKeys:
                referenceValues = self._getDQReferenceValues(referenceKey)
                if str(type(referenceValues)) == "<class 'list'>":
                    kwArgs["condition"] = (
                        kwArgs["condition"]
                        .replace(referenceKey, f"""'{"', '".join(referenceValues)}'""")
                    )
        
        return kwArgs
    
    def _getSuiteConfig(self, databaseName: str, tableName: str) -> Tuple[MapType, MapType]:
        expectationsDF = self._getDQConfiguration(databaseName, tableName)

        geSuiteConfig = {
            "expectation_suite_name": "greatExpectationsSuite",
            "expectations": []
        }
        
        customSuiteConfig = {
            "expectation_suite_name": "customRulesSuite",
            "expectations": []
        }

        for expectation in expectationsDF.rdd.collect():
            try:
                if expectation["ExpectationType"] in ["expect_column_values_to_not_match_condition","compare_datasets"]:
                    suite = customSuiteConfig
                else:
                    suite = geSuiteConfig
                
                suite["expectations"].append({
                    "expectation_type": expectation["ExpectationType"],
                    "kwargs": self._getExpectationKwArgs(expectation["KwArgs"]),
                    "meta": {
                        "WK_ID": expectation["WK_ID"],
                        "Quarantine": expectation["Quarantine"],
                        "Description": expectation["Description"],
                        "DQLogOutput": expectation["DQLogOutput"],
                    }
                })
            except:
                print(f"""Exception details
                    WK_ID: {expectation["WK_ID"]}
                    ExpectationType: {expectation["ExpectationType"]}
                    KwArgs: {expectation["KwArgs"]}
                """)
                raise
            
        return (geSuiteConfig, customSuiteConfig)
    
    def _findJoinTableColumns(self, dqLogOutputColumns: str, joinTable: str) -> list:
        """Extract JoinTable columns from dqLogOutputColumns.

        Returns:
        - list: List of JoinTable columns.
        """

        pattern = re.compile(fr'\b{joinTable}\.(\w+)\b')
        joinTableColumns = re.findall(pattern, dqLogOutputColumns)
        return joinTableColumns

    def _getAdditionalColumns(self, dqLogOutputColumns: str, joinTable: str, condition: str) -> str:
        joinTableColumns = self._findJoinTableColumns(dqLogOutputColumns, joinTable)
        
        # if GROUP BY is in condition, check if the additional columns are present in the condition and only keep those:
        if "GROUP BY" in condition:
            pattern1 = re.compile(rf"\b{joinTable}\.({'|'.join(joinTableColumns)})\b")
            joinTableColumns = pattern1.findall(condition)  
        
        if joinTableColumns:
            additionalColumns = ", " + ", ".join([f"{joinTable}.{col} as joinTable_{col}" for col in joinTableColumns])
        else:
            additionalColumns = ""

        if "GROUP BY" not in condition:
            additionalColumns += f", {self.MGT_TEMP_ID_COLUMN_NAME}"

        return additionalColumns
      
    def _getAdditionalData(self, invalidDF: DataFrame, dataDF: DataFrame, condition: str, column: str) -> list[dict]:
        """Identify JoinTable values from invalidDF.

        Returns:
        - List[dict]: Additional data based on the condition.
        """

        if "GROUP BY" in condition:
            # since we aren't selecting MGT_TEMP_id in invalidDF when GROUP BY is present, select MGT_TEMP_id from original DF
            invalidDF = (
                invalidDF
                .join(dataDF, (invalidDF[f"{column}"] == dataDF[f"{column}"]), "left")
                .select(invalidDF["*"], dataDF[f"{self.MGT_TEMP_ID_COLUMN_NAME}"])
            )
        
        joinTableData = (
            invalidDF
            .select([col for col in invalidDF.columns if col.startswith("joinTable_") or col == f"{self.MGT_TEMP_ID_COLUMN_NAME}"])
            .collect()
        )

        additionalData = [row.asDict() for row in joinTableData]
        return additionalData
    
    def _getDqLogOutputColumns(self, result: Type["great_expectations.dataset.dataset.Dataset"], invalidDF: DataFrame) -> Tuple[list, DataFrame]:
        """Extract dqLogOutput columns from invalidDF based on joinTable conditions.

        Returns:
        - List: dqLogOutput columns.
        """
        
        # add additional_columns to json output column from joinTable
        if "joinTable" in result.expectation_config.kwargs.keys():
            joinTable = result.expectation_config.kwargs["joinTable"]
        else:
            joinTable = None
        
        if joinTable and joinTable in result.expectation_config.meta["DQLogOutput"]:
            joinTableColumns = self._findJoinTableColumns(result.expectation_config.meta["DQLogOutput"], joinTable)

            # extract the schema for joinTable columns
            joinTableFields = [
                StructField("joinTable_" + field.name, field.dataType, field.nullable) 
                for field in self._spark.sql(f"SELECT * FROM {joinTable} LIMIT 1").schema.fields 
                if field.name in joinTableColumns
            ]
            joinTableFields.append(StructField(self.MGT_TEMP_ID_COLUMN_NAME, LongType(), False))
            
            additionalDataSchema = StructType(joinTableFields)

            # additional_data in result has the fields from join table logged with prefix joinTable_
            additionalDataDF = (
                self._spark.createDataFrame(data=result.result["additional_data"], schema=additionalDataSchema)
                .withColumnRenamed(f"{self.MGT_TEMP_ID_COLUMN_NAME}", f"joinTable_{self.MGT_TEMP_ID_COLUMN_NAME}")
            )

            # join this dataFrame with invalidDF based on MGT_TEMP_ID
            invalidDF = invalidDF.join(
                additionalDataDF,
                (invalidDF[f"{self.MGT_TEMP_ID_COLUMN_NAME}"] == additionalDataDF[f"joinTable_{self.MGT_TEMP_ID_COLUMN_NAME}"]),
                "left"
            )

            dqLogOutputColumns = result.expectation_config.meta["DQLogOutput"].replace(" ", "").replace(f"{joinTable}.", "joinTable_").split(",")
        else:
            dqLogOutputColumns = result.expectation_config.meta["DQLogOutput"].replace(" ", "").split(",")

        dqLogOutputColumns = [col for col in dqLogOutputColumns if col in invalidDF.columns]
        return dqLogOutputColumns, invalidDF

    def _getAdditionalDataCompareDataset(self, invalidDF: DataFrame, comparedColumn: list, expectationMatch: str) -> list[dict]:

        columnName = f"joinTable_{comparedColumn[0].split(',')[1]}"
        missingDF = invalidDF.filter(col(columnName).isNotNull())
        missingRows = [missingDF.columns] + [list(row) for row in missingDF.collect()]
        unexpectedDF = invalidDF.filter(col(columnName).isNull())
        unexpectedRows = [unexpectedDF.columns] + [list(row) for row in unexpectedDF.collect()]

        additionalData = []
        # Append data based on expectationMatch
        if expectationMatch == "fullMatch":
            additionalData.append(
                {
                    "missingRows": missingRows, 
                    "unexpectedRows": unexpectedRows
                }
            )
        elif expectationMatch == "noMissing":
            additionalData.append({"missingRows": missingRows})
        elif expectationMatch == "noAddition":
            additionalData.append({"unexpectedRows": unexpectedRows})
        return additionalData
    
    def _getInvalidDFCompareDataset(self, columns: str, joinTable: str, condition: str, joinOnColumns: str, nullCheckCondition: str) -> DataFrame:
        invalidDF = self._spark.sql(f"""
            SELECT {columns} FROM
            (SELECT * FROM table WHERE EXISTS (SELECT NULL FROM {joinTable} WHERE {condition})) AS table
            FULL JOIN
            (SELECT * FROM {joinTable} WHERE EXISTS (SELECT NULL FROM table WHERE {condition})) AS joinTable
            ON {joinOnColumns}
            AND {condition.replace(joinTable, "joinTable")}
            WHERE {nullCheckCondition}
        """)
        return invalidDF
    
    def _processCustomExpectationRule(self, expectation: dict, dataDF: DataFrame) -> Tuple[list, list]:
        if "column" in expectation["kwargs"].keys():
            column = expectation["kwargs"]["column"]
        else:
            column = self.MGT_TEMP_ID_COLUMN_NAME
        
        condition = expectation["kwargs"]["condition"]
        additionalData = []
        
        # identify invalid records
        if ("joinTable" not in expectation["kwargs"].keys()
            and "joinColumns" not in expectation["kwargs"].keys()
            ):
            invalidDF = self._spark.sql(f"""
                SELECT {column}
                FROM table
                WHERE {condition}
            """)
        elif ("joinTable" in expectation["kwargs"].keys()
                and "joinColumns" in expectation["kwargs"].keys()
                ):
            joinTable = expectation["kwargs"]["joinTable"]
            joinType = expectation["kwargs"]["joinType"]

            # transforms "CustomerId,CustomerNo|SystemId,SysId"
            # to "table.CustomerId = joinTable.CustomerNo AND table.SystemId = joinTable.SysId"
            joinOnColumns = map(lambda x: x.split(","), expectation["kwargs"]["joinColumns"].replace(" ", "").split("|"))
            joinOnColumns = map(lambda x: f"""table.{x[0]} = {joinTable}.{x[1]}""", joinOnColumns)
            joinOnColumns = " AND ".join(list(joinOnColumns))

            # alter condition if group by is used - prepend truthy condition statement
            if condition.strip().upper().startswith("GROUP BY"):
                condition = f"TRUE {condition}"

            # select columns from joinTable only when data from joinTable has to be logged
            dqLogOutputColumns = expectation["meta"]["DQLogOutput"]
            if joinTable in dqLogOutputColumns:
                additionalColumns = self._getAdditionalColumns(dqLogOutputColumns, joinTable, condition)
            else:
                additionalColumns = ""

            invalidDF = self._spark.sql(f"""
                SELECT table.{column}{additionalColumns}
                FROM table
                {joinType} JOIN {joinTable} ON {joinOnColumns}
                WHERE {condition}
            """)

            # collect additionalData from joinTable to add to result
            if joinTable in dqLogOutputColumns:
                additionalData = self._getAdditionalData(invalidDF, dataDF, condition, column)                 
        else:
            raise NotImplementedError

        invalidArray = invalidDF.select(invalidDF.columns[0]).rdd.map(lambda x: x[0])
        unexpectedList = invalidArray.take(invalidArray.count())
        return unexpectedList, additionalData
    
    def _processCompareDatasetRule(self, expectation: dict) -> Tuple[list, list]:
        joinTable = expectation["kwargs"]["compareAgainst"]
        comparedColumn = expectation["kwargs"]["comparedColumn"].replace(" ", "").split("|")
        dqLogOutputColumns = expectation["meta"]["DQLogOutput"].replace(" ", "").split(",")
        expectationMatch = expectation["kwargs"]["expectation"]

        if "condition" in expectation["kwargs"].keys():
            condition = expectation["kwargs"]["condition"]
        else:
            condition = "TRUE"

        joinOnColumns = []
        comparedColumns = []
        for column_pair in comparedColumn:
            joinOnColumns.append(f"table.{column_pair.split(',')[0]} = joinTable.{column_pair.split(',')[1]}")
            comparedColumns.append(f"table.{column_pair.split(',')[0]},joinTable.{column_pair.split(',')[1]} as joinTable_{column_pair.split(',')[1]}")

        joinOnColumns = " AND ".join(joinOnColumns)
        comparedColumns = ",".join(comparedColumns)
        # here we take only the first pair
        nullCheckCondition = (f"table.{comparedColumn[0].split(',')[0]} IS NULL OR joinTable.{comparedColumn[0].split(',')[1]} IS NULL")

        # select columns from dqLogOutputColumns and take unique values
        if dqLogOutputColumns:
            columnsList = [
            f"table.{column}" if not column.startswith(joinTable) else
            f"joinTable.{','.join(self._findJoinTableColumns(column, joinTable))} as joinTable_{','.join(self._findJoinTableColumns(column, joinTable))}"
            for column in dqLogOutputColumns
            ]
            comparedColumnsList = comparedColumns.split(",")
            columnsList = comparedColumnsList + columnsList
            columns = []
            [columns.append(x) for x in columnsList if x not in columns]
            columns = ",".join(columns)
        else:
            columns = comparedColumns

        # set unexpectedList to None for dataset based validator and collect additionalData
        unexpectedList = None
        joinTableCount = self._spark.sql(f"select count(*) as rowCount from {joinTable}").first()[0]

        if joinTableCount == 0 and expectationMatch in ("noAddition","fullMatch"):
            additionalData = "There is no data existing in compared table that matches the condition"
        else:
            # identify invalid records
            invalidDF = self._getInvalidDFCompareDataset(columns, joinTable, condition, joinOnColumns, nullCheckCondition)
            if invalidDF.isEmpty():
                additionalData = None
            else:
                additionalData = self._getAdditionalDataCompareDataset(invalidDF, comparedColumn, expectationMatch)
        return unexpectedList, additionalData

    def _validateCustomRules(self, dataDF: DataFrame, customSuiteConfig: MapType, geOutput: Type["great_expectations.dataset.dataset.Dataset"]) -> Type["great_expectations.dataset.dataset.Dataset"]:
        # class definitions
        class ExpectationConfig:
            def __init__(self, expectation):
                self.expectation_type = expectation["expectation_type"]
                self.kwargs = expectation["kwargs"]
                self.meta = expectation["meta"]

        class ExpectationResult:
            def __init__(self, expectation: MapType, unexpectedList: ArrayType, additionalData: ArrayType):
                self.success = (
                    (unexpectedList is None and additionalData is None) or
                    (unexpectedList is not None and len(unexpectedList) == 0)
                )
                self.expectation_config = ExpectationConfig(expectation)
                if unexpectedList is None:
                    if additionalData is not None:
                        self.result = {"details": {"details": additionalData}}
                else:
                    self.result = {
                        "unexpected_list": unexpectedList,
                        "additional_data": additionalData
                    }

            def keys(self) -> ArrayType:
                return ["success", "expectation_config", "result"]    

        # register DataFrame as temporary view
        dataDF.createOrReplaceTempView("table")
        
        # evaluate expectations
        for expectation in customSuiteConfig["expectations"]:
            try:
                if expectation["expectation_type"] == "expect_column_values_to_not_match_condition":
                    unexpectedList, additionalData = self._processCustomExpectationRule(expectation, dataDF)
                elif expectation["expectation_type"] == "compare_datasets":
                    unexpectedList, additionalData = self._processCompareDatasetRule(expectation)
                else:
                    raise NotImplementedError

                # create expectation result
                result = ExpectationResult(expectation, unexpectedList, additionalData)
                
                # add result into great expectation output and update overall success flag
                geOutput.results.append(result)
                if geOutput.success and not result.success:
                    geOutput.success = False
            except:
                print(f"""Exception details
                    WK_ID: {expectation["meta"]["WK_ID"]}
                    ExpectationType: {expectation["expectation_type"]}
                    KwArgs: {json.dumps(expectation["kwargs"])}
                """)
                raise
        
        return geOutput
    
    def _isResultSuccessful(self, result: Type["great_expectations.dataset.dataset.Dataset"]) -> bool:
        if not result.success:
            if (result.expectation_config.expectation_type == "expect_table_columns_to_match_set"
                and "details" in result.result.keys()
                and "mismatched" in result.result["details"]
                and "missing" not in result.result["details"]["mismatched"]
                and "unexpected" in result.result["details"]["mismatched"]
            ):
                # if there is no missing column and there are only unexpected columns starting
                # with '__eval_col_' then validation is successful
                unexpected = result.result["details"]["mismatched"]["unexpected"]
                systemGenerated = [col for col in unexpected if col.startswith("__eval_col_")]
                return len(systemGenerated) == len(unexpected)
            elif (result.expectation_config.expectation_type == "expect_table_columns_to_match_ordered_list"
                  and "details" in result.result.keys()
                  and "mismatched" in result.result["details"]
            ):
                # if there are only unexpected columns starting with '__eval_col_' then validation is successful
                mismatched = result.result["details"]["mismatched"]
                systemGenerated = [col for col in mismatched if (col["Expected"] == None and col["Found"].startswith("__eval_col_"))]
                return len(systemGenerated) == len(mismatched)
        
        return result.success
    
    def _getInvalidData(self, dataDF: DataFrame, result: Type["great_expectations.dataset.dataset.Dataset"]) -> DataFrame:
        # if DataFrame-based validator has failed, return whole DataFrame
        if "unexpected_list" not in result.result.keys():
            return dataDF
        
        # if row-based validator has failed, return invalid rows
        if ("column_A" in result.expectation_config.kwargs.keys()
            and "column_B" in result.expectation_config.kwargs.keys()
        ):
            # colum pair validator
            columnNameA = result.expectation_config.kwargs["column_A"]
            columnNameB = result.expectation_config.kwargs["column_B"]
            
            unexpectedListSchema = StructType([
                StructField(columnNameA, dataDF.schema[columnNameA].dataType, True),
                StructField(columnNameB, dataDF.schema[columnNameB].dataType, True)
            ])
            unexpectedListDF = (
                self._spark.createDataFrame(data=result.result["unexpected_list"], schema=unexpectedListSchema)
                .distinct()
            )
            
            return (
                dataDF
                .alias("dataDF")
                .join(
                    unexpectedListDF,
                    (dataDF[columnNameA] == unexpectedListDF[columnNameA]) & (dataDF[columnNameB] == unexpectedListDF[columnNameB]),
                    "inner"
                )
                .select("dataDF.*")
            )
        else:
            if "column_list" in result.expectation_config.kwargs.keys():
                # multicolumn validator
                filterString = (
                    ') OR ('
                    .join(
                        ' AND '
                        .join(
                            [f"{k} = '{v}'" for (k, v) in combination.items()]
                        ) for combination in result.result["unexpected_list"]
                    )
                )
                return dataDF.filter(f"({filterString})")
            else:
                # column validator
                if "column" in result.expectation_config.kwargs.keys():
                    columnName = result.expectation_config.kwargs["column"]
                else:
                    columnName = self.MGT_TEMP_ID_COLUMN_NAME
                
                column = dataDF[columnName]

                if result.expectation_config.expectation_type == "expect_column_values_to_not_be_null":
                    return dataDF.filter(column.isNull())
                else:
                    # Command "dataDF.filter(column.isin())" was causing performance issue. It took 200 minutes
                    # to identify 2.3M invalid rows from 2.4M rows in dataDF based on an array of 2.3M values.
                    # The solution is to use join instead which requires converting array of invalid values
                    # into a DataFrame. If the size of this DataFrame is small enough to fit into worker's memory,
                    # Databricks will use broadcast join under the hood, which significantly improves processing
                    # time to just couple of seconds. If the DataFrame does not fit into memory, regular join
                    # operation will be used.

                    # return dataDF.filter(column.isin(result.result["unexpected_list"]))

                    return dataDF.join(
                        self._spark.createDataFrame(
                            [[value] for value in result.result["unexpected_list"]],
                            f"`{columnName}` {dict(dataDF.dtypes)[columnName]}"
                        ).distinct(),
                        on=columnName
                    )
    
    def createDQLogTable(self, dataPath: str):
        """Creates DQ Log table in HIVE.
        
        DQ Log table is created in metadata database and is used to store
        validation log data.
        """

        if not self._isDQConfigurationDefined():
            return
        
        self._spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self.METADATA_DQ_LOG_TABLE} (
                FwkEntityId STRING,
                ExpectationType STRING,
                KwArgs STRING,
                Description STRING,
                Output STRING,
                Quarantine BOOLEAN,
                ValidationDatetime TIMESTAMP,
                EntRunId STRING,
                EntRunDatetime TIMESTAMP
            )
            USING DELTA
            LOCATION "{dataPath}/{self.METADATA_DQ_LOG_TABLE}"
        """)
    
    def _writeDQLogData(self, dqLogDF: DataFrame):
        if self._logToDatabase:
            # determine DQ log delta table path
            if self.METADATA_DQ_LOG_TABLE_PATH is None:
                self.METADATA_DQ_LOG_TABLE_PATH = (
                    self._spark.sql(f"""DESC FORMATTED {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self.METADATA_DQ_LOG_TABLE}""")
                    .filter("col_name == 'Location'")
                    .collect()[0].data_type
                ).replace("dbfs:", "")

            # append data into DQ log
            self._dataLakeHelper.writeData(
                dqLogDF,
                self.METADATA_DQ_LOG_TABLE_PATH,
                WRITE_MODE_APPEND,
                "delta",
                {},
                self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"],
                self.METADATA_DQ_LOG_TABLE
            )
        else:
            if not self._dqLogToDisplayDF:
                self._dqLogToDisplayDF = dqLogDF
            else:
                self._dqLogToDisplayDF = self._dqLogToDisplayDF.union(dqLogDF)

    def _customEncoder(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, bytes):
            return obj.decode("utf-8")
        else:
            return str(obj)
    
    def _logValidationResult(self, result: Type["great_expectations.dataset.dataset.Dataset"], invalidDF: DataFrame, databaseName: str, tableName: str):
        
        if "unexpected_list" in result.result.keys():
            # row-based validator
            # create json output column (collect values from specified columns)
            if result.expectation_config.meta["DQLogOutput"] is not None:
                # add additional_columns to json output column from joinTable
                dqLogOutputColumns, invalidDF = self._getDqLogOutputColumns(result, invalidDF)
                outputColumn = to_json(struct(dqLogOutputColumns))
            else:
                outputColumn = lit(None)
        else:
            # DataFrame-based validator
            if "details" in result.result.keys():
                if result.expectation_config.expectation_type != "compare_datasets":
                    result.result["details"]["RowCount"] = invalidDF.count()

                outputColumn = lit(json.dumps(result.result["details"], default = self._customEncoder))
            else:
                details = { "RowCount": invalidDF.count() }
                outputColumn = lit(json.dumps(details))
            
            invalidDF = invalidDF.limit(1)
        
        if "result_format" in result.expectation_config.kwargs.keys():
            result.expectation_config.kwargs.pop("result_format")
        
        # create DQ log DataFrame
        dqLogDF = (
            invalidDF
            .withColumn("Output", outputColumn)
            .withColumn("FwkEntityId", lit(f"{databaseName}.{tableName}"))
            .withColumn("ExpectationType", lit(result.expectation_config.expectation_type))
            .withColumn("KwArgs", lit(json.dumps(result.expectation_config.kwargs)))
            .withColumn("Description", lit(result.expectation_config.meta["Description"]))
            .withColumn("Quarantine", lit(result.expectation_config.meta["Quarantine"]))
            .withColumn("ValidationDatetime", lit(datetimeNow))
            .withColumn("EntRunId", lit(self._entRunId))
            .withColumn("EntRunDatetime", lit(self._entRunDatetime))
            .select(self.METADATA_DQ_LOG_TABLE_COLUMNS)
        )
        
        self._writeDQLogData(dqLogDF)
    
    def _logValidationSummary(self, validationDF: DataFrame, databaseName: str, tableName: str):
        
        details = {
            "TotalRowCount": validationDF.count(),
            "ValidRowCount": validationDF.filter(f"{VALIDATION_STATUS_COLUMN_NAME} == '{self.VALIDATION_STATUS_VALID}'").count(),
            "InvalidRowCount": validationDF.filter(f"{VALIDATION_STATUS_COLUMN_NAME} == '{self.VALIDATION_STATUS_INVALID}'").count(),
            "QuarantinedRowCount": validationDF.filter(f"{VALIDATION_STATUS_COLUMN_NAME} == '{self.VALIDATION_STATUS_QUARANTINED}'").count()
        }
        
        # create DQ log DataFrame
        dqLogDF = (
            validationDF
            .limit(1)
            .withColumn("Output", lit(json.dumps(details)))
            .withColumn("FwkEntityId", lit(f"{databaseName}.{tableName}"))
            .withColumn("ExpectationType", lit(None).cast("string"))
            .withColumn("KwArgs", lit("{}"))
            .withColumn("Description", lit("Summary"))
            .withColumn("Quarantine", lit(None).cast("boolean"))
            .withColumn("ValidationDatetime", lit(datetimeNow))
            .withColumn("EntRunId", lit(self._entRunId))
            .withColumn("EntRunDatetime", lit(self._entRunDatetime))
            .select(self.METADATA_DQ_LOG_TABLE_COLUMNS)
        )
        
        self._writeDQLogData(dqLogDF)
    
    def _getValidationResult(self, validationDF: DataFrame, databaseName: str, tableName: str) -> Tuple[Type["great_expectations.dataset.dataset.Dataset"], DataFrame]:
        # prepare validation suite
        (geSuiteConfig, customSuiteConfig) = self._getSuiteConfig(databaseName, tableName)
        
        # validate DataFrame with great expectation suite
        geSuite = great_expectations.core.ExpectationSuite(**geSuiteConfig)
        geDataDS = great_expectations.dataset.SparkDFDataset(validationDF)
        geOutput = geDataDS.validate(geSuite, result_format="COMPLETE")
        
        # add unique identifier to each row
        validationDF = validationDF.withColumn(
            self.MGT_TEMP_ID_COLUMN_NAME,
            monotonically_increasing_id()
        )
        
        # validate DataFrame with custom suite
        geOutput = self._validateCustomRules(validationDF, customSuiteConfig, geOutput)

        return (geOutput, validationDF)
    
    def _setValidationStatus(self, validationDF: DataFrame, rowIDs: ArrayType, validationStatus: str) -> (DataFrame):
        if not rowIDs.any():
            return validationDF
        
        if len(rowIDs) == validationDF.count():
            validationDF = validationDF.withColumn(VALIDATION_STATUS_COLUMN_NAME, lit(validationStatus))
        else:
            matchingRowIDsRecordsDF = (
                self._spark.createDataFrame(rowIDs, f"{self.MGT_TEMP_ID_COLUMN_NAME} long")
                .withColumn("MGT_UpdatedValidationStatus", lit(validationStatus))
            )
            
            validationDF = (
                validationDF
                .join(matchingRowIDsRecordsDF, self.MGT_TEMP_ID_COLUMN_NAME, "left")
                .withColumn(VALIDATION_STATUS_COLUMN_NAME,
                    when(col("MGT_UpdatedValidationStatus").isNotNull(), col("MGT_UpdatedValidationStatus"))
                    .otherwise(col(VALIDATION_STATUS_COLUMN_NAME))
                )
                .drop("MGT_UpdatedValidationStatus")
            )

        return validationDF
    
    def _processNonValidRecords(self, validationDF: DataFrame, databaseName: str, tableName: str, geOutput: Type["great_expectations.dataset.dataset.Dataset"]) -> DataFrame:
        invalidRowIDs = np.array([])
        quarantinedRowIDs = np.array([])
        
        # identity invalid and quarantined records by looping through all validation rule results

        for result in geOutput.results:
            # check for exception
            if ("exception_info" in result.keys()
                and result["exception_info"]["raised_exception"]
            ):
                # if there's an exception regarding missing column, which means that the framework is not able to validate the expectation,
                # instead of propagating the exception, framework will ignore it, because anyway the expectation is marked as not successful
                # and all rows will be marked as INVALID or QUARANTINED
                if (re.search("Column .* does not exist.", result["exception_info"]["exception_message"])
                    or re.search("A column or function parameter with name .* cannot be resolved.", result["exception_info"]["exception_message"])
                   ):
                    print(f"""Following exception regarding missing column was handled by the framework instead of being propagated:\n{result}""")
                else:
                    raise Exception(result)
            
            try:
                if not self._isResultSuccessful(result):
                    # filter invalid records
                    invalidDF = self._getInvalidData(validationDF, result)
                    
                    # log validation result into DQLog
                    self._logValidationResult(result, invalidDF, databaseName, tableName)
                    
                    # collect IDs of quarantined / invalid records
                    rowIDs = invalidDF.select(self.MGT_TEMP_ID_COLUMN_NAME).rdd.map(lambda x: x[0])
                    rowIDs = rowIDs.take(rowIDs.count())

                    if result.expectation_config.meta["Quarantine"]:
                        quarantinedRowIDs = np.unique(np.concatenate((quarantinedRowIDs, rowIDs)))
                    else:
                        invalidRowIDs = np.unique(np.concatenate((invalidRowIDs, rowIDs)))
            except:
                print(f"""Exception details
                    WK_ID: {result.expectation_config.meta["WK_ID"]}
                    ExpectationType: {result.expectation_config.expectation_type}
                    KwArgs: {json.dumps(result.expectation_config.kwargs)}
                """)
                raise
        
        # reflect validation status in validationDF

        if len(quarantinedRowIDs) == validationDF.count():
            # if all records are identified as quarantined, none of them should be marked as invalid
            # as quarantined status is has higher priority then invalid status
            invalidRowIDs = np.array([])
        else:
            # invalid records should only be those that were not identified as quarantined
            invalidRowIDs = np.setdiff1d(invalidRowIDs, quarantinedRowIDs)

        validationDF = self._setValidationStatus(validationDF, quarantinedRowIDs, self.VALIDATION_STATUS_QUARANTINED)
        validationDF = self._setValidationStatus(validationDF, invalidRowIDs, self.VALIDATION_STATUS_INVALID)

        return validationDF
    
    def _handleEmptyDFForCompareDataset(self, validationDF: DataFrame, databaseName: str, tableName: str):

        dqConfigurationDF = self._getDQConfiguration(databaseName, tableName) if validationDF is None or validationDF.count() == 0 else None
        compareDatasetRuleDF = dqConfigurationDF.filter("ExpectationType = 'compare_datasets'").first() if dqConfigurationDF else None
        expectationMatch = json.loads(compareDatasetRuleDF.KwArgs)['expectation'] if compareDatasetRuleDF else None
        
        if expectationMatch in ("noMissing","fullMatch"):
            tempDF = self._spark.createDataFrame([[1]], f"{self.MGT_TEMP_ID_COLUMN_NAME} long")
            outputColumn = """{"details": "There is no data existing in compared table that matches the condition"}"""
            dqLogDF = (
                tempDF
                .withColumn("Output", lit(outputColumn))
                .withColumn("FwkEntityId", lit(f"{databaseName}.{tableName}"))
                .withColumn("ExpectationType", lit(compareDatasetRuleDF.ExpectationType))
                .withColumn("KwArgs", lit(compareDatasetRuleDF.KwArgs))
                .withColumn("Description", lit(compareDatasetRuleDF.Description))
                .withColumn("Quarantine", lit(compareDatasetRuleDF.Quarantine))
                .withColumn("ValidationDatetime", lit(datetimeNow))
                .withColumn("EntRunId", lit(self._entRunId))
                .withColumn("EntRunDatetime", lit(self._entRunDatetime))
                .select(self.METADATA_DQ_LOG_TABLE_COLUMNS)
            )

            self._writeDQLogData(dqLogDF)

    def validate(self, validationDF: DataFrame, databaseName: str, tableName: str):
        """Validates data in the DataFrame and appends validation status column.

        Data are validated against expectations defined in DQ Configuration and
        data quality log is created.
        """
        
        self._handleEmptyDFForCompareDataset(validationDF, databaseName, tableName)

        if validationDF is None :
            return validationDF
        elif validationDF.count() == 0:
            return validationDF.withColumn(f"{VALIDATION_STATUS_COLUMN_NAME}", lit(""))
        
        # validate data and get validation result
        (geOutput, validationDF) = self._getValidationResult(validationDF, databaseName, tableName)
        
        # mark all records valid
        validationDF = validationDF.withColumn(
            VALIDATION_STATUS_COLUMN_NAME,
            lit(self.VALIDATION_STATUS_VALID)
        )

        self._dqLogToDisplayDF = None
        
        if not geOutput.success:
            # mark records with invalid and quarantined status and log them to DQLog
            validationDF = self._processNonValidRecords(validationDF, databaseName, tableName, geOutput)        
        
        self._logValidationSummary(validationDF, databaseName, tableName)
        
        # if validator works in a mode to display data instead of logging, display it
        if self._dqLogToDisplayDF:
            self._display(self._dqLogToDisplayDF)
        
        return validationDF.drop(self.MGT_TEMP_ID_COLUMN_NAME)
