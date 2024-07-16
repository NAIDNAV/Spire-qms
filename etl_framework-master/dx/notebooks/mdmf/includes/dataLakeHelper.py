# Databricks notebook source
import json
import numpy as np
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Row, Column
from pyspark.sql.types import ArrayType, MapType
from pyspark.sql.functions import col, lit, concat_ws, sha2, row_number
from pyspark.sql.window import Window
from typing import Tuple

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.includes.frameworkConfig import *


class DataLakeHelper:
    """DataLakeHelper contains helper functions related to writing data to
    ADLS storage and interacting with HIVE.

    Public methods:
        getADLSParams
        fileExists
        allowAlterForTable
        writeData
        writeCSV
        writeDataAsParquet
    """

    FWK_TECHNICAL_COLUMN_NAMES = [
        VALIDATION_STATUS_COLUMN_NAME,
        INSERT_TIME_COLUMN_NAME,
        UPDATE_TIME_COLUMN_NAME,
        IS_ACTIVE_RECORD_COLUMN_NAME,
        IS_DELETED_RECORD_COLUMN_NAME,
        VALID_FROM_DATETIME_COLUMN_NAME,
        VALID_UNTIL_DATETIME_COLUMN_NAME,
        BUSINESS_KEYS_HASH_COLUMN_NAME,
        VALUE_KEY_HASH_COLUMN_NAME,
        "MGT_TEMP_watermark"
    ]

    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._sqlContext = globals()["sqlContext"] if "sqlContext" in globals() else None
        self._dbutils = globals()["dbutils"] if "dbutils" in globals() else None
        self._DeltaTable = DeltaTable
        

    @staticmethod
    def getADLSParams(instanceUrl: str, path: str) -> Tuple[str, str]:    
        """Derives abfss storage parameters from https format.
        
        Returns:
            ADLS name.
            Full data uri.
        """
        
        adlsUri = instanceUrl.split("//")[1].rstrip("/")
        adlsName = adlsUri.split(".")[0]
        containerName = path.split("/")[0]
        pathWithoutContainerName = path.replace(containerName, '', 1)
        dataUri = f"abfss://{containerName}@{adlsUri}{pathWithoutContainerName}"
        
        return (
            adlsName,
            dataUri
        )

    def fileExists(self, path: str) -> bool:
        """Returns whether file exists.
        
        Returns:
            True if file exists.
        """

        try:
            self._dbutils.fs.ls(path)
            return True
        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                return False
            else:
                print(f"""Exception Details
                    Command: dbutils.fs.ls("{path}")""")
                raise

    def allowAlterForTable(self, fullTableName: str):
        """Enables altering of a table in HIVE."""

        self._spark.sql(f"""
            ALTER TABLE {fullTableName}
            SET TBLPROPERTIES (
                'delta.columnMapping.mode' = 'name',
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '6'
            )"""
        )
    
    def _getIdentityColumn(self, databaseName: str, tableName: str) -> Row:
        return (
            self._spark.sql(f"""
                SELECT *
                FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}
                WHERE DatabaseName = '{databaseName}'
                    AND EntityName = '{tableName}'
                    AND IsIdentity
            """)
            .first()
        )

    def _evolveSchema(self, databaseName: str, tableName: str, sourceDF: DataFrame) -> ArrayType:
        targetColumns = self._spark.sql(f"SELECT * FROM {databaseName}.{tableName}").limit(0).columns
        newColumns = [col for col in sourceDF.columns if col not in targetColumns]
        
        if newColumns:
            sourceColumnTypes = dict(sourceDF.dtypes)
            addColumns = []
            for newColumn in newColumns:
                addColumns.append(f"`{newColumn}` {sourceColumnTypes[newColumn]}")
            
            sqlCommand = f"""ALTER TABLE {databaseName}.{tableName} ADD COLUMNS ({", ".join(addColumns)})"""
            
            self.allowAlterForTable(f"{databaseName}.{tableName}")
            self._spark.sql(sqlCommand)
    
    def _registerTableInHive(self, dataPath: str, databaseName: str, tableName: str):
        if dataPath and databaseName and tableName:            
            self._spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {databaseName}.{tableName}
                USING DELTA
                LOCATION "{dataPath}"
            """)
    
    def _registerTableWithIdentityColumnInHive(self, sinkDF: DataFrame, dataPath: str, format: str, params: MapType, databaseName: str, tableName: str) -> bool:
        if "createIdentityColumn" in params.keys() and params["createIdentityColumn"]:
            tableColumns = []
            for field in sinkDF.schema.fields:
                tableColumns.append(f"{field.name} {field.dataType.typeName()}")

            sqlCommand = f"""
                CREATE TABLE IF NOT EXISTS {databaseName}.{tableName} (
                    WK_ID BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                    {", ".join(tableColumns)}
                )
                USING {format.upper()}
                LOCATION "{dataPath}"
            """
            self._spark.sql(sqlCommand)

            return True
    
        return False

    def _validateWriteMode(self, writeMode: str, format: str, dataPath: str) -> str:
        try:
            isDeltaTable = self._DeltaTable.isDeltaTable(self._spark, dataPath)
        except:
            print(f"""Exception Details
                Command: DeltaTable.isDeltaTable(spark, "{dataPath}")""")
            raise
        if (format == "delta"
            and writeMode in [
                WRITE_MODE_SCD_TYPE1, 
                WRITE_MODE_SCD_TYPE2, 
                WRITE_MODE_SCD_TYPE2_DELTA, 
                WRITE_MODE_SNAPSHOT
            ]
            and not isDeltaTable
        ):
            # If user selected SCD Type 1/2 or Snapshot and there's no sink Delta table yet (this is the first creation on it), 
            # then change write mode to Overwrite
            writeMode = WRITE_MODE_OVERWRITE
        elif (format == "delta"
            and writeMode == WRITE_MODE_SCD_TYPE2_DELETE 
            and not isDeltaTable
        ):
            # If user selected SCD Type 2 Delete and there's no sink Delta table yet then write operation is skipped
            # because there's no table to delete data from
            return None
        
        return writeMode

    def writeData(self, sinkDF: DataFrame, dataPath: str, writeMode: str, format: str, params: MapType, databaseName: str, tableName: str) -> MapType:
        """Writes data to sink storage as Delta and returns write statistics.

        It validates desired write mode and it changes it if necessary (e.g. desired write mode
        is SCD Type 2, but there is no delta table yet, so data are stored with Override mode
        instead of merge)

        Once the data are stored, it's registered as a table in HIVE.
        
        Returns:
            Statistics about how many records were inserted, updated and deleted.
        """
        
        statistics = {
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }

        if sinkDF is None:
            return statistics

        # Validate write mode

        newWriteMode = self._validateWriteMode(writeMode, format, dataPath)
        
        if not newWriteMode:
            print(f"'{writeMode}' skipped for table '{databaseName}.{tableName}' - no Delta table present.")
            return statistics
        elif newWriteMode != writeMode:
            if writeMode in [WRITE_MODE_SCD_TYPE2, WRITE_MODE_SCD_TYPE2_DELTA]:
                sinkDF = self._prepareDataForSCDType2FirstWrite(sinkDF, params)

            print(f"Changing writeMode from '{writeMode}' to '{newWriteMode}' for {databaseName}.{tableName}")
            writeMode = newWriteMode

        # Perform write operation

        if writeMode == WRITE_MODE_SCD_TYPE1:
            statistics = self._writeDataSCDType1(
                sinkDF,
                dataPath,
                params,
                databaseName,
                tableName
            )
        elif writeMode in [WRITE_MODE_SCD_TYPE2, WRITE_MODE_SCD_TYPE2_DELTA]:
            statistics = self._writeDataSCDType2(
                sinkDF,
                dataPath,
                writeMode,
                format,
                params,
                databaseName,
                tableName
            )
        elif writeMode == WRITE_MODE_SCD_TYPE2_DELETE:
            statistics = self._writeDataSCDType2Delete(
                sinkDF,
                dataPath,
                writeMode,
                format,
                params,
                databaseName,
                tableName
            )
        elif writeMode == WRITE_MODE_SNAPSHOT:
            statistics = self._writeDataSnapshot(
                sinkDF,
                dataPath,
                format,
                params,
                databaseName,
                tableName
            )
        elif writeMode in [WRITE_MODE_OVERWRITE, WRITE_MODE_APPEND]:
            statistics = self._writeDataOverwriteOrAppend(
                sinkDF,
                dataPath,
                writeMode,
                format,
                params,
                databaseName,
                tableName
            )
        else:
            assert False, f"The write mode '{writeMode}' is not supported"
        
        self._registerTableInHive(dataPath, databaseName, tableName)

        return statistics

    def _writeDataSCDType1(self, sinkDF: DataFrame, dataPath: str, params: MapType, databaseName: str, tableName: str) -> MapType:
        if sinkDF.count() == 0:
            return {
                "recordsInserted": 0,
                "recordsUpdated": 0,
                "recordsDeleted": 0
            }
        
        # Evolve schema

        if "schemaEvolution" in params.keys() and params["schemaEvolution"]:
            self._evolveSchema(databaseName, tableName, sinkDF)

        # Get columns from sink table in HIVE

        columns = self._spark.sql(f"SELECT * FROM {databaseName}.{tableName}").limit(1).columns

        # if table has identity column, it is excluded from update and insert statement
        identityColumn = self._getIdentityColumn(databaseName, tableName)
        if identityColumn:
            columns.remove(identityColumn["Attribute"])

        # Prepare insert & update statement and merge keys

        insertStatement = {}
        for column in columns:
            if column in sinkDF.columns:
                insertStatement[column] = f"updates.{column}"

        updateStatement = insertStatement.copy()

        if INSERT_TIME_COLUMN_NAME in sinkDF.schema.fieldNames():
            updateStatement.pop(INSERT_TIME_COLUMN_NAME)

        mergeKeys = []
        for key in params["keyColumns"]:
            mergeKeys.append((f"table.{key} = updates.{key}"))

        
        try:
            sinkTable = self._DeltaTable.forPath(self._spark, dataPath)
        except:
            print(f"""Exception Details
                Command: DeltaTable.forPath(spark, "{dataPath}")""")
            raise
        
        # Perform merge    
        (
            sinkTable
            .alias("table")
            .merge(
                sinkDF.alias("updates"),
                " AND ".join(mergeKeys)
            )
            .whenMatchedUpdate(set = updateStatement) 
            .whenNotMatchedInsert(values = insertStatement)
            .execute()
        )
    
        # Collect statistics
        
        if INSERT_TIME_COLUMN_NAME in sinkDF.schema.fieldNames():
            lastSinkInsert = sinkDF.first()[INSERT_TIME_COLUMN_NAME]
            countInserted = self._spark.sql(f"SELECT * FROM {databaseName}.{tableName} WHERE {INSERT_TIME_COLUMN_NAME} = '{lastSinkInsert}'").count()
            statistics = {
                "recordsInserted": countInserted,
                "recordsUpdated": sinkDF.count() - countInserted,
                "recordsDeleted": 0
            }
        else:
            statistics = None
            
        return statistics
    
    def _calculateHashesForSCDType2(self, sinkDF: DataFrame, params: MapType) -> Tuple[DataFrame, ArrayType]:
        if not sinkDF:
            return (sinkDF, [])
        
        assert "keyColumns" in params.keys(), "'keyColumns' is a mandatory property in 'params' argument"
        
        valColumns = [col for col in sinkDF.columns if (col not in params["keyColumns"] and col not in self.FWK_TECHNICAL_COLUMN_NAMES)]
        
        sinkDF = (
            sinkDF
            .withColumn(BUSINESS_KEYS_HASH_COLUMN_NAME, sha2(concat_ws("|", *params["keyColumns"]), 256))
            .withColumn(VALUE_KEY_HASH_COLUMN_NAME, sha2(concat_ws("|", *valColumns), 256))
        )

        return (sinkDF, valColumns)

    def _handleWatermarkColumnForSCDType2(self, sinkDF: DataFrame, params: MapType, populateOnlyWhenMissing: bool) -> Tuple[DataFrame, MapType]:
        if not sinkDF:
            return (sinkDF, params)
        
        assert "entTriggerTime" in params.keys(), "'entTriggerTime' is a mandatory property in 'params' argument"
        
        isMissing = False
        watermarkColumnSpecified = "watermarkColumn" in params.keys()
        if not watermarkColumnSpecified or (watermarkColumnSpecified and not params["watermarkColumn"]):
            # create temporary watermark column if there is none (applies to situation when data were full loaded)
            params["watermarkColumn"] = "MGT_TEMP_watermark"
            isMissing = True

        if (populateOnlyWhenMissing and isMissing) or not populateOnlyWhenMissing:
            sinkDF = sinkDF.withColumn(params["watermarkColumn"], lit(params["entTriggerTime"]))

        return (sinkDF, params)

    @staticmethod
    def _getMaxDate() -> datetime:
        return datetime(9999, 12, 31)

    def _prepareDataForSCDType2FirstWrite(self, sinkDF: DataFrame, params: MapType) -> DataFrame:
        # Initial dataFrame enhancements

        (sinkDF, valColumns) = self._calculateHashesForSCDType2(sinkDF, params)
        (sinkDF, params) = self._handleWatermarkColumnForSCDType2(sinkDF, params, True)
        
        # Populate columns

        # populate - technical columns
        sinkDF = (
            sinkDF
            .withColumn(IS_ACTIVE_RECORD_COLUMN_NAME, lit(True))
            .withColumn(IS_DELETED_RECORD_COLUMN_NAME, lit(False))
            .withColumn(VALID_FROM_DATETIME_COLUMN_NAME, col(params["watermarkColumn"]))
            .withColumn(VALID_UNTIL_DATETIME_COLUMN_NAME, lit(self._getMaxDate()))
        )
        
        # populate - other technical columns
        optionalColumns = []
        if VALIDATION_STATUS_COLUMN_NAME in sinkDF.schema.fieldNames():
            optionalColumns.append(VALIDATION_STATUS_COLUMN_NAME)
        if INSERT_TIME_COLUMN_NAME in sinkDF.schema.fieldNames():
            optionalColumns.append(INSERT_TIME_COLUMN_NAME)
        if UPDATE_TIME_COLUMN_NAME in sinkDF.schema.fieldNames():
            optionalColumns.append(UPDATE_TIME_COLUMN_NAME)
        
        # Prepare final dataFrame

        selectedColumns = (np.concatenate((
            params["keyColumns"],
            valColumns,
            [
                IS_ACTIVE_RECORD_COLUMN_NAME,
                IS_DELETED_RECORD_COLUMN_NAME, 
                VALID_FROM_DATETIME_COLUMN_NAME, 
                VALID_UNTIL_DATETIME_COLUMN_NAME, 
                BUSINESS_KEYS_HASH_COLUMN_NAME, 
                VALUE_KEY_HASH_COLUMN_NAME
            ],
            optionalColumns
        )))
        
        return sinkDF.select(*selectedColumns)

    def _writeDataSCDType2(self, sinkDF: DataFrame, dataPath: str, writeMode: str, format: str, params: MapType, databaseName: str, tableName: str) -> MapType:
        assert writeMode in [
                WRITE_MODE_SCD_TYPE2,
                WRITE_MODE_SCD_TYPE2_DELTA
            ], f"The write mode '{writeMode}' is not supported"
        
        # Initial dataFrame enhancements

        (sinkDF, valColumns) = self._calculateHashesForSCDType2(sinkDF, params)
        (sinkDF, params) = self._handleWatermarkColumnForSCDType2(sinkDF, params, True)
        
        # Identify records to insert
        try:
            tableCurrentDF = (
                self._spark.read.format(format)
                .load(dataPath)
                .filter(f"""{IS_ACTIVE_RECORD_COLUMN_NAME} = true""")
            )
        except:
            print(f"""Exception Details
                Command: spark.read.format("{format}").load("{dataPath}")""")
            raise

        updatedRecordsToInsertDF = (
            sinkDF
            .alias("updates")
            .join(tableCurrentDF.select(BUSINESS_KEYS_HASH_COLUMN_NAME, VALUE_KEY_HASH_COLUMN_NAME, IS_DELETED_RECORD_COLUMN_NAME).alias("table"), BUSINESS_KEYS_HASH_COLUMN_NAME, "leftouter")
            .where(f"""(updates.{VALUE_KEY_HASH_COLUMN_NAME} <> table.{VALUE_KEY_HASH_COLUMN_NAME}) OR table.{BUSINESS_KEYS_HASH_COLUMN_NAME} is null OR (updates.{VALUE_KEY_HASH_COLUMN_NAME} = table.{VALUE_KEY_HASH_COLUMN_NAME} AND table.{IS_DELETED_RECORD_COLUMN_NAME})""")
            .selectExpr("updates.*", f"""table.{VALUE_KEY_HASH_COLUMN_NAME} as target_{VALUE_KEY_HASH_COLUMN_NAME}""", IS_DELETED_RECORD_COLUMN_NAME)
        )

        # Stage the update by union-ing two sets of rows
        # 1. Rows that will be inserted in the `whenNotMatchedInsert` clause (updated current + new records + reopened deleted records)
        # 2. Rows that will update the current values of existing data in the `whenMatchedUpdate` clause (closed current records)
        
        countInserted = updatedRecordsToInsertDF.filter((f"""{BUSINESS_KEYS_HASH_COLUMN_NAME} is not null""") and (f"""target_{VALUE_KEY_HASH_COLUMN_NAME} is null""")).count()
        countUpdated = updatedRecordsToInsertDF.filter(f"""target_{VALUE_KEY_HASH_COLUMN_NAME} is not null""").count()
        countDeleted = 0

        stagedUpdatesDF = (
            updatedRecordsToInsertDF
            .selectExpr("NULL as mergeKey", "updates.*", f"""FALSE as {IS_DELETED_RECORD_COLUMN_NAME}""") # Rows for 1.
            .union(
                updatedRecordsToInsertDF
                .where(f"""target_{VALUE_KEY_HASH_COLUMN_NAME} is not null""")
                .selectExpr(f"""updates.{BUSINESS_KEYS_HASH_COLUMN_NAME} as mergeKey""", "updates.*", IS_DELETED_RECORD_COLUMN_NAME) # Rows for 2.
            )
        )
        
        # Handle deletions
        
        if writeMode == WRITE_MODE_SCD_TYPE2:
            updatedRecordsToDeletedDF = (
                tableCurrentDF
                .filter(f"""{IS_DELETED_RECORD_COLUMN_NAME} = false""")
                .alias("table")
                .join(sinkDF.alias("updates"), BUSINESS_KEYS_HASH_COLUMN_NAME, "left")
                .where(f"""updates.{BUSINESS_KEYS_HASH_COLUMN_NAME} IS NULL""")
                .select("table.*")
            )
            countDeleted = updatedRecordsToDeletedDF.count()

            # Align columns to staged updates
            
            if INSERT_TIME_COLUMN_NAME in sinkDF.schema.fieldNames() and UPDATE_TIME_COLUMN_NAME in sinkDF.schema.fieldNames():            
                updatedRecordsToDeletedDF = (
                    updatedRecordsToDeletedDF
                    .withColumn(INSERT_TIME_COLUMN_NAME, lit(params["executionStart"]))
                    .withColumn(UPDATE_TIME_COLUMN_NAME, lit(params["executionStart"]))
                )
            
            updatedRecordsToDeletedDF = (
                updatedRecordsToDeletedDF
                .withColumn(params["watermarkColumn"], lit(params["entTriggerTime"]))
                .withColumn("mergeKey", lit(None))
                .select(stagedUpdatesDF.columns)
            )
            
            # Stage the deletes by union-ing two sets of rows
            # 1. Rows that will be inserted in the `whenNotMatchedInsert` clause (deleted records)
            # 2. Rows that will update the current values of existing data in the `whenMatchedUpdate` clause (closed current records)
            
            stagedUpdatesDF = (stagedUpdatesDF
                .union(
                    updatedRecordsToDeletedDF
                    .withColumn(IS_DELETED_RECORD_COLUMN_NAME, lit(True)) # Rows for 1.
                )
                .union(
                    updatedRecordsToDeletedDF
                    .withColumn("mergeKey", col(BUSINESS_KEYS_HASH_COLUMN_NAME)) # Rows for 2.
                    .withColumn(VALUE_KEY_HASH_COLUMN_NAME, lit("different value"))
                )
            )

        # Collect statistics

        statistics = {
            "recordsInserted": countInserted,
            "recordsUpdated": countUpdated,
            "recordsDeleted": countDeleted
        }

        if stagedUpdatesDF.count() == 0:
            print(f"Merge skipped for {databaseName}.{tableName}")
            return statistics

        # Evolve schema

        if "schemaEvolution" in params.keys() and params["schemaEvolution"]:
            # omit internal temporary technical column for schema evolution
            self._evolveSchema(databaseName, tableName, sinkDF.drop("MGT_TEMP_watermark"))

        # Prepare insert & update statement
    
        # insert - technical columns
        insertStatement = {
            IS_ACTIVE_RECORD_COLUMN_NAME: lit(True),
            IS_DELETED_RECORD_COLUMN_NAME: col(IS_DELETED_RECORD_COLUMN_NAME),
            VALID_FROM_DATETIME_COLUMN_NAME: col(params["watermarkColumn"]),
            VALID_UNTIL_DATETIME_COLUMN_NAME: lit(self._getMaxDate()),
            BUSINESS_KEYS_HASH_COLUMN_NAME: sha2(concat_ws("|", *params["keyColumns"]), 256),
            VALUE_KEY_HASH_COLUMN_NAME: sha2(concat_ws("|", *valColumns), 256)
        }

        # insert - business columns
        for colName in params["keyColumns"]:
            insertStatement[colName] = col(f"{colName}")
        
        # insert - value columns
        for colName in valColumns:
            insertStatement[colName] = col(f"{colName}")
        
        # insert - other technical columns
        if VALIDATION_STATUS_COLUMN_NAME in sinkDF.schema.fieldNames():
            insertStatement[VALIDATION_STATUS_COLUMN_NAME] = col(VALIDATION_STATUS_COLUMN_NAME)
            validationStatusPresent = "1 = 1"
            validationStatusSet = {
                VALIDATION_STATUS_COLUMN_NAME: col(f"staged_updates.{VALIDATION_STATUS_COLUMN_NAME}")
            }
        else:
            validationStatusPresent = "0 = 1"
            validationStatusSet = {}
            
        if INSERT_TIME_COLUMN_NAME in sinkDF.schema.fieldNames():
            insertStatement[INSERT_TIME_COLUMN_NAME] = col(INSERT_TIME_COLUMN_NAME)
        
        # update update-time column for records that will be closed (MGT_isCurrent = False)
        if UPDATE_TIME_COLUMN_NAME in sinkDF.schema.fieldNames():
            insertStatement[UPDATE_TIME_COLUMN_NAME] = col(UPDATE_TIME_COLUMN_NAME)
            setUpdateTime = {
                UPDATE_TIME_COLUMN_NAME: col(f"staged_updates.{UPDATE_TIME_COLUMN_NAME}")
            }
        else:
            setUpdateTime = {}
            
        # Apply SCD Type 2
        try:
            sinkTable = self._DeltaTable.forPath(self._spark, dataPath)
        except:
            print(f"""Exception Details
                Command: DeltaTable.forPath(spark, "{dataPath}")""")
            raise
        (
            sinkTable
            .alias("table")
            .merge(
                stagedUpdatesDF.alias("staged_updates"),
                f"""table.{IS_ACTIVE_RECORD_COLUMN_NAME} = true and table.{BUSINESS_KEYS_HASH_COLUMN_NAME} = mergeKey"""
            )
            .whenMatchedUpdate(
                condition = f"""(table.{VALUE_KEY_HASH_COLUMN_NAME} <> staged_updates.{VALUE_KEY_HASH_COLUMN_NAME}) OR (table.{VALUE_KEY_HASH_COLUMN_NAME} = staged_updates.{VALUE_KEY_HASH_COLUMN_NAME} AND table.{IS_DELETED_RECORD_COLUMN_NAME})""",
                set = {
                    IS_ACTIVE_RECORD_COLUMN_NAME: lit(False),
                    VALID_UNTIL_DATETIME_COLUMN_NAME: col(f"""staged_updates.{params["watermarkColumn"]}"""),
                    **setUpdateTime
                }
            )
            .whenMatchedUpdate(
                condition = f"""{validationStatusPresent} AND table.{VALUE_KEY_HASH_COLUMN_NAME} == staged_updates.{VALUE_KEY_HASH_COLUMN_NAME}""",
                set = validationStatusSet
            )
            .whenNotMatchedInsert(values = insertStatement)
            .execute()
        )

        return statistics

    def _writeDataSCDType2Delete(self, sinkDF: DataFrame, dataPath: str, writeMode: str, format: str, params: MapType, databaseName: str, tableName: str) -> MapType:
        sinkDF = (
            sinkDF
            .withColumn(BUSINESS_KEYS_HASH_COLUMN_NAME, sha2(concat_ws("|", *params["keyColumns"]), 256))
        )

        # Identify deleted records
        try:
            tableCurrentDF = (
                self._spark.read.format(format)
                .load(dataPath)
                .filter(f"""{IS_ACTIVE_RECORD_COLUMN_NAME} = true""")
            )
        except:
            print(f"""Exception Description
                Command: spark.read.format("{format}").load("{dataPath}")""")
            raise
            
        updatedRecordsToDeletedDF = (tableCurrentDF
            .filter(f"""{IS_DELETED_RECORD_COLUMN_NAME} = false""")
            .alias("table")
            .join(sinkDF.alias("updates"), BUSINESS_KEYS_HASH_COLUMN_NAME, "left")
            .where(f"""updates.{BUSINESS_KEYS_HASH_COLUMN_NAME} IS NULL""")
            .select("table.*")
        )
        
        countDeleted = updatedRecordsToDeletedDF.count()

        if countDeleted == 0:
            print(f"'{writeMode}' skipped for table '{databaseName}.{tableName}' - no data for deletion.")
            return {
                "recordsInserted": 0,
                "recordsUpdated": 0,
                "recordsDeleted": 0
            }
        
        # Populate technical columns with respective datetimes
        
        (updatedRecordsToDeletedDF, params) = self._handleWatermarkColumnForSCDType2(updatedRecordsToDeletedDF, params, False)

        if (INSERT_TIME_COLUMN_NAME in sinkDF.schema.fieldNames()
            and UPDATE_TIME_COLUMN_NAME in sinkDF.schema.fieldNames()
            ):
            updatedRecordsToDeletedDF = (
                updatedRecordsToDeletedDF
                .withColumn(INSERT_TIME_COLUMN_NAME, lit(params["executionStart"]))
                .withColumn(UPDATE_TIME_COLUMN_NAME, lit(params["executionStart"]))
            )
        
        # Stage the deletes by union-ing two sets of rows
        # 1. Rows that will be inserted in the `whenNotMatchedInsert` clause (deleted records)
        # 2. Rows that will update the current values of existing data in the `whenMatchedUpdate` clause (closed current records)

        stagedUpdatesDF = (
            updatedRecordsToDeletedDF
            .withColumn(IS_DELETED_RECORD_COLUMN_NAME, lit(True)) # Rows for 1.
            .union(
                updatedRecordsToDeletedDF
                .withColumn(IS_DELETED_RECORD_COLUMN_NAME, lit(False)) # Rows for 2.
            )
        )

        # Prepare insert & update statement
        
        # insert - technical columns
        insertStatement = {
            IS_ACTIVE_RECORD_COLUMN_NAME: lit(True),
            IS_DELETED_RECORD_COLUMN_NAME: col(IS_DELETED_RECORD_COLUMN_NAME),
            VALID_FROM_DATETIME_COLUMN_NAME: col(params["watermarkColumn"]),
            VALID_UNTIL_DATETIME_COLUMN_NAME: lit(self._getMaxDate()),
            BUSINESS_KEYS_HASH_COLUMN_NAME: col(BUSINESS_KEYS_HASH_COLUMN_NAME),
            VALUE_KEY_HASH_COLUMN_NAME: col(VALUE_KEY_HASH_COLUMN_NAME)
        }

        # insert - business columns
        for colName in params["keyColumns"]:
            insertStatement[colName] = col(f"{colName}")
        
        # insert - value columns
        valColumns = [col for col in tableCurrentDF.columns if (
            (
                col not in params["keyColumns"]                 # not business columns
                and col not in self.FWK_TECHNICAL_COLUMN_NAMES  # not technical columns
            )
            or col in [                                         # allowed technical columns
                VALIDATION_STATUS_COLUMN_NAME,
                INSERT_TIME_COLUMN_NAME,
                UPDATE_TIME_COLUMN_NAME
            ]
        )]
        
        for colName in valColumns:
            insertStatement[colName] = col(f"{colName}")
        
        # update update-time column for records that will be closed (MGT_isCurrent = False)
        if UPDATE_TIME_COLUMN_NAME in sinkDF.schema.fieldNames():
            setUpdateTime = {
                UPDATE_TIME_COLUMN_NAME: col(f"staged_updates.{UPDATE_TIME_COLUMN_NAME}")
            }
        else:
            setUpdateTime = {}
        
        # Apply SCD Type 2 DELETE operation
        try:
            sinkTable = self._DeltaTable.forPath(self._spark, dataPath)
        except:
            print(f"""Exception Details
                Command: DeltaTable.forPath(spark, "{dataPath}")""")
            raise
        
        (
            sinkTable
            .alias("table")
            .merge(
                stagedUpdatesDF.alias("staged_updates"),
                f"""table.{IS_ACTIVE_RECORD_COLUMN_NAME} = true and staged_updates.{IS_DELETED_RECORD_COLUMN_NAME} = false and table.{BUSINESS_KEYS_HASH_COLUMN_NAME} = staged_updates.{BUSINESS_KEYS_HASH_COLUMN_NAME}"""
            )
            .whenMatchedUpdate(
                set = {
                    IS_ACTIVE_RECORD_COLUMN_NAME: lit(False),
                    VALID_UNTIL_DATETIME_COLUMN_NAME: col(f"""staged_updates.{params["watermarkColumn"]}"""),
                    **setUpdateTime
                }
            )
            .whenNotMatchedInsert(values = insertStatement)
            .execute()
        )

        # Collect statistics (business perspective, not technical)

        statistics = {
            "recordsInserted": 0,
            "recordsUpdated": 0,
            "recordsDeleted": countDeleted
        }

        return statistics

    def _deleteSnapshotData(self, sinkDF: DataFrame, params: MapType, databaseName: str, tableName: str) -> int:
        if (
            tableName.lower() not in self._sqlContext.tableNames(databaseName)
            or sinkDF.count() == 0
        ):
            return 0
        
        distinctSnapshotValues = (
            sinkDF
            .dropDuplicates(params["keyColumns"])
            .select(params["keyColumns"])
            .rdd.collect()
        )

        # Prepare delete and select count commands
        
        whereConditionPairs = []
        for snapshotValues in distinctSnapshotValues:
            columPairValues = []
            for index, column in enumerate(params["keyColumns"]):
                columPairValues.append(f"{column} = '{snapshotValues[index]}'")
            
            whereConditionPairs.append(f"""({" AND ".join(columPairValues)})""")
        
        whereCondition = " OR ".join(whereConditionPairs)
        selectCommand = f"SELECT count(*) AS countDeleted FROM {databaseName}.{tableName} WHERE {whereCondition}"
        deleteCommand = f"DELETE FROM {databaseName}.{tableName} WHERE {whereCondition}"

        # Execute select count

        countDeleted = (
            self._spark.sql(selectCommand)
            .first()["countDeleted"]
        )

        # Execute delete

        if countDeleted:
            self._spark.sql(deleteCommand)

        return countDeleted

    def _writeDataSnapshot(self, sinkDF: DataFrame, dataPath: str, format: str, params: MapType, databaseName: str, tableName: str) -> MapType:
        if sinkDF.count() == 0:
            return {
                "recordsInserted": 0,
                "recordsUpdated": 0,
                "recordsDeleted": 0
            }
        
        # Delete existing snapshot data
        
        countDeleted = self._deleteSnapshotData(sinkDF, params, databaseName, tableName)

        # Insert new snapshot data

        if "schemaEvolution" in params.keys() and params["schemaEvolution"]:
            mergeSchemaValue = "true"
        else:
            mergeSchemaValue = "false"
        
        try:
            (
                sinkDF.write
                .format(format)
                .mode("append")
                .option("mergeSchema", mergeSchemaValue)
                .save(dataPath)
            )
        except:
            print(f"""Exception Details
                Command: sinkDF.write.format("{format}").mode("append").option("mergeSchema", "{mergeSchemaValue}").save("{dataPath}")""")
            raise

        # Collect statistics

        statistics = {
            "recordsInserted": sinkDF.count(),
            "recordsUpdated": 0,
            "recordsDeleted": countDeleted
        }

        return statistics
    
    def _writeDataOverwriteOrAppend(self, sinkDF: DataFrame, dataPath: str, writeMode: str, format: str, params: MapType, databaseName: str, tableName: str) -> MapType:  
        # Create table with identity column upfront

        tableHasBeenRegistered = self._registerTableWithIdentityColumnInHive(sinkDF, dataPath, format, params, databaseName, tableName)
        
        if tableHasBeenRegistered:
            writeMode = WRITE_MODE_APPEND
        
        # Collect count of deleted rows

        if (writeMode == WRITE_MODE_OVERWRITE
            and self._spark.sql(f"SHOW DATABASES LIKE '{databaseName}'").count() == 1
            and tableName.lower() in self._sqlContext.tableNames(databaseName)
        ):
            countDeletedDF = self._spark.sql(f"SELECT count(*) AS countDeleted FROM {databaseName}.{tableName}")
            countDeleted = countDeletedDF.first()["countDeleted"]
        else:
            countDeleted = 0

        if writeMode == WRITE_MODE_OVERWRITE and ("schemaEvolution" not in params.keys() or not params["schemaEvolution"]):
            if self._spark.catalog.tableExists(f'{databaseName}.{tableName}'):
                columns = self._spark.sql(f"SELECT * FROM {databaseName}.{tableName}").limit(1).columns
                sinkDF = sinkDF.select([col for col in sinkDF.columns if col in columns])
            else:
                print(f"The table {databaseName}.{tableName} does not exist.")

        # Write data as overwrite or append

        writeDF = (
            sinkDF.write
            .format(format)
            .mode(writeMode)
        )

        if "schemaEvolution" in params.keys() and params["schemaEvolution"]:
            if writeMode == WRITE_MODE_OVERWRITE:
                writeDF = writeDF.option("overwriteSchema", "true")
            elif writeMode == WRITE_MODE_APPEND:
                writeDF = writeDF.option("mergeSchema", "true")
    
        if "partitionColumns" in params.keys() and params["partitionColumns"]:
            writeDF = writeDF.partitionBy(params["partitionColumns"])

        try:
            writeDF.save(dataPath)
        except:
            print(f"""Exception Details
                Command: writeDF.save("{dataPath}")""")
            raise

        # Collect statistics

        statistics = {
            "recordsInserted": sinkDF.count(),
            "recordsUpdated": 0,
            "recordsDeleted": countDeleted
        }

        return statistics

    def _readCSV(self, filePath: str, options: MapType) -> DataFrame:
        try:
            return self._spark.read.options(**options).csv(filePath)
        except:
            print(f"""Exception Details
                Command: spark.read.options({options}).csv("{filePath}")""")
            raise

    def _createSingleCSV(self, sinkDF: DataFrame, filePath: str, options: MapType):
        filePathTemp = filePath + ".tmp"
        
        try:
            sinkDF.coalesce(1).write.options(**options).csv(filePathTemp)
        except:
            print(f"""Exception Details
                Command: sinkDF.coalesce(1).write.options({options}).csv("{filePathTemp}")""")
            raise
        
        try:
            fileName = [f.name for f in self._dbutils.fs.ls(filePathTemp) if f.name.endswith(".csv")][0]
        except:
            print(f"""Exception Details
                Command: dbutils.fs.ls("{filePathTemp}")""")
            raise
        
        try:
            self._dbutils.fs.cp(f"{filePathTemp}/{fileName}", filePath)
        except:
            print(f"""Exception Details
                Command: dbutils.fs.cp("{filePathTemp}/{fileName}", "{filePath}")""")
            raise
        
        try:
            self._dbutils.fs.rm(filePathTemp, True)
        except:
            print(f"""Exception Details
                Command: dbutils.fs.rm("{filePathTemp}", True)""")
            raise
    
    @staticmethod
    def _dropDuplicates(dataDF: DataFrame, byColumn: ArrayType, orderSpec: Column) -> DataFrame:
        windowSpec = Window.partitionBy(byColumn).orderBy(orderSpec)
        dataDF = (
            dataDF
            .withColumn("row_number", row_number().over(windowSpec))
            .filter("row_number == 1")
            .drop("row_number")
        )
        return dataDF

    def writeCSV(self, sinkDF: DataFrame, filePath: str, writeMode: str, options: str = '{ "header": true, "nullValue": null }', dropDuplicatesByColumn: ArrayType = None, dropDuplicatesOrderSpec: Column = None):
        """Writes data to sink storage as CSV file.
        
        Returns:
            Statistics about how many records were inserted, updated and deleted.
        """
        
        assert writeMode in [WRITE_MODE_OVERWRITE, WRITE_MODE_APPEND], f"The write mode '{writeMode}' is not supported"
        
        if sinkDF is None:
            return {
                "recordsInserted": 0,
                "recordsUpdated": 0,
                "recordsDeleted": 0
            }
        
        # Prepare read options

        options = json.loads(options)

        # Read existing file for append

        if writeMode == WRITE_MODE_APPEND and self.fileExists(filePath):
            sinkCurrDF = self._readCSV(filePath, options)
            sinkDF = sinkCurrDF.union(sinkDF)

            if dropDuplicatesByColumn and dropDuplicatesOrderSpec is not None:
                sinkDF = self._dropDuplicates(sinkDF, dropDuplicatesByColumn, dropDuplicatesOrderSpec)
        
        # Write data

        self._createSingleCSV(sinkDF, filePath, options)

        # Collect statistics

        statistics = {
            "recordsInserted": sinkDF.count(),
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }

        return statistics
    
    def writeDataAsParquet(self, sinkDF: DataFrame, folderPath: str, writeMode: str, dropDuplicatesByColumn: ArrayType = None, dropDuplicatesOrderSpec: Column = None):
        """Writes data to sink storage as parquet file.
        
        Returns:
            Statistics about how many records were inserted, updated and deleted.
        """
        
        assert writeMode in [WRITE_MODE_OVERWRITE, WRITE_MODE_APPEND], f"The write mode '{writeMode}' is not supported"
        
        if sinkDF is None:
            return {
                "recordsInserted": 0,
                "recordsUpdated": 0,
                "recordsDeleted": 0
            }
           
        if dropDuplicatesByColumn and dropDuplicatesOrderSpec is not None:
            if writeMode == WRITE_MODE_APPEND and self.fileExists(folderPath):
                sinkCurrDF = self._spark.read.format("parquet").load(folderPath)
                sinkDF = sinkCurrDF.union(sinkDF)
                sinkDF.cache() # without these two steps spark is not able to overwrite the parquet files
                sinkDF.count()
                writeMode = WRITE_MODE_OVERWRITE

            sinkDF = self._dropDuplicates(sinkDF, dropDuplicatesByColumn, dropDuplicatesOrderSpec)
        
        # Write data
        sinkDF.write.mode(writeMode).parquet(folderPath)

        # Collect statistics

        statistics = {
            "recordsInserted": sinkDF.count(),
            "recordsUpdated": 0,
            "recordsDeleted": 0
        }

        return statistics
