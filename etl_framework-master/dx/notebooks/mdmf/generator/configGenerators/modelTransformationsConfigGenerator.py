# Databricks notebook source
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.functions import col, lit, monotonically_increasing_id
from pyspark.sql.types import ArrayType, MapType

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.generator.configGenerators.configGeneratorHelper import ConfigGeneratorHelper
    from notebooks.mdmf.includes.dataLakeHelper import DataLakeHelper
    from notebooks.mdmf.includes.frameworkConfig import *
    from notebooks.mdmf.maintenance.maintenanceManager import MaintenanceManager


class ModelTransformationsConfigGenerator:
    """ModelTransformationsConfigGenerator is responsible for generating model transformations
    configuration.

    It generates DtOutput.csv and FwkEntity.csv that contain instructions and configuration which
    are later used during ETL to transform data. It also generates metadata information about 
    all entities (tables) that are created as output (sink) of these transformations instructions.

    Public methods:
        createOrAlterDeltaTablesBasedOnMetadata
        generateConfiguration
    """

    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._sqlContext = globals()["sqlContext"] if "sqlContext" in globals() else None
        self._dataLakeHelper = DataLakeHelper(spark, compartmentConfig)
        self._compartmentConfig = compartmentConfig
        self._configGeneratorHelper = ConfigGeneratorHelper(spark, compartmentConfig)
        self._maintenanceManager = MaintenanceManager(spark, compartmentConfig)


    @staticmethod
    def _convertModelToSparkDataType(modelDataType: str) -> str:
        modelDataType = modelDataType.lower()
        
        typeMapping = {
            "bigint": "BIGINT",
            "tinyint": "INT",
            "int": "INT",
            "date": "DATE",
            "datetime": "TIMESTAMP"
        }
        
        if modelDataType.startswith("varchar") or modelDataType.startswith("char"):
            return "STRING"
        elif modelDataType.startswith("decimal") or modelDataType.startswith("numeric"):
            return "DECIMAL(10,0)" if not modelDataType[modelDataType.find("("):-1] else f"""DECIMAL{modelDataType[modelDataType.find("("):-1]})"""
        elif modelDataType.startswith("double"):
            return "DOUBLE"
        elif modelDataType in typeMapping.keys():
            return typeMapping[modelDataType]
        
        assert False, f"ModelDataType '{modelDataType}' is not supported"

    def _getBatchesBasedOnTableRelations(self, metadataAttributesTable: str, metadataRelationsTable: str) -> ArrayType:
        tablesToProcess = (
            self._spark.sql(f"""
                SELECT DISTINCT Entity 
                FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataAttributesTable}
            """)
            .rdd.collect()
        )
        relationsToProcess = (
            self._spark.sql(f"""
                SELECT DISTINCT Parent, Child 
                FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataRelationsTable}
            """)
            .rdd.collect()
        )

        batchNumber = 1
        batches = {}

        while len(tablesToProcess) > 0:
            # identify tables with no (remaining) dependencies
            noDependencyTables = []
            for table in tablesToProcess:
                tableHasDependency = False
                
                for relation in relationsToProcess:
                    if relation["Child"] == table["Entity"]:
                        tableHasDependency = True
                        break
                
                if tableHasDependency == False:
                    noDependencyTables.append(table)
            
            assert noDependencyTables, (
                f"Cyclic dependency found in batch {batchNumber}. Remaining tables to process: {len(tablesToProcess)}")

            # remove processed tables        
            tablesToProcess = [table for table in tablesToProcess if table not in noDependencyTables]
            
            # remove processed dependencies
            nextRelationsToProcess = []
            for relation in relationsToProcess:
                parentNotProcessedYet = True
                
                for table in noDependencyTables:
                    if relation["Parent"] == table["Entity"]:
                        parentNotProcessedYet = False
                        break
                
                if parentNotProcessedYet:
                    nextRelationsToProcess.append(relation)
            relationsToProcess = nextRelationsToProcess

            # collect table names for the current batch
            batches[batchNumber] = noDependencyTables        
            batchNumber = batchNumber + 1
            
        return batches
    
    def _getSQLQueryForTable(self, sinkDatabaseName: str, tableName: str, metadataRelationsTable: str, unknownMemberDefaultValue: int) -> str:
        selectParentPKs = ""
        joinParentsOnBKs = ""

        # extend sql query if table has parents with business key(s)

        parentTables = (
            self._spark.sql(f"""
                SELECT DISTINCT Parent, RolePlayingGroup
                FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataRelationsTable}
                WHERE Child = '{tableName}'
                    AND KeyType != 'PIdentifier'
            """)
            .rdd.collect()
        )

        if len(parentTables) > 0:
            for index, parentTable in enumerate(parentTables):
                parentTableName = parentTable["Parent"]
                rolePlayingGroupExpression = f"""= '{parentTable["RolePlayingGroup"]}'""" if parentTable["RolePlayingGroup"] is not None else "IS NULL"
                parentTableAlias = f"p{index + 1}"

                # select part

                parentPK = (
                    self._spark.sql(f"""
                        SELECT ParentAttribute, ChildAttribute
                        FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataRelationsTable}
                        WHERE Child = '{tableName}'
                            AND Parent = '{parentTableName}'
                            AND KeyType = 'PIdentifier'
                            AND RolePlayingGroup {rolePlayingGroupExpression}
                    """)
                    .first()
                )

                # if there is no PIdentifier relation, this relation should be skipped
                # and should not have an impact on SQL statement
                if not parentPK:
                    continue

                if unknownMemberDefaultValue is not None:
                    selectParentPKs += f", COALESCE({parentTableAlias}.{parentPK['ParentAttribute']}, {unknownMemberDefaultValue}) AS {parentPK['ChildAttribute']}"
                else:
                    selectParentPKs += f", {parentTableAlias}.{parentPK['ParentAttribute']} AS {parentPK['ChildAttribute']}"

                # join part
                joinParentsOnBKs += f" LEFT JOIN {sinkDatabaseName}.{parentTableName} {parentTableAlias} ON"
                parentBKs = (
                    self._spark.sql(f"""
                        SELECT ParentAttribute, ChildAttribute
                        FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataRelationsTable}
                        WHERE Child = '{tableName}'
                            AND Parent = '{parentTableName}'
                            AND KeyType != 'PIdentifier'
                            AND RolePlayingGroup {rolePlayingGroupExpression}
                    """)
                    .rdd.collect()
                )

                for bkIndex, parentBK in enumerate(parentBKs):
                    if bkIndex > 0:
                        joinParentsOnBKs += " AND"
                    joinParentsOnBKs += f" {parentTableAlias}.{parentBK['ParentAttribute']} = tv.{parentBK['ChildAttribute']}"

        return f"SELECT tv.*{selectParentPKs} FROM {sinkDatabaseName}.TV_{tableName} tv{joinParentsOnBKs}"

    def _getKeyColumnsForTable(self, sinkDatabaseName: str, tableName: str, tableTransformationConfig: Row, metadataKeysTable: str, transformationTable: str, tableWriteMode: str) -> ArrayType:
        if tableTransformationConfig and tableTransformationConfig["KeyColumns"]:
            keyColumns = tableTransformationConfig["KeyColumns"]
        else:
            keyColumns = []

        if tableWriteMode == WRITE_MODE_SNAPSHOT:
            assert len(keyColumns) > 0, (
                f"""Table '{sinkDatabaseName}.{tableName}' does not have property 'keyColumns' defined in 'Params' in '{self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{transformationTable}' or it is empty""")

        if not keyColumns:
            keyColumns = (
                self._spark.sql(f"""
                    SELECT Attribute
                    FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataKeysTable}
                    WHERE Entity = '{tableName}'
                        AND KeyType != 'PIdentifier'
                """)
                .rdd.map(lambda x: x[0])
            )
            keyColumns = keyColumns.take(keyColumns.count())

        if not keyColumns:
            keyColumns = (
                self._spark.sql(f"""
                    SELECT Attribute
                    FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataKeysTable}
                    WHERE Entity = '{tableName}'
                        AND KeyType == 'PIdentifier'
                """)
                .rdd.map(lambda x: x[0])
            )
            keyColumns = keyColumns.take(keyColumns.count())

        # validate key columns
        if tableWriteMode == WRITE_MODE_SCD_TYPE1:
            assert len(keyColumns) > 0, (
                f"""Table '{sinkDatabaseName}.{tableName}' does not have any primary column defined in '{self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataKeysTable}'""")

        return keyColumns

    def _generateDtOutputInstructionForTable(self, sinkDatabaseName: str, tableName: str, tableTransformationConfig: Row, metadataKeysTable: str, metadataRelationsTable: str, transformationTable: str, writeMode: str, fwkLayerId: str, fwkTriggerId: str, unknownMemberDefaultValue: int, batchNumber: int, insertTime: datetime, runMaintenanceManager: bool) -> ArrayType:
        # write mode
        if tableTransformationConfig and tableTransformationConfig["WriteMode"]:
            tableWriteMode = tableTransformationConfig["WriteMode"]
        else:
            tableWriteMode = writeMode

        # validate write mode
        assert tableWriteMode in [
                WRITE_MODE_SCD_TYPE1,
                WRITE_MODE_SNAPSHOT,
                WRITE_MODE_OVERWRITE
            ], (
            f"""WriteMode '{tableWriteMode}' defined in '{self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{transformationTable}' for '{tableName}' is not supported""")
        
        # inputParameters
        sqlQuery = self._getSQLQueryForTable(
            sinkDatabaseName,
            tableName,
            metadataRelationsTable,
            unknownMemberDefaultValue
        )

        keyColumns = self._getKeyColumnsForTable(
            sinkDatabaseName,
            tableName,
            tableTransformationConfig,
            metadataKeysTable,
            transformationTable,
            tableWriteMode
        )

        if tableTransformationConfig and tableTransformationConfig["PartitionColumns"]:
            partitionColumns = f"""["{'", "'.join(tableTransformationConfig["PartitionColumns"])}"]"""
        else:
            partitionColumns = "[]"

        inputParameters = f"""{{ "keyColumns": ["{'", "'.join(keyColumns)}"], "partitionColumns": {partitionColumns}, "schemaEvolution": false, "functions": [{{"transformation": "sqlQuery", "params": "{sqlQuery}"}}] }}"""

        # batch number
        if tableTransformationConfig and tableTransformationConfig["BatchNumber"] is not None:
            tableBatchNumber = int(tableTransformationConfig["BatchNumber"])
        else:
            tableBatchNumber = batchNumber

        # create maintenance task for partitioning if necessary
        if runMaintenanceManager:
            self._maintenanceManager.arrangeTablePartition(
                f"{sinkDatabaseName}.{tableName}",
                tableTransformationConfig["PartitionColumns"] if tableTransformationConfig and tableTransformationConfig["PartitionColumns"] else [],
                tableWriteMode
            )

        # generate dtOutput instruction
        return [
            None,
            f"{sinkDatabaseName}.{tableName}",
            inputParameters,
            tableWriteMode,
            tableTransformationConfig["FwkTriggerId"] if tableTransformationConfig and tableTransformationConfig["FwkTriggerId"] else fwkTriggerId,
            fwkLayerId,
            tableBatchNumber,
            "Y",
            insertTime,
            insertTime,
            self._configGeneratorHelper.getDatabricksId()
        ]

    def _generateDtOutput(self, metadataKeysTable: str, metadataRelationsTable: str, transformationTable: str, transformationKey: str, sinkDatabaseName: str, writeMode: str, fwkLayerId: str, fwkTriggerId: str, unknownMemberDefaultValue: int, batches: ArrayType, runMaintenanceManager: bool) -> DataFrame:
        dtOutputRecords = []
        insertTime = datetime.now()

        if (transformationTable
            and transformationKey
            and transformationTable.lower() in self._sqlContext.tableNames(self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"])
            ):
            transformationDF = (
                self._spark.sql(f"""
                    SELECT EntityName, Params
                    FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{transformationTable}
                    WHERE Key = '{transformationKey}'
                """)
                .withColumn("WriteMode", col("Params.writeMode"))
                .withColumn("KeyColumns", col("Params.keyColumns"))
                .withColumn("PartitionColumns", col("Params.partitionColumns"))
                .withColumn("FwkTriggerId", col("Params.fwkTriggerId"))
                .withColumn("BatchNumber", col("Params.batchNumber"))
            )
        else:
            transformationDF = None
        
        for batchNumber, batch in batches.items():
            for table in batch:
                try:
                    tableName = table["Entity"]

                    # read transformation info for the table
                    if transformationDF:
                        tableTransformationConfig = transformationDF.filter(f"EntityName = '{tableName}'").first()
                    else:
                        tableTransformationConfig = None

                    dtOutputRecord = self._generateDtOutputInstructionForTable(
                        sinkDatabaseName,
                        tableName,
                        tableTransformationConfig,
                        metadataKeysTable,
                        metadataRelationsTable,
                        transformationTable,
                        writeMode,
                        fwkLayerId,
                        fwkTriggerId,
                        unknownMemberDefaultValue,
                        batchNumber,
                        insertTime,
                        runMaintenanceManager
                    )

                    dtOutputRecords.append(dtOutputRecord)
                except:
                    print(f"""Exception details
                        Sink entity: {sinkDatabaseName}.{tableName}
                    """)
                    raise
        
        return self._spark.createDataFrame(dtOutputRecords, DT_OUTPUT_SCHEMA)

    def _generateFwkEntities(self, metadataAttributesTable: str, sinkDatabaseName: str, fwkLinkedServiceId: str, path: str) -> DataFrame:
        fwkEntityRecords = []
        lastUpdate = datetime.now()
        
        tables = (
            self._spark.sql(f"""SELECT DISTINCT Entity FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataAttributesTable}""")
            .rdd.collect()
        )
        
        for table in tables:
            try:
                # generate sink fwkEntity record
                tableName = table["Entity"]

                fwkEntityRecords.append([
                    f"{sinkDatabaseName}.{tableName}",
                    fwkLinkedServiceId,
                    f"{path}/{tableName}",
                    "Delta",
                    None,
                    None,
                    None,
                    None,
                    lastUpdate,
                    self._configGeneratorHelper.getDatabricksId()
                ])
            except:
                print(f"""Exception details
                    Linked service: {fwkLinkedServiceId}
                    Entity: {sinkDatabaseName}.{tableName}
                """)
                raise
        
        return self._spark.createDataFrame(fwkEntityRecords, FWK_ENTITY_SCHEMA) 
    
    def _generateAttributes(self, metadataAttributesTable: str, metadataKeysTable: str, sinkDatabaseName: str) -> DataFrame:
        # primary key columns coming from Model Keys Table
        keyColumnsDF = self._spark.sql(f"""
            SELECT Entity, Attribute
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataKeysTable}
            WHERE KeyType != 'PIdentifier'
        """)

        fallbackKeyColumnsDF = (
            self._spark.sql(f"""
                SELECT Entity, Attribute
                FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataKeysTable}
                WHERE KeyType == 'PIdentifier'
            """)
            .join(keyColumnsDF, "Entity", "leftanti")
        )

        primaryColumnsDF = (
            keyColumnsDF
            .union(fallbackKeyColumnsDF)
            .join(
                self._spark.sql(f"""
                    SELECT Entity, Attribute, IsIdentity
                    FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataAttributesTable}
                """),
                ["Entity", "Attribute"],
                "left"
            )
            .selectExpr(
                "Entity",
                "Attribute",
                "TRUE AS IsPrimaryKey",
                "IsIdentity" # <- get this information
            )
        )

        # identity columns coming from Model Attributes Table
        identityColumnsDF = self._spark.sql(f"""
            SELECT 
                Entity,
                Attribute,
                FALSE AS IsPrimaryKey,
                IsIdentity
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataAttributesTable}
            WHERE IsIdentity
        """)

        nonPrimaryIdentityColumnsDF = identityColumnsDF.join(primaryColumnsDF, ["Entity", "Attribute"], "leftanti")

        # merge primary and identity data
        attributesDataDF = primaryColumnsDF.union(nonPrimaryIdentityColumnsDF)

        # tables without primary and identity columns
        allEntitiesDF = self._spark.sql(f"""
            SELECT Distinct Entity
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataAttributesTable}
        """)

        missingEntitiesDF = allEntitiesDF.join(attributesDataDF, "Entity", "leftanti")
        missingAttributesDataDF = missingEntitiesDF.selectExpr(
            "Entity",
            "NULL AS Attribute",
            "NULL AS IsPrimaryKey",
            "NULL AS IsIdentity"
        )

        # prepare attributes DF
        attributesDF = (
            attributesDataDF
            .union(missingAttributesDataDF)
            .selectExpr(
                f"'{sinkDatabaseName}' AS DatabaseName",
                "Entity AS EntityName",
                "Attribute",
                "IsPrimaryKey",
                "IsIdentity",
                f"'{METADATA_ATTRIBUTES_TRANSFORMATION_KEY}' AS MGT_ConfigTable" 
            )
        )
        
        return attributesDF
    
    def _createDeltaTableBasedOnMetadata(self, databaseName: str, tableName: str, deltaPath: str, partitionColumns: ArrayType, metadataAttributesTable: str):
        identityColumn = (
            self._spark.sql(f"""
                SELECT Attribute, DataType
                FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataAttributesTable}
                WHERE Entity = '{tableName}'
                    AND IsIdentity = TRUE
            """)
            .first()
        )

        if identityColumn is None:
            identityColumnCondition = ""
            identityColumnDefinition = ""
        else:
            identityColumnCondition = f"AND Attribute <> '{identityColumn['Attribute']}'"
            identityColumnDefinition = f"{identityColumn['Attribute']} BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),"

        tableColumnsDF = self._spark.sql(f"""
            SELECT Attribute, DataType
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataAttributesTable}
            WHERE Entity = '{tableName}' {identityColumnCondition}
        """)

        tableColumnsMappedDF = tableColumnsDF.rdd.map(lambda x: f"""`{x["Attribute"]}` {ModelTransformationsConfigGenerator._convertModelToSparkDataType(x["DataType"])}""")

        if partitionColumns:
            partitionedBy = f"PARTITIONED BY (`{'`, `'.join(partitionColumns)}`)"
        else:
            partitionedBy = ""

        sqlCommand = f"""
            CREATE TABLE IF NOT EXISTS {databaseName}.{tableName} (
                {identityColumnDefinition}
                {", ".join(tableColumnsMappedDF.take(tableColumnsMappedDF.count()))},
                {INSERT_TIME_COLUMN_NAME} TIMESTAMP, 
                {UPDATE_TIME_COLUMN_NAME} TIMESTAMP 
            )
            USING DELTA
            LOCATION "{deltaPath}"
            {partitionedBy}
        """

        try:
            self._spark.sql(sqlCommand)
        except:
            print(f"""Exception details
                HIVE table: {databaseName}.{tableName}
                SQL command: {sqlCommand}
            """)
            raise

    def _addOrDropColumnsForDeltaTable(self, databaseName: str, tableName: str, changesDF: DataFrame, uniqueCL: list) -> ArrayType:
        columnChangeLogs = []
        allowAlterForTableExecuted = False
        
        addOrDropColumnChangesDF = (
            changesDF
            .filter("Attribute IS NULL OR CurrentAttribute IS NULL")
        )

        # iterate over changes

        for change in addOrDropColumnChangesDF.rdd.collect():
            # get column to add and column to remove
            columnToAdd = change["Attribute"]
            columnToRemove = change["CurrentAttribute"]

            # build alter table command
            if columnToAdd is not None:
                dataType = self._convertModelToSparkDataType(change["DataType"])
                # get previous column name for columnToAdd
                columnIndex = uniqueCL.index(columnToAdd)
                previousColumn = f"AFTER {uniqueCL[columnIndex - 1]}" if columnIndex > 0 else "FIRST"
                sqlCommand = (f"ALTER TABLE {databaseName}.{tableName} ADD COLUMN `{columnToAdd}` {dataType} {previousColumn}")
                operationDescription = "added"
            elif columnToRemove is not None:
                sqlCommand = (f"ALTER TABLE {databaseName}.{tableName} DROP COLUMN `{columnToRemove}`")
                operationDescription = "dropped"

            # alter table column
            try:
                if not allowAlterForTableExecuted:
                    self._dataLakeHelper.allowAlterForTable(f"{databaseName}.{tableName}")
                    allowAlterForTableExecuted = True

                self._spark.sql(sqlCommand)
                operationMessage = (f"The column '{'' if columnToAdd is None else columnToAdd}{'' if columnToRemove is None else columnToRemove}' on table '{databaseName}.{tableName}' has been {operationDescription}.")
                columnChangeLogs.append(operationMessage)
            except:
                print(f"""Exception details
                    HIVE table: {databaseName}.{tableName}
                    SQL command: {sqlCommand}
                """)
                raise

        return columnChangeLogs
    
    def _changeColumnDataTypesForDeltaTable(self, databaseName: str, tableName: str, changesDF: DataFrame) -> ArrayType:
        columnChangeLogs = []
        allowAlterForTableExecuted = False

        dataTypeChangesDF = (
            changesDF
            .filter("Attribute IS NOT NULL AND CurrentAttribute IS NOT NULL AND Attribute = CurrentAttribute")
        )

        # iterate over changes

        for changesDataType in dataTypeChangesDF.rdd.collect():
            # get column to modify data type
            columnToModify = changesDataType["Attribute"]
            dataTypeTable = changesDataType["CurrentDataType"]
            dataTypeToModify = self._convertModelToSparkDataType(changesDataType["DataType"]).lower()

            # build alter table command
            if dataTypeTable != dataTypeToModify:
                sqlCommand1 = (f"ALTER TABLE {databaseName}.{tableName} ADD COLUMN `{columnToModify}_tempFwk` {dataTypeToModify} AFTER `{columnToModify}`")
                sqlCommand2 = (f"UPDATE {databaseName}.{tableName} SET `{columnToModify}_tempFwk` = `{columnToModify}`")
                sqlCommand3 = (f"ALTER TABLE {databaseName}.{tableName} DROP COLUMN IF EXISTS `{columnToModify}`")
                sqlCommand4 = (f"ALTER TABLE {databaseName}.{tableName} RENAME COLUMN `{columnToModify}_tempFwk` TO `{columnToModify}`")

                # alter table column
                try:
                    if not allowAlterForTableExecuted:
                        self._dataLakeHelper.allowAlterForTable(f"{databaseName}.{tableName}")
                        allowAlterForTableExecuted = True
                    
                    self._spark.sql(sqlCommand1)
                    self._spark.sql(sqlCommand2)
                    self._spark.sql(sqlCommand3)
                    self._spark.sql(sqlCommand4)
                    operationMessage = f"The column '{columnToModify}' on table '{databaseName}.{tableName}' has been changed from '{dataTypeTable}' to '{dataTypeToModify}'."
                    columnChangeLogs.append(operationMessage)
                except:
                    print(f"""Exception details
                        HIVE table: {databaseName}.{tableName}
                        SQL command 1: {sqlCommand1}
                        SQL command 2: {sqlCommand2}
                        SQL command 3: {sqlCommand3}
                        SQL command 4: {sqlCommand4}
                    """)
                    raise

        return columnChangeLogs

    def _alterDeltaTableBasedOnMetadata(self, databaseName: str, tableName: str, metadataAttributesTable: str) -> ArrayType:
        columnChangeLogs = []
        
        columnDF = self._spark.sql(f"""
            SELECT Attribute, DataType
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataAttributesTable}
            WHERE Entity = '{tableName}'
        """)
        
        configDF = (
            columnDF
            .withColumn("OrderID", monotonically_increasing_id())   # ensure that data in changesDF will have the same order as Attributes file
            .dropDuplicates(['Attribute', 'DataType'])
        )

        columnList = columnDF.select("Attribute").rdd.flatMap(lambda x: x).collect()
        uniqueCL = []
        [uniqueCL.append(x) for x in columnList if x not in uniqueCL]

        # get existing columns of the table from hive definition
        # (except validation status, because the validator is responsible for handling it)
        tableColumnsDF = (
            self._spark.sql(f"DESCRIBE TABLE {databaseName}.{tableName}")
            .distinct()
            .filter(f"""
                col_name != '{VALIDATION_STATUS_COLUMN_NAME}'
                AND col_name != '{INSERT_TIME_COLUMN_NAME}'
                AND col_name != '{UPDATE_TIME_COLUMN_NAME}'
                AND col_name NOT LIKE '#%'
            """)
            .selectExpr("col_name AS CurrentAttribute", "data_type AS CurrentDataType")
        )

        changesDF = (
            configDF
            .join(tableColumnsDF, configDF["Attribute"] == tableColumnsDF["CurrentAttribute"], "full_outer")
            .orderBy("OrderID")
        )

        columnChangeLogs += self._addOrDropColumnsForDeltaTable(databaseName, tableName, changesDF, uniqueCL)
        columnChangeLogs += self._changeColumnDataTypesForDeltaTable(databaseName, tableName, changesDF)
        
        return columnChangeLogs

    def createOrAlterDeltaTablesBasedOnMetadata(self, metadataConfig: MapType) -> ArrayType:
        """Either creates or alters tables based on metadata information stored
        in model attributes file.
        
        Tables has to be created/altered before ETL run, because model transformation 
        instructions are forbidden to evolve schema during saving data to model tables.
        If the schema of data won't match sink model table schema, it's desired that
        the operation fails.

        Returns:
            Information about table schema changes.
        """

        (sinkPath, sinkUri) = self._configGeneratorHelper.getADLSPathAndUri(metadataConfig["sink"])

        metadataAttributesTable = metadataConfig["metadata"]["attributesTable"]
        databaseName = metadataConfig["sink"]["databaseName"]
        columnChangeLogs = []
    
        # get all tables
        tables = (
            self._spark.sql(f"""
                SELECT DISTINCT Entity
                FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{metadataAttributesTable}
            """)
            .rdd.collect()
        )

        # get compartment configuration for the tables
        transformationTable = metadataConfig["metadata"]["transformationTable"] if "transformationTable" in metadataConfig["metadata"].keys() else None
        transformationKey = metadataConfig["metadata"]["key"] if "key" in metadataConfig["metadata"].keys() else None
        if (transformationTable
            and transformationKey
            and transformationTable.lower() in self._sqlContext.tableNames(self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"])
        ):
            transformationDF = (
                self._spark.sql(f"""
                    SELECT EntityName, Params
                    FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{transformationTable}
                    WHERE Key = '{transformationKey}'
                """)
                .withColumn("PartitionColumns", col("Params.partitionColumns"))
            )
        else:
            transformationDF = None
        
        for table in tables:
            try:
                tableName = table["Entity"]
                deltaPath = f"{sinkUri}/{tableName}"
                
                # check if table already exists in the database
                if not tableName.lower() in self._sqlContext.tableNames(databaseName):
                    # check if partition columns are defined for the table
                    if transformationDF:
                        tableTransformationConfig = transformationDF.filter(f"EntityName = '{tableName}'").first()
                        if tableTransformationConfig and tableTransformationConfig["PartitionColumns"]:
                            partitionColumns = tableTransformationConfig["PartitionColumns"]
                        else:
                            partitionColumns = []
                    else:
                        partitionColumns = []

                    self._createDeltaTableBasedOnMetadata(databaseName, tableName, deltaPath, partitionColumns, metadataAttributesTable)
                else:
                    columnChangeLogs += self._alterDeltaTableBasedOnMetadata(databaseName, tableName, metadataAttributesTable)
            except:
                print(f"""Exception details
                    HIVE table: {databaseName}.{tableName}
                """)
                raise

        return columnChangeLogs

    def generateConfiguration(self, metadataConfig: MapType, configurationOutputPath: str, metadataDeltaTablePath: str, runMaintenanceManager: bool) -> DataFrame:
        """Generates model transformations configuration (DtOutput.csv and FwkEntity.csv) and
        metadata information about all sink entities.

        Returns:
            Metadata information about all sink entities.
        """

        (sinkPath, sinkUri) = self._configGeneratorHelper.getADLSPathAndUri(metadataConfig["sink"])

        # identify batches
        batches = self._getBatchesBasedOnTableRelations(
            metadataConfig["metadata"]["attributesTable"],
            metadataConfig["metadata"]["relationsTable"]
        )

        # generate DtOutput for each batch
        dtOutputDF = self._generateDtOutput(
            metadataConfig["metadata"]["keysTable"],
            metadataConfig["metadata"]["relationsTable"],
            metadataConfig["metadata"]["transformationTable"] if "transformationTable" in metadataConfig["metadata"].keys() else None,
            metadataConfig["metadata"]["key"] if "key" in metadataConfig["metadata"].keys() else None,
            metadataConfig["sink"]["databaseName"],
            metadataConfig["sink"]["writeMode"] if "writeMode" in metadataConfig["sink"].keys() else WRITE_MODE_OVERWRITE,
            metadataConfig["FwkLayerId"],
            metadataConfig["FwkTriggerId"],
            metadataConfig["metadata"]["unknownMemberDefaultValue"] if "unknownMemberDefaultValue" in metadataConfig["metadata"].keys() else None,
            batches,
            runMaintenanceManager
        )
        self._dataLakeHelper.writeDataAsParquet(dtOutputDF, f"{configurationOutputPath}/{DT_OUTPUT_FOLDER}/", WRITE_MODE_APPEND)

        # save the dtOutputDF as delta table
        self._dataLakeHelper.writeData(
            dtOutputDF, 
            f"{metadataDeltaTablePath}/{FWK_DT_INST_TABLE}/",
            WRITE_MODE_APPEND, 
            "delta", 
            {}, 
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"], 
            FWK_DT_INST_TABLE
        )
        
        # generate FwkEntities
        fwkEntitiesDF = self._generateFwkEntities(
            metadataConfig["metadata"]["attributesTable"],
            metadataConfig["sink"]["databaseName"],
            metadataConfig["sink"]["FwkLinkedServiceId"],
            sinkPath
        )
        self._dataLakeHelper.writeDataAsParquet(fwkEntitiesDF, f"{configurationOutputPath}/{FWK_ENTITY_FOLDER}/", WRITE_MODE_APPEND)

        # save the fwkEntitiesDF as delta table

        self._dataLakeHelper.writeData(
            fwkEntitiesDF, 
            f"{metadataDeltaTablePath}/{FWK_ENTITY_TABLE}/",
            WRITE_MODE_SCD_TYPE1, 
            "delta", 
            {"keyColumns": ["FwkEntityId"]},
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"], 
            FWK_ENTITY_TABLE
        )
        
        # generate Attributes
        attributesDF = self._generateAttributes(
            metadataConfig["metadata"]["attributesTable"],
            metadataConfig["metadata"]["keysTable"],
            metadataConfig["sink"]["databaseName"]
        )
        
        return attributesDF
