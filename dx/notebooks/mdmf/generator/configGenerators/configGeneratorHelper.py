# Databricks notebook source
import json
from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, current_timestamp
from pyspark.sql.types import ArrayType, MapType, StructType
from typing import Tuple, Union

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.includes.dataLakeHelper import DataLakeHelper
    from notebooks.mdmf.includes.frameworkConfig import *


class ConfigGeneratorHelper:
    """ConfigGeneratorHelper contains helper functions used by config
    generators and for logging.

    Public methods:
        getDatabricksId
        getADLSPathAndUri
        stringifyArray
        convertAndValidateTransformationsToFunctions
        isDeltaTableChanged
        readMetadataConfig
        saveMetadataConfigAsNewDeltaVersion
        saveMetadataConfigAsSCDType2
        createFwkFlagsTable
        getFwkFlag
        setFwkFlag
        moveUnprocessedFilesToArchive
        upsertIntoFwkLog
    """

    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._dbutils = globals()["dbutils"] if "dbutils" in globals() else None
        self._DeltaTable = DeltaTable
        self._dataLakeHelper = DataLakeHelper(spark, compartmentConfig)

    def getDatabricksId(self) -> str:
        """Returns Databricks cluster ID.
        
        Returns:
            Databricks cluster ID.
        """

        return f"""Databricks {self._spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")}"""

    def getADLSPathAndUri(self, metadataADLSConfig: MapType) -> Tuple[str, str]:
        """Returns relative data path and full data uri.
        
        Returns:
            Relative data path.
            Full data uri.
        """
        
        fwkLinkedService = (
            self._compartmentConfig.FWK_LINKED_SERVICE_DF
            .filter(f"""FwkLinkedServiceId = '{metadataADLSConfig["FwkLinkedServiceId"]}'""")
            .first()
        )

        assert fwkLinkedService, f"""FwkLinkedService with FwkLinkedServiceId '{metadataADLSConfig["FwkLinkedServiceId"]}' was not found in environment config"""

        instanceUrl = fwkLinkedService["InstanceURL"]
        path = f"""{metadataADLSConfig["containerName"]}{metadataADLSConfig["dataPath"] if "dataPath" in metadataADLSConfig.keys() else ""}"""
        
        (adlsName, uri) = self._dataLakeHelper.getADLSParams(instanceUrl, path)
        
        return (
            path,
            uri
        )

    @staticmethod
    def stringifyArray(array: ArrayType) -> str:
        """Creates string representation of an Array.
        
        Returns:
            String representation of an Array.
        """

        if array:
            return f"""["{'", "'.join(array)}"]"""
        else:
            return "[]"
        
    @staticmethod
    def convertAndValidateTransformationsToFunctions(transformations: ArrayType) -> ArrayType:
        """Converts entity Transformations configuration to array of function strings that are
        needed by the TransformationManager.
        
        Returns:
            Array of transformation function strings.
        """

        functions = []
        
        if not transformations:
            return functions

        for transformation in transformations:
            # validate mandatory properties
            if transformation["TransformationType"] in [
                "Deduplicate",
                "Pseudonymization",
                "Select"
            ]:
                assert transformation["Params"] and "columns" in transformation["Params"] and transformation["Params"]["columns"] is not None, (
                    f"""Parameter 'columns' is mandatory for TransformationType '{transformation["TransformationType"]}'""")

            if transformation["TransformationType"] in [
                "Aggregate",
                "Cast",
                "DeriveColumn",
                "Join",
                "RenameColumn",
                "ReplaceNull"
            ]:
                assert transformation["Params"] and "transformationSpecs" in transformation["Params"] and transformation["Params"]["transformationSpecs"] is not None, (
                    f"""Parameter 'transformationSpecs' is mandatory for TransformationType '{transformation["TransformationType"]}'""")
            
            match transformation["TransformationType"]:
                case "Aggregate":
                    # validate mandatory properties
                    transformationSpecs = transformation["Params"]["transformationSpecs"]
                    if str(type(transformationSpecs)) == "<class 'str'>":
                        transformationSpecs = json.loads(transformationSpecs)
                    
                    assertCondition = (
                        transformationSpecs
                        and "aggregations" in transformationSpecs
                        and transformationSpecs["aggregations"]
                    )

                    assert assertCondition, (
                        f"""Attribute 'aggregations' of 'transformationSpecs' parameter is mandatory and can't be empty for TransformationType '{transformation["TransformationType"]}'""")
                    
                    for aggregation in transformationSpecs["aggregations"]:
                        assertCondition = (
                            aggregation
                            and "operation" in aggregation
                            and "column" in aggregation
                            and "alias" in aggregation
                        )

                        assert assertCondition, (
                            f"""Attributes 'operation', 'column' and 'alias' of 'transformationSpecs.aggregations' object are mandatory for TransformationType '{transformation["TransformationType"]}'""")
                    functions.append(f'{{"transformation": "aggregate", "params": {transformation["Params"]["transformationSpecs"]}}}')
                case "Cast":
                    functions.append(f'{{"transformation": "cast", "params": {transformation["Params"]["transformationSpecs"]}}}')
                case "Deduplicate":
                    functions.append(f'{{"transformation": "deduplicate", "params": {ConfigGeneratorHelper.stringifyArray(transformation["Params"]["columns"])}}}')
                case "DeriveColumn":
                    functions.append(f'{{"transformation": "deriveColumn", "params": {transformation["Params"]["transformationSpecs"]}}}')
                case "Filter":
                    # validate mandatory properties
                    assert transformation["Params"] and "condition" in transformation["Params"] and transformation["Params"]["condition"], (
                        f"""Parameter 'condition' is mandatory for TransformationType '{transformation["TransformationType"]}'""")

                    functions.append(f'{{"transformation": "filter", "params": "{transformation["Params"]["condition"]}"}}')
                case "Join":
                    # validate mandatory properties
                    transformationSpecs = transformation["Params"]["transformationSpecs"]
                    if str(type(transformationSpecs)) == "<class 'str'>":
                        transformationSpecs = json.loads(transformationSpecs)

                    assertCondition = (
                        transformationSpecs
                        and "joinTable" in transformationSpecs
                        and "joinType" in transformationSpecs
                        and "joinColumns" in transformationSpecs
                        and "selectColumns" in transformationSpecs
                    )

                    assert assertCondition, (
                        f"""Attributes 'joinTable', 'joinType', 'joinColumns' and 'selectColumns' of 'transformationSpecs' parameter are mandatory for TransformationType '{transformation["TransformationType"]}'""")

                    functions.append(f'{{"transformation": "join", "params": {transformation["Params"]["transformationSpecs"]}}}')
                case "Pseudonymization":
                    functions.append(f'{{"transformation": "pseudonymize", "params": {ConfigGeneratorHelper.stringifyArray(transformation["Params"]["columns"])}}}')
                case "RenameColumn":
                    # validate column length
                    transformationSpecs = transformation["Params"]["transformationSpecs"]
                    if str(type(transformationSpecs)) == "<class 'str'>":
                        transformationSpecs = json.loads(transformationSpecs)
                    
                    assert len(transformationSpecs['column']) == len(transformationSpecs['value']), (
                        "Length of the column and value mismatch in transformationSpecs of RenameColumn transformation")

                    functions.append(f'{{"transformation": "renameColumn", "params": {transformation["Params"]["transformationSpecs"]}}}')
                case "ReplaceNull":
                    functions.append(f'{{"transformation": "replaceNull", "params": {transformation["Params"]["transformationSpecs"]}}}')
                case "Select":
                    functions.append(f'{{"transformation": "select", "params": {ConfigGeneratorHelper.stringifyArray(transformation["Params"]["columns"])}}}')
                case "SelectExpression":
                    # validate mandatory properties
                    assert transformation["Params"] and "expressions" in transformation["Params"] and transformation["Params"]["expressions"] is not None, (
                        f"""Parameter 'expressions' is mandatory for TransformationType '{transformation["TransformationType"]}'""")
                    
                    functions.append(f'{{"transformation": "selectExpression", "params": {ConfigGeneratorHelper.stringifyArray(transformation["Params"]["expressions"])}}}')
                case _:
                    raise NotImplementedError(f"""TransformationType '{transformation["TransformationType"]}' is not implemented""")
        
        return functions
    
    @staticmethod
    def _isContentOfDataframesSame(firstDF: DataFrame, secondDF: DataFrame) -> bool:
        if firstDF.schema != secondDF.schema:
            return False
        
        if firstDF.exceptAll(secondDF).count() != 0:
            return False
        else:
            return secondDF.exceptAll(firstDF).count() == 0

    def isDeltaTableChanged(self, dataPath: str, deltaTableName: str, filterExpression: str, columnsToCompare: ArrayType) -> bool:
        """Compares Delta table with previous version and returns flag whether the columnsToCompare have changed.

        Returns:
            True if the content of Delta has changed, False otherwise.
        """
        
        dataPath = f"{dataPath}/{deltaTableName}"

        try:
            isDeltaTable = self._DeltaTable.isDeltaTable(self._spark, dataPath)
        except:
            print(f"""Exception Details
                Command: DeltaTable.isDeltaTable(spark, "{dataPath}")""")
            raise

        if not isDeltaTable:
            return False
        
        # Read the current and previous version of the delta table.
        versionNumber = (
            self._spark.sql(f"""DESCRIBE HISTORY {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{deltaTableName}""")
            .select("version")
            .orderBy("version", ascending=False)
            .first()["version"]
        )
        
        if versionNumber == 0:
            return False
        
        currentVersionDF = (
            self._spark.read.table(f"""{self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{deltaTableName}@v{versionNumber}""")
            .filter(filterExpression)
            .select(columnsToCompare)
        )

        previousVersionDF = (
            self._spark.read.table(f"""{self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{deltaTableName}@v{versionNumber - 1}""")
            .filter(filterExpression)
            .select(columnsToCompare)
        )

        return not self._isContentOfDataframesSame(currentVersionDF, previousVersionDF)

    def readMetadataConfig(self, configPath: str, configSchema: str) -> DataFrame:
        """Reads metadata config and returns its content as a DataFrame.
        
        Returns:
            Content of the metadata config.
        """

        configFullPath = f"{METADATA_INPUT_PATH}/{configPath}"
        
        if not self._dataLakeHelper.fileExists(configFullPath):
            return self._spark.createDataFrame(self._spark.sparkContext.emptyRDD(), f"col BOOLEAN")
        
        # Try to parse the config first, because it might be an invalid JSON.
        # If the config is indeed invalid, JSONDecodeError will be thrown describing
        # why the content is not valid JSON
        try:
            content = self._dbutils.fs.head(configFullPath, 1000000000)
        except:
            print(f"""Exception Details
                Command: dbutils.fs.head("{configFullPath}", 1000000000)""")
            raise
        
        try:
            json.loads(content)
        except:
            print(f"""Exception Details
                File Path: {configFullPath}""")
            raise

        configDF = (
            self._spark.read.schema(configSchema)
            .option("multiline", "true")
            .json(configFullPath)
        )

        # If the config is split into the groups, flatten the content

        if "ARRAY" in configSchema:
            configSchemaKeyValues = configSchema.split(",")
            groupKey = configSchemaKeyValues[0].strip().split(" ")[0]
            arrayKey = configSchemaKeyValues[1].strip().split(" ")[0]

            configDF = (
                configDF
                .select(col(groupKey), explode(col(arrayKey)))
                .select(col(groupKey), col("col.*"))
            )

        if "Expectations" in configSchema:
            configDF = configDF.selectExpr("*", "explode(Expectations) as exploded")
            configDF = configDF.select("DatabaseName", "EntityName", "exploded.*")

        return configDF
    
    def saveMetadataConfigAsNewDeltaVersion(self, configPath: str, configSchema: str, dataPath: str, tableName: str) -> bool:
        """Saves metadata config as new delta version and returns flag whether the content changed.
        
        Reads metadata config and compares its content with previous version stored as DeltaTable.
        If the content has changes, config is stored as new Delta version.
        
        Returns:
            Flag whether the content of the config has changed.
        """
        
        configDF = self.readMetadataConfig(configPath, configSchema)
        dataPath = f"{dataPath}/{tableName}"

        try:
            isDeltaTable = self._DeltaTable.isDeltaTable(self._spark, dataPath)
        except:
            print(f"""Exception Details
                Command: DeltaTable.isDeltaTable(spark, "{dataPath}")""")
            raise
        
        if isDeltaTable:
            # Read current version of the config from delta table
            deltaDF = self._spark.sql(f"""SELECT * FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{tableName}""")

            if self._isContentOfDataframesSame(configDF, deltaDF):
                return False

        self._dataLakeHelper.writeData(
            configDF,
            dataPath,
            WRITE_MODE_OVERWRITE,
            "delta",
            {
                "schemaEvolution": True
            },
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"],
            tableName
        )
    
        return True

    def saveMetadataConfigAsSCDType2(self, configPath: Union[str, list], configSchema: str, dataPath: str, tableName: str, bkColumns: ArrayType, validateDataFunction = None) -> bool:
        """Saves metadata config by using SCD Type 2 to keep history.
        
        Reads metadata config, validates its content if desired and merges its content
        to existing Delta table by using SCD Type 2.
        
        Returns:
            Flag whether the content of the config has changed.
        """
        if isinstance(configPath, str):
            configDF = self.readMetadataConfig(configPath, configSchema)
        elif isinstance(configPath, list):
            configDF = self._spark.createDataFrame(self._spark.sparkContext.emptyRDD(), StructType([]))
            for path in configPath:
                configDF = configDF.unionByName(self.readMetadataConfig(path, configSchema), allowMissingColumns = True)
        else:
            raise ValueError("configPath should be either str or list")
        
        if configDF.count() == 0:
            return False

        if validateDataFunction is not None:
            validateDataFunction(configDF)

        dataPath = f"{dataPath}/{tableName}"
        
        params = {
            "keyColumns": bkColumns,
            "createIdentityColumn": True,
            "entTriggerTime": datetime.now()
        }

        statistics = self._dataLakeHelper.writeData(
            configDF,
            dataPath,
            WRITE_MODE_SCD_TYPE2,
            "delta",
            params,
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"],
            tableName
        )

        if statistics["recordsInserted"] or statistics["recordsUpdated"] or statistics["recordsDeleted"]:
            return True
        else:
            return False

    def createFwkFlagsTable(self, dataPath: str):
        """Creates Framework Flags table in HIVE.
        
        Framework Flags table is created in metadata database and is used to store
        flags related to processing of metadata configuration files.
        """

        self._spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_FWK_FLAGS} (
                key STRING,
                value STRING,
                updated TIMESTAMP
            )
            USING DELTA
            LOCATION "{dataPath}/{METADATA_FWK_FLAGS}"
        """)

    def getFwkFlag(self, key: str) -> str:
        """Returns value of Framework Flag.

        Returns:
            Value of Framework Flag.
        """

        result = (
            self._spark.sql(f"""
                SELECT value FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_FWK_FLAGS} 
                WHERE key = '{key}'
            """)
            .rdd.collect()
        )

        return None if not result else result[0]["value"]

    def setFwkFlag(self, key: str, value: str):
        """Sets value of Framework Flag."""

        updated = datetime.now()
        
        if self.getFwkFlag(key) == None:
            self._spark.sql(f"""
                INSERT INTO {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_FWK_FLAGS} 
                VALUES ('{key}', '{value}', '{updated}')
            """)
        else:
            self._spark.sql(f"""
                UPDATE {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_FWK_FLAGS}
                SET value = '{value}', updated = '{updated}' WHERE key = '{key}'
            """)

    def moveUnprocessedFilesToArchive(self, configurationOutputPath: str, executionStart: datetime):
        """Moves unprocessed CSVs from previous run from Configuration folder to Archive folder"""

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
            if '.csv' not in unprocessedFile:
                unprocessedFile = f"{unprocessedFile}/"

            fileUnprocessedPath = f"{configurationOutputPath}/{unprocessedFile}"
                
            if self._dataLakeHelper.fileExists(fileUnprocessedPath):
                try:
                    self._dbutils.fs.mv(
                        f"{fileUnprocessedPath}",
                        f"{configurationOutputPath}/Archive/{executionStart.strftime('%Y%m%d%H%M%S')}_unprocessed_{unprocessedFile}",
                        True
                    )
                except:
                    print(f"""Exception Details
                        Command: dbutils.fs.mv("{fileUnprocessedPath}", "{configurationOutputPath}/Archive/{executionStart.strftime('%Y%m%d%H%M%S')}_unprocessed_{unprocessedFile}", True)""")
                    raise
    
    def _getJobRunUrl(self):
        context = json.loads(self._dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

        if context["currentRunId"]:
            # notebook executed as a job
            if "parentRunId" in context["tags"].keys():
                # nested job
                runId = context["tags"]["idInJob"]
            else:
                # parent job
                runId = context["currentRunId"]["id"]

            return f"""https://{self._spark.conf.get("spark.databricks.workspaceUrl")}/jobs/{context["tags"]["jobId"]}/runs/{runId}?o={context["tags"]["orgId"]}"""
        else:
            # notebook executed directly
            return None
        
    def upsertIntoFwkLog(self, entRunId: str, module: str, moduleRunId: str, startDate: datetime, adfTriggerName: str):

        """Upsert the log into the Framework Log table in HIVE."""

        # current user and timestamp
        createdBy = self.getDatabricksId()
        lastUpdate = self._spark.sql("SELECT current_timestamp()").first()[0]
        jobRunUrl = self._getJobRunUrl()

        mergeQuery = f"""
        MERGE INTO {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{FWK_LOG_TABLE} AS target
        USING (
            SELECT 
                '{entRunId}' AS entRunId, 
                '{moduleRunId}' AS moduleRunId, 
                '{adfTriggerName}' AS adfTriggerName, 
                '{module}' AS module, 
                '{startDate}' AS startDate, 
                '{jobRunUrl}' AS jobRunUrl, 
                '{lastUpdate}' AS lastUpdate, 
                '{createdBy}' AS createdBy
        ) AS source
        ON target.ModuleRunId = source.moduleRunId
        WHEN MATCHED THEN
            UPDATE SET 
                PipelineStatus = 'Succeeded',
                EndDate = source.lastUpdate,
                JobRunUrl = source.jobRunUrl
        WHEN NOT MATCHED THEN
            INSERT (
                EntRunId, 
                ModuleRunId, 
                ADFTriggerName, 
                Module, 
                PipelineStatus, 
                StartDate, 
                EndDate, 
                JobRunUrl, 
                ErrorMessage, 
                LastUpdate, 
                CreatedBy
            )
            VALUES (
                source.entRunId, 
                source.moduleRunId, 
                source.adfTriggerName, 
                source.module, 
                'InProgress', 
                source.startDate, 
                NULL, 
                source.jobRunUrl, 
                NULL, 
                source.lastUpdate, 
                source.createdBy
            );
        """
        
        self._spark.sql(mergeQuery)
