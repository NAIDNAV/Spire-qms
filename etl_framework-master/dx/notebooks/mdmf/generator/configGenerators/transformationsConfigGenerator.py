# Databricks notebook source
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession, Row
from pyspark.sql.functions import col, lit, expr, coalesce, row_number, when
from pyspark.sql.types import ArrayType, MapType
from pyspark.sql.window import Window

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.generator.configGenerators.configGeneratorHelper import ConfigGeneratorHelper
    from notebooks.mdmf.includes.dataLakeHelper import DataLakeHelper
    from notebooks.mdmf.includes.frameworkConfig import *
    from notebooks.mdmf.maintenance.maintenanceManager import MaintenanceManager


class TransformationsConfigGenerator:
    """TransformationsConfigGenerator is responsible for generating transformations configuration.

    It generates DtOutput.csv and FwkEntity.csv that contain instructions and configuration which
    are later used during ETL to transform data. It also generates metadata information about 
    all entities (tables) that are created as output (sink) of these transformations instructions.

    Public methods:
        generateConfiguration
    """

    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._sqlContext = globals()["sqlContext"] if "sqlContext" in globals() else None
        self._dataLakeHelper = DataLakeHelper(spark, compartmentConfig)
        self._configGeneratorHelper = ConfigGeneratorHelper(spark, compartmentConfig)
        self._maintenanceManager = MaintenanceManager(spark, compartmentConfig)
        self._minDtOrderId = None

    @staticmethod
    def _getDistinctDatabaseNames(entitiesDF: DataFrame) -> ArrayType:
        databaseNames = (
            entitiesDF
            .select("DatabaseName")
            .distinct()
            .rdd.map(lambda x: x[0])
        )

        return databaseNames.take(databaseNames.count())

    def _getAttributesForEntitiesDatabaseNames(self, entitiesDF: DataFrame) -> DataFrame:
        databaseNames = self._getDistinctDatabaseNames(entitiesDF)

        return self._spark.sql(f"""
            SELECT * FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}
            WHERE DatabaseName IN ('{"', '".join(databaseNames)}')
        """)
        
    def _getAllEntitiesForDatabaseNames(self, entitiesDF: DataFrame) -> DataFrame:
        databaseNames = self._getDistinctDatabaseNames(entitiesDF)

        return self._spark.sql(f"""
            SELECT DISTINCT DatabaseName, EntityName FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}
            WHERE DatabaseName IN ('{"', '".join(databaseNames)}')
        """)

    def _getEntitiesFromTransformationTableByKey(self, transformationTable: str, key: str) -> DataFrame:
        return self._spark.sql(f"""
            SELECT * 
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{transformationTable}
            WHERE Key = '{key}'
        """)

    @staticmethod
    def _isInDataFrame(entitiesDF: DataFrame, databaseName: str, entityName: str) -> bool:
        # DEV NOTE: keep this in a separate method, because this line causes performance issues
        # when unit tests are executed locally. As a workaround, this method is mocked in unit tests.
        return entitiesDF.filter(f"""DatabaseName = '{databaseName}' and EntityName = '{entityName}'""").first() != None

    def _getWildcardEntitiesReplacedByRealEntities(self, entitiesDF: DataFrame) -> DataFrame:
        wildcardEntitiesDF = entitiesDF.filter(("EntityName like '%[%'") and ("EntityName like '%]%'"))
        
        if wildcardEntitiesDF.count() == 0:
            return entitiesDF

        attributesDF = self._getAllEntitiesForDatabaseNames(wildcardEntitiesDF)
        entitiesDF = entitiesDF.exceptAll(wildcardEntitiesDF)
        
        wildcardEntitiesDF = (
            wildcardEntitiesDF
            .withColumn("positionStart", expr('locate("[", EntityName)'))
            .withColumn("positionEnd", expr('locate("]", EntityName)'))
            .withColumn("lengthEntityName", expr('length(EntityName)'))
            .withColumn("entityArray", expr("substring(EntityName, positionStart+1, positionEnd - positionStart-1)"))
            .withColumn("entityLeftPart", expr("substring(EntityName, 1, positionStart-1)"))
            .withColumn("entityRightPart", expr("substring(EntityName, positionEnd+1, lengthEntityName)"))
            .alias("entity")
            .join(attributesDF.alias("attribute"), "DatabaseName", "inner")
            .selectExpr("entity.*", "attribute.EntityName AS EntityName_Attribute")
        )

        entitiesMatchingWildcards = []

        for wildcardEntity in wildcardEntitiesDF.rdd.collect():
            databaseName = wildcardEntity["DatabaseName"]
            entityName = wildcardEntity["EntityName_Attribute"]
            entityWildcardName = wildcardEntity["entityArray"]
            
            # if the entity is in the list already, don't verify wildcard rule for it
            if self._isInDataFrame(entitiesDF, databaseName, entityName):
                continue
            
            matchFound = False
            
            #2.1 * match
            if (entityWildcardName == "*"
                and entityName.startswith(wildcardEntity["entityLeftPart"])
                and entityName.endswith(wildcardEntity["entityRightPart"])
            ):
                matchFound = True
            #2.2 Array match
            elif entityWildcardName != "*":
                entityWildcardNames = entityWildcardName.split(",")
                for singleEntityWildcardName in entityWildcardNames:
                    if wildcardEntity["entityLeftPart"] + singleEntityWildcardName.strip(" ") + wildcardEntity["entityRightPart"] == entityName:
                        matchFound = True
                        break
            
            if matchFound:
                entitiesMatchingWildcards.append([
                    wildcardEntity["Key"],
                    databaseName,
                    entityName,
                    wildcardEntity["Transformations"],
                    wildcardEntity["Params"]
                ])
        
        if entitiesMatchingWildcards:
            entitiesDF = entitiesDF.union(
                self._spark.createDataFrame(entitiesMatchingWildcards, entitiesDF.schema)
            )

        return entitiesDF

    def _appendRemainingEntities(self, entitiesDF: DataFrame, metadataConfig: MapType) -> DataFrame:
        if "copyRest" in metadataConfig["metadata"].keys() and metadataConfig["metadata"]["copyRest"]:
            attributesDF = self._getAllEntitiesForDatabaseNames(entitiesDF)

            entitiesRestDF = (
                attributesDF
                .exceptAll(entitiesDF.select("DatabaseName", "EntityName"))
                .withColumn("Transformations", lit(None))
                .withColumn("Key", lit(None))
                .withColumn("Params", lit(None))
                .select(entitiesDF.columns)
            )

            entitiesDF = entitiesDF.union(entitiesRestDF)
            
        return entitiesDF
        
    def _getEntities(self, metadataConfig: MapType) -> DataFrame:
        transformationTable = metadataConfig["metadata"]["transformationTable"]
        key = metadataConfig["metadata"]["key"]
        
        # Get entities from transformation config by key
        entitiesDF = self._getEntitiesFromTransformationTableByKey(transformationTable, key)
        
        # Wildcard rules
        entitiesDF = self._getWildcardEntitiesReplacedByRealEntities(entitiesDF)
        
        # Copy Rest
        entitiesDF = self._appendRemainingEntities(entitiesDF, metadataConfig)
        
        return entitiesDF

    def _identifyAndValidateBusinessKeys(self, table: Row, sourceDatabaseName: str, sourceTableName: str) -> ArrayType:
        bkColumns = []

        if table["KeyColumns"] is not None and len(table["KeyColumns"]) > 0:
            # take user defined business keys
            bkColumns = table["KeyColumns"]
        elif table["WriteMode"] in [
                WRITE_MODE_SCD_TYPE1,
                WRITE_MODE_SCD_TYPE2,
                WRITE_MODE_SCD_TYPE2_DELTA,
                WRITE_MODE_SCD_TYPE2_DELETE
            ]:
            # identify business keys based on metadata in attributes table
            bkColumns = (
                self._spark.sql(f"""
                    SELECT Attribute FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}
                    WHERE DatabaseName = '{sourceDatabaseName}'
                        AND EntityName = '{sourceTableName}'
                        AND IsPrimaryKey
                """)
                .rdd.map(lambda x: x[0])
            )
            bkColumns = bkColumns.take(bkColumns.count())

        # validate business keys

        if table["WriteMode"] in [
                WRITE_MODE_SCD_TYPE1,
                WRITE_MODE_SCD_TYPE2,
                WRITE_MODE_SCD_TYPE2_DELTA,
                WRITE_MODE_SCD_TYPE2_DELETE
            ]:
            assert len(bkColumns) > 0, (
                f"""Table '{sourceDatabaseName}.{sourceTableName}' does not have any primary column defined neither in '{self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{METADATA_ATTRIBUTES}' nor in 'keyColumns' defined in 'Params' in transformation configuration file""")
        elif table["WriteMode"] == WRITE_MODE_SNAPSHOT:
            assert len(bkColumns) > 0, (
                f"""Table '{sourceDatabaseName}.{sourceTableName}' does not have property 'keyColumns' defined in 'Params' in transformation configuration file or it is empty""")

        return bkColumns
    
    def _generateDtOutput(self, entitiesDF: DataFrame, sinkDatabaseName: str, fwkLayerId: str, fwkTriggerId: str, runMaintenanceManager: bool) -> DataFrame:
        dtOutputRecords = []
        lastUpdate = datetime.now()
        insertTime = lastUpdate
        
        # Set default values for missing params keys

        tables = (
            entitiesDF
            .withColumn("KeyColumns", col("Params.keyColumns"))
            .withColumn("PartitionColumns", col("Params.partitionColumns"))
            .withColumn("FwkTriggerId", coalesce(col("Params.fwkTriggerId"), lit(fwkTriggerId)))
            .withColumn("BatchNumber", coalesce(col("Params.batchNumber"), lit(1)))
            .select("DatabaseName", "EntityName", "Transformations", "KeyColumns", "PartitionColumns", "SinkEntityName", "FwkTriggerId", "WriteMode", "BatchNumber")
            .rdd.collect()
        )
        
        for table in tables:
            try:
                sourceTableName = table["EntityName"]
                sourceDatabaseName = table["DatabaseName"]
                sinkTableName = table["SinkEntityName"]
                
                # validate write mode
                assert table["WriteMode"] in [
                        WRITE_MODE_SCD_TYPE1, 
                        WRITE_MODE_SCD_TYPE2, 
                        WRITE_MODE_SCD_TYPE2_DELTA, 
                        WRITE_MODE_SCD_TYPE2_DELETE, 
                        WRITE_MODE_SNAPSHOT,
                        WRITE_MODE_OVERWRITE,
                        WRITE_MODE_APPEND
                    ], (
                    f"""WriteMode '{table["WriteMode"]}' for '{sourceDatabaseName}.{sourceTableName}' is not supported""")

                bkColumns = self._identifyAndValidateBusinessKeys(table, sourceDatabaseName, sourceTableName)
                keyColumns = ConfigGeneratorHelper.stringifyArray(bkColumns)

                partitionColumns = ConfigGeneratorHelper.stringifyArray(table["PartitionColumns"])

                functions = ConfigGeneratorHelper.convertAndValidateTransformationsToFunctions(table["Transformations"])
                functions = ", ".join(functions)

                inputParameters = f"""{{ "keyColumns": {keyColumns}, "partitionColumns": {partitionColumns}, "schemaEvolution": true, "functions": [{functions}] }}"""

                # generate dtOutput record
                dtOutputRecords.append([
                    f"{sourceDatabaseName}.{sourceTableName}",
                    f"{sinkDatabaseName}.{sinkTableName}",
                    inputParameters,
                    table["WriteMode"],
                    table["FwkTriggerId"],
                    fwkLayerId,
                    table["BatchNumber"],
                    "Y",
                    insertTime,
                    lastUpdate,
                    self._configGeneratorHelper.getDatabricksId()
                ])

                # create maintenance task for partitioning if necessary
                if runMaintenanceManager:
                    self._maintenanceManager.arrangeTablePartition(
                        f"{sinkDatabaseName}.{sinkTableName}",
                        table["PartitionColumns"],
                        table["WriteMode"]
                    )
            except:
                print(f"""Exception details
                    Source entity: {sourceDatabaseName}.{sourceTableName}
                    Sink entity: {sinkDatabaseName}.{sinkTableName}
                """)
                raise
        
        return self._spark.createDataFrame(dtOutputRecords, DT_OUTPUT_SCHEMA)

    def _generateFwkEntities(self, entitiesDF: DataFrame, sinkDatabaseName: str, dtOrderId: str, fwkLinkedServiceId: str, path: str) -> DataFrame:
        fwkEntityRecords = []
        lastUpdate = datetime.now()

        tables = (
            entitiesDF
            .select("EntityName", "SinkEntityName", "DatabaseName")
            .rdd.collect()
        )
        
        for table in tables:
            sinkTableName = None
            try:
                # generate source fwkEntity record (for transformation views)
                # (expect for the first DT layer - source would be landed parquet data and that won't be loaded by a view)
                if self._minDtOrderId != dtOrderId:
                    sourceTableName = table["EntityName"]
                    sourceDatabaseName = table["DatabaseName"]
                    fwkEntityRecords.append([
                        f"{sourceDatabaseName}.{sourceTableName}",
                        "na",
                        None,
                        "Delta",
                        None,
                        None,
                        None,
                        None,
                        lastUpdate,
                        self._configGeneratorHelper.getDatabricksId()
                    ])

                # generate sink fwkEntity record
                sinkTableName = table["SinkEntityName"]

                fwkEntityRecords.append([
                    f"{sinkDatabaseName}.{sinkTableName}",
                    fwkLinkedServiceId,
                    f"{path}/{sinkTableName}",
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
                    Parent parent entity: {table["Entity"]}
                    Linked service: {fwkLinkedServiceId}
                    Entity: {sinkDatabaseName}.{sinkTableName}
                """)
                raise
        
        return self._spark.createDataFrame(fwkEntityRecords, FWK_ENTITY_SCHEMA).distinct()

    def _selectAttributesFromPreviousLayer(self, entitiesDF: DataFrame, sinkDatabaseName: str) -> DataFrame: 
        attributesDF = self._getAttributesForEntitiesDatabaseNames(entitiesDF)
        
        attributesForEntitiesDF = (
            entitiesDF
            .select("DatabaseName", "EntityName", "SinkEntityName")
            .alias("entities")
            .join(
                attributesDF.filter("IsPrimaryKey OR IsIdentity").alias("attributes"),
                (entitiesDF.DatabaseName == attributesDF.DatabaseName) & (entitiesDF.EntityName == attributesDF.EntityName),
                "left"
            )
            .select("entities.EntityName", "attributes.Attribute", "attributes.IsPrimaryKey", "attributes.IsIdentity", "SinkEntityName")
            .selectExpr(
                f"'{sinkDatabaseName}' AS DatabaseName",
                "IF(SinkEntityName IS NULL, EntityName, SinkEntityName) AS EntityName",
                "Attribute",
                "IsPrimaryKey",
                "IsIdentity",
                f"'{METADATA_ATTRIBUTES_TRANSFORMATION_KEY}' AS MGT_ConfigTable" 
            )
            .distinct()
        )

        return attributesForEntitiesDF

    def _generateAttributes(self, entitiesDF: DataFrame, sinkDatabaseName: str) -> DataFrame:        
        attributesForEntitiesDF = self._selectAttributesFromPreviousLayer(entitiesDF, sinkDatabaseName)

        # Override primary columns for SCDType1 & 2 tables if keyColumns where specified by the user

        entitiesWithKeyColumnsDF = (
            entitiesDF
            .withColumn("KeyColumns", col("Params.keyColumns"))
            .filter("KeyColumns IS NOT NULL AND WriteMode LIKE 'SCDType%'")
            .withColumn("DatabaseName", lit(sinkDatabaseName))
            .withColumn("EntityName", coalesce(col("SinkEntityName"), col("EntityName")))
            .select("DatabaseName", "EntityName", "KeyColumns")
        )

        attributesForEntitiesDF = attributesForEntitiesDF.join(
            entitiesWithKeyColumnsDF,
            (attributesForEntitiesDF.DatabaseName == entitiesWithKeyColumnsDF.DatabaseName) & (attributesForEntitiesDF.EntityName == entitiesWithKeyColumnsDF.EntityName),
            "leftanti"
        )

        entitiesWithKeyColumnsDF = (
            entitiesWithKeyColumnsDF
            .selectExpr(
                "DatabaseName",
                "EntityName",
                "EXPLODE(KeyColumns) AS Attribute",
                "TRUE AS IsPrimaryKey",
                "FALSE AS IsIdentity",
                f"'{METADATA_ATTRIBUTES_TRANSFORMATION_KEY}' AS MGT_ConfigTable" 
            )
        )

        attributesForEntitiesDF = attributesForEntitiesDF.union(entitiesWithKeyColumnsDF)

        # Add primary column MGT_effFrom for SCDType2 tables

        entitiesSCDType2DF = entitiesDF.filter(f"WriteMode = '{WRITE_MODE_SCD_TYPE2}' or WriteMode = '{WRITE_MODE_SCD_TYPE2_DELTA}'")

        attributesForEntitiesSCDType2DF = (
            entitiesSCDType2DF
            .selectExpr(
                f"'{sinkDatabaseName}' AS DatabaseName",
                "IF(SinkEntityName IS NULL, EntityName, SinkEntityName) AS EntityName",
                f"'{VALID_FROM_DATETIME_COLUMN_NAME}' AS Attribute",
                "TRUE AS IsPrimaryKey",
                "FALSE AS IsIdentity",
                f"'{METADATA_ATTRIBUTES_TRANSFORMATION_KEY}' AS MGT_ConfigTable" 
            )
        )

        return attributesForEntitiesDF.union(attributesForEntitiesSCDType2DF)

    def generateConfiguration(self, metadataConfig: MapType, configurationOutputPath: str, metadataDeltaTablePath: str, runMaintenanceManager: bool) -> DataFrame:
        """Generates transformations configuration (DtOutput.csv and FwkEntity.csv) and
        metadata information about all sink entities.

        Returns:
            Metadata information about all sink entities.
        """

        if self._minDtOrderId is None:
            self._minDtOrderId = metadataConfig["DtOrder"]

        (sinkPath, sinkUri) = self._configGeneratorHelper.getADLSPathAndUri(metadataConfig["sink"])
        
        # identify entities for which to generate instructions
        entitiesDF = self._getEntities(metadataConfig)

        defaultWriteMode = metadataConfig["sink"]["writeMode"] if "writeMode" in metadataConfig["sink"].keys() else WRITE_MODE_OVERWRITE

        entitiesDF = (
            entitiesDF
            .withColumn("SinkEntityName", coalesce(col("Params.sinkEntityName"), col("EntityName")))
            .withColumn("WriteMode", coalesce(col("Params.writeMode"), lit(defaultWriteMode)))
        )

        # generate DtOutput
        dtOutputDF = self._generateDtOutput(
            entitiesDF,
            metadataConfig["sink"]["databaseName"],
            metadataConfig["FwkLayerId"],
            metadataConfig["FwkTriggerId"],
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
            entitiesDF,
            metadataConfig["sink"]["databaseName"],
            metadataConfig["DtOrder"],
            metadataConfig["sink"]["FwkLinkedServiceId"],
            sinkPath
        )
        
        self._dataLakeHelper.writeDataAsParquet(
            fwkEntitiesDF,
            f"{configurationOutputPath}/{FWK_ENTITY_FOLDER}/",
            WRITE_MODE_APPEND,
            dropDuplicatesByColumn=["FwkEntityId"],
            dropDuplicatesOrderSpec=when(col("FwkLinkedServiceId") == "na", 1).otherwise(0)
        )

        # having the same functionality as in the previous version of the code

        fwkEntitiesCurrDF = self._spark.sql(f"SELECT * FROM {self._compartmentConfig.FWK_METADATA_CONFIG['metadata']['databaseName']}.{FWK_ENTITY_TABLE}")
        fwkEntitiesDF = fwkEntitiesCurrDF.union(fwkEntitiesDF)
        dropDuplicatesOrderSpec = when(col("FwkLinkedServiceId") == "na", 1).otherwise(0)
        fwkEntitiesDF = self._dataLakeHelper._dropDuplicates(fwkEntitiesDF, ["FwkEntityId"], dropDuplicatesOrderSpec)

        # save the fwkEntitiesDF as delta table
        self._dataLakeHelper.writeData(
            fwkEntitiesDF, 
            f"{metadataDeltaTablePath}/{FWK_ENTITY_TABLE}/",
            WRITE_MODE_OVERWRITE, 
            "delta", 
            {"keyColumns": ["FwkEntityId"]},
            self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"], 
            FWK_ENTITY_TABLE
        )

        # generate Attributes
        attributesDF = self._generateAttributes(
            entitiesDF,
            metadataConfig["sink"]["databaseName"]
        )

        return attributesDF
