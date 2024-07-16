# Databricks notebook source
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, MapType
from datetime import datetime

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.includes.frameworkConfig import *


class MaintenanceManager:
    """MaintenanceManager is responsible for creating and executing maintenance tasks.
    
    MaintenanceManager creates tasks in the Maintenance table in HIVE. Execution of the
    tasks is triggered by the maintenance pipeline.

    Public methods:
        createMaintenanceTable
        arrangeTablePartition
        executeMaintenanceTasks
    """

    METADATA_MAINTENANCE_TABLE = "Maintenance"

    METADATA_MAINTENANCE_OPERATION_PARTITION = "Partition"

    METADATA_MAINTENANCE_STATUS_ACTIVE = "Active"
    METADATA_MAINTENANCE_STATUS_CANCELED = "Canceled"
    METADATA_MAINTENANCE_STATUS_PROCESSED = "Processed"
    METADATA_MAINTENANCE_STATUS_FAILED = "Failed"


    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._compartmentConfig = compartmentConfig

    def createMaintenanceTable(self, dataPath: str):
        """Creates Maintenance table in HIVE.
            
        Maintenance table is created in metadata database and is used to store
        maintenance tasks that will be triggered by the maintenance pipeline.
        """
        
        self._spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self.METADATA_MAINTENANCE_TABLE} (
                FwkEntityId STRING,
                Operation STRING,
                Parameters STRING,
                Status STRING,
                Message STRING,
                {INSERT_TIME_COLUMN_NAME} TIMESTAMP,
                {UPDATE_TIME_COLUMN_NAME} TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (Status)
            LOCATION "{dataPath}/{self.METADATA_MAINTENANCE_TABLE}"
        """)

    def _createMaintenanceTask(self, fwkEntityId: str, operation: str, parameters: str):
        self._deactivateMaintenanceTask(fwkEntityId, operation)
        
        currentTime = datetime.now()
        
        self._spark.sql(f"""
            INSERT INTO {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self.METADATA_MAINTENANCE_TABLE}
            VALUES ('{fwkEntityId}', '{operation}', '{parameters}', '{self.METADATA_MAINTENANCE_STATUS_ACTIVE}', NULL, '{currentTime}', '{currentTime}')
        """)

    def _deactivateMaintenanceTask(self, fwkEntityId: str, operation: str, status: str = None, message: str = None):
        if status is None:
            status = self.METADATA_MAINTENANCE_STATUS_CANCELED

        setMessage = f""", Message = '{message.replace("'", "''")}'""" if message else ""
        
        self._spark.sql(f"""
            UPDATE {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self.METADATA_MAINTENANCE_TABLE}
            SET
                Status = '{status}',
                {UPDATE_TIME_COLUMN_NAME} = '{datetime.now()}'
                {setMessage}
            WHERE
                FwkEntityId = '{fwkEntityId}'
                AND Operation = '{operation}'
                AND Status = '{self.METADATA_MAINTENANCE_STATUS_ACTIVE}'
        """)

    def _getParametersOfActiveMaintenanceTask(self, fwkEntityId: str, operation: str) -> str:
        parameters = self._spark.sql(f"""
            SELECT Parameters
            FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self.METADATA_MAINTENANCE_TABLE}
            WHERE
                FwkEntityId = '{fwkEntityId}'
                AND Operation = '{operation}'
                AND Status = '{self.METADATA_MAINTENANCE_STATUS_ACTIVE}'
            ORDER BY {INSERT_TIME_COLUMN_NAME} DESC
        """).first()

        if parameters:
            return parameters[0]
        else:
            return None

    @staticmethod
    def _printTasks(title: str, tasks: ArrayType):
        if tasks:
            print(title)

            for task in tasks:
                print(f"   - {task}")

    @staticmethod
    def printOutput(output: MapType):
        "Pretty prints the result of the maintenance task execution."

        print("\n\033[1mRESULT\x1b[0m")
        print("\033[1m------\x1b[0m")
        box = ("\x1b[42m  \x1b[0m" if output["success"] else "\x1b[41m  \x1b[0m")
        print(f"""Success: {box} {output["success"]}""")
        print(f"""Duration: {output["duration"]}s""")
        MaintenanceManager._printTasks("Failed tasks:", output["tasksByStatus"]["failed"])
        MaintenanceManager._printTasks("Processed tasks:", output["tasksByStatus"]["processed"])

    @staticmethod
    def _createOutput(executionStart: datetime, tasksByStatus: MapType) -> MapType:
        executionEnd = datetime.now()

        output = {
            "duration": (executionEnd - executionStart).seconds,
            "success": len(tasksByStatus["failed"]) == 0,
            "tasksByStatus": tasksByStatus
        }

        return output

    def executeMaintenanceTasks(self) -> MapType:
        """Executes maintenance tasks.

        Reads active maintenance tasks from the Maintenance table in HIVE and executes them
        one by one.

        Returns:
            Output containing information about execution duration and execution statistics.
        """

        # Initialization

        executionStart = datetime.now()

        self._spark.catalog.clearCache()

        # Get tasks
        
        tasks = (
            self._spark.sql(f"""
                SELECT FwkEntityId, Operation, Parameters
                FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self.METADATA_MAINTENANCE_TABLE}
                WHERE Status = '{self.METADATA_MAINTENANCE_STATUS_ACTIVE}'
                ORDER BY {INSERT_TIME_COLUMN_NAME} ASC
            """)
            .rdd.collect()
        )

        tasksByStatus = {
            "processed": [],
            "failed": []
        }

        # Execute tasks

        for task in tasks:
            try:
                fwkEntityId = task["FwkEntityId"]
                operation = task["Operation"]
                parameters = task["Parameters"]
                tasksByStatusEntry = f"{operation} {fwkEntityId}"

                print(f"""{operation} {fwkEntityId} - {parameters} :""")

                if operation == self.METADATA_MAINTENANCE_OPERATION_PARTITION:
                    self._executeTablePartition(fwkEntityId, parameters)
                else:
                    raise NotImplementedError(f"'{operation}' is not implemented")

                tasksByStatus["processed"].append(tasksByStatusEntry)
                print("\x1b[42m  \x1b[0m\x1b[32m PROCESSED\x1b[0m\n")

                self._deactivateMaintenanceTask(fwkEntityId, operation, self.METADATA_MAINTENANCE_STATUS_PROCESSED)
            except Exception as e:
                tasksByStatus["failed"].append(tasksByStatusEntry)
                print(f"\x1b[31m{e}\x1b[0m")
                print("\x1b[41m  \x1b[0m\x1b[31m FAILED\x1b[0m\n")
                
                self._deactivateMaintenanceTask(fwkEntityId, operation, self.METADATA_MAINTENANCE_STATUS_FAILED, str(e))

        # Optimize Maintenance table to mitigate small files problem

        if len(tasks) > 0:
            self._spark.sql(f"""OPTIMIZE {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self.METADATA_MAINTENANCE_TABLE}""")

        # Create output
        
        output = self._createOutput(executionStart, tasksByStatus)

        return output


    # Partition operation

    def _getPartitionColumnsFormMaintenanceTable(self, fwkEntityId: str) -> ArrayType:
        parameters = self._getParametersOfActiveMaintenanceTask(fwkEntityId, self.METADATA_MAINTENANCE_OPERATION_PARTITION)
        
        if parameters is None:
            return None
        
        partitionColumns = []

        parsedParameters = json.loads(parameters)
        if "partitionColumns" in parsedParameters.keys():
            partitionColumns = parsedParameters["partitionColumns"]

        return partitionColumns

    def _getPartitionColumnsFromHIVE(self, fwkEntityId: str) -> ArrayType:
        partitionColumns = []
        
        if not self._spark.catalog.tableExists(fwkEntityId):
            return partitionColumns

        descColumns = (
            self._spark.sql(f"DESC FORMATTED {fwkEntityId}")
            .select("col_name")
            .rdd.map(lambda x: x[0])
            .collect()
        )
        
        if "# Partition Information" in descColumns:
            for index in range(descColumns.index("# Partition Information") + 2, len(descColumns)):    
                if descColumns[index] == '':
                    break
                
                partitionColumns.append(descColumns[index])
        
        return partitionColumns

    def arrangeTablePartition(self, fwkEntityId: str, partitionColumns: ArrayType, writeMode: str):
        """Decides if the table has to be (re)partitioned and creates a maintenance task.

        Maintenance task is created in the Maintenance table in HIVE and execution is
        triggered by the maintenance pipeline.
        """

        if (writeMode == WRITE_MODE_OVERWRITE
            or not self._spark.catalog.tableExists(fwkEntityId)
        ):
            # ETL will repartition data using writeData function
            self._deactivateMaintenanceTask(fwkEntityId, self.METADATA_MAINTENANCE_OPERATION_PARTITION)
            return
        
        if partitionColumns is None:
            partitionColumns = []

        partitionColumnsInHive = self._getPartitionColumnsFromHIVE(fwkEntityId)
        if partitionColumns == partitionColumnsInHive:
            # table already has desired partition columns
            self._deactivateMaintenanceTask(fwkEntityId, self.METADATA_MAINTENANCE_OPERATION_PARTITION)
            return

        partitionColumnsInMaintenanceTable = self._getPartitionColumnsFormMaintenanceTable(fwkEntityId)
        if partitionColumns == partitionColumnsInMaintenanceTable:
            # table is already scheduled to get desired partition columns
            return
        else:
            # schedule table to get desired partition columns
            parameters = json.dumps({
                "partitionColumns": partitionColumns
            })
            self._createMaintenanceTask(fwkEntityId, self.METADATA_MAINTENANCE_OPERATION_PARTITION, parameters)

    def _executeTablePartition(self, fwkEntityId: str, parameters: str):
        parsedParameters = json.loads(parameters)
        
        assert "partitionColumns" in parsedParameters.keys(), f"'partitionColumns' is a mandatory property of 'Parameters' for {self.METADATA_MAINTENANCE_OPERATION_PARTITION} operation."
        
        partitionColumns = parsedParameters["partitionColumns"]
        partitionedBy = f"PARTITIONED BY (`{'`, `'.join(partitionColumns)}`)" if partitionColumns else ""

        sqlCommand = f"""
            CREATE OR REPLACE TABLE {fwkEntityId}
            USING DELTA
            {partitionedBy}
            AS SELECT * FROM {fwkEntityId}
        """

        self._spark.sql(sqlCommand)
