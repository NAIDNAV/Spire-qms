# Databricks notebook source
import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from unittest.mock import MagicMock, ANY

# local environment imports
if "dbutils" not in globals():
    from notebooks.compartment.includes.compartmentConfig import *
    from notebooks.mdmf.maintenance.maintenanceManager import MaintenanceManager
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../../compartment/includes/compartmentConfig

# COMMAND ----------

# MAGIC %run ../../../../maintenance/maintenanceManager

# COMMAND ----------

# MAGIC %run ../../../../includes/frameworkConfig

# COMMAND ----------

class MaintenanceManagerTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()
                
        self._compartmentConfig = MagicMock()
        self._compartmentConfig.FWK_METADATA_CONFIG = {
            "metadata": {
                "databaseName": "Application_MEF_Metadata"
            }
        }
    
    def setUp(self):
        self._maintenanceManager = MaintenanceManager(self._spark, self._compartmentConfig)
        self._maintenanceManager._spark = MagicMock()
    
    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()
    
    
    # createMaintenanceTable tests

    def test_createMaintenanceTable(self):
        # act
        self._maintenanceManager.createMaintenanceTable("test_data_path")

        # assert
        self._maintenanceManager._spark.sql.assert_called_once()

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn(
            f"""CREATE TABLE IF NOT EXISTS {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}""",
            sqlCommand
        )
        #  Conditions part
        self.assertIn("USING DELTA", sqlCommand)
        self.assertIn("PARTITIONED BY (Status)", sqlCommand)
        self.assertIn(f"LOCATION \"test_data_path/{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}\"", sqlCommand)


    # createMaintenanceTask tests

    def test_createMaintenanceTask(self):
        # arrange        
        fwkEntityId = "Application_MEF_History.Customer" 
        operation = self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION
        parameters = '{"partitionColumns": ["Col1"]}'

        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()
        
        # act
        self._maintenanceManager._createMaintenanceTask(fwkEntityId, operation, parameters)
        
        # assert
        self._maintenanceManager._deactivateMaintenanceTask.assert_called_once_with(fwkEntityId, operation)
        
        self._maintenanceManager._spark.sql.assert_called_once()

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn(
            f"""INSERT INTO {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}""", 
            sqlCommand
        )
        self.assertIn(
            f"""VALUES ('{fwkEntityId}', '{operation}', '{parameters}', '{self._maintenanceManager.METADATA_MAINTENANCE_STATUS_ACTIVE}', NULL,""",
            sqlCommand
        )


    # deactivateMaintenanceTask tests

    def test_deactivateMaintenanceTask_setsCanceled(self):
        # arrange
        expectedStatus = self._maintenanceManager.METADATA_MAINTENANCE_STATUS_CANCELED
        
        # act
        self._maintenanceManager._deactivateMaintenanceTask(
            "Application_MEF_History.Customer",
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION
        )

        # assert
        self._maintenanceManager._spark.sql.assert_called_once()

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn(
            f"""UPDATE {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}""",
            sqlCommand
        )
        # SET part
        self.assertIn(f"Status = '{expectedStatus}'", sqlCommand)
        self.assertIn(f"{UPDATE_TIME_COLUMN_NAME} = ", sqlCommand)
        self.assertNotIn(f"Message = ", sqlCommand)
        # WHERE part
        self.assertIn(f"FwkEntityId = 'Application_MEF_History.Customer'", sqlCommand)
        self.assertIn(f"AND Operation = '{self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION}'", sqlCommand)
        self.assertIn(f"AND Status = '{self._maintenanceManager.METADATA_MAINTENANCE_STATUS_ACTIVE}'", sqlCommand)

    def test_deactivateMaintenanceTask_setsProcessed(self):
        # arrange
        expectedStatus = self._maintenanceManager.METADATA_MAINTENANCE_STATUS_PROCESSED
        
        # act
        self._maintenanceManager._deactivateMaintenanceTask(
            "Application_MEF_History.Customer",
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION,
            self._maintenanceManager.METADATA_MAINTENANCE_STATUS_PROCESSED
        )

        # assert
        self._maintenanceManager._spark.sql.assert_called_once()

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn(
            f"""UPDATE {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}""",
            sqlCommand
        )
        # SET part
        self.assertIn(f"Status = '{expectedStatus}'", sqlCommand)
        self.assertIn(f"{UPDATE_TIME_COLUMN_NAME} = ", sqlCommand)
        self.assertNotIn(f"Message = ", sqlCommand)
        # WHERE part
        self.assertIn(f"FwkEntityId = 'Application_MEF_History.Customer'", sqlCommand)
        self.assertIn(f"AND Operation = '{self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION}'", sqlCommand)
        self.assertIn(f"AND Status = '{self._maintenanceManager.METADATA_MAINTENANCE_STATUS_ACTIVE}'", sqlCommand)

    def test_deactivateMaintenanceTask_setsFailed(self):
        # arrange
        expectedStatus = self._maintenanceManager.METADATA_MAINTENANCE_STATUS_FAILED
        failureMessage = "Failure Message"
        
        # act
        self._maintenanceManager._deactivateMaintenanceTask(
            "Application_MEF_History.Customer",
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION,
            self._maintenanceManager.METADATA_MAINTENANCE_STATUS_FAILED,
            failureMessage
        )

        # assert
        self._maintenanceManager._spark.sql.assert_called_once()

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn(
            f"""UPDATE {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}""", 
            sqlCommand
        )
        # SET part
        self.assertIn(f"Status = '{expectedStatus}'", sqlCommand)
        self.assertIn(f"{UPDATE_TIME_COLUMN_NAME} = ", sqlCommand)
        self.assertIn(f"Message = '{failureMessage}'" , sqlCommand)
        # WHERE part
        self.assertIn(f"FwkEntityId = 'Application_MEF_History.Customer'", sqlCommand)
        self.assertIn(f"AND Operation = '{self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION}'", sqlCommand)
        self.assertIn(f"AND Status = '{self._maintenanceManager.METADATA_MAINTENANCE_STATUS_ACTIVE}'", sqlCommand)
    
    
    # getParametersOfActiveMaintenanceTask tests

    def test_getParametersOfActiveMaintenanceTask_noActiveRecord(self):
        # arrange
        expectedParameters = None

        activeTaskDF = self._spark.createDataFrame([
            self._spark.sparkContext.emptyRDD(),
        ], "Parameters STRING")

        self._maintenanceManager._spark.sql.return_value = activeTaskDF

        # act
        parameters = self._maintenanceManager._getParametersOfActiveMaintenanceTask( 
            "Application_MEF_History.Customer",
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION
        )

        # assert
        self.assertEqual(parameters, expectedParameters)
        
        self._maintenanceManager._spark.sql.assert_called_once()

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn("SELECT Parameters",sqlCommand)
        self.assertIn(f"""FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}""",sqlCommand)
        self.assertIn("WHERE""", sqlCommand)
        
        # WHERE part
        self.assertIn(f"FwkEntityId = 'Application_MEF_History.Customer'", sqlCommand)
        self.assertIn(f"AND Operation = '{self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION}'", sqlCommand)
        self.assertIn(f"AND Status = '{self._maintenanceManager.METADATA_MAINTENANCE_STATUS_ACTIVE}'", sqlCommand)
        # ORDER BY part 
        self.assertIn(f"ORDER BY {INSERT_TIME_COLUMN_NAME} DESC", sqlCommand)

    def test_getParametersOfActiveMaintenanceTask_activeRecord(self):
        # arrange
        expectedParameters = '{"partitionColumns": ["Col1"]}'

        activeTaskDF = self._spark.createDataFrame([
            [expectedParameters],
        ], "Parameters STRING")

        self._maintenanceManager._spark.sql.return_value = activeTaskDF

        # act
        parameters = self._maintenanceManager._getParametersOfActiveMaintenanceTask( 
            "Application_MEF_History.Customer",
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION
        )

        # assert
        self.assertEqual(parameters, expectedParameters)
        
        self._maintenanceManager._spark.sql.assert_called_once()

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn("SELECT Parameters",sqlCommand)
        self.assertIn(f"""FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}""",sqlCommand)
        self.assertIn("WHERE""", sqlCommand)

        # WHERE part
        self.assertIn(f"FwkEntityId = 'Application_MEF_History.Customer'", sqlCommand)
        self.assertIn(f"AND Operation = '{self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION}'", sqlCommand)
        self.assertIn(f"AND Status = '{self._maintenanceManager.METADATA_MAINTENANCE_STATUS_ACTIVE}'", sqlCommand)
        # ORDER BY part 
        self.assertIn(f"ORDER BY {INSERT_TIME_COLUMN_NAME} DESC", sqlCommand)


    # createOutput tests

    def test_createOutput_success(self):
        # arrange
        executionStart = datetime.now()

        tasksByStatus = {
            "failed": [],
            "processed": ["partitionEntity1", "partitionEntity2"]
        }

        expectedOutput = {
            "duration": ANY,
            "success": True,
            "tasksByStatus": {
                "failed": [],
                "processed": ["partitionEntity1", "partitionEntity2"]
            }
        }

        # act
        output = self._maintenanceManager._createOutput(executionStart, tasksByStatus)

        # assert
        self.assertEqual(output, expectedOutput)

        self.assertIsInstance(output["duration"], int)
    
    def test_createOutput_failure(self):
        # arrange
        executionStart = datetime.now()

        tasksByStatus = {
            "failed": ["partitionEntity1"],
            "processed": ["partitionEntity2"]
        }

        expectedOutput = {
            "duration": ANY,
            "success": False,
            "tasksByStatus": {
                "failed": ["partitionEntity1"],
                "processed": ["partitionEntity2"]
            }
        }

        # act
        output = self._maintenanceManager._createOutput(executionStart, tasksByStatus)

        # assert
        self.assertEqual(output, expectedOutput)

        self.assertIsInstance(output["duration"], int)

    def test_createOutput_noTasksProcessed(self):
        # arrange
        executionStart = datetime.now()

        tasksByStatus = {
            "failed": [],
            "processed":[]
        }

        expectedOutput = {
            "duration": ANY,
            "success": True,
            "tasksByStatus":  {
                "failed": [],
                "processed":[]
            }
        }

        # act
        output = self._maintenanceManager._createOutput(executionStart, tasksByStatus)

        # assert
        self.assertEqual(output, expectedOutput)

        self.assertIsInstance(output["duration"], int)


    # executeMaintenanceTasks test

    def test_executeMaintenanceTasks_activeTasks(self):
        # arrange
        self._maintenanceManager._spark.sql.return_value = self._spark.createDataFrame([
            ["Application_MEF_History.Customer", self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION, '{"partitionColumns": ["Col1", "Col2"]}'],
            ["Application_MEF_History.Address", self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION, '{"partitionColumns": []}'],
        ], "FwkEntityId STRING, Operation STRING, Parameters STRING")

        self._maintenanceManager._executeTablePartition = MagicMock()
        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()

        expectedOutput = {
            "duration": ANY,
            "success": True,
            "tasksByStatus":  {
                "processed": [
                    f"{self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION} Application_MEF_History.Customer",
                    f"{self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION} Application_MEF_History.Address"
                ],
                "failed": []
            }
        }

        # act
        output = self._maintenanceManager.executeMaintenanceTasks()

        # assert
        self.assertEqual(output, expectedOutput)

        self._maintenanceManager._spark.catalog.clearCache.assert_called_once()

        self.assertEqual(self._maintenanceManager._executeTablePartition.call_count, 2)
        self._maintenanceManager._executeTablePartition.assert_has_calls([
            [("Application_MEF_History.Customer", '{"partitionColumns": ["Col1", "Col2"]}'),],
            [("Application_MEF_History.Address", '{"partitionColumns": []}'),],
        ])

        self.assertEqual(self._maintenanceManager._deactivateMaintenanceTask.call_count, 2)
        self._maintenanceManager._deactivateMaintenanceTask.assert_has_calls([
            [("Application_MEF_History.Customer", self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION, self._maintenanceManager.METADATA_MAINTENANCE_STATUS_PROCESSED),],
            [("Application_MEF_History.Address", self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION, self._maintenanceManager.METADATA_MAINTENANCE_STATUS_PROCESSED),],
        ])

        self.assertEqual(self._maintenanceManager._spark.sql.call_count, 2)

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn("SELECT FwkEntityId, Operation, Parameters", sqlCommand)
        self.assertIn(f"""FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}""", sqlCommand)
        self.assertIn(f"WHERE Status = '{self._maintenanceManager.METADATA_MAINTENANCE_STATUS_ACTIVE}'", sqlCommand)
        self.assertIn(f"ORDER BY {INSERT_TIME_COLUMN_NAME} ASC", sqlCommand)

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[1].args[0]
        self.assertIn(f"""OPTIMIZE {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}""", sqlCommand)

    def test_executeMaintenanceTasks_noActiveTasks(self):
        # arrange
        self._maintenanceManager._spark.sql.return_value = self._spark.createDataFrame(
            self._spark.sparkContext.emptyRDD(),
            "FwkEntityId STRING, Operation STRING, Parameters STRING"
        )

        self._maintenanceManager._executeTablePartition = MagicMock()
        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()

        expectedOutput = {
            "duration": ANY,
            "success": True,
            "tasksByStatus":  {
                "processed": [],
                "failed": []
            }
        }

        # act
        output = self._maintenanceManager.executeMaintenanceTasks()

        # assert
        self.assertEqual(output, expectedOutput)

        self._maintenanceManager._spark.catalog.clearCache.assert_called_once()

        self.assertEqual(self._maintenanceManager._executeTablePartition.call_count, 0)

        self.assertEqual(self._maintenanceManager._deactivateMaintenanceTask.call_count, 0)

        self.assertEqual(self._maintenanceManager._spark.sql.call_count, 1)

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn("SELECT FwkEntityId, Operation, Parameters", sqlCommand)
        self.assertIn(f"""FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}""", sqlCommand)
        self.assertIn(f"WHERE Status = '{self._maintenanceManager.METADATA_MAINTENANCE_STATUS_ACTIVE}'", sqlCommand)
        self.assertIn(f"ORDER BY {INSERT_TIME_COLUMN_NAME} ASC", sqlCommand)

    def test_executeMaintenanceTasks_exceptionDuringExecutionOfOneTask(self):
        # arrange
        self._maintenanceManager._spark.sql.return_value = self._spark.createDataFrame([
            ["Application_MEF_History.Customer", "Not_supported_operation", '{"partitionColumns": ["Col1", "Col2"]}'],
            ["Application_MEF_History.Address", self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION, '{"partitionColumns": []}'],
        ], "FwkEntityId STRING, Operation STRING, Parameters STRING")

        self._maintenanceManager._executeTablePartition = MagicMock()
        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()

        expectedOutput = {
            "duration": ANY,
            "success": False,
            "tasksByStatus":  {
                "processed": [
                    f"{self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION} Application_MEF_History.Address"
                ],
                "failed": [
                    f"Not_supported_operation Application_MEF_History.Customer"
                ]
            }
        }

        # act
        output = self._maintenanceManager.executeMaintenanceTasks()

        # assert
        self.assertEqual(output, expectedOutput)

        self._maintenanceManager._spark.catalog.clearCache.assert_called_once()

        self.assertEqual(self._maintenanceManager._executeTablePartition.call_count, 1)
        self._maintenanceManager._executeTablePartition.assert_has_calls([
            [("Application_MEF_History.Address", '{"partitionColumns": []}'),],
        ])

        self.assertEqual(self._maintenanceManager._deactivateMaintenanceTask.call_count, 2)
        self._maintenanceManager._deactivateMaintenanceTask.assert_has_calls([
            [("Application_MEF_History.Customer", "Not_supported_operation", self._maintenanceManager.METADATA_MAINTENANCE_STATUS_FAILED, "'Not_supported_operation' is not implemented"),],
            [("Application_MEF_History.Address", self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION, self._maintenanceManager.METADATA_MAINTENANCE_STATUS_PROCESSED),],
        ])

        self.assertEqual(self._maintenanceManager._spark.sql.call_count, 2)

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn("SELECT FwkEntityId, Operation, Parameters", sqlCommand)
        self.assertIn(f"""FROM {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}""", sqlCommand)
        self.assertIn(f"WHERE Status = '{self._maintenanceManager.METADATA_MAINTENANCE_STATUS_ACTIVE}'", sqlCommand)
        self.assertIn(f"ORDER BY {INSERT_TIME_COLUMN_NAME} ASC", sqlCommand)

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[1].args[0]
        self.assertIn(f"""OPTIMIZE {self._compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}.{self._maintenanceManager.METADATA_MAINTENANCE_TABLE}""", sqlCommand)


    # getPartitionColumnsFormMaintenanceTable tests

    def test_getPartitionColumnsFormMaintenanceTable_noActiveTask(self):
        # arrange
        self._maintenanceManager._getParametersOfActiveMaintenanceTask = MagicMock(return_value=None)
        
        # act
        partitionColumns = self._maintenanceManager._getPartitionColumnsFormMaintenanceTable("Application_MEF_History.Customer")

        # assert
        self.assertEqual(partitionColumns, None)
    
    def test_getPartitionColumnsFormMaintenanceTable_emptyPartitionColumns(self):
        # arrange
        self._maintenanceManager._getParametersOfActiveMaintenanceTask = MagicMock(return_value='{"partitionColumns": []}')

        # act
        partitionColumns = self._maintenanceManager._getPartitionColumnsFormMaintenanceTable("Application_MEF_History.Customer")

        # assert
        self.assertEqual(partitionColumns, [])

    def test_getPartitionColumnsFormMaintenanceTable_nonEmptyPartitionColumns(self):        
        # arrange
        self._maintenanceManager._getParametersOfActiveMaintenanceTask = MagicMock(return_value='{"partitionColumns": ["Col1","Col2"]}')

        # act
        partitionColumns = self._maintenanceManager._getPartitionColumnsFormMaintenanceTable("Application_MEF_History.Customer")

        # assert
        self.assertEqual(partitionColumns, ["Col1","Col2"])


    # getPartitionColumnsFromHIVE tests

    def test_getPartitionColumnsFromHIVE_nonExistingTable(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        expectedPartitionColumns = []

        self._maintenanceManager._spark.catalog.tableExists.return_value = False
                
        # act
        partitionColumns = self._maintenanceManager._getPartitionColumnsFromHIVE(fwkEntityId)

        # assert
        self.assertEqual(partitionColumns, expectedPartitionColumns)
        
        self._maintenanceManager._spark.catalog.tableExists.assert_called_once_with(fwkEntityId)

        self._maintenanceManager._spark.sql.assert_not_called()

    def test_getPartitionColumnsFromHIVE_emptyPartitionColumns(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        expectedPartitionColumns = []

        self._maintenanceManager._spark.catalog.tableExists.return_value = True
            
        tableDescriptionDF = self._spark.createDataFrame([
            ["Col1"],
            ["Col2"],
            ["Col3"],
            ["Col4"],
            [""],
            ["additional info"]
        ], "col_name STRING")

        self._maintenanceManager._spark.sql.return_value = tableDescriptionDF
                
        # act
        partitionColumns = self._maintenanceManager._getPartitionColumnsFromHIVE(fwkEntityId)

        # assert
        self.assertEqual(partitionColumns, expectedPartitionColumns)
        
        self._maintenanceManager._spark.catalog.tableExists.assert_called_once_with(fwkEntityId)

        self._maintenanceManager._spark.sql.assert_called_once()

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"DESC FORMATTED {fwkEntityId}", sqlCommand)

    def test_getPartitionColumnsFromHIVE_nonEmptyPartitionColumns(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        expectedPartitionColumns = ["Col1", "Col2"]

        self._maintenanceManager._spark.catalog.tableExists.return_value = True
            
        tableDescriptionDF = self._spark.createDataFrame([
            ["Col1"],
            ["Col2"],
            ["Col3"],
            ["Col4"],
            [""],
            ["# Partition Information"],
            ["# col_name"],
            ["Col1"],
            ["Col2"],
            [""],
            ["additional info"]
        ], "col_name STRING")

        self._maintenanceManager._spark.sql.return_value = tableDescriptionDF
                
        # act
        partitionColumns = self._maintenanceManager._getPartitionColumnsFromHIVE(fwkEntityId)

        # assert
        self.assertEqual(partitionColumns, expectedPartitionColumns)
        
        self._maintenanceManager._spark.catalog.tableExists.assert_called_once_with(fwkEntityId)

        self._maintenanceManager._spark.sql.assert_called_once()

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"DESC FORMATTED {fwkEntityId}", sqlCommand)


    # arrangeTablePartition tests

    def test_arrangeTablePartition_writeModeOverwrite(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        partitionColumns = ["Col1", "Col2"]
        
        self._maintenanceManager._spark.catalog.tableExists.return_value = True

        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()
        self._maintenanceManager._getPartitionColumnsFromHIVE = MagicMock()
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable = MagicMock()
        self._maintenanceManager._createMaintenanceTask = MagicMock()
           
        # act
        self._maintenanceManager.arrangeTablePartition(
            fwkEntityId, 
            partitionColumns, 
            WRITE_MODE_OVERWRITE
        )
        
        # assert
        self._maintenanceManager._deactivateMaintenanceTask.assert_called_once_with(
            fwkEntityId,
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION
        )
        self._maintenanceManager._createMaintenanceTask.assert_not_called()

        self._maintenanceManager._getPartitionColumnsFromHIVE.assert_not_called()
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable.assert_not_called()
    
    def test_arrangeTablePartition_tableDoesNotExists(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        partitionColumns = ["Col1", "Col2"]
        
        self._maintenanceManager._spark.catalog.tableExists.return_value = False

        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()
        self._maintenanceManager._getPartitionColumnsFromHIVE = MagicMock()
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable = MagicMock()
        self._maintenanceManager._createMaintenanceTask = MagicMock()
           
        # act
        self._maintenanceManager.arrangeTablePartition(
            fwkEntityId, 
            partitionColumns, 
            WRITE_MODE_SCD_TYPE1
        )
        
        # assert
        self._maintenanceManager._deactivateMaintenanceTask.assert_called_once_with(
            fwkEntityId,
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION
        )
        self._maintenanceManager._createMaintenanceTask.assert_not_called()

        self._maintenanceManager._spark.catalog.tableExists.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFromHIVE.assert_not_called()
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable.assert_not_called()
    
    def test_arrangeTablePartition_samePartitionColumnsAsInHive_empty(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        partitionColumns = []
        partitionColumnsInHive = []
        
        self._maintenanceManager._spark.catalog.tableExists.return_value = True

        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()
        self._maintenanceManager._getPartitionColumnsFromHIVE = MagicMock(return_value=partitionColumnsInHive)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable = MagicMock()
        self._maintenanceManager._createMaintenanceTask = MagicMock()
           
        # act
        self._maintenanceManager.arrangeTablePartition(
            fwkEntityId, 
            partitionColumns, 
            WRITE_MODE_SCD_TYPE1
        )
        
        # assert
        self._maintenanceManager._deactivateMaintenanceTask.assert_called_once_with(
            fwkEntityId,
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION
        )
        self._maintenanceManager._createMaintenanceTask.assert_not_called()

        self._maintenanceManager._spark.catalog.tableExists.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFromHIVE.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable.assert_not_called()
    
    def test_arrangeTablePartition_samePartitionColumnsAsInHive_emptySentAsNone(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        partitionColumns = None
        partitionColumnsInHive = []
        
        self._maintenanceManager._spark.catalog.tableExists.return_value = True

        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()
        self._maintenanceManager._getPartitionColumnsFromHIVE = MagicMock(return_value=partitionColumnsInHive)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable = MagicMock()
        self._maintenanceManager._createMaintenanceTask = MagicMock()
           
        # act
        self._maintenanceManager.arrangeTablePartition(
            fwkEntityId, 
            partitionColumns, 
            WRITE_MODE_SCD_TYPE1
        )
        
        # assert
        self._maintenanceManager._deactivateMaintenanceTask.assert_called_once_with(
            fwkEntityId,
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION
        )
        self._maintenanceManager._createMaintenanceTask.assert_not_called()

        self._maintenanceManager._spark.catalog.tableExists.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFromHIVE.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable.assert_not_called()

    def test_arrangeTablePartition_samePartitionColumnsAsInHive_nonEmpty(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        partitionColumns = ["Col1", "Col2"]
        partitionColumnsInHive = ["Col1", "Col2"]
        
        self._maintenanceManager._spark.catalog.tableExists.return_value = True

        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()
        self._maintenanceManager._getPartitionColumnsFromHIVE = MagicMock(return_value=partitionColumnsInHive)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable = MagicMock()
        self._maintenanceManager._createMaintenanceTask = MagicMock()
           
        # act
        self._maintenanceManager.arrangeTablePartition(
            fwkEntityId, 
            partitionColumns, 
            WRITE_MODE_SCD_TYPE1
        )
        
        # assert
        self._maintenanceManager._deactivateMaintenanceTask.assert_called_once_with(
            fwkEntityId,
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION
        )
        self._maintenanceManager._createMaintenanceTask.assert_not_called()

        self._maintenanceManager._spark.catalog.tableExists.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFromHIVE.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable.assert_not_called()

    def test_arrangeTablePartition_samePartitionColumnsAsInMaintenanceTable_empty(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        partitionColumns = []
        partitionColumnsInHive = ["Col1", "Col2"]
        partitionColumnsInMaintenanceTable = []
        
        self._maintenanceManager._spark.catalog.tableExists.return_value = True

        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()
        self._maintenanceManager._getPartitionColumnsFromHIVE = MagicMock(return_value=partitionColumnsInHive)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable = MagicMock(return_value=partitionColumnsInMaintenanceTable)
        self._maintenanceManager._createMaintenanceTask = MagicMock()
           
        # act
        self._maintenanceManager.arrangeTablePartition(
            fwkEntityId, 
            partitionColumns, 
            WRITE_MODE_SCD_TYPE1
        )
        
        # assert
        self._maintenanceManager._deactivateMaintenanceTask.assert_not_called()
        self._maintenanceManager._createMaintenanceTask.assert_not_called()

        self._maintenanceManager._spark.catalog.tableExists.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFromHIVE.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable.assert_called_once_with(fwkEntityId)

    def test_arrangeTablePartition_samePartitionColumnsAsInMaintenanceTable_nonEmpty(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        partitionColumns = ["Col1", "Col2"]
        partitionColumnsInHive = ["Col1"]
        partitionColumnsInMaintenanceTable = ["Col1", "Col2"]
        
        self._maintenanceManager._spark.catalog.tableExists.return_value = True

        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()
        self._maintenanceManager._getPartitionColumnsFromHIVE = MagicMock(return_value=partitionColumnsInHive)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable = MagicMock(return_value=partitionColumnsInMaintenanceTable)
        self._maintenanceManager._createMaintenanceTask = MagicMock()
           
        # act
        self._maintenanceManager.arrangeTablePartition(
            fwkEntityId, 
            partitionColumns, 
            WRITE_MODE_SCD_TYPE1
        )
        
        # assert
        self._maintenanceManager._deactivateMaintenanceTask.assert_not_called()
        self._maintenanceManager._createMaintenanceTask.assert_not_called()

        self._maintenanceManager._spark.catalog.tableExists.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFromHIVE.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable.assert_called_once_with(fwkEntityId)

    def test_arrangeTablePartition_differentPartitionColumnsInMaintenanceTable_empty(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        partitionColumns = []
        partitionColumnsInHive = ["Col1"]
        partitionColumnsInMaintenanceTable = ["Col1", "Col2"]
        
        self._maintenanceManager._spark.catalog.tableExists.return_value = True

        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()
        self._maintenanceManager._getPartitionColumnsFromHIVE = MagicMock(return_value=partitionColumnsInHive)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable = MagicMock(return_value=partitionColumnsInMaintenanceTable)
        self._maintenanceManager._createMaintenanceTask = MagicMock()
           
        # act
        self._maintenanceManager.arrangeTablePartition(
            fwkEntityId, 
            partitionColumns, 
            WRITE_MODE_SCD_TYPE1
        )
        
        # assert
        self._maintenanceManager._createMaintenanceTask.assert_called_once_with(
            fwkEntityId,
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION,
            '{"partitionColumns": []}'
        )
        self._maintenanceManager._deactivateMaintenanceTask.assert_not_called()

        self._maintenanceManager._spark.catalog.tableExists.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFromHIVE.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable.assert_called_once_with(fwkEntityId)

    def test_arrangeTablePartition_differentPartitionColumnsInMaintenanceTable_nonEmpty(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        partitionColumns = ["Col1", "Col2"]
        partitionColumnsInHive = ["Col1"]
        partitionColumnsInMaintenanceTable = ["Col2", "Col1"]
        
        self._maintenanceManager._spark.catalog.tableExists.return_value = True

        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()
        self._maintenanceManager._getPartitionColumnsFromHIVE = MagicMock(return_value=partitionColumnsInHive)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable = MagicMock(return_value=partitionColumnsInMaintenanceTable)
        self._maintenanceManager._createMaintenanceTask = MagicMock()
           
        # act
        self._maintenanceManager.arrangeTablePartition(
            fwkEntityId, 
            partitionColumns, 
            WRITE_MODE_SCD_TYPE1
        )
        
        # assert
        self._maintenanceManager._createMaintenanceTask.assert_called_once_with(
            fwkEntityId,
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION,
            '{"partitionColumns": ["Col1", "Col2"]}'
        )
        self._maintenanceManager._deactivateMaintenanceTask.assert_not_called()

        self._maintenanceManager._spark.catalog.tableExists.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFromHIVE.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable.assert_called_once_with(fwkEntityId)

    def test_arrangeTablePartition_noRecordInMaintenanceTable(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        partitionColumns = ["Col1", "Col2"]
        partitionColumnsInHive = ["Col1"]
        partitionColumnsInMaintenanceTable = None
        
        self._maintenanceManager._spark.catalog.tableExists.return_value = True

        self._maintenanceManager._deactivateMaintenanceTask = MagicMock()
        self._maintenanceManager._getPartitionColumnsFromHIVE = MagicMock(return_value=partitionColumnsInHive)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable = MagicMock(return_value=partitionColumnsInMaintenanceTable)
        self._maintenanceManager._createMaintenanceTask = MagicMock()
           
        # act
        self._maintenanceManager.arrangeTablePartition(
            fwkEntityId, 
            partitionColumns, 
            WRITE_MODE_SCD_TYPE1
        )
        
        # assert
        self._maintenanceManager._createMaintenanceTask.assert_called_once_with(
            fwkEntityId,
            self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION,
            '{"partitionColumns": ["Col1", "Col2"]}'
        )
        self._maintenanceManager._deactivateMaintenanceTask.assert_not_called()

        self._maintenanceManager._spark.catalog.tableExists.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFromHIVE.assert_called_once_with(fwkEntityId)
        self._maintenanceManager._getPartitionColumnsFormMaintenanceTable.assert_called_once_with(fwkEntityId)


    # executeTablePartition tests
    
    def test_executeTablePartition_partitionColumns(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        expectedColumn = "(`Col1`, `Col2`)"
        parameters = '{"partitionColumns": ["Col1", "Col2"]}'
        
        # act
        self._maintenanceManager._executeTablePartition(fwkEntityId, parameters)
        
        # assert
        self._maintenanceManager._spark.sql.assert_called_once()

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"CREATE OR REPLACE TABLE {fwkEntityId}", sqlCommand)
        self.assertIn("USING DELTA", sqlCommand)
        self.assertIn(f"PARTITIONED BY {expectedColumn}", sqlCommand)
        self.assertIn(f"AS SELECT * FROM {fwkEntityId}", sqlCommand)
    
    def test_executeTablePartition_noPartitionColumns(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        parameters = '{"partitionColumns": []}'
        
        # act
        self._maintenanceManager._executeTablePartition(fwkEntityId, parameters)
        
        # assert
        self._maintenanceManager._spark.sql.assert_called_once()

        sqlCommand = self._maintenanceManager._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"CREATE OR REPLACE TABLE {fwkEntityId}", sqlCommand)
        self.assertIn("USING DELTA", sqlCommand)
        self.assertNotIn(f"PARTITIONED BY ", sqlCommand)
        self.assertIn(f"AS SELECT * FROM {fwkEntityId}", sqlCommand)

    def test_executeTablePartition_noPartitionColumnProperty(self):
        # arrange
        fwkEntityId = "Application_MEF_History.Customer"
        parameters = '{}'
        
        # act
        with self.assertRaisesRegex(AssertionError, f"'partitionColumns' is a mandatory property of 'Parameters' for {self._maintenanceManager.METADATA_MAINTENANCE_OPERATION_PARTITION} operation."):
            self._maintenanceManager._executeTablePartition(fwkEntityId, parameters)
        
        # assert
        self._maintenanceManager._spark.sql.assert_not_called()
