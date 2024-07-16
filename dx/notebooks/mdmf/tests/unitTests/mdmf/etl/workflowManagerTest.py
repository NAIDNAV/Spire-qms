# Databricks notebook source
import unittest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.etl.workflowManager import WorkflowManager
    from notebooks.mdmf.includes.frameworkConfig import *

# COMMAND ----------

# MAGIC %run ../../../../etl/workflowManager

# COMMAND ----------

# MAGIC %run ../../../../includes/frameworkConfig

# COMMAND ----------

class WorkflowManagerTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()
        self._compartmentConfig = MagicMock()
        self._compartmentConfig.WORKFLOWS_CONFIGURATION_FILE = "Workflows_Configuration.json"

    def setUp(self):
        self._workflowManager = WorkflowManager(self._spark, self._compartmentConfig)
        self._workflowManager._spark = MagicMock()
        self._workflowManager._dbutils = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()

    
    # getWorkflowDefinition tests

    def test_getWorkflowDefinition_noWorkflowsDefinition(self):
        # arrange
        compartmentConfig = MagicMock()
        compartmentConfig.WORKFLOWS_CONFIGURATION_FILE = None
        
        workflowManager = WorkflowManager(self._spark, compartmentConfig)
        workflowManager._configGeneratorHelper.readMetadataConfig = MagicMock()

        # act
        output = workflowManager.getWorkflowDefinition("Post_Historization_Approval")

        # assert
        self.assertEqual(output, {})

        workflowManager._configGeneratorHelper.readMetadataConfig.assert_not_called()

    def test_getWorkflowDefinition_noWorkflowsFileContent(self):
        # arrange
        emptyDF = self._spark.createDataFrame(self._spark.sparkContext.emptyRDD(), f"col BOOLEAN")
        self._workflowManager._configGeneratorHelper.readMetadataConfig = MagicMock(return_value=emptyDF)

        # act
        output = self._workflowManager.getWorkflowDefinition("Post_Historization_Approval")

        # assert
        self.assertEqual(output, {})

        self._workflowManager._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "Workflows_Configuration.json",
            METADATA_WORKFLOWS_CONFIGURATION_SCHEMA
        )

    def test_getWorkflowDefinition_workflowNotFount(self):
        # arrange
        workflowsDF = self._spark.createDataFrame([
            ["ADFTrigger1", "Trigger1", "Post_Historization_Approval", "https://url-to-the-workflow.com", '{"Approval_Email_Address":"approver@mercedes-benz.com","ADFTriggerName":"ADFTrigger1","FwkLayerId":"SDM"}']
        ], "ADFTriggerName STRING, FwkTriggerId STRING, WorkflowId STRING, URL STRING, RequestBody STRING")
        self._workflowManager._configGeneratorHelper.readMetadataConfig = MagicMock(return_value=workflowsDF)

        self._workflowManager._dbutils.widgets.get.return_value = "ADFTrigger1"

        # act
        output = self._workflowManager.getWorkflowDefinition("Pre_Historization_Approval")

        # assert
        self.assertEqual(output, {})

        self._workflowManager._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "Workflows_Configuration.json",
            METADATA_WORKFLOWS_CONFIGURATION_SCHEMA
        )

    def test_getWorkflowDefinition_workflowFountByTriggerIdFromDbutils(self):
        # arrange
        workflowsDF = self._spark.createDataFrame([
            ["ADFTrigger1", "Trigger1", "Post_Historization_Approval", "https://url-to-the-workflow.com", '{"Approval_Email_Address":"approver@mercedes-benz.com","ADFTriggerName":"ADFTrigger1","FwkLayerId":"SDM"}']
        ], "ADFTriggerName STRING, FwkTriggerId STRING, WorkflowId STRING, URL STRING, RequestBody STRING")
        self._workflowManager._configGeneratorHelper.readMetadataConfig = MagicMock(return_value=workflowsDF)

        self._workflowManager._dbutils.widgets.get.return_value = "ADFTrigger1"

        expectedOutput = {
            'WorkflowId': 'Post_Historization_Approval',
            'URL': 'https://url-to-the-workflow.com',
            'RequestBody': '{"Approval_Email_Address":"approver@mercedes-benz.com","ADFTriggerName":"ADFTrigger1","FwkLayerId":"SDM"}'
        }

        # act
        output = self._workflowManager.getWorkflowDefinition("Post_Historization_Approval")

        # assert
        self.assertEqual(output, expectedOutput)

        self._workflowManager._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "Workflows_Configuration.json",
            METADATA_WORKFLOWS_CONFIGURATION_SCHEMA
        )

        self._workflowManager._dbutils.widgets.get.assert_called_once_with("ADFTriggerName")

    def test_getWorkflowDefinition_workflowFountByProvidedTriggerId(self):
        # arrange
        workflowsDF = self._spark.createDataFrame([
            ["ADFTrigger1", "Trigger1", "Post_Historization_Approval", "https://url-to-the-workflow.com", '{"Approval_Email_Address":"approver@mercedes-benz.com","ADFTriggerName":"ADFTrigger1","FwkLayerId":"SDM"}']
        ], "ADFTriggerName STRING, FwkTriggerId STRING, WorkflowId STRING, URL STRING, RequestBody STRING")
        self._workflowManager._configGeneratorHelper.readMetadataConfig = MagicMock(return_value=workflowsDF)

        expectedOutput = {
            'WorkflowId': 'Post_Historization_Approval',
            'URL': 'https://url-to-the-workflow.com',
            'RequestBody': '{"Approval_Email_Address":"approver@mercedes-benz.com","ADFTriggerName":"ADFTrigger1","FwkLayerId":"SDM"}'
        }

        # act
        output = self._workflowManager.getWorkflowDefinition("Post_Historization_Approval", "ADFTrigger1")

        # assert
        self.assertEqual(output, expectedOutput)

        self._workflowManager._configGeneratorHelper.readMetadataConfig.assert_called_once_with(
            "Workflows_Configuration.json",
            METADATA_WORKFLOWS_CONFIGURATION_SCHEMA
        )

        self._workflowManager._dbutils.widgets.get.assert_not_called()
