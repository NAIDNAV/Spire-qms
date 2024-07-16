# Databricks notebook source
# MAGIC %run ../includes/dataLakeHelper

# COMMAND ----------

# MAGIC %run ../generator/configGenerators/configGeneratorHelper

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import MapType

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.generator.configGenerators.configGeneratorHelper import ConfigGeneratorHelper
    from notebooks.mdmf.includes.frameworkConfig import *

class WorkflowManager:
    """WorkflowManager is responsible for retrieving workflow definition.

    Public methods:
        getWorkflowDefinition
    """

    def __init__(self, spark: SparkSession, compartmentConfig: MapType):
        self._spark = spark
        self._compartmentConfig = compartmentConfig
        self._dbutils = globals()["dbutils"] if "dbutils" in globals() else None
        self._configGeneratorHelper = ConfigGeneratorHelper(spark, compartmentConfig)

    def getWorkflowDefinition(self, workflowId: str, adfTriggerName: str = None) -> MapType:
        """Returns workflow definition if defined, otherwise returns None."""

        if not self._compartmentConfig.WORKFLOWS_CONFIGURATION_FILE:
            # no workflows definition
            return {}
        
        workflowsDF = self._configGeneratorHelper.readMetadataConfig(
            self._compartmentConfig.WORKFLOWS_CONFIGURATION_FILE,
            METADATA_WORKFLOWS_CONFIGURATION_SCHEMA
        )

        if workflowsDF.count() == 0:
            # file not found, wrong schema or empty config
            return {}
        
        if not adfTriggerName:
            # get ADFTriggerName if not provided
            self._dbutils.widgets.text("ADFTriggerName", "", "")
            adfTriggerName = self._dbutils.widgets.get("ADFTriggerName")
        
        workflowDF = workflowsDF.filter(f"ADFTriggerName = '{adfTriggerName}' AND WorkflowId = '{workflowId}'")
        
        if workflowDF.count() == 0:
            # no workflow defined for current ADFTriggerName
            return {}

        return workflowDF.select("WorkflowId", "URL", "RequestBody").first().asDict()
