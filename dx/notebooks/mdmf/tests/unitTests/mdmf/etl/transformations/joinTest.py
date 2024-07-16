# Databricks notebook source
import unittest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.etl.transformations.join import Join

# COMMAND ----------

# MAGIC %run ../../../../../etl/transformations/join

# COMMAND ----------

class JoinTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

    def setUp(self):
        self._join = Join(self._spark)
        self._join._spark = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # execute tests

    def test_execute_noCondition(self):
        # arrange
        sourceDF = MagicMock()

        joinSpecs = {
            "joinTable": "Application_MEF_StagingSQL.CustomerAddress",
            "joinType": "LEFT",
            "joinColumns": "AddressID,AddressIdentifier|colA,colB",
            "selectColumns": "table.*, AddressType"
        }

        # act
        self._join.execute(sourceDF, joinSpecs)

        # assert
        sourceDF.createOrReplaceTempView.assert_called_once_with("table")

        sqlCommand = self._join._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"SELECT table.*, AddressType", sqlCommand)
        self.assertIn("FROM table", sqlCommand)
        self.assertIn("LEFT JOIN Application_MEF_StagingSQL.CustomerAddress ON table.AddressID = Application_MEF_StagingSQL.CustomerAddress.AddressIdentifier AND table.colA = Application_MEF_StagingSQL.CustomerAddress.colB", sqlCommand)

    def test_execute_withCondition(self):
        # arrange
        sourceDF = MagicMock()

        joinSpecs = {
            "joinTable": "Application_MEF_StagingSQL.CustomerAddress",
            "joinType": "LEFT",
            "joinColumns": "AddressID,AddressIdentifier",
            "selectColumns": "table.*, AddressType",
            "condition": "Application_MEF_StagingSQL.CustomerAddress.AddressType IS NOT NULL"
        }

        # act
        self._join.execute(sourceDF, joinSpecs)

        # assert
        sourceDF.createOrReplaceTempView.assert_called_once_with("table")

        sqlCommand = self._join._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"SELECT table.*, AddressType", sqlCommand)
        self.assertIn("FROM table", sqlCommand)
        self.assertIn("LEFT JOIN Application_MEF_StagingSQL.CustomerAddress ON table.AddressID = Application_MEF_StagingSQL.CustomerAddress.AddressIdentifier", sqlCommand)
        self.assertIn("WHERE Application_MEF_StagingSQL.CustomerAddress.AddressType IS NOT NULL", sqlCommand)

    def test_execute_joinSpecAsString(self):
        # arrange
        sourceDF = MagicMock()

        joinSpecs = """{
            "joinTable": "Application_MEF_StagingSQL.CustomerAddress",
            "joinType": "LEFT",
            "joinColumns": "AddressID,AddressIdentifier",
            "selectColumns": "table.*, AddressType"
        }"""

        # act
        self._join.execute(sourceDF, joinSpecs)

        # assert
        sourceDF.createOrReplaceTempView.assert_called_once_with("table")

        sqlCommand = self._join._spark.sql.call_args_list[0].args[0]
        self.assertIn(f"SELECT table.*, AddressType", sqlCommand)
        self.assertIn("FROM table", sqlCommand)
        self.assertIn("LEFT JOIN Application_MEF_StagingSQL.CustomerAddress ON table.AddressID = Application_MEF_StagingSQL.CustomerAddress.AddressIdentifier", sqlCommand)
