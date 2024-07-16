# Databricks notebook source
import unittest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock, ANY

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.etl.transformations.pseudonymize import Pseudonymize

# COMMAND ----------

# MAGIC %run ../../../../../etl/transformations/pseudonymize

# COMMAND ----------

class PseudonymizeTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

    def setUp(self):
        self._pseudonymize = Pseudonymize()
        self._pseudonymize._spire = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # execute tests

    def test_execute_fewColumnNames(self):
        # arrange
        sourceDF = MagicMock()
        sourceDF.withColumn.return_value = sourceDF

        columnNames = ["FirstName", "LastName", "Email"]

        # act
        resultDF = self._pseudonymize.execute(sourceDF, columnNames)

        # assert
        self.assertEqual(resultDF, sourceDF)

        self.assertEqual(sourceDF.withColumn.call_count, 3)
        sourceDF.withColumn.assert_any_call(
            "FirstName",
            ANY
        )
        sourceDF.withColumn.assert_any_call(
            "LastName",
            ANY
        )
        sourceDF.withColumn.assert_any_call(
            "Email",
            ANY
        )

        self.assertEqual(self._pseudonymize._spire.functions.pseudo.call_count, 3)

    def test_execute_noColumnNames(self):
        # arrange
        sourceDF = MagicMock()
        sourceDF.withColumn.return_value = sourceDF

        columnNames = []

        # act
        resultDF = self._pseudonymize.execute(sourceDF, columnNames)

        # assert
        self.assertEqual(resultDF, sourceDF)

        self.assertEqual(sourceDF.withColumn.call_count, 0)
        self.assertEqual(self._pseudonymize._spire.functions.pseudo.call_count, 0)
