# Databricks notebook source
import unittest
from pyspark.sql import SparkSession

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.etl.transformations.renameColumn import RenameColumn

# COMMAND ----------

# MAGIC %run ../../../../../etl/transformations/renameColumn

# COMMAND ----------

class RenameColumnTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

    def setUp(self):
        self._renameColumn = RenameColumn()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # execute tests

    def test_execute_withMismatchSpecs(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value13", 3, 103],
                ["Value14", 4, 104]
            ],
            schema="""
                Column1 STRING,
                Column2 INTEGER,
                Column3 INTEGER
            """
        )

        transformationSpecs = """{
            "column": ["Column1", "Column3"],
            "value": ["NewColumn1", "NewColumn3", "NewColumn4"]
        }"""

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("Length of the column and value mismatch in transformationSpecs of RenameColumn transformation")):
            self._renameColumn.execute(sourceDF, transformationSpecs)


    def test_execute_withValidSpecs(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
               ["Value11", 1, 101],
               ["Value12", 2, 102],
               ["Value13", 3, 103],
               ["Value14", 4, 104]
            ],
            schema="""
                Column1 STRING,
                Column2 INTEGER,
                Column3 INTEGER
            """
        )

        transformationSpecs = """{
            "column": ["Column1", "Column3"],
            "value": ["NewColumn1", "NewColumn3"]
        }"""

        expectedDF = self._spark.createDataFrame(
            [
               ["Value11", 1, 101],
               ["Value12", 2, 102],
               ["Value13", 3, 103],
               ["Value14", 4, 104]
            ],
            schema="""
                NewColumn1 STRING,
                Column2 INTEGER,
                NewColumn3 INTEGER
            """
        )

        # act
        resultDF = self._renameColumn.execute(sourceDF, transformationSpecs)

        # assert
        expectedDF = expectedDF.collect()
        resultDF = resultDF.collect()

        self.assertEqual(expectedDF, resultDF)
