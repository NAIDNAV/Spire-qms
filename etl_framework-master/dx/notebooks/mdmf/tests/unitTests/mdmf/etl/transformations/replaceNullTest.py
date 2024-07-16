# Databricks notebook source
import unittest
from pyspark.sql import SparkSession

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.etl.transformations.replaceNull import ReplaceNull

# COMMAND ----------

# MAGIC %run ../../../../../etl/transformations/replaceNull

# COMMAND ----------

SCHEMA = """
    Column1 STRING,
    Column2 INTEGER,
    Column3 INTEGER
"""

class ReplaceNullTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

    def setUp(self):
        self._replaceNull = ReplaceNull()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # execute tests

    def test_execute_withNoNull(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value13", 3, 103],
                ["Value14", 4, 104]
            ],
            schema=SCHEMA
        )

        nullFillSpecs = [
            {
                "column": ["Column1"],
                "value": "Value11"
            },
            {
                "column": ["Column3"],
                "value": 111
            }
        ]

        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value13", 3, 103],
                ["Value14", 4, 104]
            ],
            schema=SCHEMA
        )

        # act
        resultDF = self._replaceNull.execute(sourceDF, nullFillSpecs)

        # assert
        expectedDF = expectedDF.orderBy("Column2").collect()
        resultDF = resultDF.orderBy("Column2").collect()
        self.assertEqual(expectedDF, resultDF)

    def test_execute_withNull(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                [None, 2, 102],
                ["Value13", None, 103],
                ["Value14", 4, None]
            ],
            schema=SCHEMA
        )

        nullFillSpecs = [
            {
                "column": ["Column1"],
                "value": "Value11"
            },
            {
                "column": ["Column2", "Column3"],
                "value": 10
            }
        ]

        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value11", 2, 102],
                ["Value13", 10, 103],
                ["Value14", 4, 10]
            ],
            schema=SCHEMA
        )

        # act
        resultDF = self._replaceNull.execute(sourceDF, nullFillSpecs)

        # assert
        expectedDF = expectedDF.orderBy("Column2").collect()
        resultDF = resultDF.orderBy("Column2").collect()
        self.assertEqual(expectedDF, resultDF)

    def test_execute_fillSpecAsString(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                [None, 2, 102],
                ["Value13", None, 103],
                ["Value14", 4, None]
            ],
            schema=SCHEMA
        )

        nullFillSpecs = """[
            {
                \"column\": [\"Column1\"],
                \"value\": \"Value11\"
            },
            {
                \"column\": [\"Column2\", \"Column3\"],
                \"value\": 10
            }
        ]"""

        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value11", 2, 102],
                ["Value13", 10, 103],
                ["Value14", 4, 10]
            ],
            schema=SCHEMA
        )

        # act
        resultDF = self._replaceNull.execute(sourceDF, nullFillSpecs)

        # assert
        expectedDF = expectedDF.orderBy("Column2").collect()
        resultDF = resultDF.orderBy("Column2").collect()
        self.assertEqual(expectedDF, resultDF)
