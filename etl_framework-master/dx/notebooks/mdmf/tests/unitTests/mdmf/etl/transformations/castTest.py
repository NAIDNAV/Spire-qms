# Databricks notebook source
import unittest
from pyspark.sql import SparkSession

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.etl.transformations.cast import Cast

# COMMAND ----------

# MAGIC %run ../../../../../etl/transformations/cast

# COMMAND ----------

class CastTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

    def setUp(self):
        self._cast = Cast()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()

    
    # execute tests

    def test_execute_withNotCompatibleDatatypes(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 1, 102],
                ["Value13", 1, 103],
                ["Value14", 0, 104]
            ],
            schema="""
                Column1 STRING,
                Column2 INTEGER,
                Column3 INTEGER
            """
        )

        castSpecs = [
            {
                "column": ["Column1"],
                "dataType": "integer"
            },
            {
                "column": ["Column2"],
                "dataType": "boolean"
            }
        ]

        expectedDF = self._spark.createDataFrame(
            [
                [None, True, 101],
                [None, True, 102],
                [None, True, 103],
                [None, False, 104]
            ],
            schema="""
                Column1 STRING,
                Column2 BOOLEAN,
                Column3 INTEGER
            """
        )

        # act
        resultDF = self._cast.execute(sourceDF, castSpecs)

        # assert
        expectedDF = expectedDF.collect()
        resultDF = resultDF.collect()

        self.assertEqual(expectedDF, resultDF)

    def test_execute_withCompatibleDatatypes(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", True, 101, "true", "1001"],
                ["Value12", True, 102, "false", "1002"],
                ["Value13", True, 103, "true", "1003"],
                ["Value14", False, 104, "false", "1004"]
            ],
            schema="""
                Column1 STRING,
                Column2 BOOLEAN,
                Column3 INTEGER,
                Column4 STRING,
                Column5 STRING
            """
        )

        castSpecs = [
            {
                "column": ["Column2", "Column3"],
                "dataType": "string"
            },
            {
                "column": ["Column4"],
                "dataType": "boolean"
            },
            {
                "column": ["Column5"],
                "dataType": "integer"
            }
        ]

        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", "true", "101", True, 1001],
                ["Value12", "true", "102", False, 1002],
                ["Value13", "true", "103", True, 1003],
                ["Value14", "false", "104", False, 1004]
            ],
            schema="""
                Column1 STRING,
                Column2 STRING,
                Column3 STRING,
                Column4 BOOLEAN,
                Column5 INTEGER
            """
        )

        # act
        resultDF = self._cast.execute(sourceDF, castSpecs)

        # assert
        expectedDF = expectedDF.collect()
        resultDF = resultDF.collect()

        self.assertEqual(expectedDF, resultDF)
