# Databricks notebook source
import unittest
from pyspark.sql import SparkSession

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.etl.transformations.deduplicate import Deduplicate

# COMMAND ----------

# MAGIC %run ../../../../../etl/transformations/deduplicate

# COMMAND ----------

DEDUPLICATE_SCHEMA = """
    Column1 STRING,
    Column2 INTEGER,
    Column3 INTEGER
"""


class DeduplicateTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

    def setUp(self):
        self._deduplicate = Deduplicate()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # execute tests

    def test_execute_withoutDuplicatesForAllColumns(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value13", 3, 103],
                ["Value14", 4, 104]
            ],
            schema=DEDUPLICATE_SCHEMA
        )

        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value13", 3, 103],
                ["Value14", 4, 104]
            ],
            schema=DEDUPLICATE_SCHEMA
        )
        
        deduplicateColumnNames = ["*"]

        # act
        resultDF = self._deduplicate.execute(sourceDF, deduplicateColumnNames)

        # assert
        expectedDF = expectedDF.orderBy("Column2").collect()
        resultDF = resultDF.orderBy("Column2").collect()
        self.assertEqual(expectedDF, resultDF)

    def test_execute_withoutDuplicatesForColumn(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value13", 3, 103],
                ["Value14", 4, 104]
            ],
            schema=DEDUPLICATE_SCHEMA
        )

        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value13", 3, 103],
                ["Value14", 4, 104]
            ],
            schema=DEDUPLICATE_SCHEMA
        )
        
        deduplicateColumnNames = ["Column3"]

        # act
        resultDF = self._deduplicate.execute(sourceDF, deduplicateColumnNames)

        # assert
        expectedDF = expectedDF.orderBy("Column2").collect()
        resultDF = resultDF.orderBy("Column2").collect()
        self.assertEqual(expectedDF, resultDF)

    def test_execute_withDuplicateForAllColumns(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value13", 3, 103],
                ["Value11", 1, 101]
            ],
            schema=DEDUPLICATE_SCHEMA
        )

        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value13", 3, 103]
            ],
            schema=DEDUPLICATE_SCHEMA
        )

        deduplicateColumnNames = ["*"]

        # act
        resultDF = self._deduplicate.execute(sourceDF, deduplicateColumnNames)

        # assert
        expectedDF = expectedDF.orderBy("Column2").collect()
        resultDF = resultDF.orderBy("Column2").collect()
        self.assertEqual(expectedDF, resultDF)
    
    def test_execute_withDuplicateForColumns(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value13", 3, 101],
                ["Value14", 4, 104]
            ],
            schema=DEDUPLICATE_SCHEMA
        )

        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value14", 4, 104]
            ],
            schema=DEDUPLICATE_SCHEMA
        )

        deduplicateColumnNames = ["Column3"]

        # act
        resultDF = self._deduplicate.execute(sourceDF, deduplicateColumnNames)

        # assert
        expectedDF = expectedDF.orderBy("Column2").collect()
        resultDF = resultDF.orderBy("Column2").collect()
        self.assertEqual(expectedDF, resultDF)
