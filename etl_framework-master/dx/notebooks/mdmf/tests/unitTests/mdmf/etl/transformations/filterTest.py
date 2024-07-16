# Databricks notebook source
import unittest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.etl.transformations.filter import Filter

# COMMAND ----------

# MAGIC %run ../../../../../etl/transformations/filter

# COMMAND ----------

FILTER_SCHEMA = """
    Column1 STRING,
    Column2 INTEGER,
    Column3 INTEGER
"""

class FilterTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

    def setUp(self):
        self._filter = Filter()

    @classmethod   
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()

    # execute tests
            
    def test_execute_withFilteringInstruction(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value13", 3, 103],
                ["Value14", 1, None],
                ["Value15", None, 105]
            ],
            schema=FILTER_SCHEMA
        )
        
        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value14", 1, None]
            ],
            schema=FILTER_SCHEMA
        )
        
        filterInstruct = "Column2==1"
        
        # act
        actualDF = self._filter.execute(sourceDF, filterInstruct)
        
        # assert
        self.assertEqual(expectedDF.collect(), actualDF.collect())
    
    def test_execute_withFilteringInstructionAndCondition(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101],
                ["Value12", 2, 102],
                ["Value13", 3, 103],
                ["Value14", 1, None],
                ["Value15", None, None]
            ],
            schema=FILTER_SCHEMA
        )
        
        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101]
            ],
            schema=FILTER_SCHEMA
        )
        
        filterInstruct = "Column2==1 AND Column3 IS NOT NULL"
        
        # act
        actualDF = self._filter.execute(sourceDF, filterInstruct)
        
        # assert
        self.assertEqual(expectedDF.collect(), actualDF.collect())
