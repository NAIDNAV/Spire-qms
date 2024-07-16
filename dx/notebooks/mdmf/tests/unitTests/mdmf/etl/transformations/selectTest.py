# Databricks notebook source
import unittest
from pyspark.sql import SparkSession

# local environment imports
if "dbutils" not in globals():
    from dx.notebooks.mdmf.etl.transformations.select import Select

# COMMAND ----------

# MAGIC %run ../../../../../etl/transformations/select

# COMMAND ----------

    
SELECT_COLUMNS_SCHEMA = """
    Column1 STRING,
    Column2 INTEGER,
    Column3 INTEGER
"""

class SelectTest(unittest.TestCase):
        
        @classmethod
        def setUpClass(self):
            self._spark = SparkSession.builder.getOrCreate()
        
        def setUp(self):
            self._select = Select()
        
        @classmethod
        def tearDownClass(self):
            # stop spark on local environment
            if "dbutils" not in globals():
                self._spark.stop()
        
        # execute tests
        
        def test_execute_withAllColumns(self):
            # arrange
            sourceDF = self._spark.createDataFrame(
                [
                    ["Value11", 1, 101],
                    ["Value12", 2, 102],
                    ["Value13", 3, 103],
                    ["Value14", 4, 104]
                ],
                schema=SELECT_COLUMNS_SCHEMA
            )
            
            expectedDF = self._spark.createDataFrame(
                [
                    ["Value11", 1, 101],
                    ["Value12", 2, 102],
                    ["Value13", 3, 103],
                    ["Value14", 4, 104]
                ],
                schema=SELECT_COLUMNS_SCHEMA
            )
            
            # act
            actualDF = self._select.execute(sourceDF, ["Column1", "Column2", "Column3"])
            
            # assert
            self.assertEqual(expectedDF.collect(), actualDF.collect())
            
        def test_execute_withOneColumn(self):
            # arrange
            sourceDF = self._spark.createDataFrame(
                [
                    ["Value11", 1, 101],
                    ["Value12", 2, 102],
                    ["Value13", 3, 103],
                    ["Value14", 4, 104]
                ],
                schema=SELECT_COLUMNS_SCHEMA
            )
            
            expectedDF = self._spark.createDataFrame(
                [
                    ["Value11"],
                    ["Value12"],
                    ["Value13"],
                    ["Value14"]
                ],
                schema="Column1 STRING"
            )
            
            # act
            actualDF = self._select.execute(sourceDF, ["Column1"])
            
            # assert
            self.assertEqual(expectedDF.collect(), actualDF.collect())
