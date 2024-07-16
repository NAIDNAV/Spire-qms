# Databricks notebook source
import unittest
from pyspark.sql import SparkSession

# local environment imports
if "dbutils" not in globals():
    from dx.notebooks.mdmf.etl.transformations.selectExpression import SelectExpression

# COMMAND ----------

# MAGIC %run ../../../../../etl/transformations/selectExpression

# COMMAND ----------

class SelectExpressionTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()
    
    def setUp(self):
        self._selectExpression = SelectExpression()
    
    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()
    
    # execute tests
    
    def test_execute_renameAndCast(self):
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

        expressions = ["*", "CAST(Column3 AS VARCHAR(20)) as Col3"]
        
        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", 1, 101, "101"],
                ["Value12", 2, 102, "102"],
                ["Value13", 3, 103, "103"],
                ["Value14", 4, 104, "104"]
            ],
            schema="""
                Column1 STRING,
                Column2 INTEGER,
                Column3 INTEGER,
                Col3 STRING
            """
        )
        
        # act
        resultDF = self._selectExpression.execute(sourceDF, expressions)
        
        # assert
        self.assertEqual(expectedDF.collect(), resultDF.collect())

    def test_execute_selectSomeColumns(self):
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

        expressions = ["Column1 as Col1", "CAST(Column3 AS VARCHAR(20)) as Col3"]
        
        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", "101"],
                ["Value12", "102"],
                ["Value13", "103"],
                ["Value14", "104"]
            ],
            schema="""
                Column1 STRING,
                Column3 STRING
            """
        )
        
        # act
        resultDF = self._selectExpression.execute(sourceDF, expressions)
        
        # assert
        self.assertEqual(expectedDF.collect(), resultDF.collect())
