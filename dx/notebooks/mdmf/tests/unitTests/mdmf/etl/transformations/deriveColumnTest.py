# Databricks notebook source
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.etl.transformations.deriveColumn import DeriveColumn

# COMMAND ----------

# MAGIC %run ../../../../../etl/transformations/deriveColumn

# COMMAND ----------

class DeriveColumnTest(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

    def setUp(self):
        self._deriveColumn = DeriveColumn()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()
    

    # execute tests

    def test_execute_withSpecsAsString(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", True, 101],
                ["Value12", True, 102],
                ["Value13", True, 103],
                ["Value14", False, 104]
            ],
            schema="""
                Column1 STRING,
                Column2 BOOLEAN,
                Column3 INTEGER
            """
        )

        transformationSpecs = """[
            {
                "column": "Column3",
                "value": "Column3 + 10"
            },
            {
                "column": "DerivedColumn1",
                "value": "Case when Column2 = true then 'YES' else 'NO' end"
            },
            {
                "column": "DerivedColumn2",
                "value": "TRUE"
            },
            {
                "column": "DerivedColumn3",
                "value": "'100'"
            },
            {
                "column": "DerivedColumn4",
                "value": "current_date() - 1"
            }
        ]"""

        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", True, 111, "YES", True, "100"],
                ["Value12", True, 112, "YES", True, "100"],
                ["Value13", True, 113, "YES", True, "100"],
                ["Value14", False, 114, "NO", True, "100"]
            ],
            schema="""
                Column1 STRING,
                Column2 BOOLEAN,
                Column3 INTEGER,
                DerivedColumn1 STRING,
                DerivedColumn2 BOOLEAN,
                DerivedColumn3 STRING
            """
        )

        expectedDF = expectedDF.withColumn("DerivedColumn4", expr("current_date() - 1"))

        # act
        resultDF = self._deriveColumn.execute(sourceDF, transformationSpecs)

        # assert
        self.assertEqual(resultDF.columns, expectedDF.columns)

        resultDF = resultDF.collect()
        expectedDF = expectedDF.collect()
        self.assertEqual(resultDF, expectedDF)

    def test_execute_withSpecsAsJsonObject(self):
        # arrange
        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", True, 101],
                ["Value12", True, 102],
                ["Value13", True, 103],
                ["Value14", False, 104]
            ],
            schema="""
                Column1 STRING,
                Column2 BOOLEAN,
                Column3 INTEGER
            """
        )

        transformationSpecs = [
            {
                "column": "Column3",
                "value": "10"
            },
            {
                "column": "DerivedColumn1",
                "value": "Case when Column2 = true then 'YES' else 'NO' end"
            },
            {
                "column": "DerivedColumn2",
                "value": "TRUE"
            },
            {
                "column": "DerivedColumn3",
                "value": "'100'"
            },
            {
                "column": "DerivedColumn4",
                "value": "current_date() - 1"
            }
        ]

        expectedDF = self._spark.createDataFrame(
            [
                ["Value11", True, 10, "YES", True, "100"],
                ["Value12", True, 10, "YES", True, "100"],
                ["Value13", True, 10, "YES", True, "100"],
                ["Value14", False, 10, "NO", True, "100"]
            ],
            schema="""
                Column1 STRING,
                Column2 BOOLEAN,
                Column3 INTEGER,
                DerivedColumn1 STRING,
                DerivedColumn2 BOOLEAN,
                DerivedColumn3 STRING
            """
        )

        expectedDF = expectedDF.withColumn("DerivedColumn4", expr("current_date() - 1"))

        # act
        resultDF = self._deriveColumn.execute(sourceDF, transformationSpecs)

        # assert
        self.assertEqual(resultDF.columns, expectedDF.columns)

        resultDF = resultDF.collect()
        expectedDF = expectedDF.collect()
        self.assertEqual(resultDF, expectedDF)
