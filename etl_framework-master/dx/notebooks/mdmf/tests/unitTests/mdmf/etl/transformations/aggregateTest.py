# Databricks notebook source
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.etl.transformations.aggregate import Aggregate

# COMMAND ----------

# MAGIC %run ../../../../../etl/transformations/aggregate

# COMMAND ----------

class AggregateTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

    def setUp(self):
        self._aggregate = Aggregate()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()

    
    # execute tests

    def test_execute_withValidTransformationSpecs(self):
        # arrange
        transformationSpecs = """{
            "groupBy" : ["Col2"],
            "aggregations" :[
                {   
                    "operation": "collect_list",
                    "column": "Col1",
                    "alias": "Col1List"
                },
                {
                    "operation": "min",
                    "column": "Col4",
                    "alias": "Col4Min"
                },
                {   
                    "operation": "collect_list",
                    "column" : ["Col1"],
                    "alias": "Col1AnotherList"
                },
                {   
                    "operation": "collect_list",
                    "column" : ["Col1", "Col3"],
                    "alias": "Col1ListsWithTwoKeys"
                }
            ]
        }"""

        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 13, "Val21", 1995],
                ["Value12", 16, "Val22", 1980],
                ["Value13", 13, "Val23", 2001],
                ["Value14", 13, "Val24", 1998],
                ["Value15", 16, "Val25", 2000]
            ],
            schema="""
                Col1 STRING,
                Col2 INTEGER,
                Col3 STRING,
                Col4 BIGINT
            """
        )

        expectedSchema = StructType([
            StructField("Col2", IntegerType(), True),
            StructField("Col1List", ArrayType(StringType()), True),
            StructField("Col4Min", IntegerType(), True),  # Assuming bigint is mapped to IntegerType
            StructField("Col1AnotherList", ArrayType(StructType([
                StructField("Col1", StringType(), True)
            ])), True),
            StructField("Col1ListsWithTwoKeys", ArrayType(StructType([
                StructField("Col1", StringType(), True),
                StructField("Col3", StringType(), True)
            ])), True)
        ])

        expectedDF = self._spark.createDataFrame(
            [
                [13, ["Value11", "Value13", "Value14"], 1995, [{"Col1": "Value11"}, {"Col1": "Value13"}, {"Col1": "Value14"}], [{"Col1": "Value11", "Col3": "Val21"},  {"Col1": "Value13", "Col3": "Val23"}, {"Col1": "Value14", "Col3": "Val24"}]],
                [16, ["Value12", "Value15"], 1980, [{"Col1": "Value12"}, {"Col1": "Value15"}], [{"Col1": "Value12", "Col3": "Val22"}, {"Col1": "Value15", "Col3": "Val25"}]]
            ],
            schema=expectedSchema
        )

        # act
        resultDF = self._aggregate.execute(sourceDF, transformationSpecs)

        # assert
        self.assertEqual(resultDF.collect(), expectedDF.collect())

    def test_execute_withoutGroupBy(self):
        # arrange
        transformationSpecs = """{
            "aggregations" :[
                {   
                    "operation": "collect_list",
                    "column": "Col1",
                    "alias": "Col1List"
                }
            ]
        }"""

        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 13, "Val21", 1995],
                ["Value12", 16, "Val22", 1980],
                ["Value13", 13, "Val23", 2001],
                ["Value14", 13, "Val24", 1998],
                ["Value15", 16, "Val25", 2000]
            ],
            schema="""
                Col1 STRING,
                Col2 INTEGER,
                Col3 STRING,
                Col4 BIGINT
            """
        )

        expectedSchema = StructType([
            StructField("Col1List", ArrayType(StringType()), True)
            ])
        expectedDF = self._spark.createDataFrame(
            [
                [["Value11", "Value12", "Value13", "Value14", "Value15"]]
            ],
            schema = expectedSchema
        )

        # act
        resultDF = self._aggregate.execute(sourceDF, transformationSpecs)

        # assert
        self.assertEqual(resultDF.collect(), expectedDF.collect())

    def test_execute_columnNotInData(self):
        # arrange
        transformationSpecs = """{
            "aggregations" :[
                {   
                    "operation": "min",
                    "column": "ColumnNotInData",
                    "alias": "minColumnNotInData"
                }
            ]
        }"""

        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 13, "Val21", 1995],
                ["Value12", 16, "Val22", 1980],
                ["Value13", 13, "Val23", 2001],
                ["Value14", 13, "Val24", 1998],
                ["Value15", 16, "Val25", 2000]
            ],
            schema="""
                Col1 STRING,
                Col2 INTEGER,
                Col3 STRING,
                Col4 BIGINT
            """
        )

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("ColumnNotInData not present in data.")):
            self._aggregate.execute(sourceDF, transformationSpecs)

    def test_execute_columnNotInData(self):
        # arrange
        transformationSpecs = """{
            "aggregations" :[
                {   
                    "operation": "collect_array",
                    "column": "ColumnNotInData",
                    "alias": "minColumnNotInData"
                }
            ]
        }"""

        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 13, "Val21", 1995],
                ["Value12", 16, "Val22", 1980],
                ["Value13", 13, "Val23", 2001],
                ["Value14", 13, "Val24", 1998],
                ["Value15", 16, "Val25", 2000]
            ],
            schema="""
                Col1 STRING,
                Col2 INTEGER,
                Col3 STRING,
                Col4 BIGINT
            """
        )

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("collect_array operation not supported.")):
            self._aggregate.execute(sourceDF, transformationSpecs)

    def test_execute_oneOfColumnNotPresentInData(self):
        # arrange
        transformationSpecs = """{
            "aggregations" :[
                {   
                    "operation": "collect_list",
                    "column": ["Col2", "ColumnNotInData"],
                    "alias": "minColumnNotInData"
                }
            ]
        }"""

        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 13, "Val21", 1995],
                ["Value12", 16, "Val22", 1980],
                ["Value13", 13, "Val23", 2001],
                ["Value14", 13, "Val24", 1998],
                ["Value15", 16, "Val25", 2000]
            ],
            schema="""
                Col1 STRING,
                Col2 INTEGER,
                Col3 STRING,
                Col4 BIGINT
            """
        )

        # assert & assert
        with self.assertRaisesRegex(AssertionError, (".* one of the columns not present in data.")):
            self._aggregate.execute(sourceDF, transformationSpecs)

    def test_execute_aliasNotPresent(self):
        # arrange
        transformationSpecs = """{
            "aggregations" :[
                {   
                    "operation": "collect_list",
                    "column": ["Col2", "ColumnNotInData"]
                }
            ]
        }"""

        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 13, "Val21", 1995],
                ["Value12", 16, "Val22", 1980],
                ["Value13", 13, "Val23", 2001],
                ["Value14", 13, "Val24", 1998],
                ["Value15", 16, "Val25", 2000]
            ],
            schema="""
                Col1 STRING,
                Col2 INTEGER,
                Col3 STRING,
                Col4 BIGINT
            """
        )

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("Mandatory parameters both alias and column needs to be present.")):
            self._aggregate.execute(sourceDF, transformationSpecs)

    def test_execute_columnsNotPresent(self):
        # arrange
        transformationSpecs = """{
            "aggregations" :[
                {   
                    "operation": "collect_list",
                    "alias": "minColumnNotInData"
                }
            ]
        }"""

        sourceDF = self._spark.createDataFrame(
            [
                ["Value11", 13, "Val21", 1995],
                ["Value12", 16, "Val22", 1980],
                ["Value13", 13, "Val23", 2001],
                ["Value14", 13, "Val24", 1998],
                ["Value15", 16, "Val25", 2000]
            ],
            schema="""
                Col1 STRING,
                Col2 INTEGER,
                Col3 STRING,
                Col4 BIGINT
            """
        )

        # assert & assert
        with self.assertRaisesRegex(AssertionError, ("Mandatory parameters both alias and column needs to be present.")):
            self._aggregate.execute(sourceDF, transformationSpecs)
