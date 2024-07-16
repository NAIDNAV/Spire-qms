# Databricks notebook source
import unittest
from pyspark.sql import SparkSession
from unittest.mock import MagicMock

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.etl.transformations.sqlQuery import SqlQuery

# COMMAND ----------

# MAGIC %run ../../../../../etl/transformations/sqlQuery

# COMMAND ----------

class SqlQueryTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self._spark = SparkSession.builder.getOrCreate()

    def setUp(self):
        self._sqlQuery = SqlQuery(self._spark)
        self._sqlQuery._spark = MagicMock()

    @classmethod
    def tearDownClass(self):
        # stop spark on local environment
        if "dbutils" not in globals():
            self._spark.stop()


    # execute tests

    def test_execute(self):
        # arrange
        sqlQueryString = "SELECT * FROM DatabaseName.TableName"

        workingDF = MagicMock()
        self._sqlQuery._spark.sql.return_value = workingDF

        # act
        resultDF = self._sqlQuery.execute(None, sqlQueryString)

        # assert
        self.assertEqual(resultDF, workingDF)

        self._sqlQuery._spark.sql.assert_called_once_with(sqlQueryString)
