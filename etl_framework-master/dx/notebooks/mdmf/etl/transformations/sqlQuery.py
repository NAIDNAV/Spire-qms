# Databricks notebook source
from pyspark.sql import DataFrame, SparkSession

class SqlQuery:
    """SqlQuery transformation reads data from HIVE.

    Public methods:
        execute
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def execute(self, emptyDF: DataFrame, sqlQuery: str) -> DataFrame:
        """Reads data from HIVE based on provided SQL query.
        
        Returns:
            DataFrame with data matching provided SQL query.
        """

        workingDF = self._spark.sql(sqlQuery)
    
        return workingDF
