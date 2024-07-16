# Databricks notebook source
import json
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import ArrayType

class DeriveColumn:
    """DerivedColumn transformation performs adding new derived columns to the DataFrame.

    Public methods:
        execute
    """

    def execute(self, sourceDF: DataFrame, deriveColumnSpecs: ArrayType) -> DataFrame:
        """Adds new derived columns to the DataFrame.

        Returns:
            DataFrame with added derived columns.
        """
        
        if str(type(deriveColumnSpecs)) == "<class 'str'>":
            deriveColumnSpecs = json.loads(deriveColumnSpecs)

        for item in deriveColumnSpecs:
            column = item["column"]
            value = item["value"]
            
            sourceDF = sourceDF.withColumn(column, expr(value))

        return sourceDF