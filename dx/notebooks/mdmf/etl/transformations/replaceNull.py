# Databricks notebook source
import json
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType

class ReplaceNull:
    """ReplaceNull transformation replaces null values in specified columns with specified values.

    Public methods:
        execute
    """

    def execute(self, sourceDF: DataFrame, nullFillSpecs: ArrayType) -> DataFrame:
        """Replaces null values in sourceDF with value in columnNames.

        Returns:
            DataFrame with replaced null values.
        """

        if str(type(nullFillSpecs)) == "<class 'str'>":
            nullFillSpecs = json.loads(nullFillSpecs)

        for item in nullFillSpecs:
            sourceDF = sourceDF.na.fill(value=item["value"], subset=item["column"])
    
        return sourceDF
