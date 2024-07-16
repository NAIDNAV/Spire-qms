# Databricks notebook source
import json
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType

class Cast:
    """Cast transformation performs type casting of data.

    Public methods:
        execute
    """

    def execute(self, sourceDF: DataFrame, castSpecs: ArrayType) -> DataFrame:
        """Performs type casting of data in sourceDF. ColumnNames argument specifies data in which
        column will be casted. DataTypes argument specifies data types to which columns will be casted.
        
        Returns:
            DataFrame with casted data.
        """
        
        if str(type(castSpecs)) == "<class 'str'>":
            castSpecs = json.loads(castSpecs)

        for item in castSpecs:
            for col in item["column"]:
                sourceDF = sourceDF.withColumn(col, sourceDF[col].cast(item["dataType"]))
        
        return sourceDF