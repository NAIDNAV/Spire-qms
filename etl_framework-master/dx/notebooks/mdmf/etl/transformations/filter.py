# Databricks notebook source
from pyspark.sql import DataFrame

class Filter:
    """Filter transformation performs filtering the data.

    Public methods:
        execute
    """

    def execute(self, sourceDF: DataFrame, condition: str) -> DataFrame:
        """Performs filtering of data in sourceDF. filterInstruct argument specifies 
        filtering instruction which is applied to sourceDF.
        
        Returns:
            DataFrame with filtered data.
        """

        sourceDF = sourceDF.filter(condition)
    
        return sourceDF
    