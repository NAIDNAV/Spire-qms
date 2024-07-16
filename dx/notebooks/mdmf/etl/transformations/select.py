# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType

class Select:
    """SelectColumns transformation performs selecting columns from data.

    Public methods:
        execute
    """

    def execute(self, sourceDF: DataFrame, columnNames: ArrayType) -> DataFrame:
        """Performs selecting columns from data in sourceDF. ColumnNames argument specifies data in which
        column will be selected.
        
        Returns:
            DataFrame with selected columns.
        """
        
        sourceDF = sourceDF.select(columnNames)
    
        return sourceDF