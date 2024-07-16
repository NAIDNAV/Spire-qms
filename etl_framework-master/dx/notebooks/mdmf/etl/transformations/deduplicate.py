# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType

class Deduplicate:
    """Deduplicate transformation performs deduplication of data.

    Public methods:
        execute
    """

    def execute(self, sourceDF: DataFrame, columnNames: ArrayType) -> DataFrame:
        """Performs deduplication of data in sourceDF. ColumnNames argument specifies data in which
        column will be deduplicated.
        
        Returns:
            DataFrame with deduplicated data.
        """
        
        if "*" in columnNames:
            sourceDF = sourceDF.dropDuplicates()
        else:
            sourceDF = sourceDF.dropDuplicates(subset=columnNames)
    
        return sourceDF