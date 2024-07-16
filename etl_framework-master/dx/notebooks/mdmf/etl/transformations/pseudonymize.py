# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType

class Pseudonymize:
    """Pseudonymize transformation performs pseudonymization of data.

    Public methods:
        execute
    """

    def __init__(self):
        self._spire = globals()["spire"] if "spire" in globals() else None

    def execute(self, sourceDF: DataFrame, columnNames: ArrayType) -> DataFrame:
        """Performs pseudonymization of data in sourceDF. ColumnNames argument specifies data in which
        column will be pseudonymized.
        
        Returns:
            DataFrame with pseudonymized data.
        """
        
        for columnName in columnNames:
            sourceDF = sourceDF.withColumn(columnName, self._spire.functions.pseudo(col(columnName)))
    
        return sourceDF
