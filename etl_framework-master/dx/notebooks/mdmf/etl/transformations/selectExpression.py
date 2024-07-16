# Databricks notebook source
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType

class SelectExpression:
    """SelectExpression transformation applies expression on data.
    Public methods:
        execute
    """

    def execute(self, sourceDF: DataFrame, expressions: ArrayType) -> DataFrame:
        """Applies selection expressions on data in sourceDF. expressions argument specifies list of expression
           which are applied on columns.
        
        Returns:
            DataFrame with applied select expressions columns.
        """
        
        sourceDF = sourceDF.selectExpr(expressions)
    
        return sourceDF