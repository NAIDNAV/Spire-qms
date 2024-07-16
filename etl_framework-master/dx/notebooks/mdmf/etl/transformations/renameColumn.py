# Databricks notebook source
import json
from pyspark.sql import DataFrame
from pyspark.sql.types import MapType

class RenameColumn:
    """RenameColumn transformation renames columns in a DataFrame.

    Public methods:
        execute
    """

    def execute(self, sourceDF: DataFrame, renameSpecs: MapType) -> DataFrame:
        """Renames columns in sourceDF according to renameSpecs.

        Returns:
            DataFrame with renamed columns.
        """
        
        if str(type(renameSpecs)) == "<class 'str'>":
            renameSpecs = json.loads(renameSpecs)
        
        assert len(renameSpecs['column']) == len(renameSpecs['value']), ("Length of the column and value mismatch in transformationSpecs of RenameColumn transformation")
        
        for i in range(len(renameSpecs['column'])):
            sourceDF = sourceDF.withColumnRenamed(renameSpecs['column'][i], renameSpecs['value'][i])
        
        return sourceDF
