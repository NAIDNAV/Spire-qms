# Databricks notebook source
import json
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import MapType

class Join:
    """Join transformation joins a DataFrame with an existing table in Unity Catalog.

    Public methods:
        execute
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def execute(self, sourceDF: DataFrame, joinSpecs: MapType) -> DataFrame:
        """Joins sourceDF with another table according to joinSpecs.

        Returns:
            DataFrame with renamed columns.
        """
        
        if str(type(joinSpecs)) == "<class 'str'>":
            joinSpecs = json.loads(joinSpecs)
        
        # register DataFrame as temporary view
        sourceDF.createOrReplaceTempView("table")

        joinTable = joinSpecs["joinTable"]

        # transforms "CustomerId,CustomerNo|SystemId,SysId"
        # to "table.CustomerId = joinTable.CustomerNo AND table.SystemId = joinTable.SysId"
        joinOnColumns = map(lambda x: x.split(","), joinSpecs["joinColumns"].replace(" ", "").split("|"))
        joinOnColumns = map(lambda x: f"""table.{x[0]} = {joinTable}.{x[1]}""", joinOnColumns)
        joinOnColumns = " AND ".join(list(joinOnColumns))

        if "condition" in joinSpecs.keys() and joinSpecs["condition"]:
            condition = f"""WHERE {joinSpecs["condition"]}""" 
        else:
            condition = ""

        sourceDF = self._spark.sql(f"""
            SELECT {joinSpecs["selectColumns"]}
            FROM table
            {joinSpecs["joinType"]} JOIN {joinTable} ON {joinOnColumns}
            {condition}
        """)

        return sourceDF
