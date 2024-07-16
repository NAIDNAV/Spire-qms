# Databricks notebook source
import json
from pyspark.sql import DataFrame
from pyspark.sql.types import MapType
from pyspark.sql.functions import avg, min, max, count, collect_list, sum, mean, collect_set, struct

class Aggregate:
    """Aggregate transformation performs aggregate operations on columns in a DataFrame.

    Public methods:
        execute
    """

    def execute(self, sourceDF: DataFrame, aggregateSpecs: MapType) -> DataFrame:
        """Performs aggregate functions on columns in sourceDF according to aggregateSpecs.

        Returns:
            DataFrame with columns after aggregate functions are applied to it .
        """

        aggregateFunctions = {
            "avg": avg,
            "min": min,
            "max": max,
            "sum": sum,
            "mean": mean,
            "count": count,
            "collect_list": collect_list,
            "collect_set": collect_set
        }

        if str(type(aggregateSpecs)) == "<class 'str'>":
            aggregateSpecs = json.loads(aggregateSpecs)
        
        aggregateSpecifications = aggregateSpecs.get("aggregations",[])
        
        expression = []
        for i in aggregateSpecifications:
            operation = i.get("operation", "")
            aliasName = i.get("alias", "")
            aggColumn = i.get("column", "")
            
            if operation not in aggregateFunctions:
                assert False, f"{operation} operation not supported."
            elif not aliasName or not aggColumn:
                assert False, f"Mandatory parameters both alias and column needs to be present."
            elif type(aggColumn) != list and aliasName or aggColumn == "*":
                if aggColumn not in sourceDF.columns and aggColumn != "*":
                    assert False, f"{aggColumn} not present in data."
                else:
                    expression.append(aggregateFunctions[operation](aggColumn).alias(aliasName))
            elif type(aggColumn) == list and aliasName :
                if set(aggColumn).issubset(sourceDF.columns):
                    expression.append(aggregateFunctions[operation](struct(aggColumn)).alias(aliasName))
                else:
                    assert False, f"{aggColumn} one of the columns not present in data."
            
        if aggregateSpecs.__contains__("groupBy"):
            groupBy = aggregateSpecs.get("groupBy",[])
            sourceDF = sourceDF.groupBy(*groupBy).agg(*expression)
        else:
            sourceDF = sourceDF.agg(*expression)
        
        return sourceDF
