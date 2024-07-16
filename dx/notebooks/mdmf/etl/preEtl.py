# Databricks notebook source
# This notebook runs as the first step of the Transformation Module
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.catalog.clearCache()