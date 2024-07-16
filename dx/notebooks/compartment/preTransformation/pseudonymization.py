# Databricks notebook source
# MAGIC %run ../../mdmf/includes/init

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

dataDF = spark.createDataFrame([
            ["PTL", 1, datetime(2024, 1, 1), "Active"],
            ["PTL", 2, datetime(2024, 1, 1), "Active"],
            ["PTL", 3, datetime(2024, 1, 1), "Active"],
            ["PTL", 4, datetime(2024, 1, 1), "Active"],
            ["PTL", 5, datetime(2024, 1, 1), "Active"],
            ["GER", 8, datetime(2024, 2, 1), "Active"],
            ["Ita", 9, datetime(2024, 2, 1), "Active"],
            ["UK", 10, datetime(2024, 2, 1), "Active"]
        ], "Market STRING, ContractNumber INT, ReportDate Timestamp, ContractStatus STRING")

dataDF.write.format("delta").mode("overwrite").save("abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/history/Contract")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS westeurope_spire_platform_dev.application_mef_history.contract
    USING DELTA
    LOCATION "abfss://dldata@ddlasmefdevxx.dfs.core.windows.net/history/Contract"
""")

# COMMAND ----------

incomingDF = spark.createDataFrame([
            ["PTL", 1, datetime(2024, 2, 1), "Active"],
            ["PTL", 2, datetime(2024, 2, 1), "Active"],
            ["PTL", 3, datetime(2024, 2, 1), "In-Active"],
            ["PTL", 6, datetime(2024, 2, 1), "Active"],
            ["PTL", 7, datetime(2024, 2, 1), "Active"],
            ["IND", 10, datetime(2024, 2, 1), "Active"] 
        ], "Market STRING, ContractNumber INT, ReportDate Timestamp, ContractStatus STRING")

incomingDF.write.mode("overwrite").parquet("abfss://asmefdevxx@dlzasmefdevxx.dfs.core.windows.net/SQL/Contract/")
