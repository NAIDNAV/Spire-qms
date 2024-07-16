# Databricks notebook source
if environmentLetter == "p":
    
    # Fwk Configuration

    FWK_LAYER_DF = spark.createDataFrame([
    ], "FwkLayerId STRING, DtOrder INTEGER")

    FWK_TRIGGER_DF = spark.createDataFrame([
    ], "FwkTriggerId STRING, StartTime STRING, EndTime STRING, TimeZone STRING, Frequency STRING, Interval STRING, Hours STRING, Minutes STRING, WeekDays STRING, MonthDays STRING, StorageAccountResourceGroup STRING, StorageAccount STRING, Container STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING")

    FWK_LINKED_SERVICE_DF = spark.createDataFrame([
    ], "FwkLinkedServiceId STRING, SourceType STRING, InstanceURL STRING, Port STRING, UserName STRING, SecretName STRING, IPAddress STRING")

    FWK_METADATA_CONFIG = {
        "ingestion": [],
        "transformations": [],
        "modelTransformations": [],
        "metadata": {}
    }
