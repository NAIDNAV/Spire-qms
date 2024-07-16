# Databricks notebook source
if environmentLetter == "i":

    # Metadata input
    
    METADATA_DQ_CONFIGURATION_FILE = "DQ_Configuration.json"
    METADATA_DQ_REFERENCE_VALUES_FILE = "DQ_Reference_Values.json"
    METADATA_DQ_REFERENCE_PAIR_VALUES_FILE = "DQ_Reference_Pair_Values.json"
    METADATA_CONFIGURATION_FILE_VALIDATION_FILE = "Configuration_File_Validation.json"
    
    # Fwk Configuration
    GENERAL_PURPOSE_CLUSTER_ID = "0214-064848-zi0j9fjh"
    MEMORY_OPTIMIZED_CLUSTER_ID = "0214-064848-zi0j9fjh"

    FWK_LAYER_DF = spark.createDataFrame([
        ["Landing", -1, None, None, None, True],
        ["Pseudonymization", 1, None, "postTransformation/pseudonymization", GENERAL_PURPOSE_CLUSTER_ID, True],
        ["Historization", 2, None, "postTransformation/historization", GENERAL_PURPOSE_CLUSTER_ID, True],
        ["SDM", 3, "transformationViews/sdm", None, MEMORY_OPTIMIZED_CLUSTER_ID, True],
        ["Dimensional", 4, "transformationViews/dimensional", None, MEMORY_OPTIMIZED_CLUSTER_ID, True],
        ["Export", 5, None, None, GENERAL_PURPOSE_CLUSTER_ID, True],
    ], "FwkLayerId STRING, DtOrder INTEGER, PreTransformationNotebookRun STRING, PostTransformationNotebookRun STRING, ClusterId STRING, StopIfFailure BOOLEAN")    

    FWK_TRIGGER_DF = spark.createDataFrame([
        ["Sandbox", "Sandbox", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
        ["Manual", "Manual", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
        [FWK_TRIGGER_ID_DEPLOYMENT, FWK_TRIGGER_ID_DEPLOYMENT, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
        [FWK_TRIGGER_ID_MAINTENANCE, FWK_TRIGGER_ID_MAINTENANCE, "2023-07-15 15:00:00.000", None, "UTC", "Week", "1", "3", "00", "\"Sunday\"", None, None, None, None, None, None, "Stopped"],
        ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "2023-07-15 00:15:00.000", None, "UTC", "Day", "1", "0", "00", None, None, None, None, None, None, None, "Stopped"],
        ["Daily_1am", "Daily", "2023-07-15 15:00:00.000", "2023-12-31 00:00:00.000", "UTC", "Day", "1", "0", "00", None, None, None, None, None, None, None, "Stopped"],
        ["Daily_1pm", "Daily", "2023-07-15 15:00:00.000", "2023-12-31 00:00:00.000", "UTC", "Day", "13", "30", "00", None, None, None, None, None, None, None, "Stopped"],
        ["Weekly", "Weekly", "2023-07-15 15:00:00.000", "2023-12-31 00:00:00.000", "UTC", "Week", "1", "0", "00", "\"Sunday\"", None, None, None, None, None, None, "Stopped"],
        ["MarketStorageEvent", "MarketStorageEvent", None, None, None, None, None, None, None, None, None, "/subscriptions/775f9e05-bbee-446a-aa43-3d2296410d19/resourceGroups/0001-d-asmefdevxx", "dlzasmefdevxx", "asmefdevxx", "eventFolder/eventFile", ".csv", "Stopped"],
    ], "ADFTriggerName STRING, FwkTriggerId STRING, StartTime STRING, EndTime STRING, TimeZone STRING, Frequency STRING, Interval STRING, Hours STRING, Minutes STRING, WeekDays STRING, MonthDays STRING, StorageAccountResourceGroup STRING, StorageAccount STRING, Container STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING")

    FWK_LINKED_SERVICE_DF = spark.createDataFrame([
        ["SourceSQL", "SQL", None, None, None, "sqldb-client-connectionString", None],
        ["LandingADLS", "ADLS", "https://dlzasmefdevxx.dfs.core.windows.net/", None, None, None, None],
        ["StagingADLS", "ADLS", "https://ddlasmefdevxx.dfs.core.windows.net/", None, None, None, None],
        ["CompartmentADLS", "ADLS", "https://ddlasmefdevxx.dfs.core.windows.net/", None, None, None, None],
        ["MetadataADLS", "ADLS", "https://ddlasmefdevxx.dfs.core.windows.net/", None, None, None, None],
        ["ExportADLS", "ADLS", "https://ddlasmefdevxx.dfs.core.windows.net/", None, None, None, None],
        ["ExportSFTP", "SFTP", "53.44.123.9", None, "spire_cl_MoBI_erdr_devm", "sftp-erdr-pass", None],
    ], "FwkLinkedServiceId STRING, SourceType STRING, InstanceURL STRING, Port STRING, UserName STRING, SecretName STRING, IPAddress STRING")
    
    FWK_METADATA_CONFIG = {
        "ingestion": [
            {
                "FwkLayerId": "Landing",
                "FwkTriggerId": "Sandbox",
                "source": {
                    "FwkLinkedServiceId": "SourceSQL"
                },
                "sink": {
                    "FwkLinkedServiceId": "LandingADLS",
                    "containerName": "asmefdevxx",
                    "dataPath": "/SQL",
                    "databaseName": "Application_MEF_LandingSQL"
                },
                "metadata": {
                    "ingestionFile": "Ingestion_Configuration_INT.json",
                    "ingestionTable": "IngestionConfiguration"
                }
            },
            {
                "FwkLayerId": "Landing",
                "FwkTriggerId": "Sandbox",
                "source": {
                    "FwkLinkedServiceId": "ExportSFTP"
                },
                "sink": {
                    "FwkLinkedServiceId": "LandingADLS",
                    "containerName": "asmefdevxx",
                    "dataPath": "/SFTP",
                    "databaseName": "Application_MEF_LandingSFTP"
                },
                "metadata": {
                    "ingestionFile": "Ingestion_Configuration_INT.json",
                    "ingestionTable": "IngestionConfiguration"
                }
            }
        ],
        "transformations": [
            {
                "FwkLayerId": "Pseudonymization",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "StagingADLS",
                    "containerName": "dldata",
                    "dataPath": "/SQL",
                    "databaseName": "Application_MEF_StagingSQL",
                    "writeMode": "Overwrite"
                },
                "metadata": {
                    "transformationFile": "Transformation_Configuration_INT.json",
                    "transformationTable": "TransformationConfiguration",
                    "key": "LandingSQLPseudonymization",
                    "copyRest": True
                }
            },
            {
                "FwkLayerId": "Historization",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "CompartmentADLS",
                    "containerName": "dldata",
                    "dataPath": "/history",
                    "databaseName": "Application_MEF_History",
                    "writeMode": "SCDType1"
                },
                "metadata": {
                    "transformationFile": "Transformation_Configuration_INT.json",
                    "transformationTable": "TransformationConfiguration",
                    "key": "StagingHistorization",
                    "copyRest": True
                }
            }
        ],
        "modelTransformations": [
            {
                "FwkLayerId": "SDM",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "CompartmentADLS",
                    "containerName": "dldata",
                    "dataPath": "/SDM",
                    "databaseName": "Application_MEF_SDM"
                },
                "metadata": {
                    "attributesFile": "OneHOUSE_LDM_Attributes.json",
                    "attributesTable": "LDMAttributes",
                    "keysFile": "OneHOUSE_LDM_Keys.json",
                    "keysTable": "LDMKeys",
                    "relationsFile": "OneHOUSE_LDM_Relations.json",
                    "relationsTable": "LDMRelations",
                    "entitiesFile": "OneHOUSE_LDM_Entities.json",
                    "entitiesTable": "LDMEntities",
                    "transformationFile": "Model_Transformation_Configuration.json",
                    "transformationTable": "ModelTransformationConfiguration",
                    "key": "SDM"
                }
            },
            {
                "FwkLayerId": "Dimensional",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "CompartmentADLS",
                    "containerName": "dldata",
                    "dataPath": "/dimensional",
                    "databaseName": "Application_MEF_Dimensional"
                },
                "metadata": {
                    "attributesFile": "OneHOUSE_Dim_Attributes.json",
                    "attributesTable": "DimAttributes",
                    "keysFile": "OneHOUSE_Dim_Keys.json",
                    "keysTable": "DimKeys",
                    "relationsFile": "OneHOUSE_Dim_Relations.json",
                    "relationsTable": "DimRelations",
                    "entitiesFile": "OneHOUSE_Dim_Entities.json",
                    "entitiesTable": "DimEntities"
                }
            }
        ],
        "export": [
            {
                "FwkLayerId": "Export",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "ExportADLS",
                    "containerName": "dldata",
                    "dataPath": "/ExportADLS"
                },
                "metadata": {
                    "exportFile": "ExportADLS_Export.json",
                    "exportTable": "ExportADLSExport"
                }
            }
        ], 
        "metadata": {
            "FwkLinkedServiceId": "MetadataADLS",
            "containerName": "metadata",
            "databaseName": "Application_MEF_Metadata"
        }
    }
