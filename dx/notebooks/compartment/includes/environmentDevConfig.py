# Databricks notebook source
if environmentLetter == "d":
    
    # Metadata input
    WORKFLOWS_CONFIGURATION_FILE = "Workflows_Configuration.json"
    METADATA_DQ_CONFIGURATION_FILE = ["DQ_Configuration.json", "DQ_Configuration1.json"]
    METADATA_DQ_REFERENCE_VALUES_FILE = "DQ_Reference_Values.json"
    METADATA_DQ_REFERENCE_PAIR_VALUES_FILE = "DQ_Reference_Pair_Values.json"
    METADATA_CONFIGURATION_FILE_VALIDATION_FILE = "Configuration_File_Validation.json"

    # ADF Configuration
    TENANT_ID = "9652d7c2-1ccf-4940-8151-4a92bd474ed0"
    SUBSCRIPTION_ID = "775f9e05-bbee-446a-aa43-3d2296410d19"
    RESOURCE_GROUP = "0001-d-cpmef"
    FACTORY_NAME = "ddfcpmef"

    CLIENTID = client_id = dbutils.secrets.get(scope="ddcpmef", key="client-id")
    CLIENTSECRET = dbutils.secrets.get(scope="ddcpmef", key="client-secret")
    
    # Fwk Configuration
    GENERAL_PURPOSE_CLUSTER_ID = "0214-064848-zi0j9fjh"
    MEMORY_OPTIMIZED_CLUSTER_ID = "0214-064848-zi0j9fjh"

    FWK_LAYER_DF = spark.createDataFrame([
        ["Landing", -1, None, None, None, True, True],
        ["Pseudonymization", 1, "preTransformation/pseudonymization", "postTransformation/pseudonymization", GENERAL_PURPOSE_CLUSTER_ID, True, True],
        ["Historization", 2, None, "postTransformation/historization", GENERAL_PURPOSE_CLUSTER_ID, True, True],
        ["SDM", 3, "transformationViews/sdm", None, MEMORY_OPTIMIZED_CLUSTER_ID, True, True],
        ["Dimensional", 4, "transformationViews/dimensional", None, MEMORY_OPTIMIZED_CLUSTER_ID, True, True],
        ["Export", 5, None, None, GENERAL_PURPOSE_CLUSTER_ID, True, True],
    ], "FwkLayerId STRING, DtOrder INTEGER, PreTransformationNotebookRun STRING, PostTransformationNotebookRun STRING, ClusterId STRING, StopIfFailure BOOLEAN, StopBatchIfFailure BOOLEAN")

    FWK_TRIGGER_DF = spark.createDataFrame([
        ["Sandbox", "Sandbox", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, True, False],
        ["Manual", "Manual", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, True, False],
        [FWK_TRIGGER_ID_DEPLOYMENT, FWK_TRIGGER_ID_DEPLOYMENT, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, True, False],
        [FWK_TRIGGER_ID_MAINTENANCE, FWK_TRIGGER_ID_MAINTENANCE, "2023-07-15 15:00:00.000", None, "UTC", "Week", "1", "3", "00", "\"Sunday\"", None, None, None, None, None, None, "Stopped", True, False],
        ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "2023-07-15 00:15:00.000", None, "UTC", "Day", "1", "0", "00", None, None, None, None, None, None, None, "Stopped", True, False],
        ["Daily_1am", "Daily", "2023-07-15 15:00:00.000", "2023-12-31 00:00:00.000", "UTC", "Day", "1", "0", "00", None, None, None, None, None, None, None, "Stopped", True, False],
        ["Daily_1pm", "Daily", "2023-07-15 15:00:00.000", "2023-12-31 00:00:00.000", "UTC", "Day", "13", "30", "00", None, None, None, None, None, None, None, "Stopped", True, False],
        ["Weekly", "Weekly", "2023-07-15 15:00:00.000", "2023-12-31 00:00:00.000", "UTC", "Week", "1", "0", "00", "\"Sunday\"", None, None, None, None, None, None, "Stopped", True, False],
        ["MarketStorageEvent", "MarketStorageEvent", None, None, None, None, None, None, None, None, None, "/subscriptions/775f9e05-bbee-446a-aa43-3d2296410d19/resourceGroups/0001-d-asmefdevxx", "dlzasmefdevxx", "asmefdevxx", "eventFolder/eventFile", ".csv", "Stopped", True, False],
    ], "ADFTriggerName STRING, FwkTriggerId STRING, StartTime STRING, EndTime STRING, TimeZone STRING, Frequency STRING, Interval STRING, Hours STRING, Minutes STRING, WeekDays STRING, MonthDays STRING, StorageAccountResourceGroup STRING, StorageAccount STRING, Container STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING, FwkCancelIfAlreadyRunning BOOLEAN, FwkRunInParallelWithOthers BOOLEAN")


    FWK_LINKED_SERVICE_DF = spark.createDataFrame([
        ["SourceSQL", "SQL", None, None, None, "sqldb-client-connectionString", None],
        ["SourceOracle", "Oracle", None, None, None, "oracle-client-connectionString", None],
        ["SourceSnowflake", "Snowflake", "odbc-connection-string", None, "HASHANM", "odbc-password", None],
        ["SourceOData", "OData", "https://services.odata.org/V4/Northwind/Northwind.svc", None, "HASHANM", "odbc-password", None],
        ["SourceCSV", "FileShare", "\\\\dfseawsda046\\BI_Transformation_eRDR", None, "EMEA\\EFSE_s_SPIRE-FS-DEV", "dfseawsda046-password", None],
        ["LandingADLS", "ADLS", "https://dlzasmefdevxx.dfs.core.windows.net/", None, None, None, None],
        ["StagingADLS", "ADLS", "https://ddlasmefdevxx.dfs.core.windows.net/", None, None, None, None],
        ["CompartmentADLS", "ADLS", "https://ddlasmefdevxx.dfs.core.windows.net/", None, None, None, None],
        ["MetadataADLS", "ADLS", "https://ddlasmefdevxx.dfs.core.windows.net/", None, None, None, None],
        ["ExportADLS", "ADLS", "https://ddlasmefdevxx.dfs.core.windows.net/", None, None, None, None],
        ["ExportFS", "FileShare", "\\\\dfseawsda046\\BI_Transformation_eRDR", None, "EMEA\\EFSE_s_SPIRE-FS-DEV", "dfseawsda046-password", None],
        ["ExportSFTP", "SFTP", "53.44.123.9", None, "spire_cl_MoBI_erdr_devm", "sftp-erdr-pass", None],
        ["ExportSQL", "SQL", None, None, None, "sqldb-client-connectionString", None]
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
                    "ingestionFile": "Ingestion_Configuration.json",
                    "ingestionTable": "IngestionConfiguration"
                }
            },
            {
                "FwkLayerId": "Landing",
                "FwkTriggerId": "Sandbox",
                "source": {
                    "FwkLinkedServiceId": "SourceOracle"
                },
                "sink": {
                    "FwkLinkedServiceId": "LandingADLS",
                    "containerName": "asmefdevxx",
                    "dataPath": "/Oracle",
                    "databaseName": "Application_MEF_LandingOracle"
                },
                "metadata": {
                    "ingestionFile": "Ingestion_Configuration.json",
                    "ingestionTable": "IngestionConfiguration"
                }
            },
            {
                "FwkLayerId": "Landing",
                "FwkTriggerId": "Sandbox",
                "source": {
                    "FwkLinkedServiceId": "SourceSnowflake"
                },
                "sink": {
                    "FwkLinkedServiceId": "LandingADLS",
                    "containerName": "asmefdevxx",
                    "dataPath": "/Snowflake",
                    "databaseName": "Application_MEF_LandingSnowflake"
                },
                "metadata": {
                    "ingestionFile": "Ingestion_Configuration.json",
                    "ingestionTable": "IngestionConfiguration"
                }
            },
            {
                "FwkLayerId": "Landing",
                "FwkTriggerId": "Sandbox",
                "source": {
                    "FwkLinkedServiceId": "SourceOData"
                },
                "sink": {
                    "FwkLinkedServiceId": "LandingADLS",
                    "containerName": "asmefdevxx",
                    "dataPath": "/OData",
                    "databaseName": "Application_MEF_LandingOData"
                },
                "metadata": {
                    "ingestionFile": "Ingestion_Configuration.json",
                    "ingestionTable": "IngestionConfiguration"
                }
            },
            {
                "FwkLayerId": "Landing",
                "FwkTriggerId": "Sandbox",
                "source": {
                    "FwkLinkedServiceId": "SourceCSV"
                },
                "sink": {
                    "FwkLinkedServiceId": "LandingADLS",
                    "containerName": "asmefdevxx",
                    "dataPath": "/CSV",
                    "databaseName": "Application_MEF_LandingCSV"
                    },
                "metadata": {
                    "ingestionFile": "Ingestion_Configuration_Files.json",
                    "ingestionTable": "IngestionConfigurationFiles"
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
                    "dataPath": "/EXCEL",
                    "databaseName": "Application_MEF_LandingEXCEL",
                    "logFwkLinkedServiceId": "LandingADLS",
                    "logPath": "asmefdevxx/IncompatibleRowsLogs"
                },
                "metadata": {
                    "ingestionFile": "Ingestion_Configuration_Files.json",
                    "ingestionTable": "IngestionConfigurationFiles"
                }
            },
            {
                "FwkLayerId": "Landing",
                "FwkTriggerId": "MarketStorageEvent",
                "source": {
                    "FwkLinkedServiceId": "LandingADLS"
                },
                "sink": {
                    "FwkLinkedServiceId": "LandingADLS",
                    "containerName": "asmefdevxx",
                    "dataPath": "/ADLS",
                    "databaseName": "Application_MEF_LandingADLS",
                    "archivePath": "/from_erdr/archive",
					"archiveLinkedServiceId": "ExportSFTP"
                
                },
                "metadata": {
                    "ingestionFile": "Ingestion_Configuration_Files.json",
                    "ingestionTable": "IngestionConfigurationFiles"
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
                    "transformationFile": "Transformation_Configuration.json",
                    "transformationTable": "TransformationConfiguration",
                    "key": "LandingSQLPseudonymization",
                    "copyRest": True
                }
            },
            {
                "FwkLayerId": "Pseudonymization",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "StagingADLS",
                    "containerName": "dldata",
                    "dataPath": "/Oracle",
                    "databaseName": "Application_MEF_StagingOracle",
                    "writeMode": "Overwrite"
                },
                "metadata": {
                    "transformationFile": "Transformation_Configuration.json",
                    "transformationTable": "TransformationConfiguration",
                    "key": "LandingOraclePseudonymization"
                }
            },
            {
                "FwkLayerId": "Pseudonymization",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "StagingADLS",
                    "containerName": "dldata",
                    "dataPath": "/Snowflake",
                    "databaseName": "Application_MEF_StagingSQL",
                    "writeMode": "Overwrite"
                },
                "metadata": {
                    "transformationFile": "Transformation_Configuration.json",
                    "transformationTable": "TransformationConfiguration",
                    "key": "LandingSnowflakePseudonymization"
                }
            },
            {
                "FwkLayerId": "Pseudonymization",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "StagingADLS",
                    "containerName": "dldata",
                    "dataPath": "/OData",
                    "databaseName": "Application_MEF_StagingSQL",
                    "writeMode": "Overwrite"
                },
                "metadata": {
                    "transformationFile": "Transformation_Configuration.json",
                    "transformationTable": "TransformationConfiguration",
                    "key": "LandingODataPseudonymization"
                }
            },
            {
                "FwkLayerId": "Pseudonymization",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "StagingADLS",
                    "containerName": "dldata",
                    "dataPath": "/CSV",
                    "databaseName": "Application_MEF_StagingCSV",
                    "writeMode": "Overwrite"
                },
                "metadata": {
                    "transformationFile": "Transformation_Configuration.json",
                    "transformationTable": "TransformationConfiguration",
                    "key": "LandingCSVPseudonymization",
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
                    "transformationFile": "Transformation_Configuration.json",
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
                    "key": "SDM",
                    "unknownMemberDefaultValue": -1
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
            },
            {
                "FwkLayerId": "Export",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "ExportSFTP",
                    "dataPath": "/to_erdr",
                    "tempSink": {
                        "FwkLinkedServiceId": "CompartmentADLS",
                        "containerName": "dldata",
                        "dataPath": "/TempExportSFTP"
                    }
                },
                "metadata": {
                    "exportFile": "ExportSFTP_Export.json",
                    "exportTable": "ExportSFTPExport"
                }
            },
            {
                "FwkLayerId": "Export",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "ExportFS",
                    "dataPath": "/export",
                    "tempSink": {
                        "FwkLinkedServiceId": "CompartmentADLS",
                        "containerName": "dldata",
                        "dataPath": "/TempExportFS"
                    }
                },
                "metadata": {
                    "exportFile": "ExportFS_Export.json",
                    "exportTable": "ExportFSExport"
                }
            },
            {
                "FwkLayerId": "Export",
                "FwkTriggerId": "Sandbox",
                "sink": {
                    "FwkLinkedServiceId": "ExportSQL",
                    "tempSink": {
                        "FwkLinkedServiceId": "CompartmentADLS",
                        "containerName": "dldata",
                        "dataPath": "/TempExportSQL"
                    }
                },
                "metadata": {
                    "exportFile": "ExportSQL_Export.json",
                    "exportTable": "ExportSQLExport"
                }
            }
        ], 
        "metadata": {
            "FwkLinkedServiceId": "MetadataADLS",
            "containerName": "metadata",
            "databaseName": "Application_MEF_Metadata"
        }
    }
