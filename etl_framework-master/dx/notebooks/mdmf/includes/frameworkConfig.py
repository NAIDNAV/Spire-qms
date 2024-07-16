# Databricks notebook source
# Metadata input

METADATA_INPUT_PATH = "/FileStore/MetadataInput"

METADATA_INGESTION_CONFIGURATION_SCHEMA = """
    FwkLinkedServiceId STRING,
    Entities ARRAY<
        STRUCT<
            Entity STRING,
            Path STRING,
            Params STRUCT<
                sinkEntityName STRING,
                fwkTriggerId STRING,
                archivePath STRING,
                archiveLinkedServiceId STRING
            >,
            MetadataParams STRUCT<
                primaryColumns STRING,
                wmkDataType STRING
            >,
            SourceParams STRING,
            TypeLoad STRING,
            WmkColumnName STRING,
            SelectedColumnNames STRING,
            TableHint STRING,
            QueryHint STRING,
            Query STRING,
            IsMandatory BOOLEAN
        >
    >
"""

METADATA_TRANSFORMATION_CONFIGURATION_SCHEMA = """
    Key STRING,
    Entities ARRAY<
        STRUCT<
            DatabaseName STRING,
            EntityName STRING,
            Transformations ARRAY<
                STRUCT<
                    TransformationType STRING,
                    Params STRUCT<
                        columns ARRAY<STRING>,
                        condition STRING,
                        expressions ARRAY<STRING>,
                        transformationSpecs STRING
                    >
                >
            >,
            Params STRUCT<
                keyColumns ARRAY<STRING>,
                partitionColumns ARRAY<STRING>,
                sinkEntityName STRING,
                fwkTriggerId STRING,
                writeMode STRING,
                batchNumber INTEGER
            >
        >
    >
"""

METADATA_MODEL_TRANSFORMATION_CONFIGURATION_SCHEMA = """
    Key STRING,
    Entities ARRAY<
        STRUCT<
            EntityName STRING,
            Params STRUCT<
                keyColumns ARRAY<STRING>,
                partitionColumns ARRAY<STRING>,
                fwkTriggerId STRING,
                writeMode STRING,
                batchNumber INTEGER
            >
        >
    >
"""

METADATA_MODEL_ATTRIBUTES_SCHEMA = """
    Entity STRING,
    Attribute STRING,
    DataType STRING,
    IsPrimaryKey BOOLEAN,
    IsIdentity BOOLEAN
"""

METADATA_MODEL_ENTITIES_SCHEMA = """
    Entity STRING,
    Stereotype STRING
"""

METADATA_MODEL_KEYS_SCHEMA = """
    Entity STRING,
    KeyType STRING,
    Attribute STRING
"""

METADATA_MODEL_RELATIONS_SCHEMA = """
    Parent STRING,
    ParentAttribute STRING,
    Child STRING,
    ChildAttribute STRING,
    KeyType STRING,
    RolePlayingGroup STRING
"""

METADATA_EXPORT_CONFIGURATION_SCHEMA = """
    Query STRING,
    Path STRING,
    Params STRING,
    FwkLinkedServiceId STRING,
    SourcePath STRING,
    Entity STRING,
    PreCopyScript STRING
"""

METADATA_WORKFLOWS_CONFIGURATION_SCHEMA = """
    ADFTriggerName STRING,
    Workflows ARRAY<
        STRUCT<
            WorkflowId STRING,
            URL STRING,
            RequestBody STRING
        >
    >
"""

METADATA_DQ_CONFIGURATION_SCHEMA = """
    DatabaseName STRING,
    Entities ARRAY<
        STRUCT<
            EntityName STRING,
            Expectations ARRAY<
                 STRUCT<
                    Description STRING,
                    ExpectationType STRING,
                    KwArgs STRING,
                    Quarantine BOOLEAN,
                    DQLogOutput STRING
                >
            >
        >
    >
"""

METADATA_DQ_REFERENCE_VALUES_SCHEMA = """
    ReferenceKey STRING,
    Value STRING,
    Description STRING
"""

METADATA_DQ_REFERENCE_PAIR_VALUES_SCHEMA = """
    ReferenceKey1 STRING,
    Value1 STRING,
    Description1 STRING,
    ReferenceKey2 STRING,
    Value2 STRING,
    Description2 STRING
"""

# Metadata delta

METADATA_FWK_FLAGS = "FwkFlags"
METADATA_ATTRIBUTES = "Attributes"

METADATA_ATTRIBUTES_SCHEMA = """
    DatabaseName STRING,
    EntityName STRING,
    Attribute STRING,
    IsPrimaryKey BOOLEAN,
    IsIdentity BOOLEAN,
    MGT_ConfigTable STRING
"""

METADATA_ATTRIBUTES_INGESTION_KEY = "Ingestion"
METADATA_ATTRIBUTES_TRANSFORMATION_KEY = "Transformation"

# Fwk Configuration

FWK_LAYER_FILE = "FwkLayer.csv"
FWK_LAYER_TABLE = "FwkLayer"
FWK_TRIGGER_FILE = "FwkTrigger.csv"
FWK_TRIGGER_TABLE = "FwkTrigger"
FWK_LINKED_SERVICE_FILE = "FwkLinkedService.csv"
FWK_LINKED_SERVICE_TABLE = "FwkLinkedService"

FWK_TRIGGER_ID_DEPLOYMENT = "Deployment"
FWK_TRIGGER_ID_MAINTENANCE = "Maintenance"

WRITE_MODE_SCD_TYPE1 = "SCDType1"
WRITE_MODE_SCD_TYPE2 = "SCDType2"
WRITE_MODE_SCD_TYPE2_DELTA = "SCDType2Delta"
WRITE_MODE_SCD_TYPE2_DELETE = "SCDType2Delete" 
WRITE_MODE_SNAPSHOT = "Snapshot"
WRITE_MODE_OVERWRITE = "Overwrite"
WRITE_MODE_APPEND = "Append"

# Configuration Instructions

FWK_ENTITY_FOLDER = "FwkEntity"
FWK_ENTITY_TABLE = "FwkEntity"
FWK_ENTITY_VIEW = "v_FwkEntity"
FWK_ENTITY_SCHEMA = """
    FwkEntityId STRING,
	FwkLinkedServiceId STRING,
	Path STRING,
	Format STRING,
    Params STRING,
	RelativeURL STRING,
	Header01 STRING,
	Header02 STRING,
	LastUpdate TIMESTAMP,
	UpdatedBy STRING
"""

INGT_OUTPUT_FOLDER = "IngtOutput"
FWK_INGT_INST_TABLE = "FwkIngtInstruction"
INGT_OUTPUT_SCHEMA = """
	FwkSourceEntityId STRING,
	FwkSinkEntityId STRING,
	TypeLoad STRING,
	WmkColumnName STRING,
	WmkDataType STRING,
	SelectedColumnNames STRING,
    TableHint STRING,
    QueryHint STRING,
	Query STRING,
	FwkTriggerId STRING,
    ArchivePath STRING,
    ArchiveLinkedServiceId STRING,
    BatchNumber INTEGER,
    IsMandatory STRING,
    ActiveFlag STRING,
    InsertTime TIMESTAMP,
    LastUpdate TIMESTAMP,
    UpdatedBy STRING
"""

DT_OUTPUT_FOLDER = "DtOutput"
FWK_DT_INST_TABLE = "FwkDtInstruction"
DT_OUTPUT_SCHEMA = """
    FwkSourceEntityId STRING,
    FwkSinkEntityId STRING,
    InputParameters STRING,
    WriteMode STRING,
    FwkTriggerId STRING,
    FwkLayerId STRING,
    BatchNumber INTEGER,
    ActiveFlag STRING,
    InsertTime TIMESTAMP,
    LastUpdate TIMESTAMP,
    UpdatedBy STRING
"""

EXP_OUTPUT_FOLDER = "ExpOutput"
FWK_EXP_INST_TABLE = "FwkEXPInstruction"
EXP_OUTPUT_SCHEMA = """
    FwkSourceEntityId STRING,
    FwkSinkEntityId STRING,
    FwkTriggerId STRING,
    ActiveFlag STRING,
    InsertTime TIMESTAMP,
    LastUpdate TIMESTAMP,
    UpdatedBy STRING
"""

FWK_INGT_WK_TABLE = "FwkIngtWatermark"
INGT_WK_SCHEMA = """
    FwkEntityId STRING,
    NewValueWmkInt BIGINT,
    OldValueWmkInt BIGINT,
    NewValueWmkDt TIMESTAMP,
    OldValueWmkDt TIMESTAMP,
    LastUpdate TIMESTAMP,
    CreatedBy STRING
"""
# FwkLog Instructions

FWK_LOG_TABLE = "FwkLog"
FWK_LOG_SCHEMA = """
    EntRunId STRING,
    ModuleRunId STRING,
    ADFTriggerName STRING,
    Module STRING,
    PipelineStatus STRING,
    StartDate TIMESTAMP,
	EndDate TIMESTAMP,
	JobRunUrl STRING,
	ErrorMessage STRING,
	LastUpdate TIMESTAMP,
	CreatedBy STRING
"""

FWK_INGT_LOG_TABLE = "FwkIngtLog"
FWK_INGT_LOG_SCHEMA = """
    IngtOutputId INTEGER,
    EntRunId STRING,
    ModuleRunId STRING,
    PipelineStatus STRING,
    StartDate TIMESTAMP,
    EndDate TIMESTAMP,
    FileName STRING,
    RowsRead BIGINT,
    RowsCopied BIGINT,
    RowsSkipped BIGINT,
    LogFilePath STRING,
    CopyDuration INTEGER,
    Throughput FLOAT,
    ErrorMessage STRING,
    CreatedDate TIMESTAMP,
    CreatedBy STRING
"""

FWK_DT_LOG_TABLE = "FwkDtLog"
FWK_DT_LOG_SCHEMA = """
    DtOutputId INTEGER,
    EntRunId STRING,
    ModuleRunId STRING,
    PipelineStatus STRING,
    StartDate TIMESTAMP,
    EndDate TIMESTAMP,
    RecordsInserted BIGINT,
    RecordsUpdated BIGINT,
    RecordsDeleted BIGINT,
    Duration INTEGER,
    JobRunUrl STRING,
    CreatedDate TIMESTAMP,
    CreatedBy STRING 
"""
FWK_EXP_LOG_TABLE = "FwkExpLog"
FWK_EXP_LOG_SCHEMA = """
    ExpOutputId INTEGER,
    EntRunId STRING,
    ModuleRunId STRING,
    PipelineStatus STRING,
    StartDate TIMESTAMP,
    EndDate TIMESTAMP,
    CopyDuration INTEGER,
    Throughput FLOAT,
    ErrorMessage STRING,
    CreatedDate TIMESTAMP,
    CreatedBy STRING
"""