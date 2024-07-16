CREATE TABLE [dbo].[IngtOutput] (
    [IngtOutputId]           INT            IDENTITY (1, 1) NOT NULL,
    [FwkSourceEntityId]      VARCHAR (150)  NOT NULL,
    [FwkSinkEntityId]        VARCHAR (150)  NOT NULL,
    [TypeLoad]               VARCHAR (100)  NOT NULL,
    [WmkColumnName]          VARCHAR (100)  NULL,
    [WmkDataType]            VARCHAR (100)  NULL,
    [SelectedColumnNames]    VARCHAR (3000) NULL,
    [TableHint]              VARCHAR (1000) NULL,
    [QueryHint]              VARCHAR (1000) NULL,
    [Query]                  VARCHAR (8000) NULL,
    [FwkTriggerId]           VARCHAR (150)  NOT NULL,
    [ArchivePath]            VARCHAR (8000) NULL,
    [ArchiveLinkedServiceId] VARCHAR (8000) NULL,
    [BatchNumber]            INT NOT NULL,
    [IsMandatory]            VARCHAR (1)    NOT NULL,
    [ActiveFlag]             VARCHAR (1)    NOT NULL,
    [InsertTime]             DATETIME       NOT NULL,
    [LastUpdate]             DATETIME       NOT NULL,
    [UpdatedBy]              VARCHAR (100)  NOT NULL
);
GO

