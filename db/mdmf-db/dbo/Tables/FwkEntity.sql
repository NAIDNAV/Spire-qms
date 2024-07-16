CREATE TABLE [dbo].[FwkEntity] (
    [FwkEntityId]        VARCHAR (150)  NOT NULL,
    [FwkLinkedServiceId] VARCHAR (150)  NOT NULL,
    [Path]               VARCHAR (8000) NULL,
    [Format]             VARCHAR (100)  NULL,
    [Params]             VARCHAR (max) NULL,
    [RelativeURL]        VARCHAR (8000) NULL,
    [Header01]           VARCHAR (8000) NULL,
    [Header02]           VARCHAR (8000) NULL,
    [LastUpdate]         DATETIME       NOT NULL,
    [UpdatedBy]          VARCHAR (500)  NOT NULL
);
GO

