CREATE TABLE [dbo].[DtOutput] (
    [DtOutputId]        BIGINT         IDENTITY (1, 1) NOT NULL,
    [FwkSourceEntityId] VARCHAR (150)  NULL,
    [FwkSinkEntityId]   VARCHAR (150)  NOT NULL,
    [InputParameters]   VARCHAR (max) NULL,
    [WriteMode]         VARCHAR (15)   NOT NULL,
    [FwkTriggerId]      VARCHAR (150)  NOT NULL,
    [FwkLayerId]        VARCHAR (150)  NOT NULL,
    [BatchNumber]       INT            NOT NULL,
    [ActiveFlag]        VARCHAR (1)    NOT NULL,
    [InsertTime]        DATETIME       NOT NULL,
    [LastUpdate]        DATETIME       NOT NULL,
    [UpdatedBy]         VARCHAR (100)  NOT NULL
);
GO

