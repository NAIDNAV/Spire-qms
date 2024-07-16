CREATE TABLE [dbo].[FwkTrigger] (
    [ADFTriggerName]    VARCHAR (150)   NOT NULL,
    [FwkTriggerId]      VARCHAR (150)   NOT NULL,
    [StartTime]         DATETIME        NULL,
    [EndTime]           DATETIME        NULL,
    [TimeZone]          VARCHAR (100)   NULL,
    [Frequency]         VARCHAR (100)   NULL,
    [Interval]          VARCHAR (100)   NULL,
    [Hours]             VARCHAR (100)   NULL,
    [Minutes]           VARCHAR (100)   NULL,
    [WeekDays]          VARCHAR (100)   NULL,
    [MonthDays]         VARCHAR (100)   NULL,
    [StorageAccountResourceGroup]    VARCHAR(8000)    NULL,
    [StorageAccount]    VARCHAR(100)    NULL,
    [Container]         VARCHAR(100)    NULL,
    [PathBeginsWith]    VARCHAR(8000)   NULL,
    [PathEndsWith]      VARCHAR(8000)   NULL,
    [RuntimeState]      VARCHAR(100)    NULL,
    [ToBeProcessed]     BIT             NOT NULL,
    [FwkCancelIfAlreadyRunning] BIT      NOT NULL,
    [FwkRunInParallelWithOthers]    BIT      NOT NULL,
    [LastUpdate]        DATETIME        NOT NULL,
    [UpdatedBy]         VARCHAR (100)   NOT NULL
);
GO

