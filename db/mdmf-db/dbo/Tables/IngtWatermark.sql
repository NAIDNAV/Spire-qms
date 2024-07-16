CREATE TABLE [dbo].[IngtWatermark] (
    [FwkWatermarkId] BIGINT        IDENTITY (1, 1) NOT NULL,
    [FwkEntityId]    VARCHAR (150) NOT NULL,
    [NewValueWmkInt] BIGINT        NULL,
    [OldValueWmkInt] BIGINT        NULL,
    [NewValueWmkDt]  DATETIME      NULL,
    [OldValueWmkDt]  DATETIME      NULL,
    [LastUpdate]     DATETIME      NOT NULL,
    [CreatedBy]      VARCHAR (100) NOT NULL
);
GO

