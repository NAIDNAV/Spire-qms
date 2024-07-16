CREATE TABLE [dbo].[IngtLog] (
    [IngtLogId]      BIGINT        IDENTITY (1, 1) NOT NULL,
    [IngtOutputId]   INT           NOT NULL,
    [EntRunId]       VARCHAR (200) NOT NULL,
    [ModuleRunId]    VARCHAR (200) NOT NULL,
    [PipelineStatus] VARCHAR (50)  NOT NULL,
    [StartDate]      DATETIME      NULL,
    [EndDate]        DATETIME      NULL,
    [FileName]       VARCHAR(500) NULL,
    [RowsRead]       BIGINT        NULL,
    [RowsCopied]     BIGINT        NULL,
    [RowsSkipped]    BIGINT        NULL,
    [LogFilePath]    VARCHAR(5000) NULL,
    [CopyDuration]   INT           NULL,
    [Throughput]     FLOAT (53)    NULL,
    [ErrorMessage]   VARCHAR(5000) NULL,
    [CreatedDate]    DATETIME      NOT NULL,
    [CreatedBy]      VARCHAR (100) NOT NULL
);
GO

