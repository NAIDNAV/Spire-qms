CREATE TABLE [dbo].[DtLog] (
    [DtLogId] [bigint] IDENTITY(1,1) NOT NULL,
	[DtOutputId] [int] NOT NULL,
	[EntRunId] [varchar](200) NOT NULL,
	[ModuleRunId] [varchar](200) NOT NULL,
	[PipelineStatus] [varchar](50) NOT NULL,
	[StartDate] [datetime] NULL,
	[EndDate] [datetime] NULL,
	[RecordsInserted] [bigint] NULL,
	[RecordsUpdated] [bigint] NULL,
	[RecordsDeleted] [bigint] NULL,
	[Duration] [int] NULL,
	[JobRunUrl] [varchar](300) NULL,
	[CreatedDate] [datetime] NOT NULL,
	[CreatedBy] [varchar](100) NOT NULL
);
GO

