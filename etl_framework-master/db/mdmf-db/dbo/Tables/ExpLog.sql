CREATE TABLE [dbo].[ExpLog](
	[ExpLogId] [bigint] IDENTITY(1,1) NOT NULL,
	[ExpOutputId] [int] NOT NULL,
	[EntRunId] [varchar](200) NOT NULL,
	[ModuleRunId] [varchar](200) NOT NULL,
	[PipelineStatus] [varchar](50) NOT NULL,
	[StartDate] [datetime] NULL,
	[EndDate] [datetime] NULL,
	[CopyDuration] [int] NULL,
	[Throughput] [float] NULL,
	[ErrorMessage] [varchar](5000) NULL,
	[CreatedDate] [datetime] NOT NULL,
	[CreatedBy] [varchar](100) NOT NULL
);
GO

