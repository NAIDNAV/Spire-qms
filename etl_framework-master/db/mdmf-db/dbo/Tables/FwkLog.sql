CREATE TABLE [dbo].[FwkLog] (
    [FwkLogId] [bigint] IDENTITY(1,1) NOT NULL,
	[EntRunId] [nvarchar](200) NULL,
	[ModuleRunId] [nvarchar](200) NOT NULL,
	[ADFTriggerName] [nvarchar](200) NULL,
	[Module] [varchar](100) NULL,
	[PipelineStatus] [varchar](50) NULL,
	[StartDate] [datetime] NULL,
	[EndDate] [datetime] NULL,
	[JobRunUrl] [varchar](300) NULL,
	[ErrorMessage] [varchar](5000) NULL,
	[LastUpdate] [datetime] NOT NULL,
	[CreatedBy] [varchar](100) NOT NULL
);
GO

