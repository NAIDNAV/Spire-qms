CREATE TABLE [dbo].[FwkLayer]
(
	[FwkLayerId]                    VARCHAR (150) NOT NULL,
	[DtOrder]                       INT NULL,
	[PreTransformationNotebookRun]  VARCHAR (150) NULL,
	[PostTransformationNotebookRun] VARCHAR (150) NULL,
	[ClusterId]			VARCHAR (50) NULL,
	[StopIfFailure]			BIT NOT NULL,
	[StopBatchIfFailure]			BIT NOT NULL
);
GO
