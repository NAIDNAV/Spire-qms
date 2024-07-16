CREATE TABLE [dbo].[ExpOutput](
	[ExpOutputId] [int] IDENTITY(1,1) NOT NULL,
	[FwkSourceEntityId] [varchar](150) NOT NULL,
	[FwkSinkEntityId] [varchar](150) NOT NULL,
	[FwkTriggerId] [varchar](150) NOT NULL,
	[ActiveFlag] [varchar](1) NOT NULL,
	[InsertTime] [datetime] NOT NULL,
	[LastUpdate] [datetime] NOT NULL,
	[UpdatedBy] [varchar](100) NOT NULL
);
GO

