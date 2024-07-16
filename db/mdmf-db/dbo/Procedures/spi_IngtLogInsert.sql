CREATE PROC [dbo].[spi_IngtLogInsert] @IngtOutputId [int],@EntRunId [varchar](200),@ModuleRunId [varchar](200),@PipelineStatus [varchar](50),@StartDate [datetime],@FileName [varchar](500),@RowsRead [bigint],@RowsCopied [bigint],@CopyDuration [float],@Throughput [float],@ErrorMessage [varchar](5000) AS
BEGIN   

DECLARE @EndDate DATETIME, @CreatedDate DATETIME, @CreatedBy [varchar](100) 

SET @EndDate = NULL
SET @CreatedDate = GETDATE()
SET @CreatedBy = suser_sname()

    INSERT [dbo].[IngtLog]
		([IngtOutputId]
		 ,[EntRunId]			
		 ,[ModuleRunId]			
		 ,[PipelineStatus]	
		 ,[StartDate]			
		 ,[EndDate]
		 ,[FileName]				
		 ,[RowsRead]			
		 ,[RowsCopied]		
		 ,[CopyDuration]		
		 ,[Throughput]		
		 ,[ErrorMessage]
		 ,[CreatedDate]		
		 ,[CreatedBy]			
		)
    VALUES     
		(
		@IngtOutputId
		,@EntRunId		
		,@ModuleRunId			
		,@PipelineStatus	
		,@StartDate		
		,@EndDate	
		,@FileName	
		,@RowsRead		
		,@RowsCopied		
		,@CopyDuration	
		,@Throughput		
		,@ErrorMessage
		,@CreatedDate	
		,@CreatedBy	
		)
	END
GO

