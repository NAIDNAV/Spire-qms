CREATE PROC [dbo].[spi_ExpLogInsert] @ExpOutputId [int],@EntRunId [varchar](200),@ModuleRunId [varchar](200),@PipelineStatus [varchar](50),@StartDate [datetime],@CopyDuration [float],@Throughput [float],@ErrorMessage [varchar](5000) AS
BEGIN   

DECLARE @EndDate DATETIME, @CreatedDate DATETIME, @CreatedBy [varchar](100) 

SET @EndDate = NULL
SET @CreatedDate = GETDATE()
SET @CreatedBy = suser_sname()

    INSERT [dbo].[ExpLog]
		([ExpOutputId]
		 ,[EntRunId]			
		 ,[ModuleRunId]			
		 ,[PipelineStatus]	
		 ,[StartDate]			
		 ,[EndDate]			
		 ,[CopyDuration]		
		 ,[Throughput]		
		 ,[ErrorMessage]
		 ,[CreatedDate]		
		 ,[CreatedBy]			
		)
    VALUES     
		(
		@ExpOutputId
		,@EntRunId		
		,@ModuleRunId			
		,@PipelineStatus	
		,@StartDate		
		,@EndDate		
		,@CopyDuration	
		,@Throughput
		,@ErrorMessage		
		,@CreatedDate	
		,@CreatedBy	
		)
	END
GO

