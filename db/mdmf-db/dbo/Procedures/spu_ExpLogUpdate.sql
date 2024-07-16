CREATE PROC [dbo].[spu_ExpLogUpdate] @ExpOutputId [int], @EntRunId [varchar](200), @ModuleRunId [varchar](200), @PipelineStatus [varchar](50), @CopyDuration [int], @Throughput [float], @ErrorMessage [varchar](5000) = null  AS  
BEGIN  
	  
	DECLARE @EndDate DATETIME
	SET @EndDate=GETDATE()
 
	UPDATE [dbo].[ExpLog]  
	SET 
		  [PipelineStatus] = @PipelineStatus  
		 ,[EndDate] = @EndDate
		 ,[CopyDuration] = @CopyDuration
		 ,[Throughput] = @Throughput
		 ,[ErrorMessage] = @ErrorMessage
	WHERE
		[ExpOutputId] = @ExpOutputId
		AND [EntRunId] = @EntRunId
		AND [ModuleRunId] = @ModuleRunId

END
