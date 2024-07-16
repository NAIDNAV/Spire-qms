CREATE PROC [dbo].[spu_DtLogUpdate] @DtOutputId [int], @EntRunId [varchar](200), @ModuleRunId [varchar](200), @PipelineStatus [varchar](50), @RecordsInserted [bigint], @RecordsUpdated [bigint], @RecordsDeleted [bigint], @Duration [int], @JobRunUrl [varchar](300) = null  AS  
BEGIN  
	  
	DECLARE @EndDate DATETIME
	SET @EndDate=GETDATE()
 
	UPDATE [dbo].[DtLog]  
	SET 
		  [PipelineStatus] = @PipelineStatus  
		 ,[EndDate] = @EndDate
		 ,[RecordsInserted] = @RecordsInserted
		 ,[RecordsUpdated] = @RecordsUpdated
		 ,[RecordsDeleted] = @RecordsDeleted
		 ,[Duration] = @Duration
		 ,[JobRunUrl] = @JobRunUrl
	WHERE
		[DtOutputId] = @DtOutputId
		AND [EntRunId] = @EntRunId
		AND [ModuleRunId] = @ModuleRunId

END
