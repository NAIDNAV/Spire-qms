CREATE PROC [dbo].[spu_FwkLogUpdate] @PipelineStatus [varchar](200), @ModuleRunId [varchar](200), @JobRunUrl [varchar](300) = null, @ErrorMessage [varchar](5000) = null AS  
BEGIN  

	DECLARE @LastUpdate DATETIME
	SET @LastUpdate = GETDATE()

	DECLARE @MaxStartDate datetime  
  
	SELECT @MaxStartDate = MAX(StartDate)  
	FROM [dbo].[FwkLog]
	WHERE [ModuleRunId] = @ModuleRunId
 
	UPDATE [dbo].[FwkLog]  
	SET 
		[PipelineStatus] = @PipelineStatus  
		,[EndDate] = @LastUpdate
		,[LastUpdate] = @LastUpdate
		,[JobRunUrl] = case when [JobRunUrl] IS NOT NULL Then [JobRunUrl] else @JobRunUrl end
		,[ErrorMessage] = case when [ErrorMessage] IS NOT NULL Then [ErrorMessage] else @ErrorMessage end
	WHERE
		[StartDate] = @MaxStartDate  
		AND [ModuleRunId] = @ModuleRunId  

END
GO

