CREATE PROC [dbo].[spi_FwkLogInsert] @ModuleRunId nvarchar(200),@Module nvarchar(100),@StartDate datetime,@EntRunId nvarchar(200),@ADFTriggerName nvarchar(200) AS     
BEGIN

	DECLARE @CreatedBy varchar(100)
		   ,@LastUpdate DATETIME

	Set @CreatedBy=suser_sname()   
	Set @LastUpdate=GETDATE()

	INSERT INTO [dbo].[FwkLog]    
		 (	
		  [EntRunId]		
		  ,[ModuleRunId]
		  ,[ADFTriggerName]	
		  ,[Module]		
		  ,[PipelineStatus]
		  ,[StartDate]		
		  ,[EndDate]
		  ,[JobRunUrl]
		  ,[ErrorMessage]
		  ,[LastUpdate]	
		  ,[CreatedBy]	)    
	VALUES(@EntRunId,@ModuleRunId,@ADFTriggerName,@Module,'InProgress',@StartDate,NULL,NULL,NULL,@LastUpdate,@CreatedBy)

END
GO

