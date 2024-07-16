CREATE PROC [dbo].[spu_IngtLogUpdate] @IngtOutputId int, @EntRunId varchar(200), @ModuleRunId varchar(200), @PipelineStatus varchar(50), @FileName varchar(500), @RowsRead bigint, @RowsCopied bigint, @RowsSkipped bigint, @LogFilePath varchar(5000), @CopyDuration int, @Throughput float, @ErrorMessage varchar(5000) = null  AS  
BEGIN  
	  
	DECLARE @EndDate DATETIME
	SET @EndDate=GETDATE()
 
	UPDATE [dbo].[IngtLog]  
	SET 
		  [PipelineStatus] = @PipelineStatus  
		 ,[EndDate] = @EndDate
		 ,[RowsRead] = @RowsRead	
		 ,[RowsCopied] = @RowsCopied
		 ,[RowsSkipped] = @RowsSkipped
		 ,[LogFilePath] = @LogFilePath
		 ,[CopyDuration] = @CopyDuration
		 ,[Throughput] = @Throughput
		 ,[ErrorMessage] = @ErrorMessage
	WHERE
		[IngtOutputId] = @IngtOutputId
		AND [EntRunId] = @EntRunId
		AND [ModuleRunId] = @ModuleRunId
		AND ([FileName] IS NULL OR [FileName] = @FileName)

END
