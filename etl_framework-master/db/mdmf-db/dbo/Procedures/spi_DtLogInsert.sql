CREATE PROC [dbo].[spi_DtLogInsert] @DtOutputId [int],@EntRunId [varchar](200),@ModuleRunId [varchar](200),@PipelineStatus [varchar](50),@StartDate [datetime],@Duration [int], @JobRunUrl [varchar](300) = null AS
BEGIN

DECLARE @EndDate DATETIME, @CreatedDate DATETIME, @CreatedBy [varchar](100)

SET @EndDate = NULL
SET @CreatedDate = GETDATE()
SET @CreatedBy = suser_sname()

    INSERT [dbo].[DtLog]
        ([DtOutputId]
		 ,[EntRunId]         
         ,[ModuleRunId]        
         ,[PipelineStatus]  
         ,[StartDate]           
         ,[EndDate]
		 ,[Duration]
		 ,[JobRunUrl]
         ,[CreatedDate]     
         ,[CreatedBy]           
        )
    VALUES
        (@DtOutputId
        ,@EntRunId
        ,@ModuleRunId
        ,@PipelineStatus
        ,@StartDate
        ,@EndDate
		,@Duration
		,@JobRunUrl
        ,@CreatedDate
        ,@CreatedBy
        )
    END
GO

