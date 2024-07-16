CREATE PROC [dbo].[sps_LookupDtLayer] @DtOrder int, @FwkLayerId varchar(150), @FwkTriggerId varchar(150) AS
BEGIN
	DECLARE @FwkLayersDtOrder varchar(4)
	SET @FwkLayersDtOrder = (SELECT	[dtorder] FROM [dbo].[FwkLayer]	where [FwkLayerId] = @FwkLayerId)

	IF(@DtOrder < 0)
		BEGIN
			SELECT 
				FL.FwkLayerId,
				FL.PreTransformationNotebookRun,
				FL.PostTransformationNotebookRun,
				FL.ClusterId,
				FL.StopIfFailure,
				FL.StopBatchIfFailure
			FROM [dbo].[FwkLayer] AS FL	
			WHERE 
				FL.DtOrder = @DtOrder
		END
	ELSE 
		BEGIN
			SELECT 
				FL.FwkLayerId,
				FL.PreTransformationNotebookRun,
				FL.PostTransformationNotebookRun,
				FL.ClusterId,
				FL.StopIfFailure,
				FL.StopBatchIfFailure
			FROM [dbo].[FwkLayer] AS FL	
			JOIN (
				SELECT DISTINCT 
					FwkLayerId,
					FwkTriggerId 
				FROM dbo.DtOutput 
				WHERE ActiveFlag ='Y' 
			) AS DT ON FL.FwkLayerId = DT.FwkLayerId
			WHERE 
				FL.DtOrder = @DtOrder 
				AND (@FwkLayerId is null OR (FL.FwkLayerId = @FwkLayerId OR DtOrder > @FwkLayersDtOrder))
				AND DT.FwkTriggerId = @FwkTriggerId
			ORDER BY FL.FwkLayerId ASC
		END
END
