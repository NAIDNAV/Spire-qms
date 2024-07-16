CREATE PROC [dbo].[sps_LookupDtOrder] @FwkLayerId varchar(150) AS
BEGIN 

	DECLARE @FwkLayersDtOrder varchar(4)
	SET @FwkLayersDtOrder = (SELECT	dtorder FROM [dbo].[FwkLayer]	where FwkLayerId = @FwkLayerId)

	SELECT DISTINCT
		DtOrder
	FROM [dbo].[FwkLayer] 	
	WHERE DtOrder > 0 AND (@FwkLayerId is null OR DtOrder >= @FwkLayersDtOrder)
	ORDER BY DtOrder ASC

END
