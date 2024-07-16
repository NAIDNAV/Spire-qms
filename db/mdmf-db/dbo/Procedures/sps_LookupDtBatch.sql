CREATE PROC [dbo].[sps_LookupDtBatch] @FwkTriggerId varchar(150),@FwkLayerId varchar(150) AS
BEGIN 

	SELECT DISTINCT
		BatchNumber, D.FwkTriggerId
	FROM [dbo].[DtOutput] AS D
	WHERE
		D.ActiveFlag = 'Y'
		AND D.FwkLayerId = @FwkLayerId
		AND D.FwkTriggerId = @FwkTriggerId 
	ORDER BY BatchNumber ASC

END
GO

