CREATE PROC [dbo].[sps_LookupIngtBatch] @FwkTriggerId varchar(150) AS
BEGIN 

	SELECT DISTINCT
		INGT.BatchNumber, INGT.FwkTriggerId
	FROM [dbo].[IngtOutput] AS INGT
	WHERE
		INGT.ActiveFlag = 'Y'
		AND INGT.FwkTriggerId = @FwkTriggerId 
	ORDER BY INGT.BatchNumber ASC

END
