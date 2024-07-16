CREATE PROC [dbo].[sps_HasActiveIngtInstructions] @FwkTriggerId varchar(150), @FwkLayerId varchar(150) AS
BEGIN	

	DECLARE @FwkLayersDtOrder varchar(4)
	SET @FwkLayersDtOrder = (SELECT	[dtorder] FROM [dbo].[FwkLayer]	where [FwkLayerId] = @FwkLayerId)

	DECLARE @Result bit

	IF(@FwkLayerId is null or (@FwkLayersDtOrder = '-1'))
		BEGIN
			IF(
				SELECT COUNT(*)			
				FROM dbo.IngtOutput AS IO (nolock) 
				WHERE 
					IO.ActiveFlag = 'Y'		
					AND IO.FwkTriggerId = @FwkTriggerId 
			) > 0 
				SET @Result = 1
			ELSE
				SET @Result = 0
		END			
	ELSE
		SET @Result = 0

	SELECT @Result as 'Result'
END
