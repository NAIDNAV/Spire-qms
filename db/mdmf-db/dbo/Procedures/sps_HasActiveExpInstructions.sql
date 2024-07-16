CREATE PROC [dbo].[sps_HasActiveExpInstructions] @FwkTriggerId varchar(20) AS
BEGIN 
	DECLARE @Result bit

	IF(
		SELECT COUNT(*)			
		FROM dbo.ExpOutput AS IO (nolock)
		WHERE 
			IO.ActiveFlag = 'Y'		
			AND IO.FwkTriggerId = @FwkTriggerId 
	) >0
		SET @Result = 1			
	ELSE 
		SET @Result = 0

	SELECT @Result as 'Result'
END
