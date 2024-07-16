CREATE PROC [dbo].[sps_HasActiveDtInstructions]  @FwkTriggerId varchar(150), @FwkLayerId varchar(150) AS
BEGIN 
		
	DECLARE @FwkLayersDtOrder varchar(4)
	SET @FwkLayersDtOrder = (SELECT	[dtorder] FROM [dbo].[FwkLayer]	where [FwkLayerId] = @FwkLayerId)

	DECLARE @Result bit

	IF(@FwkLayerId is null )
		BEGIN
			IF(
				SELECT COUNT(*)			
				FROM dbo.DtOutput AS DT (nolock)
				WHERE 
					DT.ActiveFlag = 'Y'
					AND DT.FwkTriggerId = @FwkTriggerId																
			) >0
				SET @Result = 1		
			ELSE 
				SET @Result = 0
		END	
	ELSE 
		BEGIN		 
			IF(
				SELECT COUNT(*)			
				FROM dbo.DtOutput AS DT (nolock)
				JOIN dbo.FwkLayer AS FL ON DT.FwkLayerId = FL.FwkLayerId
				WHERE 
					1=1
					AND DT.ActiveFlag = 'Y'
					AND DT.FwkTriggerId = @FwkTriggerId
					AND (
						DT.FwkLayerId = @FwkLayerId	
						OR FL.DtOrder > @FwkLayersDtOrder
					) 
			)>0 										 
				SET @Result = 1	
			ELSE 
				SET @Result = 0
		END	
		
	SELECT @Result as 'Result'
END
