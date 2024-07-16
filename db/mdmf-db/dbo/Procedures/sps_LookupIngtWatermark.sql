CREATE PROC [dbo].[sps_LookupIngtWatermark] @FwkEntityId [varchar](150),@WmkDataType [varchar](100) AS
BEGIN 

	IF isNull(@WmkDataType,'datetime') = 'datetime'
		BEGIN 
			SELECT A.NewValueWmkDt AS NewValueWatermark
			FROM dbo.IngtWatermark A (nolock)
			WHERE A.FwkEntityId = @FwkEntityId
		END
	ELSE
			IF isNull(@WmkDataType,'stringDatetime') = 'stringDatetime'
				BEGIN 
					SELECT A.NewValueWmkDt AS NewValueWatermark
					FROM dbo.IngtWatermark A (nolock)
					WHERE A.FwkEntityId = @FwkEntityId
				END
			ELSE
				BEGIN
					SELECT A.NewValueWmkInt AS NewValueWatermark
					FROM dbo.IngtWatermark A (nolock)
					WHERE A.FwkEntityId = @FwkEntityId
				END

END
GO
