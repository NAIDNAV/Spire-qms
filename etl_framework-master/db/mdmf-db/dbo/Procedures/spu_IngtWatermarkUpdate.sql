CREATE PROC [dbo].[spu_IngtWatermarkUpdate] @FwkEntityId [varchar](150), @NewValueWmkInt [bigint], @NewValueWmkDt [datetime] AS
BEGIN
	DECLARE @LastUpdate DATETIME
	SET @LastUpdate = GETDATE()

	IF @NewValueWmkInt IS NULL 
		BEGIN
			UPDATE [dbo].[IngtWatermark]			-- Typeload: datetime
			SET [OldValueWmkDt] = A.NewValueWmkDt,	-- The old value became the new value.
				[NewValueWmkDt] = @NewValueWmkDt,	-- New value get the ingestion module start time
				[LastUpdate] = @LastUpdate
			FROM IngtWatermark A (NOLOCK) 
			WHERE FwkEntityId = @FwkEntityId
		END
	ELSE
		BEGIN
			UPDATE [dbo].[IngtWatermark]				-- Typeload: integer
			SET [OldValueWmkInt] = A.[NewValueWmkInt],	-- The old value became the new value.
				[NewValueWmkInt] = @NewValueWmkInt,		-- New value will get source data maximum value.
				[LastUpdate] = @LastUpdate
			FROM IngtWatermark A (NOLOCK) 
			WHERE FwkEntityId = @FwkEntityId
		END
END
GO