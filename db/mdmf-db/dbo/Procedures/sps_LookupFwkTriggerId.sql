CREATE PROC [dbo].[sps_LookupFwkTriggerId] @ADFTriggerName varchar(150) AS
BEGIN
	DECLARE @FwkTriggerId varchar(150)
	DECLARE @FwkCancelIfAlreadyRunning bit
	DECLARE @FwkRunInParallelWithOthers bit

	SET @FwkTriggerId = (
		SELECT TOP(1) FwkTriggerId
		FROM [dbo].[FwkTrigger]
		WHERE ADFTriggerName = @ADFTriggerName
	)

	SET @FwkCancelIfAlreadyRunning = (
		SELECT TOP(1) FwkCancelIfAlreadyRunning
		FROM [dbo].[FwkTrigger]
		WHERE ADFTriggerName = @ADFTriggerName
	)

	SET @FwkRunInParallelWithOthers = (
		SELECT TOP(1) FwkRunInParallelWithOthers
		FROM [dbo].[FwkTrigger]
		WHERE ADFTriggerName = @ADFTriggerName
	)

	IF @FwkTriggerId IS NULL
	BEGIN
		SET @FwkTriggerId = 'notFound'
	END

	IF @FwkCancelIfAlreadyRunning IS NULL
	BEGIN
		SET @FwkCancelIfAlreadyRunning = '1'
	END

	IF @FwkRunInParallelWithOthers IS NULL
	BEGIN
		SET @FwkRunInParallelWithOthers = '0'
	END

	SELECT @FwkTriggerId as 'FwkTriggerId', @FwkCancelIfAlreadyRunning as 'FwkCancelIfAlreadyRunning', @FwkRunInParallelWithOthers as 'FwkRunInParallelWithOthers'

END
GO
