CREATE PROC [dbo].[sps_LookupFwkTrigger] @FactoryName varchar(200), @AzureRestApiUrl varchar(300) AS
BEGIN

	SELECT
		T.ADFTriggerName,
		T.Frequency,
		T.StorageAccount,
		-- List trigger
		@AzureRestApiUrl + '/providers/Microsoft.DataFactory/factories/' + @FactoryName + '/triggers?api-version=2018-06-01' AS GetTriggers,
		-- Create trigger
		@AzureRestApiUrl + '/providers/Microsoft.DataFactory/factories/' + @FactoryName + '/triggers/' + T.ADFTriggerName + '?api-version=2018-06-01' AS TriggerCreateURL,
		-- run trigger
		@AzureRestApiUrl + '/providers/Microsoft.DataFactory/factories/' + @FactoryName + '/triggers/' + T.ADFTriggerName + '/start?api-version=2018-06-01' AS TriggerEnabledURL,
		-- stop trigger
		@AzureRestApiUrl + '/providers/Microsoft.DataFactory/factories/' + @FactoryName + '/triggers/' + T.ADFTriggerName + '/stop?api-version=2018-06-01' AS TriggerStopURL,
		--Property  for Body
		'{"properties": {
				"pipelines": [{"pipelineReference": {"referenceName": "' + CASE WHEN T.FwkTriggerId = 'Maintenance' THEN 'PL_MaintenancePipeline' ELSE 'PL_MasterPipeline' END + '", "type": "PipelineReference"}}],
				"type": ' + CASE WHEN T.Frequency IS NOT NULL THEN '"ScheduleTrigger"'  WHEN T.StorageAccount IS NOT NULL THEN '"BlobEventsTrigger"' END + ',
				"typeProperties": {' +
				CASE 
					WHEN T.Frequency IS NOT NULL THEN + '
					"recurrence": {
						"frequency": "' + T.Frequency + '",
						"interval": ' + CAST(T.Interval as varchar(10)) + ',
						"startTime": "' + (SELECT CONVERT(VARCHAR(20), CONVERT(DATETIMEOFFSET, T.StartTime), 127)) + '",
						"endTime": "' + CASE WHEN T.EndTime IS NULL THEN '' ELSE (SELECT CONVERT(VARCHAR(20), CONVERT(DATETIMEOFFSET, T.EndTime), 127)) END + '",
						"timeZone": "' + T.TimeZone + '"' +
							CASE
								WHEN T.Frequency = 'Month'
									THEN ', "schedule": {"hours": [' + CASE WHEN T.[Hours] IS NULL THEN '' ELSE T.[Hours] END + '], "minutes": [' + CASE WHEN T.[Minutes] IS NULL THEN '' ELSE T.[Minutes] END + '], "monthDays": [' + CASE WHEN T.[MonthDays] IS NULL THEN '' ELSE T.[MonthDays] END + ']}}}}}'
								WHEN T.Frequency = 'Week'
									THEN ', "schedule": {"hours": [' + CASE WHEN T.[Hours] IS NULL THEN '' ELSE T.[Hours] END + '], "weekDays": [' + CASE WHEN T.[WeekDays] IS NULL THEN '' ELSE T.[WeekDays]  END + '], "minutes": [' + CASE WHEN T.[Minutes] IS NULL THEN '' ELSE T.[Minutes] END + ']}}}}}'
								WHEN T.Frequency = 'Day'
									THEN CASE 
											WHEN T.[Hours] IS NULL AND T.[Minutes] IS NULL THEN ''
											WHEN T.[Hours] IS NULL AND T.[Minutes] != NULL THEN ', "schedule": {"minutes": [' +  T.[Minutes] + ']}'
											WHEN T.[Hours] != NULL AND T.[Minutes] IS NULL THEN ', "schedule": {"hours": [' +  T.[Hours] + ']}'
											ELSE ', "schedule": {"hours": [' +  T.[Hours] + '], "minutes": [' +  T.[Minutes] + ']}'
											END + '}}}}'
								ELSE '}}}}'
							END
					WHEN T.StorageAccount IS NOT NULL THEN + '
					"blobPathBeginsWith": "/' + isnull(T.Container, '') + '/blobs/' + isnull(T.PathBeginsWith, '') + '",
					"blobPathEndsWith": "' + isnull(T.PathEndsWith, '') + '",
            				"ignoreEmptyBlobs": true,
					"scope": "' + isnull(T.StorageAccountResourceGroup, '') + '/providers/Microsoft.Storage/storageAccounts/' + isnull(T.StorageAccount, '') + '",
					"events": ["Microsoft.Storage.BlobCreated"]}}}'
				END AS TriggerProperties,
		T.RuntimeState,
		T.ToBeProcessed
	FROM FwkTrigger T
	WHERE T.RuntimeState in ('Started', 'Stopped')
	
END
GO

