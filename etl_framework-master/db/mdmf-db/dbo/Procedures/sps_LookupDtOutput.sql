CREATE PROC [dbo].[sps_LookupDtOutput] @FwkTriggerId varchar(150),@FwkLayerId varchar(150),@BatchNumber int AS
BEGIN

	SELECT
		DT.DtOutputId
		, ESrc.DatabaseName AS SourceDatabaseName
		, ESrc.EntityName AS SourceTableName
		, LSSrc.InstanceURL AS SourceInstanceURL
		, ESrc.Path AS SourcePath
		, ESrc.Format AS SourceFormat
		, ESnk.DatabaseName AS SinkDatabaseName
		, ESnk.EntityName AS SinkTableName
		, LSSnk.InstanceURL AS SinkInstanceURL
		, ESnk.Path AS SinkPath
		, ESnk.Params AS SinkParams
		, DT.WriteMode AS SinkWriteMode
		, ESnk.Format AS SinkFormat
		, DT.InputParameters
	FROM dbo.DtOutput AS DT (nolock)
		LEFT JOIN dbo.v_FwkEntity AS ESrc (nolock) ON ESrc.FwkEntityId = DT.FwkSourceEntityId
		JOIN dbo.v_FwkEntity AS ESnk (nolock) ON ESnk.FwkEntityId = DT.FwkSinkEntityId
		LEFT JOIN dbo.FwkLinkedService AS LSSrc (nolock) ON ESrc.FwkLinkedServiceId = LSSrc.FwkLinkedServiceId
		JOIN dbo.FwkLinkedService AS LSSnk (nolock) ON ESnk.FwkLinkedServiceId = LSSnk.FwkLinkedServiceId
	WHERE DT.ActiveFlag = 'Y'
		--AND LSSrc.SourceType = 'ADLS' -- Reason: source can be NULL
		AND LSSnk.SourceType = 'ADLS'
		AND DT.FwkTriggerId = @FwkTriggerId
		AND DT.FwkLayerId = @FwkLayerId
		AND DT.BatchNumber = @BatchNumber
END
GO
