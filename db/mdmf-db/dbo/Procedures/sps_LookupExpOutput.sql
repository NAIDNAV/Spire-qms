CREATE PROC [dbo].[sps_LookupExpOutput] @FwkTriggerId varchar(150) AS
BEGIN
        SELECT
		IO.ExpOutputId,
		ESrc.FwkEntityId,
        	ESrc.EntityName,
        	ESrc.DatabaseName,
        	ESrc.SchemaName,
		ESrc.Path,
		ESrc.Format,
		ESrc.Params,
		ESrc.RelativeURL,
       		ESrc.Header01,
        	ESrc.Header02,
		LSSrc.FwkLinkedServiceId,
        	LSSnk.SourceType, 
		LSSrc.InstanceURL,
        	LSSrc.Port,
        	LSSnk.UserName,
        	LSSnk.SecretName,
        	LSSrc.IPAddress,
		ESnk.FwkEntityId AS FwkSinkEntityId,
			ESnk.EntityName AS SinkEntityName,
        	ESnk.SchemaName AS SinkSchemaName,
		ltrim(ESnk.Path, '/') as SinkPath,
		ESnk.Format AS SinkFormat,
		ESnk.Params AS SinkParams,
		LSSnk.FwkLinkedServiceId AS FwkSinkLinkedServiceId,
	    	LSSnk.InstanceURL AS SinkInstanceURL
	FROM dbo.ExpOutput AS IO (nolock)
    	JOIN dbo.v_FwkEntity AS ESrc (nolock) ON ESrc.FwkEntityId = IO.FwkSourceEntityId
    	JOIN dbo.FwkLinkedService AS LSSrc (nolock) ON ESrc.FwkLinkedServiceId = LSSrc.FwkLinkedServiceId
	JOIN dbo.v_FwkEntity AS ESnk (nolock) ON ESnk.FwkEntityId = IO.FwkSinkEntityId
    	JOIN dbo.FwkLinkedService AS LSSnk (nolock) ON ESnk.FwkLinkedServiceId = LSSnk.FwkLinkedServiceId
	WHERE 1=1
        	AND IO.ActiveFlag = 'Y'
		AND LSSrc.SourceType IS NOT NULL
		AND LSSnk.SourceType in ('SFTP','FileShare','SQL')
        	AND IO.FwkTriggerId = @FwkTriggerId 
END
GO

