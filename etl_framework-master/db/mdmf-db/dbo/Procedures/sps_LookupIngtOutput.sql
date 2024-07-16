CREATE PROC [dbo].[sps_LookupIngtOutput] @FwkTriggerId varchar(150), @BatchNumber int  AS
BEGIN
        SELECT
		IO.IngtOutputId,
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
	    	IO.TypeLoad,
        	IO.WmkColumnName, 
        	IO.WmkDataType,
		IO.SelectedColumnNames,
        	IO.TableHint,
        	IO.QueryHint,
		IO.Query,
		IO.ArchivePath,
		IO.ArchiveLinkedServiceId as ArchivalLinkedServiceId,
		LSArch.InstanceURL as ArchivalInstanceUrl,
		LSArch.SecretName as ArchivalSecretName,
		LSArch.UserName AS ArchivalUserName,
		IO.BatchNumber,
		LSSrc.FwkLinkedServiceId,
        	LSSrc.SourceType, 
        	LSSrc.InstanceURL,
        	LSSrc.Port,
        	LSSrc.UserName,
        	LSSrc.SecretName,
        	LSSrc.IPAddress,
		ESnk.FwkEntityId AS FwkSinkEntityId,
		ESnk.Path AS SinkPath,
		ESnk.Format AS SinkFormat,
		LSSnk.FwkLinkedServiceId AS FwkSinkLinkedServiceId,
	    	LSSnk.InstanceURL AS SinkInstanceURL,
        	LSSnk.SecretName AS SinkSecretName
	FROM dbo.IngtOutput AS IO (nolock)
    	JOIN dbo.v_FwkEntity AS ESrc (nolock) ON ESrc.FwkEntityId = IO.FwkSourceEntityId
    	JOIN dbo.FwkLinkedService AS LSSrc (nolock) ON ESrc.FwkLinkedServiceId = LSSrc.FwkLinkedServiceId
	JOIN dbo.v_FwkEntity AS ESnk (nolock) ON ESnk.FwkEntityId = IO.FwkSinkEntityId
    	JOIN dbo.FwkLinkedService AS LSSnk (nolock) ON ESnk.FwkLinkedServiceId = LSSnk.FwkLinkedServiceId
	LEFT JOIN dbo.FwkLinkedService AS LSArch (nolock) ON IO.ArchiveLinkedServiceId = LSArch.FwkLinkedServiceId
	WHERE 1=1
        	AND IO.ActiveFlag = 'Y'
		AND LSSrc.SourceType IS NOT NULL
		AND LSSnk.SourceType = 'ADLS'
        	AND IO.FwkTriggerId = @FwkTriggerId  
		AND IO.BatchNumber = @BatchNumber
END
GO

