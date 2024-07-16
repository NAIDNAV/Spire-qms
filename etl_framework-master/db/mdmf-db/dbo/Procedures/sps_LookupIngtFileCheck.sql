CREATE PROC [dbo].[sps_LookupIngtFileCheck] @FwkTriggerId varchar(150) AS
BEGIN
    SELECT
		IO.IngtOutputId,
		ESrc.FwkEntityId,
		ESrc.Path,
		LSSrc.SourceType,
		LSSrc.InstanceURL,
    LSSrc.UserName,
    LSSrc.SecretName,
		ESrc.Format,
		IO.ActiveFlag,
		IO.IsMandatory
	FROM dbo.IngtOutput AS IO (nolock)
	JOIN dbo.v_FwkEntity AS ESrc (nolock) ON ESrc.FwkEntityId = IO.FwkSourceEntityId
	JOIN dbo.FwkLinkedService AS LSSrc (nolock) ON ESrc.FwkLinkedServiceId = LSSrc.FwkLinkedServiceId
	WHERE 1=1
		AND IO.ActiveFlag = 'Y'
		AND LSSrc.SourceType IS NOT NULL
		AND IO.FwkTriggerId = @FwkTriggerId
		AND ESrc.Format in ('CSV', 'EXCEL', 'XML')
		AND IO.IsMandatory = 'Y'
END
