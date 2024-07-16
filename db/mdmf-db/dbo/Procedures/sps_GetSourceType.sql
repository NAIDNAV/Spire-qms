CREATE PROC [dbo].[sps_GetSourceType] AS
BEGIN
SELECT [FwkLinkedServiceId]
      ,[SourceType]
      ,[InstanceURL]
      ,[Port]
      ,[UserName]
      ,[SecretName]
      ,[IPAddress]
      ,[LastUpdate]
      ,[UpdatedBy]
  FROM [dbo].[FwkLinkedService]
  WHERE SourceType IN ('SQL','SQLOnPrem','Oracle')
END
GO

