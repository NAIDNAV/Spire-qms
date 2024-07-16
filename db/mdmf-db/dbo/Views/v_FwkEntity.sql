CREATE VIEW [dbo].[v_FwkEntity] AS SELECT [FwkEntityId]
      ,[FwkLinkedServiceId]
	  , CASE WHEN Format = 'Database' THEN NULL
			 WHEN Format = 'Parquet'  THEN NULL 
			 WHEN Format = 'Delta'    THEN PARSENAME(REPLACE(FwkEntityId,',','.'),2) 
			 ELSE NULL
		END AS [DatabaseName]
	  , CASE WHEN Format = 'Database' THEN PARSENAME(REPLACE(FwkEntityId,',','.'),2) 
			 WHEN Format = 'Parquet'  THEN NULL 
			 WHEN Format = 'Delta'    THEN NULL
			 ELSE  NULL
		END AS [SchemaName]
	  , CASE WHEN Format = 'Database' THEN PARSENAME(REPLACE(FwkEntityId,',','.'),1) 
			 WHEN Format = 'Parquet'  THEN NULL  
			 WHEN Format = 'Delta'    THEN PARSENAME(REPLACE(FwkEntityId,',','.'),1) 
			 ELSE  [FwkEntityId] 
		END AS [EntityName]
      ,[Path]
      ,[Format]
	  ,[Params]
      ,[RelativeURL]
      ,[Header01]
      ,[Header02]
      ,[LastUpdate]
      ,[UpdatedBy]
  FROM [dbo].[FwkEntity];
