CREATE PROC [dbo].[spu_FwkTriggerUpdate] @ADFTriggerName varchar(150) AS  
BEGIN  

DECLARE @LastUpdate DATETIME

Set @LastUpdate=GETDATE()
  
 UPDATE [dbo].[FwkTrigger]  
 SET [LastUpdate] = @LastUpdate, [ToBeProcessed] = 0
 WHERE ADFTriggerName = @ADFTriggerName
  
END
GO

