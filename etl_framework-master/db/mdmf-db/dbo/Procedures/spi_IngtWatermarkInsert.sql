CREATE PROC [dbo].[spi_IngtWatermarkInsert] @FwkEntityId [varchar](150),@WmkDataType [varchar](100) AS  
  
BEGIN  

DECLARE @CreatedBy varchar(100)
       ,@LastUpdate DATETIME

Set @CreatedBy=suser_sname()
Set @LastUpdate=GETDATE()

 IF NOT EXISTS(  
   SELECT *   
   FROM dbo.IngtWatermark a (nolock)  
    WHERE a.FwkEntityId = @FwkEntityId  
   )   
 BEGIN  
  IF IsNull(@WmkDataType,'datetime') in ('datetime', 'stringDatetime') 
   BEGIN  
	 INSERT [dbo].[IngtWatermark]([FwkEntityId], [NewValueWmkInt], [OldValueWmkInt], [NewValueWmkDt], [OldValueWmkDt], [LastUpdate], [CreatedBy])
	 VALUES(@FwkEntityId,NULL,NULL,'1900-01-01',NULL,@LastUpdate,@CreatedBy)
   END   
  ELSE  
   BEGIN  
   	 INSERT [dbo].[IngtWatermark]([FwkEntityId], [NewValueWmkInt], [OldValueWmkInt], [NewValueWmkDt], [OldValueWmkDt], [LastUpdate], [CreatedBy])
	 VALUES(@FwkEntityId,0,0,NULL,NULL,@LastUpdate,@CreatedBy)
   END   
 END  
END
GO

