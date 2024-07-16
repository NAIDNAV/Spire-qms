CREATE TABLE [dbo].[FwkLinkedService] (
    [FwkLinkedServiceId] VARCHAR (150)  NOT NULL,
    [SourceType]         VARCHAR (100)  NOT NULL,
    [InstanceURL]        VARCHAR (8000) NULL,
    [Port]               VARCHAR (100)  NULL,
    [UserName]           VARCHAR (100)  NULL,
    [SecretName]         VARCHAR (8000) NULL,
    [IPAddress]          VARCHAR (8000) NULL,
    [LastUpdate]         DATETIME       NOT NULL,
    [UpdatedBy]          VARCHAR (100)  NOT NULL
);
GO

