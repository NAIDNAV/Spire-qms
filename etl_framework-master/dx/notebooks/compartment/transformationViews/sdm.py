# Databricks notebook source
# MAGIC %run ../../mdmf/includes/init

# COMMAND ----------

dbutils.widgets.text("ADFTriggerName", "", "")
adfTriggerName = dbutils.widgets.get("ADFTriggerName")
print(adfTriggerName)

dbutils.widgets.text("FwkTriggerId", "", "")
fwkTriggerId = dbutils.widgets.get("FwkTriggerId")
print(fwkTriggerId)

# COMMAND ----------

# DBTITLE 1,Application_MEF_SDM transformation views
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW Application_MEF_SDM.TV_Address AS 
# MAGIC SELECT
# MAGIC    a.AddressID
# MAGIC   ,a.AddressLine1
# MAGIC   ,a.AddressLine2
# MAGIC   ,a.City
# MAGIC   ,a.StateProvince
# MAGIC   ,a.CountryRegion
# MAGIC   ,a.PostalCode as ZIPCode
# MAGIC   ,"abc" as ExtraColumn1
# MAGIC   ,"def" as ExtraColumn2
# MAGIC FROM Application_MEF_History.Address a
# MAGIC WHERE a.MGT_isCurrent
# MAGIC   AND a.MGT_isDeleted = FALSE;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Application_MEF_SDM.TV_Customer AS 
# MAGIC SELECT
# MAGIC    c.CustomerID
# MAGIC   ,CAST(c.NameStyle AS INT) AS NameStyle
# MAGIC   ,c.Title
# MAGIC   ,c.FirstName
# MAGIC   ,c.MiddleName
# MAGIC   ,c.LastName
# MAGIC   ,c.Suffix
# MAGIC   ,c.CompanyName
# MAGIC   ,c.SalesPerson
# MAGIC   ,c.EmailAddress AS Email
# MAGIC   ,c.Phone AS PhoneNumber
# MAGIC FROM Application_MEF_History.Customer c
# MAGIC WHERE c.MGT_isCurrent
# MAGIC   AND c.MGT_isDeleted = FALSE
# MAGIC   AND coalesce(c.MGT_ValidationStatus, '') <> 'QUARANTINED';
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Application_MEF_SDM.TV_CustomerNoIdentity AS 
# MAGIC SELECT
# MAGIC    c.CustomerID
# MAGIC   ,CAST(c.NameStyle AS INT) AS NameStyle
# MAGIC   ,c.Title
# MAGIC   ,c.FirstName
# MAGIC   ,c.MiddleName
# MAGIC   ,c.LastName
# MAGIC   ,c.Suffix
# MAGIC   ,c.CompanyName
# MAGIC   ,c.SalesPerson
# MAGIC   ,c.EmailAddress AS Email
# MAGIC   ,c.Phone AS PhoneNumber
# MAGIC FROM Application_MEF_History.Customer c
# MAGIC WHERE c.MGT_isCurrent
# MAGIC   AND c.MGT_isDeleted = FALSE;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Application_MEF_SDM.TV_CustomerAddress AS 
# MAGIC SELECT
# MAGIC    ca.CustomerID
# MAGIC   ,ca.AddressID
# MAGIC   ,ca.AddressType
# MAGIC FROM Application_MEF_History.CustomerAddress ca
# MAGIC WHERE ca.MGT_isCurrent
# MAGIC   AND ca.MGT_isDeleted = FALSE;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Application_MEF_SDM.TV_SalesOrderHeader AS 
# MAGIC SELECT
# MAGIC    soh.SalesOrderID
# MAGIC   ,soh.RevisionNumber
# MAGIC   ,soh.OrderDate
# MAGIC   ,soh.DueDate
# MAGIC   ,soh.ShipDate
# MAGIC   ,soh.Status
# MAGIC   ,CAST(soh.OnlineOrderFlag AS INT) AS OnlineOrderFlag
# MAGIC   ,soh.SalesOrderNumber
# MAGIC   ,soh.PurchaseOrderNumber
# MAGIC   ,soh.AccountNumber
# MAGIC   ,soh.CustomerID
# MAGIC   ,soh.ShipToAddressID
# MAGIC   ,soh.BillToAddressID
# MAGIC   ,soh.ShipMethod
# MAGIC   ,soh.CreditCardApprovalCode
# MAGIC   ,soh.SubTotal
# MAGIC   ,soh.TaxAmt
# MAGIC   ,soh.Freight
# MAGIC   ,soh.TotalDue
# MAGIC   ,soh.Comment
# MAGIC   ,soh.ModifiedDate
# MAGIC FROM Application_MEF_History.SalesOrderHeader soh
# MAGIC WHERE coalesce(soh.MGT_ValidationStatus, '') <> 'QUARANTINED';
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Application_MEF_SDM.TV_DimCustomerOracle AS 
# MAGIC SELECT
# MAGIC    dco.CUSTOMERKEY
# MAGIC   ,dco.ADDRESSLINE1
# MAGIC   ,dco.ADDRESSLINE2
# MAGIC   ,dco.BIRTHDATE
# MAGIC   ,dco.COMMUTEDISTANCE
# MAGIC   ,dco.CUSTOMERALTERNATEKEY
# MAGIC   ,dco.DATEFIRSTPURCHASE
# MAGIC   ,dco.EMAILADDRESS AS EMAIL
# MAGIC   ,dco.ENGLISHEDUCATION
# MAGIC   ,dco.ENGLISHOCCUPATION
# MAGIC   ,dco.FIRSTNAME
# MAGIC   ,dco.GENDER
# MAGIC   ,dco.GEOGRAPHYKEY
# MAGIC   ,dco.HOUSEOWNERFLAG
# MAGIC   ,dco.LASTNAME
# MAGIC   ,dco.MARITALSTATUS
# MAGIC   ,dco.MIDDLENAME
# MAGIC   ,dco.NAMESTYLE
# MAGIC   ,dco.NUMBERCHILDRENATHOME
# MAGIC   ,dco.PHONE
# MAGIC   ,dco.SUFFIX
# MAGIC   ,dco.TITLE
# MAGIC   ,dco.TOTALCHILDREN
# MAGIC   ,dco.YEARLYINCOME
# MAGIC FROM Application_MEF_History.DIMCUSTOMER dco
# MAGIC WHERE dco.MGT_isCurrent
# MAGIC   AND dco.MGT_isDeleted = FALSE;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Application_MEF_SDM.TV_DimCurrency AS 
# MAGIC SELECT
# MAGIC    dm.CURRENCYKEY
# MAGIC   ,dm.CURRENCYALTERNATEKEY
# MAGIC   ,dm.CURRENCYNAME
# MAGIC FROM Application_MEF_History.DIMCURRENCY dm
# MAGIC WHERE dm.MGT_isCurrent
# MAGIC   AND dm.MGT_isDeleted = FALSE;
