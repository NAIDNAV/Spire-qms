# Databricks notebook source
# MAGIC %run ../../mdmf/includes/init

# COMMAND ----------

# DBTITLE 1,Application_MEF_Dimensional transformation views
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW Application_MEF_Dimensional.TV_dimCustomer AS
# MAGIC SELECT
# MAGIC    c.CustomerID
# MAGIC   ,c.CompanyName
# MAGIC   ,c.SalesPerson
# MAGIC FROM Application_MEF_SDM.Customer c
# MAGIC WHERE coalesce(c.MGT_ValidationStatus, '') <> 'QUARANTINED';
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Application_MEF_Dimensional.TV_factCustomerSales AS
# MAGIC SELECT
# MAGIC    soh.SalesOrderID
# MAGIC   ,soh.RevisionNumber
# MAGIC   ,soh.OrderDate
# MAGIC   ,soh.DueDate
# MAGIC   ,soh.ShipDate
# MAGIC   ,soh.Status
# MAGIC   ,soh.CustomerID
# MAGIC FROM Application_MEF_SDM.SalesOrderHeader soh
# MAGIC WHERE coalesce(soh.MGT_ValidationStatus, '') <> 'QUARANTINED';
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Application_MEF_Dimensional.TV_DimCustomerOracle AS 
# MAGIC SELECT
# MAGIC    dco.CustomerKey
# MAGIC   ,dco.Email
# MAGIC   ,dco.FirstName
# MAGIC   ,dco.LastName
# MAGIC   ,dco.Phone
# MAGIC FROM Application_MEF_SDM.DimCustomerOracle dco;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW Application_MEF_Dimensional.TV_DimCurrency AS 
# MAGIC SELECT
# MAGIC    dm.CurrencyKey
# MAGIC   ,dm.CurrencyAlternateKey
# MAGIC   ,dm.CurrencyName
# MAGIC FROM Application_MEF_SDM.DimCurrency dm;
