# Databricks notebook source
# MAGIC %md
# MAGIC ####How to use this notebook
# MAGIC Follow the steps listed bellow and execute cells one by one. If you'll run into any problems, do not continue and contact MoBI Core team. If the spark session is closed (in case of inactivity), run step **1) Initialization** before you continue.
# MAGIC
# MAGIC #####Steps:
# MAGIC 1) Export this notebook (as a Source File)
# MAGIC 2) Import the notebook into Workspace > Shared root folder
# MAGIC 3) Change the next cell to "%run ../Code/mdmf/includes/init"
# MAGIC 4) Run steps **1) Initialization** and **2) Replace mnt paths with abfss URI**
# MAGIC 5) Request enabling Unity Catalog for your workspace via the spire Service Desk
# MAGIC 6) Once it's enabled, go to Data Explorer and make sure that you see catalog "westeurope_spire_platform_dev"
# MAGIC 7) In step **3) Migrate tables form hive default catalog to spire catalog** change the databaseNameMapping object. Keys are current database names of your compartment, values are new names. If you do not want to change database names at all, comment this object and uncomment the line which sets it to None
# MAGIC 8) Run step **3) Migrate tables form hive default catalog to spire catalog**
# MAGIC 9) Go to Data Explorer and make sure that all databases and tabes are under catalog "westeurope_spire_platform_dev"
# MAGIC 10) Run steps **4) Drop databases from default catalog**, **5) Unmount storages** and **6) Remove mnt folders**

# COMMAND ----------

# DBTITLE 1,1) Initialization
# MAGIC %run ../mdmf/includes/init

# COMMAND ----------

# MAGIC %run ../mdmf/includes/dataLakeHelper

# COMMAND ----------

def configureAllADLS():
    dataLakeHelper = DataLakeHelper(spark, compartmentConfig)
    
    instanceUrls = (
        FWK_LINKED_SERVICE_DF
        .filter("SourceType = 'ADLS'")
        .select("InstanceURL")
        .distinct()
        .rdd.collect()
    )

    for instanceUrl in instanceUrls:
        (adlsName, uri) = dataLakeHelper.getADLSParams(instanceUrl[0], "")

        spire.utils.configureOAuthAdlsGen2(adlsName, APPLICATION_ID)

configureAllADLS()

def reregisterAllTablesInHive(catalogName: str, databaseNameMapping: MapType):
    if catalogName:
        catalogName += "."
    else:
        catalogName = ""

    if databaseNameMapping:
        # change keys to lowercase
        for key in databaseNameMapping.copy().keys():
            if key.lower() != key:
                databaseNameMapping[key.lower()] = databaseNameMapping[key]
                del databaseNameMapping[key]

    databases = spark.catalog.listDatabases()
    for database in databases:
        if database.name == "default":
            continue

        tableNames = sqlContext.tableNames(database.name)
        for tableName in tableNames:
            tableProps = (
                spark.sql(f"DESC FORMATTED {database.name}.{tableName}")
                .filter("col_name == 'Location' or col_name == 'Provider'")
                .orderBy("col_name")
                .collect()
            )

            # view does not have location > skip it
            if not tableProps:
                continue
            
            tableLocation = tableProps[0].data_type
            provider = tableProps[1].data_type # either Delta or Parquet

            # convert mnt path to full uri
            if "/mnt/" in tableLocation:
                tableLocationParts = tableLocation.replace("//", "/").split("/")

                containerName = tableLocationParts[3]
                adlsUri = tableLocationParts[2]
                for x in range(0, 4):
                    tableLocationParts.pop(0)
                tablePath = "/".join(tableLocationParts)

                tableLocation = f"abfss://{containerName}@{adlsUri}.dfs.core.windows.net/{tablePath}"

            if not catalogName:
                dropSqlCommand = f"DROP TABLE {catalogName}{database.name}.{tableName}"
                spark.sql(dropSqlCommand)
                #print(dropSqlCommand)

            if databaseNameMapping and database.name.lower() in databaseNameMapping.keys():
                newDatabaseName = databaseNameMapping[database.name.lower()]
            else:
                newDatabaseName = database.name

            createSqlCommand = f"""
                CREATE TABLE {catalogName}{newDatabaseName}.{tableName}
                USING {provider}
                LOCATION "{tableLocation}"
            """
            spark.sql(createSqlCommand)
            #print(createSqlCommand)

            print(f"{database.name}.{tableName}")

# COMMAND ----------

# DBTITLE 1,2) Replace mnt paths with abfss URI
spark.sql(f"USE CATALOG hive_metastore")

print("Mnt path replaced for:")
reregisterAllTablesInHive(None, None)

# COMMAND ----------

# DBTITLE 1,3) Migrate tables form hive default catalog to spire catalog
spark.sql(f"USE CATALOG hive_metastore")

# databaseNameMapping = None

databaseNameMapping = {
    "StagingSQL": "Application_MEF_StagingSQL",
    "History": "Application_MEF_History",
    "SDM": "Application_MEF_SDM",
    "Dimensional": "Application_MEF_Dimensional",
    "Metadata": "Application_MEF_Metadata"
}

print("Migration from hive_metastore to spire catalog done for:")
reregisterAllTablesInHive(spire.sql.defaultCatalog().replace('`',''), databaseNameMapping)

# COMMAND ----------

# DBTITLE 1,4) Drop databases from default catalog
spark.sql(f"USE CATALOG hive_metastore")

databases = spark.catalog.listDatabases()
for database in databases:
    if database.name == "default":
        continue

    spark.sql(f"DROP DATABASE IF EXISTS {database.name} CASCADE")

# COMMAND ----------

# DBTITLE 1,5) Unmount storages
for mountInfo in dbutils.fs.mounts():   
    if mountInfo.mountPoint.startswith("/mnt/"):
        dbutils.fs.unmount(mountInfo.mountPoint)

# COMMAND ----------

# DBTITLE 1,6) Remove mnt folders
for folder in dbutils.fs.ls("/mnt/"):
    if folder.path.startswith('dbfs:/mnt/'):
        print(folder)
        dbutils.fs.rm(folder.path)
