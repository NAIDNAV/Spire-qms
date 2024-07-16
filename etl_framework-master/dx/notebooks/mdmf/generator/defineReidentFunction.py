# Databricks notebook source
prefix = f"""{spire.sql.defaultCatalog().replace('`','')}.{compartmentConfig.FWK_METADATA_CONFIG["metadata"]["databaseName"]}"""

def sqlStringLiteral(s):
    return "'" + s.replace("\\", "\\\\").replace("'", "\\'").replace("$", "\\u0024") + "'"
  
spark.sql(f"USE CATALOG hive_metastore")

spark.sql(f"""
    CREATE OR REPLACE FUNCTION ReIdent 
    AS 'com.daimler.spire.ReIdent'
    using JAR '/dbfs/FileStore/jars/266f450b_7be1_4388_a06b_3ed82094bd22-spireutils_2_12_1_6_0-ab6d8.jar'
""")

pseudoSecrets = format(sqlStringLiteral(spire.utils.pseudonymizationSecrets()))

spark.sql(f"""
    CREATE FUNCTION IF NOT EXISTS {prefix}.reident(s string)
    returns string
    return ReIdent(s, {pseudoSecrets}, "latest")
""")

spark.sql(f"""
    GRANT EXECUTE
    ON FUNCTION {prefix}.reident
    TO `account users`
""")

spark.sql(f"USE CATALOG {spire.sql.defaultCatalog()}")
