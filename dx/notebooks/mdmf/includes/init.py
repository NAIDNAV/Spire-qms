# Databricks notebook source
# MAGIC %run ../../compartment/includes/compartmentConfig

# COMMAND ----------

# DBTITLE 1,Initialize Spire
import json
import requests
import spire.functions
import spire.utils

environmentLetter = spire.utils.getEnvironmentLetter()

spark.sql(f"USE CATALOG {spire.sql.defaultCatalog()}")

# COMMAND ----------

# MAGIC %run ./frameworkConfig

# COMMAND ----------

# MAGIC %run ../../compartment/includes/environmentDevConfig $environmentLetter=environmentLetter

# COMMAND ----------

# MAGIC %run ../../compartment/includes/environmentIntConfig $environmentLetter=environmentLetter

# COMMAND ----------

# MAGIC %run ../../compartment/includes/environmentProdConfig $environmentLetter=environmentLetter

# COMMAND ----------

class CompartmentConfig:
    WORKFLOWS_CONFIGURATION_FILE = WORKFLOWS_CONFIGURATION_FILE if "WORKFLOWS_CONFIGURATION_FILE" in globals() else None
    METADATA_DQ_CONFIGURATION_FILE = METADATA_DQ_CONFIGURATION_FILE if "METADATA_DQ_CONFIGURATION_FILE" in globals() else None
    METADATA_DQ_REFERENCE_VALUES_FILE = METADATA_DQ_REFERENCE_VALUES_FILE if "METADATA_DQ_REFERENCE_VALUES_FILE" in globals() else None
    METADATA_DQ_REFERENCE_PAIR_VALUES_FILE = METADATA_DQ_REFERENCE_PAIR_VALUES_FILE if "METADATA_DQ_REFERENCE_PAIR_VALUES_FILE" in globals() else None
    METADATA_CONFIGURATION_FILE_VALIDATION_FILE = METADATA_CONFIGURATION_FILE_VALIDATION_FILE
    FWK_LAYER_DF = FWK_LAYER_DF
    FWK_TRIGGER_DF = FWK_TRIGGER_DF
    FWK_LINKED_SERVICE_DF = FWK_LINKED_SERVICE_DF
    FWK_METADATA_CONFIG = FWK_METADATA_CONFIG


compartmentConfig = CompartmentConfig()

# COMMAND ----------

# DBTITLE 1,Trigger ADF pipeline
def getAccessToken():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": CLIENTID,
        "client_secret": CLIENTSECRET,
        "resource": "https://management.azure.com/"
    }

    response = requests.post(url, data=payload).json()
    return response["access_token"]

def triggerADFPipeline(pipelineName, parameters):
    url = (
        f"https://management.azure.com/subscriptions/{SUBSCRIPTION_ID}/"
        f"resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.DataFactory/"
        f"factories/{FACTORY_NAME}/pipelines/{pipelineName}/createRun?api-version=2018-06-01"
    )
    
    headers = {
        "Authorization": f"Bearer {getAccessToken()}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, data=json.dumps(parameters))
    return response.json()
