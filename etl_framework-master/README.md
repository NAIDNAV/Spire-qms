<h1 align="center">ETL Framework</h1>

<p align = "center">💡 Metadata driven ETL Framework solution for all use cases on Spire.</p>

# Table of Contents

- [Overview](#overview)
- [Who we are](#team)
- [How are we working](#how)
  - [Branching model of ETL Framework](#branching_framework)  
- [Modules](#modules)
  - [Configuration Generator Module](#config_gen_module)
  - [Ingestion Module](#ingestion_module)
  - [Transformation Module](#transformation_module)
  - [Data Validation Module](#validation_module)
  - [Export Module](#export_module)
  - [Reporting Module](#reporting_module)
  - [Workflow Module](#workflow_module)
- [How to start using ETL Framework](#how_to_start)
  - [Branching Model of ETL Framework](#framework_branching)
  - [Using template repository](#template_repo)
    - [Cloning the template repository](#clone_repo)
    - [Branching model that comes with template repository](#usecase_branching)
    - [Enabling GitHub Self-Hosted Runners](#enable_git_shr)
    - [Updating YAML Files for Databricks Deployment](#yml_dx)
    - [Updating YAML Files for ETL Framework Deployment](#yml_framework)
    - [Deployment of the ETL Framework](#deploy_framework)
    - [Firewall Rules for SQL DB Deployment with GitHub Actions Integration](#firewall_sql)
- [Framework Configuration](#framework_config)
  - [Configuration Notebooks](#notebook_config)
    - [compartmentConfig notebook](#compartment_config)
    - [environmentConfig notebook](#env_config)
      - [environmentConfig notebook - Variables](#variables)
      - [environmentConfig notebook - Layers](#layers)
         - [preTransformationNotebook/postTransformationNotebook](#prepost)
      - [environmentConfig notebook - Triggers](#triggers)
      - [environmentConfig notebook - Linked Services](#linked_services)
      - [environmentConfig notebook - Metadata Config](#metadata_config)
  - [JSon Configuration Files](#json_config)
    - [Ingestion Configuration File](#ingestion_config)
      - [Ingestion Configuration File for SQL Server sources](#ingestion_config_sql)
      - [Ingestion Configuration File for Oracle sources](#ingestion_config_oracle)
      - [Ingestion Configuration File for Snowflake sources](#ingestion_config_snowflake)
      - [Ingestion Configuration File for OData sources](#ingestion_config_odata)
      - [Ingestion Configuration File for Fileshare, ADLS and SFTP sources](#ingestion_config_files)
    - [Transformation Configuration File](#transformation_config)
    - [Model Transformation Files](#model_transformation_files)
      - [Entities File](#entities_file)
      - [Attributes File](#attributes_file)
      - [Keys File](#keys_file)
      - [Relations File](#relations_file)
      - [Model Transformation Configuration File](#model_transformation_config)
    - [Data Validation Files](#data_validation_files)
      - [DQ Configuration File](#dq_configuration_file)
      - [DQ Reference Values File](#dq_reference_values_file)
      - [DQ Reference Pair Values File](#dq_reference_pair_values_file)
      - [Available DQ checks](#available_DQ_checks)
    - [Export Configuration File](#export_configuration_file)
    - [Workflow Configuration File](#workflow_configuration_file)
- [Pipelines](#pipelines)
- [Tables](#tables)

# Overview<a name = "overview"></a>

The ETL Framework enables metadata-based, configurable, end-to-end data ingestion, transformation and validation from various data sources in a standardized and automated process using Azure technologies.

This diagram shows how MoBI ETL Framework runs on Spire platform, showing its modules and interactions between the modules of the framework.

![image](https://media.git.i.mercedes-benz.com/user/9160/files/1fa5914c-68f5-4b81-b031-f50c17067b8f)

**Required services**

The following resources are required for the framework to operate:
- Azure Key Vault for storing connection details (connection strings, authentication tokens, password etc.) to linked services
- Azure Storage Account (ADLS type) for storing ingested and transformed data
- Azure SQL Server and SQL Database for storing framework configuration
- Azure Data Factory (ADF) for ingestion and flow orchestration
- Databricks for executing framework notebooks for config generation, transformations, validation

# Who we are<a name = "team"></a>

We are a team under spire, aiming to provide the best base solution to all use-case teams who are considering to use spire as data platform.

* Product Owner: [Burak Duran](https://mbinside.app.corpintra.net/person/ASDURAN)
* Lead Data Engineer: Michal Rutrich
* Data Engineers: [Suhas Sangolli](https://mbinside.app.corpintra.net/person/SSANGOL), [Harish Shanmugam](https://mbinside.app.corpintra.net/person/HASHANM), [Pooja Sajit](https://mbinside.app.corpintra.net/person/POSAJIT)
* DevOps Engineer: [Durga Prasad](https://mbinside.app.corpintra.net/person/PRASDUR)

# How are we working<a name = "how"></a>

We are working as a team in agile methodology, in sprints with 2 weeks duration. 
Below, you can also find out which branching model we use to implement ETL framework.

## Branching Model of ETL Framework <a name = "framework_branching"></a>

This section is just to describe how framework team developes the ETL framework, branching model used.
Our strategy is stable mainline model, which is a lightweight and simple approach that enables continuous deployment, allowing for frequent and fast releases. 
The strategy involves : the master branch, the feature branches, and the release branches. 
In this section, we provide an overview of our branching model and the guidelines that framework developers should follow when working with branching.

![image](https://media.git.i.mercedes-benz.com/user/9160/files/6304833e-7705-49de-ab2d-c4f460fefa14)

Our branching model involves the following steps: 

 - **Create a feature branch:** Developers should create a feature branch for each change they wish to make to the codebase. The branch should be named as feature/<issue_name>, with <issue_name> representing the feature or issue being addressed. 
NOTE: It is essential to create a feature branch from the master branch.
 - **Write and commit changes:** Developers should make changes to the codebase and commit them to the feature branch. Each commit should include a descriptive message that explains the changes being made.
 - **Rebase:** Developers should rebase the code from the master branch frequently to ensure that changes made by other developers are reflected in the feature branch.
 - **Open a pull request:** Once the changes are complete, developers should open a pull request to merge the feature branch into the master branch. The pull request should include a description of the changes, relevant documentation, and tests.
**NOTE:** At least one reviewer needs to be added to the pull request.
- **Review changes:** Reviewers should review the changes to ensure that the code is working correctly. Any issues or concerns should be raised in the pull request comments section.
 - **Merge changes:** Once the changes have been reviewed and tested, the feature branch can be merged into the master branch. We recommend using the "merge and commit" option to maintain a clean and easy-to-follow commit history.
**NOTE:** It is important not to delete the feature branch until all integration tests are complete.
 - **Release:** When the team decides to create a new release, a new branch should be created from the master branch named as release/vX.Y, where X represents the major changes, and Y represents the minor changes. When drafting the new release in GitHub, select the branch release/vX.Y. The release-tag should be named as vX.Y.a ("a" represents the patch or bug fix).
**NOTE:** Never merge the release branch back into the master branch.
 - **Bug Fix:** When developers or users find bugs in the application (on a released version), developers should decide where to fix this bug. There are mainly two options. 
If the bug is specific to a certain release and not existing in other releases or in main branch (which is not a very likely scenario), developers can apply the fix on the respective release branch directly and create a new release from this release branch with an incremented “a” (in vX.Y.a) release tag.
If the bug is not specific to a certain release and existing in other releases or in main branch (which is a more likely scenario), developers create a new feature branch from master, apply the fix on the respective feature branch, create PR and merge to master. The commit should be cherry picked to the release branches that are affected by the same bug. Create a new release from respective release branches with an incremented “a” (in vX.Y.a) release tag.

# Modules <a name = "modules"></a>

In ETL Framework, we have different modules, responsibility of each module is explained in detail in each modules page. Below in schema, high level you can see and understand what each module is doing and how they are interacted with each other and Azure resources in spire compartment.

![image](https://media.git.i.mercedes-benz.com/user/9160/files/87dba5ed-3051-460d-ad5e-e3d478b19a5e)

## Config Generator Module <a name = "config_gen_module"></a>

- Config Generator module’s purpose is to autogenerate ingestion, transformation and export instructions for the framework based on configuration files managed by use-case teams. (Json files and notebooks)
- This module is triggered when there is PR created by use-case teams and when this PR consist of changes in either environmentConfig notebooks or Json config files.
- As visualized in below schema, module generates framework instructions for configuration sql database in temporary CSV files or parquet files, and then ADF takes these temporary files and refreshes the configuration database of the framework.

![image](https://media.git.i.mercedes-benz.com/user/9160/files/94180f77-ecf0-4315-a44b-14255c470521)

## Ingestion Module <a name = "ingestion_module"></a>

The purpose of the ingestion module is to ingest data from source systems and store it as parquet files in ADLS container. 
Ingestion module is metadata driven – it’s controlled by instructions generated by ingestion config generator in [Config Generator Module](#config_gen_module). 
The ingestion module is implemented in ADF as a set of generic and parametric pipelines and uses Copy activity to read data from a source system and store it to an ADLS storage.

![image](https://media.git.i.mercedes-benz.com/user/9160/files/f378a0a7-4b4b-40ba-b46a-d4ed54d2cda3)

**Data Ingestion from source types;** 
- SQL Server (on prem)
- Azure SQL
- Oracle Db (on prem)
- Oracle Cloud
- Delimited text files (txt, cma, csv), Excel Files, XML Files from FileShare and SFTP sources
- Delimited text files (txt, cma, csv), Excel Files, XML Files from ADLS Storages
 
**Data Ingestion capabilities:**
- Pre-Implemented ADF pipelines for data ingestion
- Multiple ingestion configuration files if needed
- Ingest and store metadata of the source systems automatically
- Full Load/Incremental Load from SQL Server and Oracle
- Option to ingest selective columns of a table/view, or result of a query
- Option to rename columns and cast data types during ingestion from file sources (mapping parameterization)
- Wildcard option for CSV and Excel sources
- Batch mechanism for ingestion module for the case that one source file is ingested more than once
- Archive option for file sources after ingestion
- Ingest name of the file as an additional column for file sources
- Fault tolerance for file sources; capability to redirect incompatible rows

## Transformation Module <a name = "transformation_module"></a>
The purpose of the transformation module is to load data from the previous layer, transform the data and to store it as delta table on next layer in ADLS container. Transformation module is metadata driven – it’s controlled by instructions generated by transformation config generator, model transformation generator or export config generator in [Config Generator Module](#config_gen_module). Each instruction contains information about one entity – which data should be loaded, what transformation should be applied to the data, where and how the data will be stored.

![image](https://media.git.i.mercedes-benz.com/user/9160/files/fc024b47-1ebb-40b4-8abf-38d4a15b2aea)

**Data transformation capabilities:**
- Manage all transformations per layer/table in configuration files
- Combining multiple transformations at one step
- Wildcard naming to make configurations easy
- Capability to configure triggers and write modes per table
- Batch mechanism for transformation module for the case that same table is configured as a sink
- Data partitioning option

**The Transformation module supports following types of transformations:**
- Pseudonymization 
- Deduplicate
- Replace Nulls
- Select 
- SelectExpression
- Filter 
- Data type casting
- Rename columns
- Derived columns
- Join
- Aggregate

**Supported write modes:** <a name = "writemodes"></a>

Once the data is transformed with one or more of the transformation types above, the framework can apply  following write modes for storing the data:

- **Append** – new data are appended to the delta table
- **Overwrite** – new data overwrites existing data in the delta table
- **Snapshot** – existing data in the delta table that match snapshot key values of new data are removed and new data are appended to the delta table
- **SCDType1** – performs slowly changing dimension type 1; update of the existing data and appends new data
- **SCDType2** – performs slowly changing dimension type 2; update of the existing data, appends new data and marks missing data as deleted (handles inserts, updates and deletes)
- **SCDType2Delta** – performs slowly changing dimension type 2; update of the existing data and appends new data (handles inserts and updates)
- **SCDType2Delete** – performs slowly changing dimension type 2; delete for missing data (handles deletes only)

Once the data is transformed, sink table is enriched with audit columns depending on the write mode
For all write modes, below columns are added. 
- **MGT_InsertTime** represents date and time when the data were inserted into the target layer 
- **MGT_UpdateTime** represents date and time when data were modified. 

In SCDType2* write modes, additionally below columns are added. 
- **MGT_isCurrent** represents if the records is the active record of the historized dataset or not
- **MGT_isDeleted** represents the record is historized as deleted or not
- **MGT_effFrom** represents the date and time the record is valid from
- **MGT_effTo** represents the date and time the record is valid until
- **MGT_bkHash** represents the hash value of the business keys
- **MGT_vHash** represents the hash value of the other attributes except business keys

**Data Model Transformations**

Framework also supports data models and accepts metadata of data models with defined input formats. Using these model files, framework can analyze the relationships and keys of entities, so automatically generates the source queries of model tables to populate surrogate keys, also the batch groups and order of executions. In source queries generated, framework expects the transformation logic for each entity to be handled in a transformation view. For ex, for Customer table of the data model, framework expects the existance of a transformation view with name TV_Customer. Responsibility of creation of this view is on use-case teams. Ideally, the notebook that will create these transformation views is configured to be executed as a preTransformationNotebook of related layer. In these transformation views, only the business logic to transform source data to model table should exist, joins to other model tables to populate surrogate keys are handled automatically by framework.

Additionally, during config generator module execution, model tables are also created/updated upfront before ETL runs. Config generator module determines changes like new/deleted tables, new/deleted columns or data type changes and automatically reflects these changes to databricks tables. This is also a different behaviour than regular transformations.Schema evolution is not allowed for model transformations, changes to schemas can only be done using model metadata files.

## Data Validation Module <a name = "validation_module"></a>
Purpose of data validation module is to apply data quality checks on the data during ETL operations. This module is integrated to transformation module and as a general rule, validations are applied before the data is written to the sink table of a transformation step.
Module is implemented based on an open source library called [Great Expectations](https://greatexpectations.io/).

**Data Validation capabilities:**
- Integrated with transformation module
- Manage all validation configurations per table/object in one file
- 40+ ready to use expectation types
- Row based and data set based expectation types
  - **Row based** - each row is validated separately. Validated dataset can contain both valid, invalid and quarantined data
  - **Dataset based** - dataset is validated as a whole (can’t be validated on row basis). Validated dataset can be either valid, invalid or quarantined
- Capability to configure per expectation weather or not to quarantine invalid data
  All tables subject to validation are enriched with a column called **MGT_ValidationStatus** which is storing the information about the validation result;
  - **Valid** - means that row passed all validation checks
  - **Invalid** - means that row did not pass at least one validation check and none of the failed checks requires that invalid data should be quarantined
  - **Quarantined** - means that row did not pass at least one validation check which requires that invalid data should be quarantined and not processed further.
  
  Valid and invalid data are allowed to move to the next layer. However quarantined data is not be processed further and will stay on the current layer. 
  If the source of a transformation is a table, filtering quarantined data is automatic, if it is a user-defined transformation view, this view has to include a condition selecting all the data except of quarantined ones (WHERE MGT_ValidationStatus <> 'QUARANTINED').
  
- Option to configure the information columns to be logged for each expectation to validation results
- Option to manage reference values/pair values separately and use them as a reference in DQ configurations
- Cross table validations capability
- Saving all validation results in a standard format in a single table with all previous history

## Export Module <a name = "export_module"></a>
Purpose of export module is to export data to external locations outside of compartment. This module is handled with configurations similar to all other modules.

![image](https://media.git.i.mercedes-benz.com/user/9160/files/2b90097c-6f6c-428a-9471-30740496d8e6)


**Data Export capabilities:**
- Manage data export with configurations in config files
- Export data in CSV format file to ADLS, Fileshare or SFTP
- Export data to SQL servers (on-prem or cloud)

## Reporting Module <a name = "reporting_module"></a>
Purpose of reporting module is to visualize the logs that framework collects during ETL executions and provide a monitoring tool to track the process of ETL operations in live mode and also historically.

- **ETL Health Check Dashboard**

The aim of this dashboard is to provide the use case team to have a complete insight about ETL activities on their compartment. Dashboard uses the log tables of the framework as a source, so can show all the history of executions and gives the end user the chance to analyze the logs with visualizations and tables.

![image](https://media.git.i.mercedes-benz.com/user/9160/files/a00b6ee3-29e0-4729-8e2d-c48897d3a724)

![image](https://media.git.i.mercedes-benz.com/user/9160/files/de5ec4c2-642d-4843-93c8-3a1a050e7f53)

## Workflow Module <a name = "workflow_module"></a>
Framework is able to call Power Automate Flows to cover any business workflow requirement of use cases. Power Automate Flow development is on use-case side and if it is designed to be triggered with a HTTP Url call, framework is able to integrate the flow to a step in ETL process, before or after a layer and this integration is managed by configuration files.

# How to start using ETL Framework <a name = "how_to_start"></a>

To start using ETL framework for a use-case in Spire environment, first we need to understand what is happening in background. Spire provides the use-cases the necessary infrastructure to work on. This infrastructure consists of ADF, Databricks, SQL db and ADLS storage accounts.
When a use-case uses the ETL Framework solution, 3 artifacts( ADF, Databricks and SQL db) has to be deployed to related compartment resources of the use-case.
Besides of framework artifacts to be deployed, use-case teams are supposed to be dealing with one databricks repository only. No need to implement anything on ADF and SQL db by the use-case team. Below schema shows this in detail.

![image](https://media.git.i.mercedes-benz.com/user/9160/files/79a8a1b9-b1ca-4b25-a440-425a4e76157d)

## Using template repository <a name = "template_repo"></a>

Framework team is also providing a template databricks repository which use-case teams can use to start with. It can be found [here](https://git.i.mercedes-benz.com/mbm-bi-transformation/dx_usecase_template) under MBM BI Transformation organization.
It is useful to start with this template repository since it includes the folder/files structure to be ready to start right away and yml files to be ready for CI/CD pipelines already.

### Cloning the template repository<a name = "clone_repo"></a>

- [ ] Navigate to the [dx_usecase_template](https://git.i.mercedes-benz.com/mbm-bi-transformation/dx_usecase_template)
- [ ] Click on the “Use this template” button.
- [ ] Enter the repository name as per your requirements. Ensure that the repo is created under your organization.
  - Repository Name: “YOUR_REPO_NAME”
  - Organization: “YOUR_ORGANIZATION_NAME”
- [ ] Click on the “create repository from template” button.

### Branching model that comes with template repository<a name = "usecase_branching"></a>

Template repository also comes with ready to use CI/CD pipelines for deployment activities. These pipelines are implemented accordingly considering a default branching model which will be explained now. This is just a default offering from framework team to use-cases, it is not a must. If it does not fit to a use-case team for any reason, for sure use-case teams can decide on their branching model. In this case, necessary changes will have to be applied to deployment pipelines.

This schema shows the branching model that Core team proposes the Use-Case teams to use for their own repositories. 
The strategy involves : the master branch, the feature branches, and the staging branch and the production branch.
In this document, we provide an overview of branching model and the guidelines that developers should follow when working with branching.

![image](https://media.git.i.mercedes-benz.com/user/9160/files/e0585b2f-90fa-430a-9d68-a00d772b8cd7)

Branching model involves the following steps: 

 - **Create a feature branch:** Developers should create a feature branch for each change they wish to make to the codebase. The branch should be named as feature/<issue_name>, with <issue_name> representing the feature or issue being addressed. 
**NOTE:** It is essential to create a feature branch from the master branch.
 - **Write and commit changes:** Developers should make changes to the codebase and commit them to the feature branch. Each commit should include a descriptive message that explains the changes being made.
 - **Rebase:** Developers should rebase the code from the master branch frequently to ensure that changes made by other developers are reflected in the feature branch.
 - **Open a pull request:** Once the changes are complete, developers should open a pull request to merge the feature branch into the master branch. The pull request should include a description of the changes, relevant documentation, and tests.
NOTE: At least one reviewer needs to be added to the pull request.
 - **Review changes:** Reviewers should review the changes to ensure that the code is working correctly. Any issues or concerns should be raised in the pull request comments section.
 - **Merge changes:** Once the changes have been reviewed and tested, the feature branch can be merged into the master branch. We recommend using the "merge and commit" option to maintain a clean and easy-to-follow commit history.
NOTE: It is important not to delete the feature branch until all integration tests are complete.
 - **Release to staging:** When the team decides to create a new release to staging, the code from master can then be deployed to Staging by merging the desired state of master into the staging branch.
**NOTE:** Never merge the staging branch back into the master branch.
 - **Release to production:** When the team decides to create a new release to production, it is achieved similarly by merging the desired state of the staging branch into the production branch.
**NOTE:** Never merge the production branch back into the master branch.
 - **Bug Fix:** When developers or users find bugs, fixing bugs is done preferably on a feature branch (derived from master) and then cherry-picked (or merged) to the given environment branch.
**NOTE:** No development is done on environment branches.

### Enabling GitHub Self-Hosted Runners<a name = "enable_git_shr"></a>

To utilize GitHub Actions as a CI/CD tool for your compartment repo, It's necessary to enable self-hosted Azure runners on the required use case repository organization. Follow these steps for Runners setup.
- [ ] Add [PID1CE4](https://git.i.mercedes-benz.com/PID1CE4) as Owner to your Organization
  Grant the ownership access to the GitHub account with the username PID1CE4. This is essential for the setup process. 
- [ ] Update "onboardedOrganizations-mbm.json" file
  - Open the [onboardedOrganizations-mbm.json](https://git.i.mercedes-benz.com/mbm/gha-azure-runners-infra/blob/main/deploy/actions-runner-controller/onboardedOrganizations-mbm.json) file located in the [gha-azure-runners-infra](https://git.i.mercedes-benz.com/mbm/gha-azure-runners-infra) repository and add your organization to the organization list in onboardedOrganizations-mbm.json
  - You might not have access to the repository, fork it and then create a PR from your forked repo.
  - Once the PR is reviewed and approved, merge it into the main branch, Then GitHub Actions Azure runners will be enabled for your organization.
  - For more information about runners Infrastructure refer [README.md](https://git.i.mercedes-benz.com/mbm/gha-azure-runners-infra/blob/main/README.md) file.

### Updating YAML Files for Databricks Deployment<a name = "yml_dx"></a>

After successfully cloning the template repository into your use case repository, follow these steps to update the main.yml file under ‘.github/workflows’ to make the deployment pipeline ready for regular deployments to Azure Databricks.

- [ ] Open main.yml using a text editor under '.github/workflows' directory in your cloned repository
- [ ] Make the necessary changes based on your compartment, Ensure to update Environment variables and their- secrets configurations as needed.

```python
# TODO: Replace with your Databricks compartment workspace host URLs.
env:
  DATABRICKS_HOST_DEV: https://adb-xxxxxxxxxxxxxxxxxxx.azuredatabricks.net # ddxcpdmo
  DATABRICKS_HOST_INT: https://adb-xxxxxxxxxxxxxxxxxxx.azuredatabricks.net # idxcpdmo
  DATABRICKS_HOST_PROD: https://adb-xxxxxxxxxxxxxxxxxx.azuredatabricks.net # pdxcpdmo
  
  ADF_PIPELINE_NAME: "PL_GenerateConfigurationPipeline"
  
  ADFNAME_DEV: ddfcp***
  ADFNAME_INT: idfcp***
  ADFNAME_PRD: pdfcp***
  
  CLIENT_ID_DEV: ${{secrets.CLIENT_ID_***_DEV}}
  CLIENT_SECRET_DEV: ${{secrets.CLIENT_SECRET_***_DEV}}
  RESOURCEGROUP_DEV: 0001-d-cp***
  SUBSCRIPTIONID_DEV: 7*******-****-****-****-************
  
  CLIENT_ID_INT: ${{secrets.CLIENT_ID_***_INT}}
  CLIENT_SECRET_INT: ${{secrets.CLIENT_SECRET_***_INT}}
  RESOURCEGROUP_INT: 0001-i-cp***
  SUBSCRIPTIONID_INT: e*******-****-****-****-************
  
  CLIENT_ID_PRD: ${{secrets.CLIENT_ID_***_PRD}}
  CLIENT_SECRET_PRD: ${{secrets.CLIENT_SECRET_***_PRD}}
  RESOURCEGROUP_PRD: 0001-p-cp***
  SUBSCRIPTIONID_PRD: 8*******-****-****-****-************
  
  TENANT_ID: ${{secrets.TENANT_ID}}
````
- [ ] Obtain the Service Principal credentials from the spire team to update the ‘CLIENT_ID' and 'CLIENT_SECRET’ for DEV, INT and PRD environments.

### Updating YAML Files for ETL Framework Deployment<a name = "yml_framework"></a>

After cloning the template repository, we need to update the YAML file ‘deploy_mdmf_artifacts.yml’ for deploying the MoBI ETL Framework Artifacts.
This 'deploy_mdmf_artifacts.yml' file responsible for deploying the desired version of MoBI ETL Framework artifacts from JFrog artifactory to the respective SQL DB, ADF and Databricks in the compartment.

Steps to update YAML configuration:
- [ ] Open 'deploy_mdmf_artifacts.yml' using a text editor under '.github/workflows' directory in your cloned repository 
- [ ] Update Environment variables for DX, SQL DB and ADF and their respective secrets.
- [ ] Please request the Service Principal credentials for ADF and SQL DB deployments from the Spire team in order to update the 'CLIENT_ID' and 'CLIENT_SECRET' for DEV, INT, and PRD environments.

```python
# Environment variables for SQL deployments
  # TODO: Add the Service Principal Credentials within connection string of SQL DB and provide as a repository secret with names as below 
  SQL_DB_CONNECTION_STRING_DEV: ${{secrets.SQL_DB_CONNECTION_STRING_DMO_DEV}}
  SQL_DB_CONNECTION_STRING_INT: ${{secrets.SQL_DB_CONNECTION_STRING_DMO_INT}}
  SQL_DB_CONNECTION_STRING_PRD: ${{secrets.SQL_DB_CONNECTION_STRING_DMO_PRD}}
  
  
  # Environment variables of ADF ArmTemplate deployment & repalce your compartment variables
  # TODO: Add the ADF names of DEV, INT, PRD Environments
  ADFNAME_DEV: ddfcpdmo 
  ADFNAME_INT: idfcpdmo
  ADFNAME_PRD: pdfcpdmo
  
  # TODO: Add the repository secrets with Client ID and Client secrets of your ADF Deployment Service Principals of DEV, INT and PRD Environments
  CLIENT_ID_DEV: ${{secrets.CLIENT_ID_DMO_DEV}}            # TODO: Add the DEV client ID to repo secrets
  CLIENT_SECRET_DEV: ${{secrets.CLIENT_SECRET_DMO_DEV}}    # TODO: Add the DEV client secret to repo secrets
  RESOURCEGROUP_DEV: 0001-d-cpdmo                          # TODO: Add the resource group of the DEV environment
  SUBSCRIPTIONID_DEV: 775f****-****-****-****-********0d19 # TODO: Add the subscription ID of DEV environment
  
  CLIENT_ID_INT: ${{secrets.CLIENT_ID_DMO_INT}}            # TODO: Add the INT client ID to repo secrets
  CLIENT_SECRET_INT: ${{secrets.CLIENT_SECRET_DMO_INT}}    # TODO: Add the INT client secret to repo secrets
  RESOURCEGROUP_INT: 0001-i-cpdmo                          # TODO: Add the resource group of the INT environment
  SUBSCRIPTIONID_INT: e992****-****-****-****-********eba0 # TODO: Add the subscription ID of INT environment
  
  CLIENT_ID_PRD: ${{secrets.CLIENT_ID_DMO_PRD}}            # TODO: Add the PRD client ID to repo secrets
  CLIENT_SECRET_PRD: ${{secrets.CLIENT_SECRET_DMO_PRD}}    # TODO: Add the PRD client secret to repo secrets
  RESOURCEGROUP_PRD: 0001-p-cpdmo                          # TODO: Add the resource group of the PRD environment
  SUBSCRIPTIONID_PRD: 80b7****-****-****-****-********66cf # TODO: Add the subscription ID of PRD environment
````

- [ ] The Service Principal's Client ID and Client Secret values will be update in GitHub Actions Secrets under your repository settings.
- [ ] Update the ARM template variables with the necessary compartment parameters for the specified use case to override compartment resources, and then deploy the changes into the ADF within the compartment.

```python
PL_LogToMattermost_properties_parameters_MatterMostWebHookURL_defaultValue: # Add Mattermost URL

  # Environment variables for overriding the armTemplate parameters wrt usecase environments
  PL_TriggerCreation_properties_parameters_AzureRestApiUrl_defaultValue_DEV: https://management.azure.com/subscriptions/775f****-****-****-****-********0d19/resourceGroups/0001-d-cpdmo  # TODO: Add your resource values for DEV
  PL_TriggerCreation_properties_parameters_AzureRestApiUrl_defaultValue_INT: https://management.azure.com/subscriptions/e992****-****-****-****-********eba0/resourceGroups/0001-i-cpdmo  # TODO: Add your resource values for INT
  PL_TriggerCreation_properties_parameters_AzureRestApiUrl_defaultValue_PRD: https://management.azure.com/subscriptions/80b7****-****-****-****-********66cf/resourceGroups/0001-p-cpdmo  # TODO: Add your resource values for PRD

  LS_databricks_properties_parameters_ClusterId_defaultValue_DEV: # Add default development environment's ClusterID
  LS_databricks_properties_parameters_ClusterId_defaultValue_INT: # Add default Staging environment's ClusterID
  LS_databricks_properties_parameters_ClusterId_defaultValue_PRD: #Add default production environment's ClusterID

  LS_keyvault_properties_typeProperties_baseUrl_DEV: # TODO Add the KeyVult url for DEV
  LS_keyvault_properties_typeProperties_baseUrl_INT: # TODO: Add the KeyVult url for INT
  LS_keyvault_properties_typeProperties_baseUrl_PRD: # TODO: Add the KeyVult url for PRD

  LS_databricks_properties_typeProperties_workspaceResourceId_DEV: /subscriptions/775f****-****-****-****-********0d19/resourceGroups/0001-d-cpdmo/providers/Microsoft.Databricks/workspaces/ddxcpdmo # TODO: Add your Databricks workspace resource id for DEV
  LS_databricks_properties_typeProperties_workspaceResourceId_INT: /subscriptions/e992****-****-****-****-********eba0/resourceGroups/0001-i-cpdmo/providers/Microsoft.Databricks/workspaces/idxcpdmo # TODO: Add your Databricks workspace resource id for INT
  LS_databricks_properties_typeProperties_workspaceResourceId_PRD: /subscriptions/80b7****-****-****-****-********66cf/resourceGroups/0001-p-cpdmo/providers/Microsoft.Databricks/workspaces/pdxcpdmo # TODO: Add your Databricks workspace resource id for PRD

# Self hosted Integartion Runtime for PRD env
  SHIR_properties_typeProperties_linkedInfo_resourceId_DEV: # TODO: Add the value
  # SHIR_properties_typeProperties_linkedInfo_resourceId_INT: # TODO: Add the value
  # SHIR_properties_typeProperties_linkedInfo_resourceId_PRD: # TODO: Add the value
  
  TENANT_ID: ${{secrets.TENANT_ID}}
````

- [ ] If you are utilizing a different GitHub organization, you need to update the JFrog credentials in your GitHub Actions secrets.  

```python
jobs:
  # This job downloads the MDMF artifacts from JFrog and Uploads them locally
  Download_Artifact_JFrog:
    runs-on: [ azure-runners]
    steps:
      - name: Setup JFrog CLI
        uses: jfrog/setup-jfrog-cli@v3.2.0
        env:
          JF_URL: ${{env.JFrog_URL}}
          JF_USER: ${{secrets.JFROG_ARTIFACTORY_USER}}
          JF_PASSWORD: ${{secrets.JFROG_ARTIFACTORY_API_KEY}}

      - name: Download Artifacts from JFrog Artifactory
        run: |
          jf rt dl ${{env.ARTIFACTORY_NAME}}/mdmf/${{github.event.inputs.version}}/ ./
````

### Deployment of the ETL framework<a name = "deploy_framework"></a>

After these yml files are set correctly, it is ready to deploy framework artifacts to the use-case compartment. In order to do this, following steps should be followed. These steps are on-demand and will have to be done each time there is intention to switch to a newer version of the framework.

- [ ] Navigate to "Actions" tab in your repository and click on DeployMDMFArtifacts on the left side.

![image](https://media.git.i.mercedes-benz.com/user/9160/files/3c9b1a46-92da-4770-81a9-c4358852cb72)

- [ ] Click on "Run Workflow" on the right and give the necessary information. 

![image](https://media.git.i.mercedes-benz.com/user/9160/files/1f654ea2-05d5-451b-8435-e7e399224c9a)

   - "Use Workflow from" to decide from which branch the yml file should be used. This will be master generally, since yml file is set once and not touched later.
   - "Environment" - which environment will the framework be deployed to. Development, staging or production are the options.
   - "Version" - which framework version will be deployed. Framework releases newer versions frequently and it is up to the use-case team which version to use and when to switch to newer versions. Release notes about the versions of the framework can be found [here](https://git.i.mercedes-benz.com/mbm-bi-transformation/etl_framework/releases).

### Firewall Rules for SQL DB Deployment with GitHub Actions Integration<a name = "firewall_sql"></a>

To initiate SQL Database deployment, It is essential to establish firewall rules with in the SQL Server.

Specifically, GitHub Actions Runners IP addresses must be incorporated into the Sql Server firewall rules to enable seamless Sql db deployments via Github Actions.

The responsibility for whitelisting the IP addresses of GitHub Actions Runners is handled by the Spire team. To initiate this process, please raise a helpdesk on spire platform by using the [Service Desk](https://mercedes-benz-mobility.atlassian.net/servicedesk/customer/portal/48/group/169/create/631). 

## Framework Configuration <a name = "framework_config"></a>

After setting up the repository and CI/CD pipelines in previous section, use-case team can start to use the framework by providing configurations.
As mentioned before as well, the framework is implemented in a generic way and it is metadata driven.
The configuration files and notebooks that are needed to configure the framework and  additional notebooks that every use case might need to implement, like complex transformation codes, special calculations etc. are supposed to be kept in Databricks repository of the use-case which we cloned from template in previous section and modified accordingly. 

Configuration of the framework is defined in two forms:

 - **Configuration notebooks** - 4 notebooks, maintained by use-case team in databricks repository, under **notebooks/includes** folder.
 - **Json configuration files** – Json files maintained by use-case team in databricks repository, under **resources** folder. 

To remember how framework config generator module uses these inputs and generates instructions, you can check the schema [here](#config_gen_module)

As mentioned in [this section](#yml_dx), template repository already provides the CI/CD mechanism of databricks deployment with a default branching model for it. 
So, when use-case teams do changes in these configuration files and notebooks and create a PR, respective github action to deploy the changes to databricks instance is being triggered automatically. This deployment does three main things;

- Deployment of Json configuration files to databricks filestore folder **dbfs:/FileStore/MetadataInput/**
- Deployment of configuration notebooks + additional notebooks that use-case teams might have to databricks workspace folder **Code/compartment/includes**
- If there are changes to Json configuration files or configuration notebooks, config generator module is also being triggered automatically to make sure that changes are applied to the instructions of framework.  

## Configuration Notebooks <a name = "notebook_config"></a>

Preparing the configuration notebooks is the initial and crucial thing that has to be done before starting filling the json configuration files. Because in these notebooks, the layers, connections, triggers and steps in the layers are configured.

The notebooks that has to be filled are as belows;

- **compartmentConfig** – contains Spire related variables and any other variables compartment needs to define
- **environmentDevConfig** – contains layers, triggers, linked services and steps in layers configuration for Development environment (described in next chapters)
- **environmentIntConfig** – contains layers, triggers, linked services and steps in layers configuration for Integration environment (described in next chapters)
- **environmentProdConfig** – contains layers, triggers, linked services and steps in layers configuration for Production environment (described in next chapters)

### compartmentConfig notebook <a name = "compartment_config"></a>

This notebook is to keep some variables like DataAssetId, ApplicationId and below technical field names that are added by framework during ETL operations. Use-cases have the opportunity to define the names of these technical columns to meet their naming conventions.

```python
VALIDATION_STATUS_COLUMN_NAME = "MGT_ValidationStatus"
INSERT_TIME_COLUMN_NAME = "MGT_InsertTime"
UPDATE_TIME_COLUMN_NAME = "MGT_UpdateTime"
IS_ACTIVE_RECORD_COLUMN_NAME = "MGT_isCurrent"
IS_DELETED_RECORD_COLUMN_NAME = "MGT_isDeleted"
VALID_FROM_DATETIME_COLUMN_NAME = "MGT_effFrom"
VALID_UNTIL_DATETIME_COLUMN_NAME = "MGT_effTo"
BUSINESS_KEYS_HASH_COLUMN_NAME = "MGT_bkHash"
VALUE_KEY_HASH_COLUMN_NAME = "MGT_vHash"
```

### environmentConfig notebook <a name = "env_config"></a>
Notebook starts with definitions of following variables.

### Variables <a name = "variables"></a>

```python
WORKFLOWS_CONFIGURATION_FILE = "Workflows_Configuration.json"
METADATA_DQ_CONFIGURATION_FILE = ["DQ_Configuration.json", "DQ_Configuration1.json"]
METADATA_DQ_REFERENCE_VALUES_FILE = "DQ_Reference_Values.json"
METADATA_DQ_REFERENCE_PAIR_VALUES_FILE = "DQ_Reference_Pair_Values.json"
METADATA_CONFIGURATION_FILE_VALIDATION_FILE = "Configuration_File_Validation.json"
````

- **WORKFLOWS_CONFIGURATION_FILE** - It should be defined only if workflow module is used. Name of the variable must be as-is, value can be different if different file name is desired for workflow configuration file.
- **METADATA_DQ_CONFIGURATION_FILE** - It should be defined only if data validation module is used. Name of the variable must be as-is, value can be different if different file name is desired for dq configuration file. If only a single file is used, the value must be a string. If multiple files are used, pass them as a list.
- **METADATA_DQ_REFERENCE_VALUES_FILE** - Optional file used in data validation module. To avoid to define the reference values in dq configuration file, this file can be used to maintain reference values seperately. More details will be explained in further sections. Name of the variable must be as-is, value can be different if different file name is desired. If only a single file is used, the value must be a string. If multiple files are used, pass them as a list.
- **METADATA_DQ_REFERENCE_PAIR_VALUES_FILE** - Optional file used in data validation module. To avoid to define the reference pair values in dq configuration file, this file can be used to maintain reference pair values seperately. More details will be explained in further sections.  Name of the variable must be as-is, value can be different if different file name is desired. If only a single file is used, the value must be a string. If multiple files are used, pass them as a list.
- **METADATA_CONFIGURATION_FILE_VALIDATION_FILE** - This variable must be defined in any case and as-is. File is used to keep the validation expectations that are used to validate json configuration files automatically in configuration generator module. 

After variables definition, next thing to configure is layers.

### Layers <a name = "layers"></a>

Since layers of each use-case can be different from each other, framework also does not force use-case teams to a certain layer structure. Use-case teams needs to decide and align on their layer structure. It should be clear that each layer means to phsically store the data in a location regardless of what transformations happening to the data between layers.

```python
FWK_LAYER_DF = spark.createDataFrame([
        ["Landing", -1, None, None, None, True, True],
        ["Staging", 1, None, None, "0214-064848-zi0j9fjh", True, True],
        ["Dimensional", 2, None, None, "0214-064848-zi0j9fjh", True, True]        
    ], "FwkLayerId STRING, DtOrder INTEGER, PreTransformationNotebookRun STRING, PostTransformationNotebookRun STRING, ClusterId STRING, StopIfFailure BOOLEAN, StopBatchIfFailure BOOLEAN")
````

As can be seen in above example, configuration of the layers is done in a dataframe. Below are the explanation of each field in this dataframe.

- **FwkLayerId** - Represents the name of the layer, can be given any value. It’s recommended to use descriptive value explaining the purpose of the layer.
- **DtOrder** - Represents the order of the layer. Here, expactation of the framework is to give a negative value (-1 in example above) for the layer where the data is being ingested into, and positive numbers for the other layers. Order is used to understand which layer comes next in execution order. If it is desired to execute multiple layers in parallel, value for these layers can be set same.
- **PreTransformationNotebookRun** - This value is optional and represents the path of the notebook which is supposed to be executed before the instructions of corresponding layer. Explained [below](#prepost)
- **PostTransformationNotebookRun** - This value is optional and represents the path of the notebook which is supposed to be executed after the instructions of corresponding layer. Explained [below](#prepost)
- **ClusterId** - Should be left as None for the layer with negative orderId (ingestion) and should be filled for other layers. It is possible to define different clusterIds for each layer execution when it is desired to run the instructions of a layer with a specific databricks cluster. 
- **StopIfFailure** - Mandatory setting, applicable to all layers. This flag is used to configure the behaviour of framework whether to stop the execution of ETL and not continue with next layers in case of a failure in corresponding layer execution. Failures in preTransformationNotebook and postTransformationNotebook runs are also counted as failure of the layer.
- **StopBatchIfFailure** - Mandatory setting, applicable to all layers. This flag is used to configure the behaviour of framework whether to stop the execution of ETL and not continue with next batch in case of a failure in corresponding batch execution. 

#### preTransformationNotebook/postTransformationNotebook <a name = "prepost"></a>

As explained above in Layers section, it is possible to have custom developed notebooks and set them as pre or post step of a certain layer. The purpose of these notebooks could be:

 - define transformation views for Model Transformation layers or Transformation layers
 - stop ETL pipeline
 - trigger workflow
 - perform any custom logic

Following is an example of such a notebook showing the capabilities with different examples;

```python
%run ../../mdmf/includes/init
%run ../../mdmf/etl/workflowManager

dbutils.widgets.text("ADFTriggerName", "", "")
adfTriggerName= dbutils.widgets.get("ADFTriggerName")

if adfTriggerName in ["Sandbox", "Daily_1am"]:
    stopETL = "true"
else:
    stopETL = "false"

workflowManager = WorkflowManager(spark, compartmentConfig)

output = {
    "workflow": workflowManager.getWorkflowDefinition("Post_Historization_Approval"),
    "stopETL": stopETL
}

dbutils.notebook.exit(output)
```

This notebook:

 - Includes the initialization notebook ../../mdmf/includes/init - this is mandatory step
 - Includes definition of the workflow manager ../../mdmf/etl/workflowManager - this is necessary only if a workflow should be triggered
 - Reads the value of ADFTriggerName sent from ADF. It specifies for which trigger id is the PL_MasterPipeline currently running. This is optional step and it’s needed only if it’s necessary to base your logic on the current trigger id.
 - If the adfTriggerName is either "Sandbox" or "Daily_1am" we would like to stop PL_MasterPipeline after execution of this notebook. This is usually needed in combination with triggering a workflow, but not limited to this scenario.
- WorkflowManager is instantiated so getWorkflowDefinition can be called to retrieve workflow definition for current adfTriggerName and specified workflowId - "Post_Historization_Approval". 
  **Note:** workflow manager reads the ADF trigger name internally and it does not rely on the user to provide it. If a specific adfTriggerName should be used, user can pass it as a second argument of getWorkflowDefinition method.
 - Notebook returns to ADF
   - workflow definition (if found, null otherwise)
   - flag whether ETL should be stopped or not

After layers definition, next thing to configure in environmentConfig notebook is the triggers

### Triggers <a name = "triggers"></a>

Framework supports 2 types of triggers, schedule triggers and storage events triggers. Configuration of triggers is also done in a dataframe.

```python
FWK_TRIGGER_DF = spark.createDataFrame([
      ["Sandbox", "Sandbox", None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, True, False],
      [FWK_TRIGGER_ID_DEPLOYMENT, FWK_TRIGGER_ID_DEPLOYMENT, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None], True, False],
      [FWK_TRIGGER_ID_MAINTENANCE, FWK_TRIGGER_ID_MAINTENANCE, "2023-07-15 15:00:00.000", None, "UTC", "Week", "1", "3", "00", "\"Sunday\"", None, None, None, None, None, None, "Stopped", True, False],
      ["MDMF_Execute_PL_TriggerCreation", "MDMF_Execute_PL_TriggerCreation", "2023-07-15 00:15:00.000", None, "UTC", "Day", "1", "0", "00", None, None, None, None, None, None, None, "Stopped", True, False],
      ["Daily_1am", "Daily", "2023-07-15 15:00:00.000", "2023-12-31 00:00:00.000", "UTC", "Day", "1", "0", "00", None, None, None, None, None, None, None, "Started", True, False],
      ["Daily_2am", "Daily", "2023-07-15 15:00:00.000", "2023-12-31 00:00:00.000", "UTC", "Day", "2", "0", "00", None, None, None, None, None, None, None, "Started", True, False],
      ["MarketStorageEvent", "MarketStorageEvent", None, None, None, None, None, None, None, None, None, "/subscriptions/775f9e05-bbee-446a-aa43-3d2296410d19/resourceGroups/0001-d-asmefdevxx", "dlzasmefdevxx", "asmefdevxx", "eventFolder/eventFile", ".csv", "Started", True, False],
  ], "ADFTriggerName STRING, FwkTriggerId STRING, StartTime STRING, EndTime STRING, TimeZone STRING, Frequency STRING, Interval STRING, Hours STRING, Minutes STRING, WeekDays STRING, MonthDays STRING, StorageAccountResourceGroup STRING, StorageAccount STRING, Container STRING, PathBeginsWith STRING, PathEndsWith STRING, RuntimeState STRING, FwkCancelIfAlreadyRunning BOOLEAN, FwkRunInParallelWithOthers BOOLEAN")
````

Below are the explanation of each field in this dataframe.

 - **ADFTriggerName** - the name of a trigger created in ADF. Values in this column has to be unique and it’s recommended to use descriptive value explaining the purpose and/or scheduling of the trigger. Multiple ADF triggers can have the same value of FwkTriggerId. This basically means that a user wants to run the same instructions on different times.
 - **FwkTriggerId** - an internal identifier of the trigger and it’s only purpose is to create a link between scheduling (ADFTriggerName) and instructions to be executed (instructions use FwkTriggerId). There’s on 1-to-many relation between FwkTriggerId and ADFTriggerName.
 - **StartTime** - validity of the trigger. The trigger does not start before defined “StartTime”.
 - **EndTime** -  validity of the trigger.  The trigger will not start after defined “EndTime“.
 - **Timezone** - specifies time zone of the trigger (values: UTC, TC, ETC. and so on.)
 - **Frequency** - specifies frequency of main pipeline run (Minute, Day, Week, Month, Hour)
 - **Interval** - specifies a value of the frequency column (values: numbers)
 - **Hours** - specifies at which hour the pipeline will be triggered (values: numbers)
 - **Minutes** - specifies at which minute the pipeline will be triggered (values: numbers)
 - **Weekdays** - specifies on which weekdays the pipeline will be triggered (values: numbers)
 - **MonthDays** - specifies on which month days the pipeline will be triggered (values: numbers)
 - **StorageAccountResourceGroup** - specifies resources group for storage account, mandatory field for event file triggers
 - **StorageAccount** - specifies where event file is stored, mandatory field for event file triggers
 - **Container** - specifies where event file is stored, mandatory field for event file triggers
 - **PathBeginsWith** - specifies the path and file name, mandatory for event file triggers. 
 - **PathEndsWith** - specifies the end file name, mandatory for event file triggers
 - **RuntimeState** - specifies whether the trigger should be active or not (values: ‘Started’, ‘Stopped’ or None). If RuntimeState is None it means it is technical trigger.
- **FwkCancelIfAlreadyRunning** - specifies whether the pipeline should be triggered if another pipeline with same ADFTriggerName is already being executed. When  set to true , if another pipeline is already running with same ADFTriggerName then the run is cancelled .When it is set to False the new trigger will wait for the previous pipeline to finish execution before starting .  
- **FwkRunInParallelWithOthers** - specifies whether the pipeline should be triggered concurrently. When set to True  multiple pipelines with different ADFTriggerName could be triggered simultaneously . When set to False the new trigger runs only after the previous pipelines execution completes. 
**How does it work?**

When there is a change in this trigger dataframe, and change commit is PRed, config generator module is being triggered by deployment action automatically (PL_GenerateConfigurationPipeline is executed). 
This pipeline populates the data in FwkTrigger table of configuration db (sql) with new content, also checks the changes and determines which entries have changed and for the changed trigger entries, the column "ToBeProcessed" in FwkTrigger is set to "true". 
In order to apply the changes to actual triggers in ADF, there is one additional step, to execute "PL_TriggerCreation" pipeline. This pipeline takes the "ToBeProcessed" true entries from configuration db and applies the changes to ADF triggers.
If there is an any environmental error related with capacity of rest API of ADF, "PL_TriggerCreation" can be triggered again which will continue to apply the remaining 
changes left.

Next thing to configure in environmentConfig notebooks is linkedservices.

### Linked Services <a name = "linked_services"></a>

Linked services are the connection definitions, which are the source systems to ingest data from, storage accounts to store data in compartment and systems in case of exporting data.
Linked services are also defined in a dataframe, like in the example below.

```python
FWK_LINKED_SERVICE_DF = spark.createDataFrame([
    ["SourceSQL", "SQL", None, None, None, "sqldb-client-connectionString", None],
    ["SourceOracle", "Oracle", None, None, None, "oracle-client-connectionString", None],
    ["SourceSnowflake", "Snowflake", "odbc-connection-string", None, "HASHANM", "odbc-password", None],
    ["SourceOData", "OData", "https://services.odata.org/V4/Northwind/Northwind.svc", None, "HASHANM", "odbc-password", None],
    ["SourceFileShare", "FileShare", "\\\\abcdef000\folder", None, "domain\username", "keyvaultsecretforpassword", None],
    ["LandingADLS", "ADLS", "https://dlzasabctestx.dfs.core.windows.net/", None, None, None, None],
    ["StagingADLS", "ADLS", "https://dlzasabctestx.dfs.core.windows.net/", None, None, None, None],
    ["CompartmentADLS", "ADLS", "https://dlzasabctestx.dfs.core.windows.net/", None, None, None, None],
    ["MetadataADLS", "ADLS", "https://dstgcpbit.dfs.core.windows.net/", None, None, None, None],
    ["ExportSFTP", "SFTP", "10.10.10.1", None, "username", "keyvaultsecretforpassword2", None],
], "FwkLinkedServiceId STRING, SourceType STRING, InstanceURL STRING, Port STRING, UserName STRING, SecretName STRING, IPAddress STRING")
````

Below are the explanation of each field in this dataframe.

- **FwkLinkedServiceId** - Unique name of the linked service. Name can be anything, suggestion is to use descriptive names like name or kind of the system.
- **SourceType** - Must be set exactly as one of the values below
   - SQL
   - SQLOnPrem
   - Oracle
   - Snowflake
   - OData
   - FileShare
   - SFTP
   - ADLS
- **InstanceURL** - Must be given if SourceType is FileShare, SFTP, ADLS, Snowflake or OData. For others, it must be left as None. 

    As given in example above, it should provide the path of FileShare, IP of SFTP, URL of ADLS or Service URL of OData. For Snowflake, connection string should be saved in Azure KeyVault and secretname should be given here.
- **Port** - Can be given if needed when SourceType is SFTP, for others it must be left as None.
- **UserName** - Must be given if SourceType is FileShare, SFTP, Snowflake or OData. For others, it must be left as None.
- **SecretName** - Must be given for all SourceTypes except ADLS. 
    For SQL and Oracle, connection string should be saved in Azure KeyVault and secretname should be given here. 
    For FileShare, SFTP, Snowflake and OData, password of user to authenticate should be saved in Azure KeyVault and secretname should be given here. 
- **IPAddress** - specifies IP Address if it’s needed for the connection.

### Metadata Config <a name = "metadata_config"></a>

This section is in a JSON structure. The purpose of this object is to define types of operation and to bind together all high level information about the operation (ingestion / transformation / export)

This Json contains 5 main sections described in the next chapters.

```python
FWK_METADATA_CONFIG = {
    "ingestion": [],
    "transformations": [],
    "modelTransformations": [],
    "export": [], 
    "metadata": {}
}
````

**Ingestion** <a name = "ingestion_section"></a>

This part of metadata config determines the ingestion part, what is the source, sink and required inputs.
An example is as below;

```json
"ingestion": [
    {
        "FwkLayerId": "Landing",
        "FwkTriggerId": "Sandbox",
        "source": {
            "FwkLinkedServiceId": "SourceSQL"
        },
        "sink": {
            "FwkLinkedServiceId": "LandingADLS",
            "containerName": "asbittestx",
            "dataPath": "/SQL",
            "databaseName": "LandingSQL"
        },
        "metadata": {
            "ingestionFile": "Ingestion_Configuration.json", 
            "ingestionTable": "IngestionConfiguration"
        }
    }
]
````

The values of FwkLayerId, FwkTriggerId, FwkLinkedServiceId should be picked from previously defined framework variables (FWK_LAYER_DF, FWK_LINKED_SERVICE_DF, FWK_TRIGGER_DF) 

The depicted example defines that ingestion will be part of Landing Layer with Sandbox triggerId. Data will be ingested from SourceSQL linked service into LandingADLS linked service to the container asbittestx, and data path will be /SQL(/EntityName).

**Key Points :**
 - databaseName defines name of the database in which stored data will be registered as tables in Unity Catalog. This applies only in cases when data are physically stored as delta tables. In ingestion, data is stored as parquet files, so these data are not registered in Unity Catalog, but still databaseName should be given, this is used as an identifier for entity definition.
 - For ingestion, it’s mandatory to define metadata section which references Ingestion Configuration file, this file will be explained in next chapters. 
 - It is also possible to define multiple sections under ingestion part. This might be needed when there are more than one data sources to ingest data from. In this scenario, it is also possible to define different ingestionFiles to make it easier to maintain.
 - There is also two optional definitions “logFwkLinkedServiceId“ and “logsPath” that can be defined under sink object to enable logging for fault tolerance. This is applicable for ingesting file sources. Additionally, fault tolerance should be enabled in ingestion configuration file per entity. More about this can be found [here](#fault_tolerance)

**Transformations** <a name = "transformation_section"></a> 

This part of metadata config determines the transformation part, what is the source, sink and required inputs.
An example is as below;

```json
"transformations": [
    {
        "FwkLayerId": "Staging",
        "FwkTriggerId": "Sandbox",
        "sink": {
            "FwkLinkedServiceId": "StagingADLS",
            "containerName": "dldata",
            "dataPath": "/SQL",
            "databaseName": "StagingSQL",
            "writeMode": "Overwrite"
        },
        "metadata": {
            "transformationFile": "Transformation_Configuration.json",
            "transformationTable": "TransformationConfiguration",
            "key": "StagingTransformations",
            "copyRest": True
        }       
    }
]
````

Similarly, the values of FwkLayerId, FwkTriggerId, FwkLinkedServiceId should be picked from previously defined framework variables (FWK_LAYER_DF, FWK_LINKED_SERVICE_DF, FWK_TRIGGER_DF) 

The depicted example defines that the entities defined in Transformation_Configuration file will first be transformed based on the configuration in same file (this will be explained in detail later), then will be stored into 'StagingADLS' linked service to the container 'dldata', and data path will be '/SQL(/EntityName)'. Each entity (table) will be registered in Unity Catalog under database 'StagingSQL' with write mode 'Overwrite'

**Key Points**
- Similary, transformation section can also be multiple. One reason for this is having more than one layer for transformations. Another reason might be to logically devide transformations of a layer. If same transformationFile is used for different sections, 'key' field can be used as an identifier to understand which part of Transformation Configuration file belongs to which transformation section in this notebook.
- Transformations and writeMode are different things that are to be combined. Transformations are configured in Transformation_Configuration file, writeMode explains how the data will be finally stored after transformations are applied.
- Possible writeModes are documented [here](#writemodes)

**Model Transformations** <a name = "model_transformation_section"></a>

This part of metadata config defines the data model transformations.
An example is as below;

```json
"modelTransformations": [
    {
        "FwkLayerId": "Dimensional",
        "FwkTriggerId": "Sandbox",
        "source": None,
        "sink": {
            "FwkLinkedServiceId": "CompartmentADLS",
            "containerName": "dimensionaldata",
            "dataPath": "/dimensional",
            "databaseName": "Dimensional",
            "writeMode": "Overwrite" # optional; if not set, SCDType1 will be used
        },
        "metadata": {
            "attributesFile": "ModelAttributes.json",
            "attributesTable": "ModelAttributes",
            "keysFile": "ModelKeys.json",
            "keysTable": "ModelKeys",
            "relationsFile": "ModelRelations.json",
            "relationsTable": "ModelRelations",
            "entitiesFile": "ModelEntities.json",
            "entitiesTable": "ModelEntities",
            "transformationFile": "Model_Transformation_Configuration.json", # optional
            "transformationTable": "ModelTransformationConfiguration", # optional
            "key": "Dimensional", # optional
            "unknownMemberDefaultValue": -1 # optional
        }
    }
]
````

Similarly, the values of FwkLayerId, FwkTriggerId, FwkLinkedServiceId should be picked from previously defined framework variables (FWK_LAYER_DF, FWK_LINKED_SERVICE_DF, FWK_TRIGGER_DF) 

The depicted example defines that the model transformation will be part of 'Dimensional' Layer with 'Sandbox' triggerId. Data will be stored into 'CompartmentADLS' linked service to the container 'dimensionaldata', and data path will be '/dimensional(/EntityName)'. Each entity (table) will be registered in HIVE metastore under database SDM.

What is specific to model transformation is its approach. Here, data model is provided with a set of files and intention is to transform the data to a defined data model.

Properties; 'attributesFile', 'keyFile', 'relationsFile' and 'entitiesFile' define the name of the model files.
Properties; 'attributesTable', 'keyTable', 'relationsTable' and 'entitiesTable' define the name of the tables where these files content will be stored in Metadata schema.

If it’s necessary to override default behavior of model transformations for some tables of the data model (e.g. write mode or trigger id, or batch number), one can define Model Transformation Configuration file. The model transformation file is optional and lists only tables that requires changes. 
One model transformation configuration file can be used in multiple sections (objects) as transformation configuration file, therefore it’s necessary to set 'key' property that will identify which records from the model transformation file are related to particular model transformation object. 

In data models, framework populates the surrogate keys by constructing the sql query with joins between model tables that have relations to each other. As a result of these joins, for non matching records between tables, framework by default does not set a default value for the surrogate key and it remains null. If this is not desired behavior, specify 'unknownMemberDefaultValue' which defines a default value for non matching records (in the example above, -1 will be used instead of null).

**Export** <a name = "export_section"></a> 

This part of metadata config defines the details about export. If export is not neede or part of the use-case, this section should not exist.

An example is below for three different exports (to ADLS, to SFTP, to SQL db);

```json
"export": [
    {
        "FwkLayerId": "Export",
	  	    "FwkTriggerId": "Sandbox",
        "sink": {
            "FwkLinkedServiceId": "CompartmentADLS",
            "containerName": "export",
            "dataPath": "/ExportADLS"
        },
        "metadata": {
            "exportFile": "ADLSExport_Configuration.json",
            "exportTable": "ADLSExport_Configuration"
        }
    },
    {
        "FwkLayerId": "Export",
        "FwkTriggerId": "Sandbox",
        "sink": {
            "FwkLinkedServiceId": "ExportSFTP",
            "dataPath": "/foldername",
            "tempSink": {
                "FwkLinkedServiceId": "CompartmentADLS",
                "containerName": "export",
                "dataPath": "/TempExportSFTP"
            }
        },
        "metadata": {
            "exportFile": "SFTPExport_Configuration.json",
            "exportTable": "SFTPExport_Configuration"
        }
    },
    {
        "FwkLayerId": "Export",
        "FwkTriggerId": "Sandbox",
        "sink": {
            "FwkLinkedServiceId": "SourceSQL",
            "tempSink": {
                "FwkLinkedServiceId": "CompartmentADLS",
                "containerName": "export",
                "dataPath": "/TempExportSQL"
                }
     },
        "metadata": {
            "exportFile": "SQLExport_Configuration.json",
            "exportTable": "SQLExport_Configuration"
        }
     }
]
````

This structure is similar to model transformations. It again defines the layer, trigger and sink linked service. The values of FwkLayerId, FwkTriggerId, FwkLinkedServiceId are picked from previously defined framework variables (FWK_LAYER_DF, FWK_TRIGGER_DF,FWK_LINKED_SERVICE_DF).

Source is not defined, because data that will be exported are defined as SQL queries or existing CSV files in export configuration files. For export, it’s mandatory to define metadata section which references export configuration files.

If data are exported to ADLS linked service, sink property has the standard structure (except of property missing databaseName). If the data are exported to SFTP or FileShare or SQL, export is done in two steps: data are first stored as CSV files on temporary ADLS location and then exported via ADF. Therefore, it’s necessary to define tempSink object under sink object.

**Metadata**

This section defines which linked service and which of its containers will be used for storing metadata of the framework. 
Metadata storage is used for storing configuration files (Json files) as delta tables, for storing DQ logs, files that hold generated instructions data before they refresh sql database, etc.

```json
"metadata": {
    "FwkLinkedServiceId": "MetadataADLS",
    "containerName": "metadata"
}
````

With this section, the EnvironmentConfig notebook is complete and the high level configuration is done. 
Next step will be to prepare the Json configuration files which will configure steps in more detail and entity based for each module.

## Json Configuration Files <a name = "json_config"></a>

These files should be stored under resources folder in git repository. For every use-case, depending on their own requirements and designs, there might be different number of json files needed. We start with Ingestion_Configuration file.

### Ingestion Configuration File <a name = "ingestion_config"></a>

Ingestion Configuration File is basically needed if use-case is supposed to ingest data from a system. It is also possible to have multiple ingestion configuration files if it is desired to logically seperate the configuration, for ex, per source system.

Name of the ingestion configuration file is usually set as 'Ingestion_Configuration.json', but also can be changed, in this case, it should match with the defined name in environmentConfig notebook's [ingestion section](#ingestion_section).

The schema of the expected json file is as below;

```
FwkLinkedServiceId STRING,
Entities ARRAY<
    STRUCT<
        Entity STRING,
        Path STRING,
        Params STRUCT<
            sinkEntityName STRING,
            fwkTriggerId STRING,
            archivePath STRING
        >,
        MetadataParams STRUCT<
            primaryColumns STRING,
            wmkDataType STRING
        >,
        SourceParams STRING,
        TypeLoad STRING,
        WmkColumnName STRING,
        SelectedColumnNames STRING,
        TableHint STRING,
        QueryHint STRING,
        Query STRING,
        IsMandatory BOOLEAN
````

As a general rule, framework does not expect you to send the schema fully for every record in an array object. For the values are optional or not needed for certain data source types and that you don't need to set a value, you don't need to add them to the corresponding record.

For this reason, we will see for each data source type which fields are necessary/optional and key points to consider.

#### Ingestion Configuration for SQL Server sources<a name = "ingestion_config_sql"></a>

The fields that are applicable to SQL Server sources are as follows;

```
FwkLinkedServiceId STRING,
Entities ARRAY<
    STRUCT<
        Entity STRING,
        Params STRUCT<
            sinkEntityName STRING,
            fwkTriggerId STRING            
        >,
        MetadataParams STRUCT<
            primaryColumns STRING,
            wmkDataType STRING
        >,
        TypeLoad STRING,
        WmkColumnName STRING,
        SelectedColumnNames STRING,
        TableHint STRING,
        QueryHint STRING,
        Query STRING
````

Explanation for each field if as follows;

| Property  | Detailed information
| ------------- | ------------- |
| FwkLinkedServiceId | It is the FwkLinkedServiceId that was defined in the EnvironmentConfig notebook. |
| Entity | If object is a table/view, schemaname.tablename/viewname; when object is a query, the value given here will be the name of the sink object.|
| Params/sinkEntityName | Optional parameter that specifies sink entity name. If not provided sink entity name will be same as source entity name. |
| Params/fwkTriggerId | Optional parameter to overwrite the fwkTriggerId defined in environmentConfig [ingestion section](#ingestion_section) for the entity. |
| MetadataParams/primaryColumns | Optional parameter to define the primary columns of source object when it is a query or a view. For table sources, primary columns are automatically read from source system in config generator module. Primary columns information is needed when ingested data will be historized in further layers. 
| MetadataParams/wmkDataType | For incremental loads from a query, data type of watermark column is needed. For views or tables, it is not needed. Supported values: datetime, stringDatetime, numeric |
| TypeLoad | Option to ingest data full or incremental. Supported values: Full, Delta. |
| WmkColumnName | For incremental load (TypeLoad:Delta), it’s mandatory to provide this column which will be used to capture changed data in each execution. |
| SelectedColumnNames | Optional parameter to define list of columns to not to ingest all columns. For incremental load (TypeLoad:Delta), WmkColumnName should be in the list. Another option is to define as #primaryColumns which means to ingest only primary columns of source object.
| TableHint | Optional parameter to define table hint. Applicable when source is not a query. For possible table hints in sql server, check [this page](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver16). If a table hint is desired to be used with Query, query should be written including the hint. |
| QueryHint | Optional parameter to define query hint. For possible query hints in sql server, check [this page](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query?view=sql-server-ver16) If query hint has to be used with Query, hint has to be specified in the Query. |
| Query | Optional parameter to define a SQL query as a source instead of tables/views. |

Example configuration;

```json
{
    "FwkLinkedServiceId": "SourceSQL",
    "Entities": [
        {
            "Entity": "SalesLT.Address",
            "TypeLoad": "Delta",
            "WmkColumnName": "ModifiedDate",
            "TableHint": "WITH (NOLOCK)",
            "QueryHint": "OPTION (FORCE ORDER)"
        },
        {
            "Entity": "SalesLT.CustomerAddress",
            "TypeLoad": "Delta",
            "WmkColumnName": "ModifiedDate",
            "SelectedColumnNames": "CustomerID, AddressID, AddressType, ModifiedDate"
        },
        {
            "Entity": "SalesLT.Customer",
            "Params": {
                "sinkEntityName": "Customer_Full",
                "fwkTriggerId": "Weekly"
            },
            "TypeLoad": "Full",
            "SelectedColumnNames": "#primaryColumns"
        },
        {
            "Entity": "SalesLT.CustomerJoinAddress",
            "MetadataParams": {
                "primaryColumns": "AddressID, CustomerID",
                "wmkDataType": "datetime"
            },
            "TypeLoad": "Delta",
            "WmkColumnName": "ModifiedDate",
            "Query": "SELECT CU.CustomerID, CA.AddressID, CA.AddressType, CU.FirstName, CU.ModifiedDate FROM SalesLT.Customer CU JOIN SalesLT.CustomerAddress CA ON CU.CustomerID = CA.CustomerID WHERE CU.Title IS NOT NULL AND CU.MiddleName LIKE 'E.%' and CU.NameStyle = 1"
        }
    ]
}
````

#### Ingestion Configuration for Oracle sources<a name = "ingestion_config_oracle"></a>

The fields that are applicable to Oracle sources are as follows;

```
FwkLinkedServiceId STRING,
Entities ARRAY<
    STRUCT<
        Entity STRING,
        Params STRUCT<
            sinkEntityName STRING,
            fwkTriggerId STRING            
        >,
        MetadataParams STRUCT<
            primaryColumns STRING,
            wmkDataType STRING
        >,
        TypeLoad STRING,
        WmkColumnName STRING,
        SelectedColumnNames STRING,
        QueryHint STRING,
        Query STRING
````

Explanation for each field if as follows;

| Property  | Detailed information
| ------------- | ------------- |
| FwkLinkedServiceId | It is the FwkLinkedServiceId that was defined in the EnvironmentConfig notebook. |
| Entity | If object is a table/view, schemaname.tablename/viewname; when object is a query, the value given here will be the name of the sink object.|
| Params/sinkEntityName | Optional parameter that specifies sink entity name. If not provided sink entity name will be same as source entity name. |
| Params/fwkTriggerId | Optional parameter to overwrite the fwkTriggerId defined in environmentConfig [ingestion section](#ingestion_section) for the entity. |
| MetadataParams/primaryColumns | Optional parameter to define the primary columns of source object when it is a query or a view. For table sources, primary columns are automatically read from source system in config generator module. Primary columns information is needed when ingested data will be historized in further layers. 
| MetadataParams/wmkDataType | For incremental loads from a query, data type of watermark column is needed. For views or tables, it is not needed. Supported values: datetime, stringDatetime, numeric |
| TypeLoad | Option to ingest data full or incremental. Supported values: Full, Delta. |
| WmkColumnName | For incremental load (TypeLoad:Delta), it’s mandatory to provide this column which will be used to capture changed data in each execution. |
| SelectedColumnNames | Optional parameter to define list of columns to not to ingest all columns. For incremental load (TypeLoad:Delta), WmkColumnName should be in the list. Another option is to define as #primaryColumns which means to ingest only primary columns of source object.
| QueryHint | Optional parameter to define query hint. For possible query hints in oracle, check [this page](https://docs.oracle.com/cd/E15586_01/server.1111/e16638/hintsref.htm) If query hint has to be used with Query, hint has to be specified in the Query. |
| Query | Optional parameter to define a SQL query as a source instead of tables/views. |

Example configuration;

```json
{
    "FwkLinkedServiceId": "SourceOracle",
    "Entities": [
        {
            "Entity": "SALESORA.DIMCUSTOMER",
            "MetadataParams": {
                "primaryColumns": "CUSTOMERKEY"
            },
            "TypeLoad": "Full"
        },
        {
            "Entity": "SALESORA.DIMCURRENCY",
            "TypeLoad": "Delta",
            "WmkColumnName": "MODIFIEDDATE",
            "SelectedColumnNames": "CURRENCYID, CURRENCY, CURRENCYDATE, VALUE, MODIFIEDDATE"
        },
        {
            "Entity": "SALESORA.DIMCUSTOMERMSSTATUS",
            "MetadataParams": {
                "primaryColumns": "CUSTOMERKEY",
                "wmkDataType": "stringDatetime"
            },
            "TypeLoad": "Delta",
            "WmkColumnName": "BSTR",
            "Query": "SELECT CUSTOMERKEY, FIRSTNAME, LASTNAME, EMAILADDRESS, BSTR, BIRTHDATE FROM SALESORA.DIMCUSTOMER2 WHERE MARITALSTATUS = 'M' AND BIRTHDATE > TO_DATE('2000-01-01','YYYY-MM-DD')"
        }
    ]
}
````

#### Ingestion Configuration for Snowflake sources<a name = "ingestion_config_snowflake"></a>

The fields that are applicable to Snowflake sources are as follows;

```
FwkLinkedServiceId STRING,
Entities ARRAY<
    STRUCT<
        Entity STRING,
        Params STRUCT<
            sinkEntityName STRING,
            fwkTriggerId STRING            
        >,
        MetadataParams STRUCT<
            primaryColumns STRING,
            wmkDataType STRING
        >,
        TypeLoad STRING,
        WmkColumnName STRING,
        SelectedColumnNames STRING,
        TableHint STRING,
        Query STRING
````

Explanation for each field if as follows;

| Property  | Detailed information
| ------------- | ------------- |
| FwkLinkedServiceId | It is the FwkLinkedServiceId that was defined in the EnvironmentConfig notebook. |
| Entity | If object is a table/view, schemaname.tablename/viewname; when object is a query, the value given here will be the name of the sink object.|
| Params/sinkEntityName | Optional parameter that specifies sink entity name. If not provided sink entity name will be same as source entity name. |
| Params/fwkTriggerId | Optional parameter to overwrite the fwkTriggerId defined in environmentConfig [ingestion section](#ingestion_section) for the entity. |
| MetadataParams/primaryColumns | If snapshot is used as the writeMode or if ingested data will be historized in further layers, this property is mandatory. |
| MetadataParams/wmkDataType | In case of incremental loads, this property is mandatory. Supported values: datetime, stringDatetime, numeric. |
| TypeLoad | Option to ingest data full or incremental. Supported values: Full, Delta. |
| WmkColumnName | For incremental load (TypeLoad:Delta), it’s mandatory to provide this column which will be used to capture changed data in each execution. |
| SelectedColumnNames | Optional parameter to define list of columns to not to ingest all columns. For incremental load (TypeLoad:Delta), WmkColumnName should be in the list. Another option is to define as #primaryColumns which means to ingest only primary columns of source object.
| TableHint | Optional parameter to define hints. If hint must be used with Query, hint must be specified in the Query. |
| Query | Optional parameter to define a SQL query as a source instead of tables/views. |

Example configuration;

```json
{
    "FwkLinkedServiceId": "SourceSnowflake",
    "Entities": [
        {
            "Entity": "TPCDS_SF100TCL.CALL_CENTER",
            "Params": {
                "sinkEntityName": "callcenter"
            },
            "MetadataParams": {
                "primaryColumns": "CC_CALL_CENTER_SK",
                "wmkDataType": "datetime"
            },
            "TypeLoad": "Delta",
            "WmkColumnName": "MODIFIED_DATE",
            "Query": "SELECT *, GETDATE() AS MODIFIED_DATE FROM TPCDS_SF100TCL.CALL_CENTER"
        },
        {
            "Entity": "TPCDS_SF100TCL.CATALOG_PAGE",
            "Params": {
                "sinkEntityName": "catalogpage"
            },
            "MetadataParams": {
                "primaryColumns": "CP_CATALOG_PAGE_SK"
            },
            "TypeLoad": "Full",
            "SelectedColumnNames": "CP_CATALOG_PAGE_SK, CP_DEPARTMENT, CP_DESCRIPTION, CP_CATALOG_PAGE_NUMBER",
            "TableHint": "/*+ COALESCE(3) */"
        }
    ]
}
````

#### Ingestion Configuration for OData sources<a name = "ingestion_config_odata"></a>

The fields that are applicable to OData sources are as follows;

```
FwkLinkedServiceId STRING,
Entities ARRAY<
    STRUCT<
        Entity STRING,
        Params STRUCT<
            sinkEntityName STRING,
            fwkTriggerId STRING            
        >,
        MetadataParams STRUCT<
            primaryColumns STRING,
            wmkDataType STRING
        >,
        TypeLoad STRING,
        WmkColumnName STRING,
        SelectedColumnNames STRING,
        Query STRING
````

Explanation for each field if as follows;

| Property  | Detailed information
| ------------- | ------------- |
| FwkLinkedServiceId | It is the FwkLinkedServiceId that was defined in the EnvironmentConfig notebook. |
| Entity | The path to the OData resource. |
| Params/sinkEntityName | Optional parameter that specifies sink entity name. If not provided sink entity name will be same as source entity name. |
| Params/fwkTriggerId | Optional parameter to overwrite the fwkTriggerId defined in environmentConfig [ingestion section](#ingestion_section) for the entity. |
| MetadataParams/primaryColumns | If snapshot is used as the writeMode or if ingested data will be historized in further layers, this property is mandatory. |
| MetadataParams/wmkDataType | In case of incremental loads, this property is mandatory. Supported values: datetime. |
| TypeLoad | Option to ingest data full or incremental. Supported values: Full, Delta. |
| WmkColumnName | For incremental load (TypeLoad:Delta), it’s mandatory to provide this column which will be used to capture changed data in each execution. |
| SelectedColumnNames | Optional parameter to define list of columns to not to ingest all columns. For incremental load (TypeLoad:Delta), WmkColumnName should be in the list. Another option is to define as #primaryColumns which means to ingest only primary columns of source object. |
| Query | Optional parameter to define a OData query. |

Example configuration;

```json
{
    "FwkLinkedServiceId": "SourceOData",
    "Entities": [
        {
            "Entity": "Employees",
            "MetadataParams": {
                "primaryColumns": "EmployeeID"
            },
            "TypeLoad": "Full",
            "SelectedColumnNames": "EmployeeID, LastName, FirstName, Title, BirthDate, HireDate, City, Country"
        },
        {
            "Entity": "Products",
            "MetadataParams": {
                "primaryColumns": "ProductID"
            },
            "TypeLoad": "Full",
            "Query": "$select=ProductID,ProductName,UnitPrice,UnitsInStock,UnitsOnOrder,ReorderLevel,Discontinued & $filter=UnitsOnOrder gt 0"
        },
        {
            "Entity": "Orders",
            "MetadataParams": {
                "primaryColumns": "OrderID",
                "wmkDataType": "datetime"
            },
            "TypeLoad": "Delta",
            "WmkColumnName": "OrderDate",
            "Query": "$filter=EmployeeID gt 5"
        }
    ]
}
````

#### Ingestion Configuration File for Fileshare, ADLS and SFTP sources <a name = "ingestion_config_files"></a>

The fields that are applicable to FileShare, ADLS and SFTP sources are as follows;

```
FwkLinkedServiceId STRING,
Entities ARRAY<
    STRUCT<
        Entity STRING,
        Path STRING,
        Params STRUCT<
            sinkEntityName STRING,
            fwkTriggerId STRING,
            archivePath STRING,
	    archiveLinkedServiceId STRING
        >,
        MetadataParams STRUCT<
            primaryColumns STRING
        >,
        SourceParams STRING,
        TypeLoad STRING,
        IsMandatory BOOLEAN
````

Explanation for each field if as follows;

| Property  | Detailed information
| ------------- | ------------- |
| FwkLinkedServiceId | It is the FwkLinkedServiceId that was defined in the EnvironmentConfig notebook. |
| Entity | For file source from ADLS, SFTP or FileShare, the entity name is completely arbitrary, it just must be unique. and the name will be the sink parquet file that ingestion will happen.|
| Path | For file source from ADLS, SFTP or FileShare, the path must contain path to the file and file name with extension. It is allowed to use wildcard character * in file name. If more than 1 file have met wildcard criteria, only the latest file will be ingested according “Date Modified” in properties of the file. |
| Params/sinkEntityName | Optional parameter that specifies sink entity name. If not provided sink entity name will be same as source entity name. |
| Params/fwkTriggerId | Optional parameter to overwrite the fwkTriggerId defined in environmentConfig [ingestion section](#ingestion_section) for the entity. |
| Params/archivePath | Optional parameter capable to archive source file to specified archive folder. Archiving happens after successful ingestion, original file is moved to specified archive path with original file name and timestamp prefix. |
| Params/archiveLinkedServiceId | Optional parameter capable to archive source file to specified archive folder. Archiving happens after successful ingestion, original file is moved to specified archive path with original file name and timestamp prefix. If archiveLinkedServiceId is defined then archive Path must be defined. This is currently enabled only to archive files with froma ADLS storage location to SFTP location. If no archiveLinkedServiceId is specified with the path then files will be loaded into the same location as the source file in the specified folder. |
| MetadataParams/primaryColumns | Optional parameter to define the primary columns of source object when it is a query or a view. For table sources, primary columns are automatically read from source system in config generator module. Primary columns information is needed when ingested data will be historized in further layers. 
| SourceParams | This field is mandatory to be able to ingest different file types |
| TypeLoad | Must be set to "Full" for file sources. |
| IsMandatory | Optional parameter to define if an entity(file) is mandatory to start ingestion or not, if any of the files that is set to true does not exist, ingestion will not start and a notification about missing mandatory files is sent to use-case's mattermost channel |

**Examples for ADLS, FileShare and SFTP**

```json
{
        "FwkLinkedServiceId": "SourceFileShare",
        "Entities": [
            {
                "Entity": "CustomerCSV",
                "Path": "CustomerCsv.csv",
                "SourceParams": {
                    "columnDelimiter": "#semicolon",
                    "rowDelimiter": "\r\n",
                    "compressionType": "None",
                    "compressionLevel": "",
                    "encodingName": "UTF-8",
                    "escapeChar": "\\",
                    "firstRowAsHeader": true,
                    "nullValue": "",
                    "quoteChar": "\"",
                    "skipLineCount": 0
                },
                "TypeLoad": "Full"
            }
        ]
},
{
    "FwkLinkedServiceId": "ExportSFTP",
    "Entities": [
        {
            "Entity": "MP13ExcelFirst",
            "Path": "/from_erdr/MP13Excel.xlsx",
            "SourceParams": {
                "sheetName": "FirstSheet",
                "range": "",
                "firstRowAsHeader": true,
                "nullValue": ""
            },
            "TypeLoad": "Full"
        },
        {
            "Entity": "MP13ExcelSecond",
            "Path": "/from_erdr/MP13Excel.xlsx",
            "SourceParams": {
                "sheetName": "SecondSheet",
                "range": "B5:D10",
                "firstRowAsHeader": true,
                "nullValue": ""
            },
            "TypeLoad": "Full"
        },
        {
            "Entity": "ContractCSV",
            "Path": "/from_erdr/ContractMBM_*.csv",
            "SourceParams": {
                "columnDelimiter": "#semicolon",
                "rowDelimiter": "\r\n",
                "compressionType": "None",
                "compressionLevel": "",
                "encodingName": "UTF-8",
                "escapeChar": "\\",
                "firstRowAsHeader": true,
                "nullValue": "",
                "quoteChar": "\"",
                "skipLineCount": 0
            },
            "TypeLoad": "Full"
        }
    ]
}
```

For SourceParams, there are also optional sections you can define additionally. These are; 

<details>
<summary>ingesting all files matching the wildcard</summary>

Defining the 'ingestAllMatchingFiles' parameter is optional. This parameter is supported for FileShare, ADLS, and SFTP files: CSV and EXCEL.
 - if the value of this parameter is 'true', then all the files matching wildcard path is considered for the ingestion.

**Example for ingestion of CSV file with wildcard path:**

```json
{
    "Entity": "ContractsCSV",
    "Path": "/from_erdr/Contracts*MBM.csv",
    "SourceParams": {
        "ingestAllMatchingFiles": true,
        "columnDelimiter": ";",
        ...
    }
}
```

**Note:**
 - By default the parameter value is 'false'. Hence, the latest file is considered for the ingestion from the list of files matching the wildcard.

</details>
 
<details>
<summary>mapping definitions in SourceParams</summary>

Defining mapping is supported for all input data files: CSV / ZIP / EXCEL / TXT / CMA / XML. With these mapping it is possible to control :
 - change the name of the column on sink
 - change the data type
 - ingest files without headers in first row (only for CSV / EXCEL / TXT / CMA)
 - skip unnecessary columns

**Example for ingestion of CSV file without header, creating the column names and data types :**

```json
{ ...
"firstRowAsHeader": false,
"mapping": {
    "type":"TabularTranslator",
    "typeConversion": true,
    "typeConversionSettings":{"treatBooleanAsNumber":true},
    "mappings":[
        {"source":{"ordinal":1,"type":"Int32"},"sink":{"name":"OrderID"}},
        {"source":{"ordinal":2,"type":"DateTime"},"sink":{"name":"OrderDate"}},
        {"source":{"ordinal":3,"type":"String"},"sink":{"name":"SourceSystem"}},
        {"source":{"ordinal":4,"type":"String"},"sink":{"name":"Description"}},
        {"source":{"ordinal":5,"type":"Boolean"},"sink":{"name":"IsValidOrder"}},
        {"source":{"ordinal":6,"type":"Int64"},"sink":{"name":"NumberOfItems"}},
        {"source":{"ordinal":7,"type":"Double"},"sink":{"name":"ColDouble"}},
        {"source":{"ordinal":8,"type":"DateTime"},"sink":{"name":"ColDatatime"}}
        ]
    }
}
```

**Note:**

 - property "firstRowAsHeader" have to be set to false for files without header
 - in "source" property "ordinal" value have to start from 1 till the columns count in the source file. It is also possible to skip unnecessary columns in definition and adjust final output table
 - originally without mapping definition all columns from source file are ingested as "String". It is possible to change data type in "type" property inside "source". In the end it is changed in sink table
 - property "typeConversion" have to be set to true, otherwise data type conversion will not be allowed
 - renaming the source column is allowed through "sink" à "name" property
 - if there is boolean column in source data, it is possible to control values through property "typeConversionSettings" à "treatBooleanAsNumber". For text values ("true", "false") this property should be false, for integer values (0, 1) it should be true. If not defined, default value is false.
 - if needed, more properties can be defined for “mapping“ → “typeConversionSettings” : 
 
 ![image](https://media.git.i.mercedes-benz.com/user/9160/files/0cfe0ab6-5cbf-463f-938d-d4855f83a906)

 - if last column in ingested CSV file is empty so the row of data is ending with seperaor e.g. semicolon (;) and in  mapping this last column is defined as Double data type, the ingestion will fail. In this case is needed to map this last column as String data type.
 
**Example for ingestion of TXT file with header, changing the column names and data types :**

```json
{ ...
"firstRowAsHeader": true,
"mapping": {
    "type":"TabularTranslator",
    "typeConversion": true,
    "mappings":[
        {"source":{"name":"ID","type":"Int32"},"sink":{"name":"OrderID"}},
        {"source":{"name":"OrdDate","type":"DateTime"},"sink":{"name":"OrderDate"}},
        {"source":{"name":"Source","type":"String"},"sink":{"name":"SourceSystem"}},
        {"source":{"name":"Desc","type":"String"},"sink":{"name":"Description"}},
        {"source":{"name":"Valid","type":"Boolean"},"sink":{"name":"IsValidOrder"}},
        {"source":{"name":"NumItems","type":"Int64"},"sink":{"name":"NumberOfItems"}}
        ]
    }
}
```

**Notes:**

 - property "firstRowAsHeader" have to be set to true for files without header
 - column names can be modified in property "sink" --> "name"
 - originally without mapping definition all columns from source file are ingested as "String". It is possible to change data type in "type" property inside "source". In the end it is changed in sink table
 - property "typeConversion" have to be set to true, otherwise data type conversion will not be allowed

**Example for ingestion of XML file, changing the column names and data types :**

```json
{
"encodingName": "UTF-8",
"nullValue": "",
"mapping": {
    "type": "TabularTranslator",
    "mappings": [
        {"source":{"path":"['CreditEntityType']"},"sink":{"name":"CreditEntityType","type":"String"}},
        {"source":{"path":"['CreditLineReference']"},"sink":{"name":"CreditLineReference","type":"String"}},
        {"source":{"path":"['CreditLineName']"},"sink":{"name":"CreditLineName","type":"String"}},
        {"source":{"path":"['ParentCreditLineReference']"},"sink":{"name":"ParentCreditLineRef","type":"String"}},
        {"source":{"path":"['CreditLineStatus']"},"sink":{"name":"CreditLineStatus","type":"String"}},
        {"source":{"path":"['CreditLineLimit']"},"sink":{"name":"CreditLineLimit","type": "String"}}
        ],
    "collectionReference":"$['Feed']['OrganisationData']['Organisation']['CreditLines']['CreditLine']",
    "mapComplexValuesToString":true
    }
}
```

**Note:**

 - all 3 propertis "encodingName", "nullValue", "mapping" are mandatory for XML sources
 - column names can be modified in property "sink" à "name"
 - originally without mapping definition all columns from source file are ingested as "String". It is possible to change data type in "type" property inside "sink"
 - property "collectionReference" is crucial, it is pointing to these part of data inside XML file which will be looped through and then ingested
 - property "path" is relative path to level below path stored in "collectionReference"

</details>

<details>
<summary>additional columns in Source Params</summary>

Optionally it is possible to define additional columns for all supported input data files: CSV / ZIP / EXCEL / TXT / CMA / XML. Property “additionalColumns” is array, here is more information how to configure this array : [Copy activity - Azure Data Factory & Azure Synapse | Microsoft Learn](https://learn.microsoft.com/en-us/azure/data-factory/copy-activity-overview#add-additional-columns-during-copy) , section “Add additional columns during copy”.

**Example for ingestion XLSX file, without mapping, creating an additional column :**

```json
{ ...
"firstRowAsHeader": true, 
"nullValue": "", 
"additionalColumns": [
{"name": "MGT_fileName", "value": "$$FILEPATH"}
]
}
```

**Example for ingestion CSV file, with mapping, without header, creating 3 additional columns :**

```json
{ ...
"firstRowAsHeader": false,
"mapping": {
    "type":"TabularTranslator",
    "typeConversion": true,
    "typeConversionSettings":{"treatBooleanAsNumber":true},
    "mappings":[
        {"source":{"ordinal":1,"type":"Int32"},"sink":{"name":"OrderID"}},
        {"source":{"ordinal":2,"type":"DateTime"},"sink":{"name":"OrderDate"}},
        {"source":{"ordinal":3,"type":"String"},"sink":{"name":"SourceSystem"}},
        {"source":{"ordinal":4,"type":"String"},"sink":{"name":"Description"}},
        {"source":{"ordinal":5,"type":"Boolean"},"sink":{"name":"IsValidOrder"}},
        {"source":{"ordinal":6,"type":"Int64"},"sink":{"name":"NumberOfItems"}},
{"source":{"name":"MGT_fileName","type":"String"},"sink":{"name":"MGT_fileName"}},
{"source":{"name":"copiedColumn01","type":"String"},"sink":{"name":"copiedColumn01"}},
{"source":{"name":"MGT_staticValue","type":"String"},"sink":{"name":"MGT_staticValue"}}
]
    }, 
"additionalColumns": [
{"name": "MGT_fileName", "value": "$$FILEPATH"},
{"name": "copiedColumn01","value": "$$COLUMN:Prop_2"},
{"name": "MGT_staticValue","value": "sampleValue_01"}
]
}
```

**Note:**
 - complex example how to configure additional columns when mapping is used and file is without header
 - new additional columns have to be listed also in mapping, but not as ordinals, only as the names with the same names as used in “additionalColumns” property, otherwise it will cause failures during ETL ingestion process,  see example above.

</details>

<details>
<summary>fault tolerance in Source Params</summary>

Optionally it is possible to enable skip incompatible rows during ingestion (fault tolerance) with property **“enableSkipIncompatibleRow“.**
Additionally, it is possible to log skipped rows in log files, to enable this it is needed to configure 2 properties (**logFwkLinkedServiceId** and **logPath**) in environment config notebook - FWK_METADATA_CONFIG. 
It is also possible to define **logFwkLinkedServiceId** and **logPath** in ingestion Configuration files per entity. If they are not defined per entity then configuration defined in environment config section will be used.

 - **“logFwkLinkedServiceId“** -  has to be one of the 'FwkLinkedServiceId' defined in FWK_LINKED_SERVICE_DF in environmentConfig. Be aware that only logging to ADLS storage account is supported currently by ADF. 

  - **“logPath”** - path to fo	lder with log files, staring with container name, e.g. "logPath": "asmefdevxx/IncompatibleRowsLogs" - “asmefdevxx“ is container and “IncompatibleRowsLogs“ is folder

Example of definition Ingestion Config  part with logging :

```json
 [
      ...
        {
            "Entity": "OrdersCSV1",
            "Path": "/from_erdr/OrdersCSV.csv",
            "IsMandatory": true,
            "SourceParams": {
                "columnDelimiter": ";",
                "rowDelimiter": "\r\n",
                "compressionType": "None",
                "compressionLevel": "",
                "encodingName": "UTF-8",
                "escapeChar": "\\",
                "firstRowAsHeader": false,
                "nullValue": "",
                "quoteChar": "",
                "skipLineCount": 0,
		"logFwkLinkedServiceId": "LandingADLS",
		"logPath": "asmefdevxx/IncompatibleRowsLogsPath",
                "enableSkipIncompatibleRow": true,
                "mapping": {
                    "type": "TabularTranslator",
                    "typeConversion": true,
                    "typeConversionSettings": {
                        "treatBooleanAsNumber": true
                    },
                    "mappings": [
                        ....
            },
            "TypeLoad": "Full"
        }
]
```

**Note:**

 - It is possible to enable property “enableSkipIncompatibleRow“ without defining “logFwkLinkedServiceId“ and “logsPath”. 
 - If both log settings “logFwkLinkedServiceId“ and “logsPath” are not defined neither in environment config nor in ingestion config, no logs will be created. If one of them is missing config generation will throw assertion error. 
 - You can find info about skipped rows in config DB, table “IngtLog“, column “RowsSkipped“.
 - Log files are TXT files, path to these files is stored in config DB, table “IngtLog“, column “LogFilePath“.

</details>

### Transformation Configuration File <a name = "transformation_config"></a>

Transformation Configuration File is basically needed if use-case is supposed to transform data after ingestion. It is also possible to have multiple transformation configuration files if it is desired to logically seperate the configurations.

Name of the transformation configuration file is usually set as 'Transformation_Configuration.json', but also can be changed, in this case, it should match with the defined name in environmentConfig notebook's [transformation section](#transformation_section).

The schema of the expected json file is as below;

```
Key STRING,
    Entities ARRAY<
        STRUCT<
            DatabaseName STRING,
            EntityName STRING,
            Transformations ARRAY<
                STRUCT<
                    TransformationType STRING,
                    Params STRUCT<
                        columns ARRAY<STRING>,
                        condition STRING,
                        expressions ARRAY<STRING>,
                        transformationSpecs STRING
                    >
                >
            >,
            Params STRUCT<
                keyColumns ARRAY<STRING>,
                partitionColumns ARRAY<STRING>,
                sinkEntityName STRING,
                fwkTriggerId STRING,
                writeMode STRING
            >
        >
    >
```

Explanation for each field if as follows;

| Property  | Detailed information
| ------------- | ------------- |
| Key | Key determines which entries form this config file belong to which transformation section in environmentConfig notebook.  |
| DatabaseName | Database name of the source data to be transformed |
| EntityName | Table name of the source entity to be transformed. It is also possible to define entity with wildcard rules. In this case, all tables that are matching with wildcard rule will be transformed. The wildcard rules have less priority than rules where EntityName was specified exactly. This means that if a table matches both specific and wildcard rule, the setting of specific rule will be used. |
| Transformations | List of the transformations to perform on the source data |
| Transformations/TransformationType | Transformation to perform on the data. Available transformation types :<br/> <ul><li>Deduplicate</li><li>Filter</li><li>Pseudonymization</li><li>ReplaceNull</li><li>Select</li><li>DeriveColumn</li><li>Cast</li><li>RenameColumn</li><li>Aggregate</li></ul> |
| Transformations/Params/columns | Columns to deduplicate, pseudonymize or select. Mandatory for TransformationType **Deduplicate**, **Pseudonymization** and **Select**. |
| Transformations/Params/condition | Condition to filer the data. Mandatory for TransformationType **Filter**. |
| Transformations/Params/expressions | Expressions to alter the data in the select function. Mandatory for TransformationType **SelectExpression**. |
| Transformations/Params/transformationSpecs | <ul><li>Mandatory specification for TransformationType **ReplaceNull** defining what to replace null values with for which columns.</li><li>Mandatory specification for TransformationType **Cast** defining the datatypes for typecasting the columns</li><li>Mandatory specification for TransformationType **RenameColumn** defining the new names of columns and the old column names</li><li>Mandatory specification for TransformationType **DeriveColumn** defining the values for new derived column of data</li><li>Mandatory specification for TransformationType **Join** defining the table to join, jointype, condition and columns to be selected</li><li>Mandatory specification for TransformationType **Aggregate** defining the values for operation, alias and columns to be selected. Optional paramater groupBy could  define a column/list of columns for data to be grouped on before aggregation. Mandatory parameter operation supports aggregations avg, min, max, sum, mean, count, collect_list, collect_set. While using aggregate operations of collect_list/collect_set if output is expected to be of json format then the column parameter needs to be passed as a list else output will be of array type</li></ul> |
| Params/writeMode | Defines the writemode to be applied after transformation steps. For possible writeModes and details, check [here](#writemodes) |
| Params/keyColumns | In case Snapshot is used as the writeMode, this property is mandatory and defines based on which columns the snapshot will be created. |
| Params/partitionColumns | Optional parameter that specifies how the data should be partitioned on ADLS storage. |
| Params/sinkEntityName | Optional parameter that specifies sink entity name. If not provided sink entity name will be same as source entity name. |
| Params/fwkTriggerId | In case it’s desired to use different triggerid than the one specified in environmentConfig notebook transformation section per entity, it can be overridden using this property. |

**Example Transformation Configuration file with possible transformation types :**

```json
[
    {
        "Key": "StagingTransformations",
        "Entities": [
            {
                "DatabaseName": "Application_MEF_LandingSQL",
                "EntityName": "Address",
                "Transformations": [
                    {
                        "TransformationType": "Select",
                        "Params": {
                            "columns": [
                                "AddressID",
                                "AddressLine1",
                                "AddressLine2",
                                "PostalCode",
                                "City",
                                "StateProvince",
                                "CountryRegion"
                            ]
                        }
                    },
                    {
                        "TransformationType": "Filter",
                        "Params": {
                            "condition": "AddressID < 15000"
                        }
                    },
                    {
                        "TransformationType": "Deduplicate",
                        "Params": {
                            "columns": [
                                "*"
                            ]
                        }
                    },
                    {
                        "TransformationType": "ReplaceNull",
                        "Params": {
                            "transformationSpecs": [
                                {
                                    "column": [
                                        "AddressLine2"
                                    ],
                                    "value": "n/a"
                                },
                                {
                                    "column": [
                                        "AddressID"
                                    ],
                                    "value": -1
                                }
                            ]
                        }
                    },
		    {
                        "TransformationType": "DeriveColumn",
                        "Params": {
                            "transformationSpecs": [
                                {
                                    "column": "AddressIDDerived",
                                    "value": "AddressID + 10"
                                }
                            ]
                        }
                    },
                    {
                        "TransformationType": "Cast",
                        "Params": {
                            "transformationSpecs": [
                                {
                                    "column": ["AddressIDDerived"],
                                    "dataType": "string"
                                }
                            ]
                        }
                    },
                    {
                        "TransformationType": "RenameColumn",
                        "Params": {
                            "transformationSpecs": {
                                "column": ["AddressIDDerived"],
                                "value": ["AddressIDRenamed"]
                            }
                        }
                    },
                    {
                        "TransformationType": "SelectExpression",
                        "Params": {
                            "expressions": [
                                "CAST(AddressIDRenamed AS INT) AS AddressIDRenamedInt"
                            ]
                        }
                    },
                    {
                        "TransformationType": "Pseudonymization",
                        "Params": {
                            "columns": [
                                "AddressLine1",
                                "City"
                            ]
                        }
                    },
                    {
                        "TransformationType": "Join",
                        "Params": {
                            "transformationSpecs": {
                                "joinTable": "Application_MEF_StagingSQL.CustomerAddress",
                                "joinType": "LEFT",
                                "joinColumns": "AddressID,AddressID",
                                "selectColumns": "table.*, AddressType"
                            }
                        }
                    },
                    {
                        "TransformationType": "Aggregate",
                        "Params": {
                            "transformationSpecs": {
                                "groupBy": ["DueDate", "Status"],
				                "aggregations": [
                                    {
                                        "operation": "sum",
                                        "column": "TotalDue",
                                        "alias": "SumTotalDue"
                                    },
                                    {
                                        "operation": "avg",
                                        "column": "TotalDue",
                                        "alias": "AvgTotalDue"
                                    },
                                    {
                                        "operation": "min",
                                        "column": "SubTotal" ,
                                        "alias": "MinSubTotal"
                                    },
                                    {
                                        "operation": "collect_list",
                                        "column": "SalesOrderId" ,
                                        "alias": "ListSubTotal"
                                    },
                                    {
                                        "operation": "collect_list",
                                        "column": ["SalesOrderId","Status"] ,
                                        "alias": "ListSubTotal"
                                    }
                                ]
                            }                        
                        }                
                    }
                ]
            }
        ]
    }
]
```

### Model Transformation Files <a name = "model_transformation_files"></a>

There are four metadata files that are supposed to be exported from a data modeling tool and used as an input to the framework; 

 - Entities file
 - Attributes file
 - Keys file
 - Relations file
 
 and one additional optional configuration file that allows you to override some parameters for entities, which will be explained in following pages.
 
 - Model Transformation Configuration file

#### Entities File <a name = "entities_file"></a>

Keeps the list of entities in data model.
The name of the file should be the name that is defined in environmentConfig notebook [modeltransformation section](#model_transformation_section).

The schema of the expected json file is as below;

```
    Entity STRING,
    Stereotype STRING
```

| Property  | Detailed information
| ------------- | ------------- |
| Entity | Name of the tables in data model |
| StereoType | Type of table that comes from the modeling tool (optional, can be left empty) |

#### Attributes File <a name = "attributes_file"></a>

Keeps the list of table columns in data model.
The name of the file should be the name that is defined in environmentConfig notebook [modeltransformation section](#model_transformation_section).

The schema of the expected json file is as below;

```
    Entity STRING,
    Attribute STRING,
    DataType STRING,
    IsPrimaryKey BOOLEAN,
    IsIdentity BOOLEAN
```

| Property  | Detailed information
| ------------- | ------------- |
| Entity | Name of the tables in data model |
| Attribute | Name of the column of the respective table |
| DataType | Data Type of the column |
| IsPrimaryKey | Flag indicating if the attribute is primary key or not |
| IsIdentity | Flag indicating if the attribute is identity or not (which means if it is auto incremental identity column or not) |

#### Keys File <a name = "keys_file"></a>

Keeps the list of primary keys and business keys for all tables (one or more columns). 
The name of the file should be the name that is defined in environmentConfig notebook [modeltransformation section](#model_transformation_section).

The schema of the expected json file is as below;

```
    Entity STRING,
    KeyType STRING,
    Attribute STRING
```

| Property  | Detailed information
| ------------- | ------------- |
| Entity | Name of the tables in data model |
| KeyType | Keytype of the column. Supported values : PIdentifier or any other value. PIdentifier means PrimaryIdentifier. One table generally has either PIdentifier keys only or PIdentifier and other keytype identifiers together. |
| Attribute | Name of the column of the respective table |

#### Relations File <a name = "relations_file"></a>

Keeps the list of relationship between tables. 
The name of the file should be the name that is defined in environmentConfig notebook [modeltransformation section](#model_transformation_section).

The schema of the expected json file is as below;

```
    Parent STRING,
    ParentAttribute STRING,
    Child STRING,
    ChildAttribute STRING,
    KeyType STRING,
    RolePlayingGroup STRING
```

| Property  | Detailed information
| ------------- | ------------- |
| Parent | Name of the parent table for the relation |
| ParentAttribute | Name of the parent table column for the relation |
| Child | Name of the child table for the relation |
| ChildAttribute | Name of the child table column for the relation |
| KeyType | Keytype of the relation. PIdentifier means PrimaryIdentifier. <br/> If there is only PIdentifier keytype relations between two tables, during sourcesql generation for respective table, no joins are constructed.<br/> If there are PIdentifer and other keytype relations together between two tables, during sourcesql generation for respective table, joins are constructed on other keytpe relation columns. |
| RolePlayingGroup | If there are multiple relations between same two tables in this file, (roleplaying tables/dimensions in the model) this column should be filled to group the relations in order to construct the source sql correctly. |

#### Model Transformation Configuration File <a name = "model_transformation_config"></a>

This is an additional optional configuration file that allows you to override some parameters for entities.
The name of the file should be the name that is defined in environmentConfig notebook [model transformation section](#model_transformation_section).

The schema of the expected json file is as below;

```
    Key STRING,
    Entities ARRAY<
        STRUCT<
            EntityName STRING,
            Params STRUCT<
                keyColumns ARRAY<STRING>,
                partitionColumns ARRAY<STRING>,
                fwkTriggerId STRING,
                writeMode STRING,
                batchNumber INTEGER
            >
        >
    >
```

| Property  | Detailed information
| ------------- | ------------- |
| Key | Key determines which entries form this config file belong to environment model transformation Metadata config key. |
| EntityName | Table name |
| Params/keyColumns | In case **Snapshot** is used as the writeMode, this property is mandatory and defines based on which columns the snapshot will be created. In case of **SCDType1** this property is optional and should be used only if user wants to override primary columns that are identified automatically based on Keys file. |
| Params/partitionColumns | Optional parameter that specifies how the data should be partitioned on ADLS storage. |
| Params/fwkTriggerId | In case it’s desired to use different triggerid than the one specified in environmentConfig notebook model transformation section per entity, it can be overridden using this property. |
| Params/writeMode | Parameter that overrides the writemode configuration to be performed for the table. |
| Params/batchNumber | In case it is needed to overwrite batch number for model transformations. Normally framework is automatically determining the batch numbers by analyzing the relations file. So, this property is fully optional. |

### Data Validation Files <a name = "data_validation_files"></a>

There is one configuration file that can keep all the data quality expectations;

 - DQ Configuration File
 
 and two additional optional configuration files that allows you to keep reference values seperately than dq configurations.
 
 - DQ Reference Values File
 - DQ Reference Pair Values File
 
 #### DQ Configuration File <a name = "dq_configuration_file"></a>

Keeps the data quality expectations and all configurations around it. It is also possible to have multiple DQ configuration files if it is desired to logically separate the configurations.
The name of the file should be the name that is defined in environmentConfig notebook [variables section](#variables).

The schema of the expected json file is as below;

```
    DatabaseName STRING,
    Entities ARRAY<
        STRUCT<
            EntityName STRING,
            Expectations ARRAY<
                 STRUCT<
                    Description STRING,
                    ExpectationType STRING,
                    KwArgs STRING,
                    Quarantine BOOLEAN,
                    DQLogOutput STRING
                >
            >
        >
    >
```

| Property  | Detailed information
| ------------- | ------------- |
| DatabaseName | Database name of the entity that should be validated |
| EntityName | Entity name of the entity that should be validated</br> Following wildcards patterns are supported: <lu><li>"Starts[*]" – rule will be applied to a table which name starts with “Starts”</li><li>"[*]Ends" – rule will be applied to a table which name ends with “Ends”</li><li>"Starts[*]Ends" – rule will be applied to a table which name starts with “Starts” and ends with “Ends”</li><li>"Starts[A,B]Ends" – rule will be applied to table “StartsAEnds” and “StartsBEnds”. The list of possible substrings ([A,B]) can be located at the beginning, at the end or in the middle.</li></lu> |
| Description | User defined description of the data quality expectation rule. | 
| ExpectationType | Name of exception rule from the GreatExpectations library or custom-implemented rule name. List of all possible values with its description can be found in [Available DQ Checks](#available_DQ_checks). |
| KwArgs | Arguments of the selected expectation. List of required arguments per expectation type can be found in [Available DQ Checks](#available_DQ_checks).
| Quarantine | Flag whether invalid data should be quarantined and not processed further (moved to next layers) |
| DQLogOutput | Defines which column values should be stored into DQ Log for rows that did not pass an expectation. This is later used to better understand or identify bad data. |

**Sample DQ Configuration File :**

```json
[
    {
        "DatabaseName": "Application_MEF_StagingSQL",
        "Entities": [
            {
                "EntityName": "Customer",
                "Expectations": [
                    {
                        "Description": "CustomerID can not be null",
                        "ExpectationType": "expect_column_values_to_not_be_null",
                        "KwArgs": {
                            "column": "CustomerID"
                        },
                        "Quarantine": true,
                        "DQLogOutput": "FirstName, LastName, rowguid"
                    },
                    {
                        "Description": "CustomerID can be between 1 and 30110",
                        "ExpectationType": "expect_column_values_to_be_between",
                        "KwArgs": {
                            "column": "CustomerID",
                            "min_value": 1,
                            "max_value": 30110
                        },
                        "Quarantine": false,
                        "DQLogOutput": "CustomerID"
                    },
		    {
                        "Description": "Minimal order after year 2010 is 5 Euros",
                        "ExpectationType": "expect_column_values_to_not_match_condition",
                        "KwArgs": {
                            "condition": "OrderDate >= '2010-01-01' AND TotalDue < 5"
                        },
                        "Quarantine": false,
                        "DQLogOutput": "SalesOrderID, TotalDue"
                    }
                ]
            }
	]
    }
]
```

 #### DQ Reference Values File <a name = "dq_reference_values_file"></a>
 
In some dq checks, it’s desired to validate column value against pre-defined possible values – reference values. 
Instead of hardcoding these reference values into DQ Configuration File many times for different expectations, such values can be defined in DQ Reference Values File.
It is also possible to have multiple DQ reference values file if it is desired to logically separate the configurations.

The name of the file should be the name that is defined in environmentConfig notebook [variables section](#variables).

The schema of the expected json file is as below;

```
    ReferenceKey STRING,
    Value STRING,
    Description STRING
```

| Property  | Detailed information
| ------------- | ------------- |
| ReferenceKey | Reference Value Key ( to be used in DQ Configuration file later on) |
| Value | Reference Values |
| Description | User defined description of the reference value (optional). | 

Below is an example of DQ Reference Values file;

```json
[
    {
        "ReferenceKey": "Country",
        "Value": "United States",
        "Description": ""
    },
    {
        "ReferenceKey": "Country",
        "Value": "Canada",
        "Description": ""
    },
    {
        "ReferenceKey": "Country",
        "Value": "United Kingdom",
        "Description": ""
    }
]
```

With above Reference values file, it’s possible to define following example expectation in DQ Configuration file :

```json
[
    {
        "DatabaseName": "Application_MEF_StagingSQL",
        "Entities": [
            {
                "EntityName": "Address",
                "Description": "Value of column Region must match reference values",
                "ExpectationType": "expect_column_values_to_not_match_condition",
                "KwArgs": {
                    "condition": "Country NOT IN (#Country)"
                },
                "Quarantine": true,
                "DQLogOutput": "AddressID, Country"
            }
      ]
    }
]
```

#### DQ Reference Pair Values File <a name = "dq_reference_pair_values_file"></a>

In some dq checks, it’s desired to validate values of pair columns against pre-defined possible pair values.
Instead of hardcoding these reference values into DQ Configuration File many times for different expectations, such pair values can be defined in DQ Reference Pair Values File.
It is also possible to have multiple DQ reference pair values file if it is desired to logically separate the configurations.

The name of the file should be the name that is defined in environmentConfig notebook [variables section](#variables).

The schema of the expected json file is as below;

```
    ReferenceKey1 STRING,
    Value1 STRING,
    Description1 STRING,
    ReferenceKey2 STRING,
    Value2 STRING,
    Description2 STRING
```

| Property  | Detailed information
| ------------- | ------------- |
| ReferenceKey1 | Reference Value Key 1 ( to be used in DQ Configuration file later on) |
| Value1 | Reference Values for 1st key |
| Description1 | User defined description of the first reference value (optional). | 
| ReferenceKey2 | Reference Value Key 2 ( to be used in DQ Configuration file later on) |
| Value2 | Reference Values for 2nd key |
| Description2 | User defined description of the second reference value (optional). | 

Below is an example of DQ Reference Pair Values file;

```json
[
    {
        "ReferenceKey1": "Brand",
        "Value1": "Mercedes",
        "Description1": "",
        "ReferenceKey2": "Model",
        "Value2": "GLA",
        "Description2": ""
    },
    {
        "ReferenceKey1": "Brand",
        "Value1": "Mercedes",
        "Description1": "",
        "ReferenceKey2": "Model",
        "Value2": "EQA",
        "Description2": ""
    },
    {
        "ReferenceKey1": "Brand",
        "Value1": "Mercedes",
        "Description1": "",
        "ReferenceKey2": "Model",
        "Value2": "CLA",
        "Description2": ""
    }
]
```

With above Reference pair values file, it’s possible to define following example expectation in DQ Configuration file :

```json
[
    {
        "DatabaseName": "Application_MEF_StagingSQL",
        "Entities": [
            {
                "EntityName": "Vehicle",
                "Description": "Value of columns Brand and Model must match reference values",
                "ExpectationType": "expect_column_pair_values_to_be_in_set",
                "KwArgs": {
                    "column_A": "Brand", "column_B": "Model", "value_pairs_set": "#Brand#Model"
                },
                "Quarantine": true,
                "DQLogOutput": "VehicleId,Brand,Model"
            }
      ]
    }
]
```

**Available DQ Checks:**<a name = "available_dq_checks"></a>

| ExpectationType  | Validation Type | KwArgs |
| ------------- | ------------- | ------------- |
| expect_column_distinct_values_to_be_in_set | Dataset based  | { "column": { "column": "ColumnA", "value_set": [1,2,3] } |
| expect_column_distinct_values_to_contain_set | Dataset based | { "column": "ColumnA", "value_set": [1,2,3] } |
| expect_column_distinct_values_to_equal_set | Dataset based | { "column": "ColumnA", "value_set": [1,2,3] } |
| expect_column_max_to_be_between | Dataset based | { "column": "ColumnA", "min_value": 1, "max_value": 2 } |
| expect_column_mean_to_be_between | Dataset based | { "column": "ColumnA", "min_value": 1, "max_value": 5 } |
| expect_column_median_to_be_between | Dataset based | { "column": "ColumnA", "min_value": 1, "max_value": 5 } |
| expect_column_min_to_be_between | Dataset based | { "column": "ColumnA", "min_value": 1, "max_value": 5 } |
| expect_column_most_common_value_to_be_in_set | Dataset based | { "column": "ColumnA", "value_set": [1,2,3] } |
| expect_column_pair_values_to_be_equal | Row based | { "column_A": "ColumnA", "column_B": "ColumnB" } |
| expect_column_pair_values_to_be_in_set | Row based | { "column_A": "ColumnA", "column_B": "Title", "value_pairs_set": [(1,"Mr."), (2,"Mr.")] } |
| expect_column_proportion_of_unique_values_to_be_between | Dataset based | { "column": "ColumnA", "min_value": 1, "max_value": 5 } |
| expect_column_quantile_values_to_be_between | Dataset based | { "column": "ColumnA", "quantile_ranges": {"quantiles":  [0., 0.333, 0.6667, 1.]}, "value_ranges": [[0,1], [2,3], [3,4], [4,5]] } |
| expect_column_stdev_to_be_between | Dataset based | { "column": "ColumnA", "min_value": 1, "max_value": 5 } |
| expect_column_sum_to_be_between | Dataset based | { "column": "ColumnA", "min_value": 1, "max_value": 5 } |
| expect_column_to_exist | Dataset based | { "column": "ColumnA" } |
| expect_column_unique_value_count_to_be_between | Dataset based | { "column": "ColumnA", "min_value": 1, "max_value": 5 } |
| expect_column_value_lengths_to_be_between | Row based | { "column": "ColumnA", "min_value": 1, "max_value": 5 } |
| expect_column_value_lengths_to_equal | Row based | { "column": "ColumnA", "value": 1 } |
| expect_column_values_to_be_decreasing | Row based | { "column": "ColumnA" } |
| expect_column_values_to_be_in_set | Row based | { "column": "ColumnA", "value_set": [1,2,3] } |
| expect_column_values_to_be_increasing | Row based | { "column": "ColumnA" } |
| expect_column_values_to_be_json_parseable | Row based | { "column": "ColumnA" } |
| expect_column_values_to_be_null | Row based | { "column": "ColumnA" } |
| expect_column_values_to_be_of_type | Dataset based | { "column": "ColumnA", "type_": "IntegerType" } |
| expect_column_values_to_be_unique | Row based | { "column": "ColumnA" } |
| expect_column_values_to_match_regex | Row based | { "column": "ColumnA", "regex": "^\\d+$" } |
| expect_column_values_to_match_regex_list | Row based | { "column": "ColumnA", "regex_list": ["regexA","regexB"] } |
| expect_column_values_to_match_strftime_format | Row based | { "column": "ColumnA", "strftime_format": "%Y-%m-%d" } |
| expect_column_values_to_not_be_in_set | Row based | { "column": "ColumnA", "value_set": [1,2,3] } |
| expect_column_values_to_not_be_null | Row based | { "column": "ColumnA" } |
| expect_column_values_to_not_match_regex_list | Row based | { "column": "ColumnA", "regex_list": ["1","0"] } |
| expect_column_values_to_be_between | Row based | { "column": "ColumnA", "min_value": 1, "max_value": 20 } |
| expect_compound_columns_to_be_unique | Row based | { "column_list": ["ColumnA","Title"] } |
| expect_multicolumn_sum_to_equal | Row based | { "column_list": ["ColumnA","ColumnB"], "sum_total": 4 } |
| expect_select_column_values_to_be_unique_within_record | Row based | { "column_list": ["ColumnA","ColumnB"] } |
| expect_table_column_count_to_be_between | Dataset based | { "min_value": 1, "max_value": 20 } |
| expect_table_column_count_to_equal | Dataset based | { "value": 5 } |
| expect_table_columns_to_match_ordered_list | Dataset based | { "column_list": ["ColumnA","ColumnB"] } |
| expect_table_columns_to_match_set | Dataset based | { "column_set": ["ColumnA","ColumnB"], "exact_match": True } |
| expect_table_row_count_to_be_between | Dataset based | { "min_value": 1, "max_value": 20 } |
| expect_table_row_count_to_equal | Dataset based | { "value": 5 } |
| expect_column_values_to_not_match_condition | Row based | [details below](#expect_column_values_to_not_match_condition) |
| compare_datesets | Dataset based | [details below](compare_datesets) |

**expect_column_values_to_not_match_condition**<a name = "expect_column_values_to_not_match_condition"></a>

This is a custom validation rule that will mark all records that do not match specified condition as invalid or quarantined. In other words, it’s designed to find incorrect records. It can work in two modes:

 - Validation of one table
 
   ```json
   KwArgs: {
	"column": "SalesOrderID",
	"condition": "OrderDate < '2010-01-01' AND TotalDue < 10"
	}
   ```
  
  | KwArgs property  | Description |
  | ------------- | ------------- |
  | column | Optional property. It’s necessary to specify it only if condition contains GROUP BY clause. In this case, column has to be picked from columns by which the data are being grouped. E.g.: </br> { "column": "CustomerID", "condition": "GROUP BY CustomerID HAVING Count(CustomerID) > 1300" } |
  | condition | Condition defined in SQL syntax |
  
  Internally this rule is implemented as following SQL select:
  
  ```sql
  SELECT {column} FROM table WHERE {condition}
  ```

- Cross table validation

  ```json
  KwArgs: {
	"column": "CustomerID",
	"joinTable": "StagingSQL.Customer",
	"joinType": "LEFT",
	"joinColumns": "CustomerID,CustomerID",
	"condition": "StagingSQL.Customer.CustomerID IS NULL GROUP BY table.CustomerID"
	}
   ```
   
| KwArgs property  | Description |
| ------------- | ------------- |
| column | Optional property. It’s necessary to specify it only if condition contains GROUP BY clause. In this case, column has to be picked from columns by which the data are being grouped. E.g.: </br> { "column": "CustomerID", "condition": "StagingSQL.Customer.CustomerID IS NULL GROUP BY table.CustomerID " } |
| joinTable | Table that will be joined to data that are being validated. It’s necessary to specify full name (DatabaseName.TableName) and pick a existing table which is already processed and loaded. Also please keep in mind that table that you pick from the previous layer should store the data permanently; if the table is delta loaded with write mode Overwrite, again data would not be validated correctly. |
| joinType | Type of the join. Supported values:</br> <lu><li>INNER</li><li>LEFT</li><li>LEFT OUTER</li><li>RIGHT</li><li>RIGHT OUTER</li><li>FULL</li><li>FULL OUTER</li><li>SEMI</li><li>LEFT SEMI</li><li>ANTI</li><li>LEFT ANTI</li><li>CROSS</li></lu> |
| joinColumns | Information how to join two tables together in the following format: </br> C1VT,C1JT\|C2VT,C2JT\|CNVT,CNJT </br> where </br> C1VT = first column name from validated table </br> C1JT = first column name from joined table </br> C2VT = second column name from validated table </br> C2JT = second column name from joined table </br> CNVT = n-th column name from validated table </br> CNJT = n-th column name from joined table </br></br> e.g. "CustomerId,CustomerNo\|SystemId,SysId" |
| condition | Condition defined in SQL syntax |

Internally this rule is implemented as following SQL select:

```sql
SELECT table.{column} FROM table {joinType} JOIN {joinTable} ON {joinOnColumns} WHERE {condition}
```

joinOnColumns is derived from KwArgs property joinColumns in the following way:

from "CustomerId,CustomerNo|SystemId,SysId"
to "table.CustomerId = {joinTable}.CustomerNo AND table.SystemId = {joinTable}.SysId”

**Additional Logging of {joinTable} Columns:**

If required, columns from the {joinTable} can be included in the output log by specifying them in the DQLogOutput parameter

Example 1:

```json
{
  "Description": "Find customers with suspicious orders",
  "ExpectationType": "expect_column_values_to_not_match_condition",
  "KwArgs": {
              "column": "CustomerID",
              "joinTable": "Application_MEF_History.SalesOrderHeader",
              "joinType": "LEFT",
              "joinColumns": "CustomerID,CustomerID",
              "condition": "Application_MEF_History.SalesOrderHeader.SubTotal > 70000"
          },
  "Quarantine": false,
  "DQLogOutput": "CustomerID, Application_MEF_History.SalesOrderHeader.SalesOrderID, Application_MEF_History.SalesOrderHeader.SubTotal"
}
```

Ensure that the full name (DatabaseName.TableName.ColumnName) is provided in the DQLogOutput parameter for columns from {joinTable}. You can add multiple columns from the {joinTable} based on the specified {joinType}.

Example 2: GROUP BY condition

```json
{
  "Description": "Find customers with suspicious orders based on ship method",
  "ExpectationType": "expect_column_values_to_not_match_condition",
  "KwArgs": {
      "column": "CustomerID",
      "joinTable": "Application_MEF_History.SalesOrderHeader",
      "joinType": "LEFT",
      "joinColumns": "CustomerID,CustomerID",
      "condition": "GROUP BY table.CustomerID, Application_MEF_History.SalesOrderHeader.ShipMethod HAVING SUM(Application_MEF_History.SalesOrderHeader.SubTotal) > 70000"
  },
  "Quarantine": false,
  "DQLogOutput": "CustomerID, Application_MEF_History.SalesOrderHeader.ShipMethod, Application_MEF_History.SalesOrderHeader.SalesOrderID"
}
```

In cases where the condition includes a GROUP BY clause and if logging of columns from the {joinTable} is necessary, ensure that the columns are also part of the GROUP BY condition. Specify the full column name as part of the condition for the {joinTable}. Columns that are not part of GROUP BY clause will be ignored.

**compare_datasets**<a name = "compare_datasets"></a>

This custom dataset-based validation rule compares two datasets to identify unexpected differences. It flags records as invalid or quarantined if they do not match the specified expectations, including marking missing or unexpected records. Additionally, it logs columns from the datasets being compared.

```json
{
  "Description": "Compare the incoming Contract data to Contract History table",
  "ExpectationType": "compare_datasets",
  "KwArgs": {
      "compareAgainst": "Application_MEF_History.Contract",
      "comparedColumn": "ContractNumber,ContractNumber",
      "condition": "table.Market = Application_MEF_History.Contract.Market and DATEADD(month,-1,table.ReportDate) = Application_MEF_History.Contract.ReportDate",
      "expectation": "fullMatch"
  },
  "Quarantine": false,
  "DQLogOutput": "ContractNumber, Application_MEF_History.Contract.ContractNumber, Application_MEF_History.Contract.Market"
}
```

| Expectations property  | Description |
| ------------- | ------------- |
| compareAgainst | Table that will be used for comparison for the data being validated. Ensure to specify the full name (DatabaseName.TableName) of an existing table that contains the data to be compared. Additionally, please note that the selected table from the previous layer should store data permanently. If the table is delta loaded with write mode Overwrite, the data may not be validated accurately. |
| comparedColumn | Information on which columns the two tables to be compared: </br> C1VT,C1JT\|C2VT,C2JT\|CNVT,CNJT </br> where, </br> C1VT = first column name from validated table </br> C1JT = first column name from the table to be compared against. </br> C2VT = second column name from validated table </br> C2JT = second column name from the table to be compared against. </br> CNVT = n-th column name from validated table </br> CNJT = n-th column name from the table to be compared against. </br></br> e.g. "CustomerId,CustomerNo\|SystemId,SysId" |
| condition | This property is optional. Condition to be defined in SQL syntax. </br> Please utilize this property to restrict the size of the datasets being compared. |
| expectation | This property will be used to determine the way the data is logged in the dqlog and supported values are: </br></br> **fullMatch**: It will log both missing and unexpected rows from both tables that do not match. </br> **noMissing**: It will log only the missing rows from table. </br> **noAddition**: It will log only the additional/ unexpected rows from table. </br> |
| DQLogOutput | If required, columns from the {compareAgainst} can be included in the output log by specifying them in the DQLogOutput parameter.</br> Ensure that the full name (DatabaseName.TableName.ColumnName) is provided in the DQLogOutput parameter for columns from {compareAgainst}. |

Internally this rule is implemented as following SQL select:

```sql
SELECT table.{column}, {compareAgainst}.{column} FROM
(SELECT * FROM table WHERE EXISTS (SELECT NULL FROM {compareAgainst} WHERE {condition})) AS table
FULL JOIN
(SELECT * FROM {compareAgainst} WHERE EXISTS (SELECT NULL FROM table WHERE {condition})) AS joinTable
ON {joinOnColumns}
AND {condition}
WHERE {nullCheckCondition}
```

During the validation process, both the incoming Data Frame and the {compareAgainst} table are filtered based on the specified {condition}. These subqueries ensure that only the rows meeting the condition are included in the respective datasets. Following this, the incoming Data Frame table is joined with {compareAgainst} based on the specified {condition} and {comparedColumn} values.

The joinOnColumns are derived from the KwArgs property {comparedColumn} as follows:

from "CustomerId,CustomerNo|SystemId,SysId" 

to "table.CustomerId = {compareAgainst}.CustomerNo AND table.SystemId = {compareAgainst}.SysId”

The determination of missing or unexpected rows is guided by the {nullCheckCondition} generated by the Framework in the WHERE clause.

(table.{column} IS NULL OR {compareAgainst}.{column} IS NULL)

 

**Note :**

if space or special character e.g. dash (-) has been used in column name, it is needed to wrap this column name in backticks, e.g. condition property will look like this : "condition": "`A-L-D`IS NULL". this have to be applied only for custom rule “expect_column_values_to_not_match_condition” and “compare_datasets”, all other rules have to be without backticks.

### Export Configuration File <a name = "export_configuration_file"></a>

Keeps the data export configurations.
The name of the file should be the name that is defined in environmentConfig notebook [export section](#export_section).

The export configuration file has the following structure;

```
    Query STRING,
    Path STRING,
    Params STRING,
    FwkLinkedServiceId STRING,
    SourcePath STRING,
    Entity STRING,
    PreCopyScript STRING
```

| Property | Description |
| ------------- | ------------- |
| Query | Query that will be used to select data for export (applicable when exporting data) |
| Path | Path which contains export path location and name of the exported file (applicable when exporting data or existing CSV files to ADLS, SFTP and FileShare) |
| Params | Applicable when exporting data to ADLS, SFTP and FileShare. Define how the CSV file containing the exported data will be created </br> – e.g., what separator will be used, whether it should contain header, etc. </br> All possible settings can be found [here](https://spark.apache.org/docs/latest/sql-data-sources-csv.html) |
| FwkLinkedServiceId | Linked Service Id of an ADLS storage where the file to export is stored (applicable when exporting existing CSV file) |
| SourcePath | Path which contains container name, path and name of the file to export (applicable when exporting existing CSV file) |
| Entity | Entity ( table_name ) the data to be exported (applicable when exporting data to SQL) |
| PreCopyScript | Applicable when exporting data to SQL. Script to be executed on the SQL server that data will be exported, before exporting the data . |

Sample Export Configuration File (export data to ADLS, SFTP or Fileshare)

```json
[
    {
        "Query": "SELECT * FROM Application_MEF_History.Customer WHERE MGT_isCurrent = TRUE AND MGT_isDeleted = FALSE",
        "Path": "History_Customer.csv",
        "Params": {
            "header": true,
            "nullValue": null,
            "sep": ";",
            "quoteAll": true,
            "quote": "'"
        }
    },
    {
        "Query": "SELECT * FROM Application_MEF_History.DimCurrency WHERE MGT_isCurrent = TRUE AND MG T_isDeleted = FALSE",
        "Path": "CSV/History_DimCurrency.csv",
        "Params": {
            "header": true,
            "nullValue": null,
            "sep": "|"
        }
    },
    {
        "Query": "SELECT * FROM Application_MEF_History.Customer WHERE MGT_isCurrent = TRUE AND MGT_isDeleted = FALSE",
        "Path": "History_Customer.csv"
    }
]
```

Sample Export Configuration File (export data to SQL)

```json
[
    {
        "Query": "SELECT * FROM Application_MEF_History.Customer WHERE MGT_isCurrent = TRUE AND MGT_isDeleted = FALSE LIMIT 10",
        "Entity": "SalesLT.CustomerEXP",
        "PreCopyScript": "TRUNCATE TABLE SalesLT.CustomerEXP"
    },
    {
        "Query": "SELECT * FROM Application_MEF_History.CustomerAddress WHERE MGT_isCurrent = TRUE AND MGT_isDeleted = FALSE LIMIT 10",
        "Entity": "SalesLT.CustomerAddressEXP"
    }
]
```

Sample Export Configuration File (export existing CSV files to ADLS, SFTP or Fileshare)

```json
[
    {
        "FwkLinkedServiceId": "CompartmentADLS",
        "SourcePath": "dldata/history/FilesToExport/Customer.csv",
        "Path": "Customer.csv"
    },
    {
        "FwkLinkedServiceId": "CompartmentADLS",
        "SourcePath": "dldata/history/FilesToExport/CustomerAddress.csv",
        "Path": "History_CustomerAddress_File.csv"
    }
]
```

### Workflow Configuration File <a name = "workflow_configuration_file"></a>

Keeps the workflow configurations. When it’s needed to start a power automate workflow before or after particular layer for particular ADFTriggerName, Workflow configuration file defines workflow parameters and the framework triggers it.

The name of the file should be the name that is defined in environmentConfig notebook [variables section](#variables).

The workflow configuration file has the following structure;

```
ADFTriggerName STRING,
Workflows ARRAY<
  STRUCT<
    WorkflowId STRING,
    URL STRING,
    RequestBody STRING
  >
>
```

| Property | Description |
| ------------- | ------------- |
| ADFTriggerName | ADFTriggerName is one of the defined ADF trigger names </br> Combination of ADFTriggerName and WorkflowId defines if there is a workflow to be triggered. |
| WorkflowId | Workflow identifier, any arbitrary text. </br> Combination of ADFTriggerName and WorkflowId defines if there is a workflow to be triggered. |
| URL | URL of the workflow created in the Microsoft Power Automate  |
| RequestBody | Parameters for the workflow. Compartment can define any parameters that are needed for the workflow to work.  |

Sample workflow configuration file :

```json
[
    {
        "ADFTriggerName": "Sandbox",
        "Workflows": [
            {
                "WorkflowId": "Post_Historization_Approval",
                "URL": "https://prod-108.westeurope.logic.azure.com:443/workflows/719eb69ecfde41e98f78b1d50087575d/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=Lw5yluQHp1DMjgH7Bk_2EvV-DOzXqThYrwuNEW6uoFc",
                "RequestBody": {
                    "Approval_Email_Address": "burak.duran@mercedes-benz.com",
                    "ADFTriggerName": "Sandbox",
                    "FwkLayerId": "SDM"
                }
            }
        ]
    },
    {
        "ADFTriggerName": "Daily_1am",
        "Workflows": [
            {
                "WorkflowId": "Post_Historization_Approval",
                "URL": "https://prod-108.westeurope.logic.azure.com:443/workflows/719eb69ecfde41e98f78b1d50087575d/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=Lw5yluQHp1DMjgH7Bk_2EvV-DOzXqThYrwuNEW6uoFc",
                "RequestBody": {
                    "Approval_Email_Address": "burak.duran@mercedes-benz.com",
                    "ADFTriggerName": "Daily_1am",
                    "FwkLayerId": "SDM"
                }
            },
            {
                "WorkflowId": "Post_SDM_Approval",
                "URL": "https://prod-108.westeurope.logic.azure.com:443/workflows/719eb69ecfde41e98f78b1d50087575d/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=Lw5yluQHp1DMjgH7Bk_2EvV-DOzXqThYrwuNEW6uoFc",
                "RequestBody": {
                    "Approval_Email_Address": "burak.duran@mercedes-benz.com",
                    "ADFTriggerName": "Daily_1am",
                    "FwkLayerId": "Dimensional"
                }
            }
        ]
    }
]
```
