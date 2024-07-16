Test pipelines post GTE migration #3

# adf

Develop and deploy metadata driven data management framework for BI transformation.

# Overview

This repository contains the Data Factory Application resources to develop onboarding workflow in DEV and deploy it to INT as well as PRD. The repository is directly linked with  using the GIT integration feature of Azure Data Factory (https://docs.microsoft.com/en-us/azure/data-factory/source-control). The 'master' branch is protected. All pull requests need to be approved by code owners.

# Organization of the repository   

* 'dataset', 'factory' ,'linkedService' , 'pipeline' , 'integrationRuntime', 'triggers' contains data factory resources and those folders are created/updated while developing directly in the data factory interface
* 'package.json' contains reference to the Microsoft data factories utilities used to validate the factory content and create ARM templates for deployment
* 'arm-template-parameters-definition.json' contains the parametarization template that drives settings of the data factory resources that need to be parameterized for deployment
* 'build_and_deploy.yaml' contains the CI pipeline that runs for every commit on feature branch, pull request or merge to master
* 'ArmTemplate' contains the ARM templates for deployment created by the CI pipeline, using the NPM package

# How to contribute

If changes are needed to the data factory, follow the steps below:

1. Create a feature branch out of master. This can be done directly in the ADF interface.
2. Do the changes you need. After every change, the CI pipeline will run to validate the changes and create fresh ARM template describing the factory content.
3. Do a pull request to 'master'. This will trigger the CI pipeline again. This time, the CI pipeline will also deploy on DEV again, directly to the data factory live service. A successful run of the CI pipeline is a prerequisite to merge the pull request.
4. Merge the pull request. This will trigger the CI pipeline again, this time deploying all changes on both INT and PROD.



