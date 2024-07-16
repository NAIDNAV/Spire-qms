# Databricks notebook source
from typing import Type
from unittest.mock import MagicMock
import unittest
import warnings

# local environment imports
if "dbutils" not in globals():
    from notebooks.mdmf.tests.unitTests.mdmf.includes.dataLakeHelperTest import DataLakeHelperTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformationManagerTest import TransformationManagerTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformations.aggregateTest import AggregateTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformations.castTest import CastTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformations.deduplicateTest import DeduplicateTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformations.deriveColumnTest import DeriveColumnTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformations.filterTest import FilterTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformations.joinTest import JoinTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformations.pseudonymizeTest import PseudonymizeTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformations.renameColumnTest import RenameColumnTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformations.replaceNullTest import ReplaceNullTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformations.selectTest import SelectTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformations.selectExpressionTest import SelectExpressionTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.transformations.sqlQueryTest import SqlQueryTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.validatorTest import ValidatorTest
    from notebooks.mdmf.tests.unitTests.mdmf.etl.workflowManagerTest import WorkflowManagerTest
    from notebooks.mdmf.tests.unitTests.mdmf.generator.preEtlConfigGeneratorManagerTest import PreEtlConfigGeneratorManagerTest
    from notebooks.mdmf.tests.unitTests.mdmf.generator.etlConfigGeneratorManagerTest import EtlConfigGeneratorManagerTest
    from notebooks.mdmf.tests.unitTests.mdmf.generator.configGenerators.configGeneratorHelperTest import ConfigGeneratorHelperTest
    from notebooks.mdmf.tests.unitTests.mdmf.generator.configGenerators.metadataIngestionConfigGeneratorTest import MetadataIngestionConfigGeneratorTest
    from notebooks.mdmf.tests.unitTests.mdmf.generator.configGenerators.ingestionConfigGeneratorTest import IngestionConfigGeneratorTest
    from notebooks.mdmf.tests.unitTests.mdmf.generator.configGenerators.transformationsConfigGeneratorTest import TransformationsConfigGeneratorTest
    from notebooks.mdmf.tests.unitTests.mdmf.generator.configGenerators.modelTransformationsConfigGeneratorTest import ModelTransformationsConfigGeneratorTest
    from notebooks.mdmf.tests.unitTests.mdmf.generator.configGenerators.exportConfigGeneratorTest import ExportConfigGeneratorTest
    from notebooks.mdmf.tests.unitTests.mdmf.maintenance.maintenanceManagerTest import MaintenanceManagerTest
    from notebooks.mdmf.tests.unitTests.mdmf.tests.configurationTests.fileValidatorManagerTest import FileValidatorManagerTest

# COMMAND ----------

# MAGIC %run ./mdmf/includes/dataLakeHelperTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformationManagerTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformations/aggregateTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformations/castTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformations/deduplicateTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformations/deriveColumnTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformations/filterTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformations/joinTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformations/pseudonymizeTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformations/renameColumnTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformations/replaceNullTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformations/selectTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformations/selectExpressionTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/transformations/sqlQueryTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/validatorTest

# COMMAND ----------

# MAGIC %run ./mdmf/etl/workflowManagerTest

# COMMAND ----------

# MAGIC %run ./mdmf/generator/preEtlConfigGeneratorManagerTest

# COMMAND ----------

# MAGIC %run ./mdmf/generator/etlConfigGeneratorManagerTest

# COMMAND ----------

# MAGIC %run ./mdmf/generator/configGenerators/configGeneratorHelperTest

# COMMAND ----------

# MAGIC %run ./mdmf/generator/configGenerators/metadataIngestionConfigGeneratorTest

# COMMAND ----------

# MAGIC %run ./mdmf/generator/configGenerators/ingestionConfigGeneratorTest

# COMMAND ----------

# MAGIC %run ./mdmf/generator/configGenerators/transformationsConfigGeneratorTest

# COMMAND ----------

# MAGIC %run ./mdmf/generator/configGenerators/modelTransformationsConfigGeneratorTest

# COMMAND ----------

# MAGIC %run ./mdmf/generator/configGenerators/exportConfigGeneratorTest

# COMMAND ----------

# MAGIC %run ./mdmf/maintenance/maintenanceManagerTest

# COMMAND ----------

# MAGIC %run ./mdmf/tests/configurationTests/fileValidatorManagerTest

# COMMAND ----------

testCases = set([
    DataLakeHelperTest,
    TransformationManagerTest,
    AggregateTest,
    CastTest,
    DeduplicateTest,
    DeriveColumnTest,
    FilterTest,
    JoinTest,
    PseudonymizeTest,
    RenameColumnTest,
    ReplaceNullTest,
    SelectTest,
    SelectExpressionTest,
    SqlQueryTest,
    ValidatorTest,
    WorkflowManagerTest,
    PreEtlConfigGeneratorManagerTest,
    EtlConfigGeneratorManagerTest,
    ConfigGeneratorHelperTest,
    MetadataIngestionConfigGeneratorTest,
    IngestionConfigGeneratorTest,
    TransformationsConfigGeneratorTest,
    ModelTransformationsConfigGeneratorTest,
    ExportConfigGeneratorTest,
    MaintenanceManagerTest,
    FileValidatorManagerTest
])

# COMMAND ----------

def makeSuite(loader, testCases):
    suite = unittest.TestSuite()
    for testClass in testCases:
        tests = loader.loadTestsFromTestCase(testClass)
        suite.addTests(tests)
    return suite

def runTests(suite: Type[unittest.TestSuite]):
    runner = unittest.TextTestRunner()
    run = runner.run(suite)
    return run

# COMMAND ----------

# suppress print and display so test classes do not print messages or display test data
# suppress deprecated warnings from great_expectations library
print = MagicMock()
display = MagicMock()
warnings.filterwarnings("ignore", category=DeprecationWarning)

suite = makeSuite(unittest.defaultTestLoader, testCases)
runResult = runTests(suite)

# COMMAND ----------

runResult.wasSuccessful()
