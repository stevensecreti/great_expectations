from profiler_config_generator import ProfilerConfigGenerator, Rule, saveProfileConfigString

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.rule_based_profiler import RuleBasedProfilerResult
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler

import dataprofiler as dp
import pandas as pd
import numpy as np
import tensorflow as tf

from typing import List

from ruamel import yaml

context = ge.get_context()

from expectations.expect_column_values_to_be_equal_to_or_greater_than_profile_min import ExpectColumnValuesToBeEqualToOrGreaterThanProfileMin
from expectations.expect_column_values_to_be_equal_to_or_less_than_profile_max import ExpectColumnValuesToBeEqualToOrLessThanProfileMax


datasource_config = {
    "name": "df2",
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
    },
}
context.add_datasource(**datasource_config) 

data = [
    [-36, -25, -44],
    [18, 45, 46],
    [-16, -29, -49],
    [21, 4, 35],
    [-18, -7, -40],
    [22, -4, -37],
    [-17, -21, 11],
    [48, -32, -48],
    [0, -44, 20],
]
cols = ["col_a", "col_b", "col_c"]

df = pd.DataFrame(data, columns=cols)
profile_options = dp.ProfilerOptions()
profile_options.structured_options.multiprocess.is_enabled = False

profileObj = dp.Profiler(df, options=profile_options)
profileReport = profileObj.report(report_options={"output_format": "compact"})

variables = {
    "profileReport": profileReport,
}

domain = {
    "class_name": "ColumnDomainBuilder",
    "include_semantic_types": """
                - numeric
    """,
}
parameters = {}
expectations = {
    "expect_column_values_to_be_equal_to_or_greater_than_profile_min": {
        "class_name": "DefaultExpectationConfigurationBuilder",
        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
        "column": "$domain.domain_kwargs.column",
        "profile": "$variables.profileReport",
    },
    "expect_column_values_to_be_equal_to_or_less_than_profile_max": {
        "class_name": "DefaultExpectationConfigurationBuilder",
        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
        "column": "$domain.domain_kwargs.column",
        "profile": "$variables.profileReport",
    },
}

rule1 = Rule(
    domain=domain,
    parameters=parameters,
    expectations=expectations
)

rules = {
    "My_Testing_Rule": rule1
}

pfg = ProfilerConfigGenerator(
    name="Test_Profile_Config_1",
    variables=variables,
    rules=rules,
)

profile_config_string = pfg.generate_profiler_config()

full_profiler_config_dict: dict = yaml.load(profile_config_string, Loader=yaml.Loader)
rule_based_profiler: RuleBasedProfiler = RuleBasedProfiler(
    name=full_profiler_config_dict["name"],
    config_version=full_profiler_config_dict["config_version"],
    rules=full_profiler_config_dict["rules"],
    variables=full_profiler_config_dict["variables"],
    data_context=context
)

data2 = [
    [-36, -25, -44],
    [18, 42, 41],
    [-14, 1009, -49],
    [21, 4, 34],
    [-18, -7, -40],
    [22, -4, -37],
]

df2 = pd.DataFrame(data=data2, columns=cols)

batch_request = RuntimeBatchRequest(
    datasource_name="df2",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="test2",
    runtime_parameters={"batch_data": df2},
    batch_identifiers={"default_identifier_name": "BR1"},
)

result: RuleBasedProfilerResult = rule_based_profiler.run(batch_request=batch_request)
expectation_configurations: List[
    ExpectationConfiguration
] = result.expectation_configurations


expectationSuite = context.create_expectation_suite("column_bounds_suite", overwrite_existing=True)

expectationSuite.add_expectation_configurations(expectation_configurations=expectation_configurations)

context.save_expectation_suite(expectationSuite)

checkpoint_config = {
    "name": "my_missing_batch_request_checkpoint",
    "config_version": 1,
    "class_name": "SimpleCheckpoint",
    "expectation_suite_name": "column_bounds_suite",
}
context.add_checkpoint(**checkpoint_config)

print("About to run checkpoint...")
results = context.run_checkpoint(
    checkpoint_name="my_missing_batch_request_checkpoint",
    validations=[
        {"batch_request": batch_request},
    ]
)

keys = list(results["run_results"].keys())
individualResults = results["run_results"][keys[0]]["validation_result"]["results"]

print("\n-----Individual Results-----")
failingCols = {}
for res in individualResults:
    wasSuccess = res["success"]

    expConfig = res["expectation_config"]

    column = expConfig["kwargs"]["column"]

    expType = expConfig["expectation_type"]

    print("\nExpectation: " + expType + " Column: " + column + " Success: ", str(wasSuccess))
    if not wasSuccess:
        if not column in failingCols:
            failingCols[column] = 1
        else:
            failingCols[column] += 1
        failureInfo = res["result"]
        unexpectedPercent = round(failureInfo["unexpected_percent"], 2)
        partialList = failureInfo["partial_unexpected_list"]
        print("Percent of Unexpected Values: {}%, Partial List of Unexpected Values: {}".format(unexpectedPercent, partialList))

print("\n-----Failing Columns-----")
for key, val in failingCols.items():
    print("Column: {}, # of Failed Expectations: {}".format(key, val))