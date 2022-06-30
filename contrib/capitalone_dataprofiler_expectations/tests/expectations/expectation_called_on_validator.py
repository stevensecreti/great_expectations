import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations import execution_engine

from great_expectations.validator.validator import Validator
from great_expectations.core.batch import Batch

import dataprofiler as dp
import pandas as pd
import numpy as np
import tensorflow as tf

from typing import List

from ruamel import yaml
yaml = yaml.YAML(typ="safe")
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

data2 = [
    [-36, -25, -44],
    [18, 42, 41],
    [-14, -1009, -49],
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


print("Built data")

executionEngine = execution_engine.PandasExecutionEngine()
interactive_evaluation = False
print("build execution engine")

validatorObject = context.get_validator(batch_request=batch_request)

print("Built validator object", validatorObject)

print("ProfileReport: ", profileReport)

res = validatorObject.expect_column_values_to_be_equal_to_or_greater_than_profile_min(column="col_b", profile=profileReport)

print("ran validator", res)