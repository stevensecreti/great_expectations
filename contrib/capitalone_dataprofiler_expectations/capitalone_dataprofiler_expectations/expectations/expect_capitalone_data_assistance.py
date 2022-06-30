from typing import Optional, Any, Dict, List

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import Expectation
from great_expectations.expectations.metrics.metric_provider import (
    MetricConfiguration,
    metric_value,
    MetricDomainTypes
)
from ..metrics import DataProfilerMetricProvider

import dataprofiler as dp
import pandas as pd
import numpy as np
import tensorflow as tf

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations import execution_engine


class CapitaloneDataAssistanceProfileDiff(DataProfilerMetricProvider):

    metric_name = "capitalone.data_assistance.profile_diff"

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        runtime_configuration: Dict,
    ):
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )
        first_profile = metric_value_kwargs["profile"]
        
        threshold = metric_value_kwargs["threshold"]

        profiler_opts = dp.ProfilerOptions()
        profiler_opts.structured_options.multiprocess.is_enabled = False

        new_profile = dp.Profiler(df, options=profiler_opts)

        profile_diff = new_profile.diff(first_profile) #Results in diff of new_prof - first_prof
        #Values in this report indicate +/- change from old profile

        return profile_diff

        
        



    def __init__(
        self,
        profile: Optional[dp.Profiler],
        df: pd.DataFrame,
        config: Dict[str, Any],
        
    ):
        self.profile = profile

    def run(self):
        first_profile = self.profile
        new_profile = dp.Profiler(self.df)

        profile_diff = first_profile.diff(new_profile)

        for column in profile_diff['data_stats']:
            for metric in column:
                i = 1
                # iterate through all column metrics
                # and run expectation with either default / user-provided
                #       threshold value 

               # metric.['metric'].run() # instantiate the expectation for the columns' metric and run 

class ExpectCapitaloneDataAssistance(Expectation):
    examples = []

    metric_dependencies = (
        "capital_one.data_assistance.profile_diff"
    )

    #def validate_configuration()

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        profile_diff = metrics.get("capital_one.data_assistance.profile_diff")




# diff profile: looking at threshold absolute value below specific threshold
"""
"data_stats": [{
    "statistics": {
        "min": expect_column_value_to_be_less_than_or_equal_to,
        "max": expect_column_value_to_be_less_than_or_equal_to,
    }
}]
"""