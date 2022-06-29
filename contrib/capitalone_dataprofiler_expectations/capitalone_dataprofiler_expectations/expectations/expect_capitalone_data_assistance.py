IDEA:
    df.expect_capitalone_data_assistance(
        base_truth_profile,
        config={
            "min.threshold": 10,
        }
    )


class ExpectCapitaloneDataAssistance():

    def __init__(
        self,
        profile,
        df: pd.DataFrame
    ):
        self.profile = profile

    def run(self):
        first_profile = self.profile
        new_profile = Profile(df)

        profile_diff = first_profile.diff(new_profile)

        for column in profile_diff['data_stats']:
            for metric in column:
                # iterate through all column metrics
                # and run expectation with either default / user-provided
                #       threshold value 

                metric.['metric'].run() # instantiate the expectation for the columns' metric and run 



expect_datastat_bounded_by(
    column: str
    delta: Any
    stat: str (Represents one of the data statistics from profile)

)



# diff profile: looking at threshold absolute value below specific threshold
"data_stats": [{
    "statistics": {
        "min": expect_column_value_to_be_less_than_or_equal_to,
        "max": expect_column_value_to_be_less_than_or_equal_to,
    }
}]