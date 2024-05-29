import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable


class AddRetryPolicySignature(dspy.Signature):
    """
    Add `RetryPolicy` to the Dagster asset. Do not use `@op`-based retries, or `op_*` parameters to set this retry policy.

    Retries can be added to an asset by using the `RetryPolicy` object. This object can be added to the asset using the `@asset` decorator. For example, notice how this retry policy is added to the function's retry_policy argument below:

    ```
    from dagster import RetryPolicy, asset

    @asset(retry_policy=RetryPolicy(max_retries=3))
    def my_asset():
        pass
    ```
    """

    airflow_code = dspy.InputField(desc="Airflow code containing retry requirements")
    input_dagster_code = dspy.InputField(desc="Dagster code without a retry policy")
    dagster_code = dspy.OutputField(
        desc="Dagster code with equivalent retry policy on the asset (job)"
    )


class AddRetryPolicyModule(dspy.Module):
    def __init__(self):
        self.add_retry = dspy.ChainOfThought(AddRetryPolicySignature)

    def forward(self, airflow_code: str, input_dagster_code: str) -> dspy.Prediction:
        pred = self.add_retry(
            airflow_code=airflow_code,
            input_dagster_code=input_dagster_code,
        )
        pred.dagster_code = extract_code_block_from_markdown(pred.dagster_code)

        dspy.Assert(
            "RetryPolicy" in pred.dagster_code,
            "Dagster code is missing a `RetryPolicy`.",
        )
        dspy.Assert(
            "@asset" in pred.dagster_code,
            "Do not forget to include the input Dagster code in the final code.",
        )
        dspy.Suggest(
            *is_runnable(pred.dagster_code, verbose=True)
        )  # Some code cannot be run without external dependencies

        return dspy.Prediction(dagster_code=pred.dagster_code)
