import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable


class AddAssetChecksSignature(dspy.Signature):
    """Translate Airflow checks into Dagster asset checks"""

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    airflow_code = dspy.InputField(desc="Airflow code containing check operators")
    input_dagster_code = dspy.InputField(
        desc="Dagster code containing assets but without asset checks"
    )
    dagster_code = dspy.OutputField(
        desc="Dagster code with equivalent asset check logic to Airflow's"
    )


class AddAssetCheckModule(dspy.Module):
    def __init__(self):
        self.retrieve = dspy.Retrieve(k=3)
        self.add_asset_check = dspy.ChainOfThought(AddAssetChecksSignature)

    def forward(self, airflow_code: str, input_dagster_code: str) -> dspy.Prediction:
        context = self.retrieve(self.add_asset_check.signature.__doc__).passages
        pred = self.add_asset_check(
            context="\n".join(context),
            airflow_code=airflow_code,
            input_dagster_code=input_dagster_code,
        )
        pred.dagster_code = extract_code_block_from_markdown(pred.dagster_code)

        dspy.Assert(
            "@asset" in pred.dagster_code,
            "Do not forget to include the input Dagster code in the final code.",
        )
        dspy.Suggest(
            *is_runnable(pred.dagster_code, verbose=True)
        )  # Some code cannot be run without external dependencies

        return dspy.Prediction(dagster_code=pred.dagster_code)
