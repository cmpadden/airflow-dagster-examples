import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable


class AddDefinitionsSignature(dspy.Signature):
    """
    Combine all assets into a global `Definitions` object.

    Do not use the legacy `@repository` API.
    """

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    input_dagster_code = dspy.InputField(desc="Dagster code without schedule")
    dagster_code = dspy.OutputField(
        desc="Input Dagster code with similar schedule to Airflow code, as a single file"
    )


class AddDefinitionsModule(dspy.Module):
    def __init__(self):
        self.retrieve = dspy.Retrieve(k=3)
        self.add_definitions = dspy.ChainOfThought(AddDefinitionsSignature)

    def forward(self, input_dagster_code: str) -> dspy.Prediction:
        context = self.retrieve(self.add_definitions.signature.__doc__).passages
        pred = self.add_definitions(
            context="\n".join(context),
            input_dagster_code=input_dagster_code,
        )
        pred.dagster_code = extract_code_block_from_markdown(pred.dagster_code)

        dspy.Assert(
            "@asset" in pred.dagster_code,
            "Do not forget to include the input Dagster code in the final code.",
        )
        dspy.Assert(
            "defs = Definitions" in pred.dagster_code,
            "All created Dagster objects should be added to a global `Definitions` object. Do not use the legacy `@repository` syntax.",
        )
        dspy.Suggest(
            *is_runnable(pred.dagster_code, verbose=True)
        )  # Some code cannot be run without external dependencies

        return dspy.Prediction(dagster_code=pred.dagster_code)
