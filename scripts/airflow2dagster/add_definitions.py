import dspy
from airflow2dagster.utils import (
    combine_code_snippets,
    extract_code_block_from_markdown,
)
from metrics.run_validity import is_runnable


class AddDefinitionsSignature(dspy.Signature):
    """
    Create a global Definitions object that accepts all Dagster assets, schedules, jobs, sensors and asset checks.

    If any of these categories are absent in the input code, do not include them in the Definitions object.

    Do not use the legacy `@repository` API.
    """

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    input_dagster_code = dspy.InputField(
        desc="Assume that the input dagster code is located in a sibling Python module `dagster_code.py`"
    )
    dagster_code = dspy.OutputField(
        desc=(
            "Dagster code containing the `dagster.Definitions`, in a separate file. "
            "The code imports the necessary objects from the input dagster code"
        )
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
        pred.definitions_code = extract_code_block_from_markdown(pred.definitions_code)

        dspy.Assert(
            "= Definitions(" in pred.definitions_code,
            "All created Dagster objects should be added to a global `Definitions` object. Do not use the legacy `@repository` syntax.",
        )
        code_cat = combine_code_snippets([input_dagster_code, pred.definitions_code])

        dspy.Suggest(
            *is_runnable(code_cat, verbose=True)
        )  # Some code cannot be run without external dependencies

        return dspy.Prediction(dagster_code=pred.definitions_code)
