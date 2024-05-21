import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable
from pydantic import BaseModel, Field


class AddDefinitionsSignature(dspy.Signature):
    """
    Combine all assets into a global `Definitions` object.

    Do not use the legacy `@repository` API.
    Do not modify the input Dagster code. Only add a `Definitions` object, and nothing else.
    """

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    input_dagster_code = dspy.InputField(desc="Dagster code without schedule")
    dagster_code = dspy.OutputField(
        desc="Input Dagster code with similar schedule to Airflow code, as a single file"
    )


class Output(BaseModel):
    only_definition_is_added: bool = Field(
        description="Is the only difference the addition of a `Definitions` object?"
    )


class CheckOnlyDefinitionIsAddedSignature(dspy.Signature):
    """Check whether the only semantic difference between the input and output code is the addition of a `Definitions` object."""

    input_dagster_code = dspy.InputField(
        desc="Dagster code without `dagster.Definitions`"
    )
    output_dagster_code = dspy.InputField(
        desc="Input Dagster code with `dagster.Definitions`"
    )
    output: Output = dspy.OutputField()


class AddDefinitionsModule(dspy.Module):
    def __init__(self):
        self.retrieve = dspy.Retrieve(k=3)
        self.add_definitions = dspy.ChainOfThought(AddDefinitionsSignature)
        self.check_only_definition_added = dspy.TypedPredictor(
            CheckOnlyDefinitionIsAddedSignature
        )

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

        # Use LLM to check whether only a `Definitions` object is added
        pred_check = self.check_only_definition_added(
            input_dagster_code=input_dagster_code,
            output_dagster_code=pred.dagster_code,
        )
        dspy.Assert(
            pred_check.output.only_definition_is_added,
            "Ensure that *only* `dagster.Definitions` is added to the code.",
        )

        return dspy.Prediction(dagster_code=pred.dagster_code)
