import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable


class IdentifyDagsterIntegrationSignature(dspy.Signature):
    """Identifies whether a Dagster integration (other than `dagster_airflow`) exists for the code."""

    code = dspy.InputField()
    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    answer = dspy.OutputField(desc="Strictly 'Yes' or 'No'")


class AddDagsterIntegrationSignature(dspy.Signature):
    """Add a Dagster integration to the code."""

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    input_dagster_code = dspy.InputField()
    dagster_code = dspy.OutputField(
        desc="Input dagster code modified with the integration"
    )


class AddDagsterIntegrationModule(dspy.Module):
    def __init__(self):
        self.retrieve = dspy.Retrieve(k=5)
        self.integration_checker = dspy.ChainOfThought(
            IdentifyDagsterIntegrationSignature
        )
        self.add_integration = dspy.ChainOfThought(AddDagsterIntegrationSignature)

    def forward(self, input_dagster_code: str) -> dspy.Prediction:
        context = self.retrieve(self.integration_checker.signature.__doc__).passages
        integration_exists = self.integration_checker(
            context="\n".join(context), code=input_dagster_code
        )

        dspy.Assert(
            integration_exists.answer in ("Yes", "No"),
            'Answer must be strictly "Yes" or "No".',
        )

        if integration_exists.answer == "No":
            return dspy.Prediction(context=context, dagster_code=input_dagster_code)

        pred = self.add_integration(
            context="\n".join(context), input_dagster_code=input_dagster_code
        )
        pred.dagster_code = extract_code_block_from_markdown(pred.dagster_code)
        dspy.Assert(
            "@asset" in pred.dagster_code,
            "Do not forget to include the input Dagster code in the final code.",
        )
        dspy.Suggest(
            *is_runnable(pred.dagster_code, verbose=True)
        )  # Some code cannot be run without external dependencies

        return dspy.Prediction(
            context="\n".join(context), dagster_code=pred.dagster_code
        )
