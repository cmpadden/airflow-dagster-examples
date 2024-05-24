import logging

import dspy
from airflow2dagster.constants import PLACEHOLDER_MODULE_FILENAME
from airflow2dagster.utils import (
    combine_code_snippets,
    extract_code_block_from_markdown,
)
from metrics.run_validity import is_runnable
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Output(BaseModel):
    has_schedule: bool


class DetectScheduleSignature(dspy.Signature):
    """Does the following Airflow DAG contain a schedule?"""

    # Assumption: There is only one input DAG

    airflow_code = dspy.InputField(desc="Airflow code containing schedule")
    output: Output = dspy.OutputField(
        desc="Whether the Airflow code contains a schedule"
    )


class AddScheduleSignature(dspy.Signature):
    """
    Create an asset job over the Dagster assets and add a schedule to it.

    The schedule should be consistent with the Airflow code and uses `define_asset_job`.

    Do not generate the `Definitions` object or anything else.
    """

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    airflow_code = dspy.InputField(desc="Airflow code containing schedule")
    input_dagster_code = dspy.InputField(
        desc=f"Assume that it is located in a sibling Python module `{PLACEHOLDER_MODULE_FILENAME}`"
    )
    schedule_code = dspy.OutputField(
        desc=(
            "Dagster schedule code that is equivalent to Airflow's, in a separate file. "
            "The code imports the necessary objects from the input dagster code"
        )
    )


class AddScheduleModule(dspy.Module):
    def __init__(self):
        self.retrieve = dspy.Retrieve(k=3)
        self.detect_schedule = dspy.TypedPredictor(DetectScheduleSignature)
        self.add_schedule = dspy.ChainOfThought(AddScheduleSignature)

    def forward(self, airflow_code: str, input_dagster_code: str) -> dspy.Prediction:
        if not self.detect_schedule(airflow_code=airflow_code).output.has_schedule:
            logger.info(
                "No schedule detected in the Airflow code, so no schedule will be added to the translated Dagster code"
            )
            return dspy.Prediction(dagster_code=input_dagster_code)

        context = self.retrieve(self.add_schedule.signature.__doc__).passages

        pred = self.add_schedule(
            context="\n".join(context),
            airflow_code=airflow_code,
            input_dagster_code=input_dagster_code,
        )
        pred.schedule_code = extract_code_block_from_markdown(pred.schedule_code)

        dspy.Assert(
            "define_asset_job" in pred.schedule_code,
            "Use `define_asset_job` to create a job over the assets",
        )

        dspy.Assert(
            "Definitions" not in pred.schedule_code,
            "Do not generate the `Definitions` object. Only the asset jobs and schedules are needed",
        )

        concatenated_code = combine_code_snippets([input_dagster_code, pred.schedule_code])
        dspy.Suggest(
            *is_runnable(concatenated_code, verbose=True)
        )  # Some code cannot be run without external dependencies

        return dspy.Prediction(dagster_code=concatenated_code)
