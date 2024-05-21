import logging

import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
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
    Create an asset job over the assets and add a schedule to it.

    The schedule should be consistent with the Airflow code and uses `define_asset_job`.

    Do not modify the input code.

    Example:
        from dagster import asset, define_asset_job, AssetSelection, ScheduleDefinition, Definitions

        @asset
        def my_asset():
            ...

        asset_job = define_asset_job("my_asset_job", AssetSelection.assets(my_asset))

        job_schedule = ScheduleDefinition(
            job=asset_job,
            cron_schedule="0 0 * * *",
        )

        definitions = Definitions(
            assets=[my_asset],
            schedules=[job_schedule],
            jobs=[asset_job]
        )
    """

    airflow_code = dspy.InputField(desc="Airflow code containing schedule")
    input_dagster_code = dspy.InputField(desc="Dagster code without schedule")
    dagster_code = dspy.OutputField(
        desc="Input Dagster code with similar schedule to Airflow code, as a single file"
    )


class AddScheduleModule(dspy.Module):
    def __init__(self):
        self.detect_schedule = dspy.TypedPredictor(DetectScheduleSignature)
        self.add_schedule = dspy.ChainOfThought(AddScheduleSignature)

    def forward(self, airflow_code: str, input_dagster_code: str) -> dspy.Prediction:
        if not self.detect_schedule(airflow_code=airflow_code).output.has_schedule:
            logger.info(
                "No schedule detected in the Airflow code, so no schedule will be added to the translated Dagster code"
            )
            return dspy.Prediction(dagster_code=input_dagster_code)

        pred = self.add_schedule(
            airflow_code=airflow_code,
            input_dagster_code=input_dagster_code,
        )
        pred.dagster_code = extract_code_block_from_markdown(pred.dagster_code)

        dspy.Assert(
            "define_asset_job" in pred.dagster_code,
            "Use `define_asset_job` to create a job over the assets",
        )
        dspy.Assert(
            "@asset" in pred.dagster_code,
            "Do not forget to include the input Dagster code in the final code.",
        )
        dspy.Suggest(
            *is_runnable(pred.dagster_code, verbose=True)
        )  # Some code cannot be run without external dependencies

        return dspy.Prediction(dagster_code=pred.dagster_code)
