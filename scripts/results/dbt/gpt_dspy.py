import logging
import os
import subprocess

from dagster import (
    AssetSelection,
    Config,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from pydantic import Field

# Configure logging
logging.basicConfig(level=logging.INFO)


class DbtRunConfig(Config):
    dbt_profiles_dir: str = Field(
        default="/path/to/profiles", description="Directory of the DBT profiles"
    )
    project_dir: str = Field(
        default="/usr/local/airflow/dags/dbt", description="DBT project directory"
    )
    profile: str = Field(default="default", description="DBT profile to use")
    target: str = Field(default="dev", description="DBT target environment")


@asset
def dbt_run_results(config: DbtRunConfig) -> MaterializeResult:
    try:
        # Set DBT profiles directory
        os.environ["DBT_PROFILES_DIR"] = config.dbt_profiles_dir
        dbt_command = [
            "dbt",
            "run",
            "--project-dir",
            config.project_dir,
            "--profile",
            config.profile,
            "--target",
            config.target,
        ]
        # Execute DBT command using subprocess to avoid sys.argv override
        result = subprocess.run(dbt_command, capture_output=True, text=True, check=True)
        logging.info("DBT run completed successfully")
        # Attach command output as metadata
        return MaterializeResult(
            metadata={
                "output": MetadataValue.text(result.stdout),
                "command": MetadataValue.text(" ".join(dbt_command)),
            }
        )
    except subprocess.CalledProcessError as e:
        logging.error(f"DBT run failed: {e.stderr}")
        # Attach error details as metadata
        return MaterializeResult(
            metadata={
                "error": MetadataValue.text(e.stderr),
                "command": MetadataValue.text(" ".join(dbt_command)),
            }
        )


# Define the asset job with retry policy
retry_policy = RetryPolicy(max_retries=2)
dbt_asset_job = define_asset_job(
    "dbt_asset_job",
    selection=AssetSelection.assets(dbt_run_results),
    op_retry_policy=retry_policy,
)

# Define the schedule for the job
dbt_job_schedule = ScheduleDefinition(
    job=dbt_asset_job,
    cron_schedule="0 0 * * *",  # Daily at midnight
    name="daily_dbt_run_schedule",
)

# Define the Dagster definitions including assets, jobs, and schedules
definitions = Definitions(
    assets=[dbt_run_results], jobs=[dbt_asset_job], schedules=[dbt_job_schedule]
)
