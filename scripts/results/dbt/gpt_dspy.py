
from dagster import asset, MaterializeResult, MetadataValue, Config, define_asset_job, ScheduleDefinition, Definitions, RetryPolicy
from pydantic import Field
from datetime import datetime
import os
import subprocess

class DbtRunConfig(Config):
    project_dir: str
    profiles_dir: str
    target: str
    profile_name: str
    dbt_executable_path: str

@asset(
    metadata={
        "schedule": "@daily",
        "start_date": datetime(2023, 1, 1),
        "catchup": False,
    },
    retry_policy=RetryPolicy(max_retries=2),
)
def dbt_run(context, config: DbtRunConfig) -> MaterializeResult:
    project_dir = config.project_dir
    profiles_dir = config.profiles_dir
    target = config.target
    profile_name = config.profile_name
    dbt_executable_path = config.dbt_executable_path

    os.environ["DBT_PROFILES_DIR"] = profiles_dir
    os.environ["DBT_TARGET"] = target
    os.environ["DBT_PROFILE"] = profile_name

    dbt_args = [
        dbt_executable_path,
        "run",
        "--project-dir", project_dir,
        "--profiles-dir", profiles_dir,
        "--target", target,
    ]

    result = subprocess.run(dbt_args, capture_output=True, text=True)
    context.log.info(f"DBT run succeeded: {result.stdout}")

    if result.returncode != 0:
        context.log.error(f"DBT run failed: {result.stderr}")
        raise Exception(f"DBT run failed: {result.stderr}")

    # Attach relevant metadata to the asset
    metadata = {
        "dbt_output": MetadataValue.text(result.stdout),
        "dbt_error": MetadataValue.text(result.stderr) if result.returncode != 0 else MetadataValue.text("No errors"),
        "execution_time": MetadataValue.float(result.elapsed.total_seconds())
    }

    return MaterializeResult(metadata=metadata)

# Example configuration for the asset
dbt_run_config = DbtRunConfig(
    project_dir="/usr/local/airflow/dags/dbt",
    profiles_dir="/path/to/profiles",
    target="dev",
    profile_name="default",
    dbt_executable_path=f"{os.environ.get('AIRFLOW_HOME', '/default/path')}/dbt_venv/bin/dbt",
)

# Define the job that will materialize the asset
dbt_run_job = define_asset_job("dbt_run_job", selection=[dbt_run])

# Define the schedule for the job
dbt_run_schedule = ScheduleDefinition(
    job=dbt_run_job,
    cron_schedule="0 0 * * *",  # daily at midnight
)

# Definitions object to include the asset, job, and schedule
defs = Definitions(
    assets=[dbt_run],
    jobs=[dbt_run_job],
    schedules=[dbt_run_schedule],
)
