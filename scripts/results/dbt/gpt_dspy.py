from dagster import (
    asset,
    Config,
    MaterializeResult,
    MetadataValue,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
    RetryPolicy,
)
import os
import subprocess
import time
from pydantic import Field
from typing import List

from dagster import asset, Config, MaterializeResult, MetadataValue, Definitions, define_asset_job, ScheduleDefinition, AssetSelection, RetryPolicy
import os
import subprocess
import time
from pydantic import Field
from typing import List

class DBTConfig(Config):
    dbt_profiles_dir: str = Field(default='/path/to/profiles', description='Directory where DBT profiles are stored')
    dbt_project_dir: str = Field(default='/usr/local/airflow/dags/dbt', description='DBT project directory')
    dbt_profile: str = Field(default='default', description='DBT profile name')
    dbt_target: str = Field(default='dev', description='DBT target environment')
    dbt_args: List[str] = Field(default_factory=lambda: ['run'], description='Additional DBT command line arguments')

@asset
def dbt_project_execution(config: DBTConfig) -> MaterializeResult:
    # Set up the environment variables for DBT
    os.environ['DBT_PROFILES_DIR'] = config.dbt_profiles_dir
    # Prepare DBT arguments
    dbt_args = config.dbt_args + [
        '--project-dir', config.dbt_project_dir,
        '--profile', config.dbt_profile,
        '--target', config.dbt_target
    ]
    # Capture the start time
    start_time = time.time()
    # Execute the DBT project and capture the output
    result = subprocess.run(['dbt'] + dbt_args, capture_output=True, text=True)
    # Calculate execution time
    execution_time = time.time() - start_time
    # Prepare metadata
    metadata = {
        "execution_time": MetadataValue.float(execution_time),
        "output_log": MetadataValue.md(result.stdout)
    }
    return MaterializeResult(metadata=metadata)

# Define the retry policy
retry_policy = RetryPolicy(max_retries=2)

# Define the job with retry policy
dbt_job = define_asset_job(
    "dbt_job",
    selection=AssetSelection.assets(dbt_project_execution),
    op_retry_policy=retry_policy
)

# Define the schedule
dbt_schedule = ScheduleDefinition(
    job=dbt_job,
    cron_schedule="0 0 * * *",  # Daily at midnight
)

# Update Definitions object
defs = Definitions(
    assets=[dbt_project_execution],
    jobs=[dbt_job],
    schedules=[dbt_schedule],
)
