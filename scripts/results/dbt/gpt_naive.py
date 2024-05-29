Here's the equivalent Dagster code for the provided Airflow script. This code uses Dagster's asset-based approach to replicate the functionality of the Airflow DAG that runs a dbt project.

```python
from dagster import job, op, AssetIn, AssetOut, DailyPartitionsDefinition, Out
from dagster_dbt import dbt_cli_resource, dbt_run_op
import os
from datetime import datetime

# Define the dbt CLI resource configuration
dbt_cli_config = {
    "project_dir": "/usr/local/airflow/dags/dbt",
    "profiles_dir": "/usr/local/airflow/dags/dbt/profiles",
    "profile_target": "dev",
    "dbt_executable": f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
}

# Define an operation that sets up the dbt profile using environment variables
@op(out=Out(io_manager_key="io_manager"))
def setup_dbt_profile(context):
    os.environ["DBT_USER"] = "your_snowflake_user"
    os.environ["DBT_PASSWORD"] = "your_snowflake_password"
    os.environ["DBT_ACCOUNT"] = "your_snowflake_account"
    # Add other necessary environment variables
    context.log.info("DBT profile environment variables set.")

# Define the main job
@job(
    resource_defs={"dbt": dbt_cli_resource.configured(dbt_cli_config)},
    config={
        "ops": {
            "setup_dbt_profile": {"config": {"retries": 2}},
            "dbt_run": {"config": {"select": ["+your_model"]}}
        }
    },
    partitions_def=DailyPartitionsDefinition(start_date=datetime(2023, 1, 1))
)
def my_cosmos_dag():
    setup_dbt_profile()
    dbt_run_op()

# If you need to execute the job, you can uncomment the following line
# my_cosmos_dag.execute_in_process()
```

### Key Points:
1. **Resource Configuration**: The `dbt_cli_resource` is configured with paths and executable details similar to the Airflow setup.
2. **Environment Setup**: An operation `setup_dbt_profile` is used to configure environment variables necessary for dbt to connect to Snowflake, mimicking the profile setup in Airflow.
3. **DBT Run Operation**: The `dbt_run_op` from `dagster_dbt` is used to execute dbt commands. It's configured to run specific models or tests as needed.
4. **Job Definition**: The job is defined with resources and a partition definition to schedule it daily starting from January 1, 2023, similar to the Airflow DAG's schedule.
5. **Execution**: The job can be executed in process by uncommenting the last line, which is useful for testing locally.

This script is ready to be saved into a `.py` file and executed in a Dagster environment configured with the necessary dbt and Snowflake settings. Adjust the environment variable assignments and dbt model selections as per your specific project requirements.