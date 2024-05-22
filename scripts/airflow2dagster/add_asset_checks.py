import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable

relevant_documentation = """
### Asset Checks
Dagster assets can be used to check the validity of data. This is similar to Airflow's `CheckOperator`, which is used to check the validity of data before it is used in a pipeline. In Dagster, this can be done by adding a check to the asset function.

Asset checks can be added to an @asset-decorated function by adding an AssetCheckSpec object to the asset decorator for each test, creating an AssetCheckResult for each test, and returning the results as part of the check_results argument in the MaterializeResult object, or if not using MaterializeResult, yielding the AssetCheckResult objects.

Below is an example of how one might do an check in Airflow, and the equivalent in Dagster:

```
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.sensors.snowflake import SnowflakeCheckOperator
from datetime import datetime

# Define the DAG
dag = DAG(
    'snowflake_example_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
)

# Task to create Snowflake table
create_table = SnowflakeOperator(
    task_id='create_table',
    sql=\"""
        CREATE TABLE IF NOT EXISTS my_table (
            id INTEGER,
            name STRING,
            value INTEGER
        );
        INSERT INTO my_table (id, name, value) VALUES
        (1, 'A', 10), (2, 'B', 20), (3, 'C', 30);
    \""",
    snowflake_conn_id='my_snowflake_conn',
    dag=dag,
)

# Task to check the table
check_table = SnowflakeCheckOperator(
    task_id='check_table',
    sql="SELECT COUNT(*) FROM my_table WHERE value > 0;",
    snowflake_conn_id='my_snowflake_conn',
    dag=dag,
)

create_table >> check_table
```

Here is how it would look in Dagster. Ignore the logic about connecting to Snowflake, as that is not relevant to this example and we have already given you better practices for that in other steps:
    
```
from dagster import asset, AssetCheckSpec, AssetCheckResult, MetadataValue
import snowflake.connector

@asset
def snowflake_table_asset(context) -> AssetCheckResult:
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user='YOUR_USER',
        password='YOUR_PASSWORD',
        account='YOUR_ACCOUNT'
    )
    cursor = conn.cursor()

    # Create table and insert data
    cursor.execute(\"""
        CREATE TABLE IF NOT EXISTS my_table (
            id INTEGER,
            name STRING,
            value INTEGER
        );
        INSERT INTO my_table (id, name, value) VALUES
        (1, 'A', 10), (2, 'B', 20), (3, 'C', 30);
    \""")
    
    # Perform check
    cursor.execute("SELECT COUNT(*) FROM my_table WHERE value > 0;")
    result = cursor.fetchone()[0]

    # Close connection
    cursor.close()
    conn.close()

    # Return AssetCheckResult
    return AssetCheckResult(
        passed=result > 0,
        metadata={
            "row_count": MetadataValue.int(result),
        }
    )
```

"""

class AddAssetChecksSignature(dspy.Signature):
    """Translate Airflow checks into Dagster asset checks"""

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    airflow_code = dspy.InputField(desc="Airflow code containing check operators")
    input_dagster_code = dspy.InputField(
        desc="Dagster code containing assets but without asset checks"
    )
    dagster_code = dspy.OutputField(
        desc="Dagster code with equivalent asset check logic to Airflow's"
    )


class AddAssetCheckModule(dspy.Module):
    def __init__(self):
        self.retrieve = dspy.Retrieve(k=3)
        self.add_asset_check = dspy.ChainOfThought(AddAssetChecksSignature)

    def forward(self, airflow_code: str, input_dagster_code: str) -> dspy.Prediction:
        context = [
            self.retrieve(self.add_asset_check.signature.__doc__).passages,
            relevant_documentation,
        ]
        pred = self.add_asset_check(
            context="\n".join(context),
            airflow_code=airflow_code,
            input_dagster_code=input_dagster_code,
        )
        pred.dagster_code = extract_code_block_from_markdown(pred.dagster_code)

        dspy.Assert(
            "@asset" in pred.dagster_code,
            "Do not forget to include the input Dagster code in the final code.",
        )
        dspy.Suggest(
            *is_runnable(pred.dagster_code, verbose=True)
        )  # Some code cannot be run without external dependencies

        return dspy.Prediction(dagster_code=pred.dagster_code)
