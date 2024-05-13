
from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    DefaultScheduleStatus,
    asset,
    MaterializeResult,
    MetadataValue
)
import pandas as pd
import duckdb

# Define the assets
@asset
def duck_info():
    data = {
        "name": ["Mallard", "Pekin", "Muscovy", "Rouen", "Indian Runner"],
        "country_of_origin": ["North America", "China", "South America", "France", "India"],
        "job": ["Friend", "Friend, Eggs", "Friend, Pest Control", "Friend, Show", "Eggs, Friend"],
        "num": [3, 5, 6, 2, 3],
        "num_quacks_per_hour": [10, 20, 25, 5, 44],
    }
    df = pd.DataFrame(data)
    file_path = "file://include/duck_info.parquet"
    df.to_parquet(file_path)
    conn = duckdb.connect(database="include/duckdb.db")
    conn.execute(f"CREATE OR REPLACE TABLE ducks_table AS SELECT * FROM read_parquet('{file_path}')")
    conn.close()
    return MaterializeResult(
        value=df,
        metadata={
            "file_path": MetadataValue.path(file_path),
            "num_rows": MetadataValue.int(len(df))
        }
    )

@asset
def filtered_duck_info(duck_info):
    query = "SELECT * FROM ducks_table WHERE num_quacks_per_hour > 15"
    conn = duckdb.connect(database="include/duckdb.db")
    ducks_info = conn.execute(query).fetchdf()
    conn.close()
    return MaterializeResult(
        value=ducks_info,
        metadata={
            "query": MetadataValue.text(query),
            "num_rows_filtered": MetadataValue.int(len(ducks_info))
        }
    )

# Load all assets
all_assets = [duck_info, filtered_duck_info]

# Define a job that will materialize the assets
duckdb_job = define_asset_job("duckdb_job", selection=AssetSelection.all())

# Define a schedule for the job with a cron schedule matching the Airflow schedule
duckdb_schedule = ScheduleDefinition(
    job=duckdb_job,
    cron_schedule="0 0 * * 0",  # every Sunday at midnight
    default_status=DefaultScheduleStatus.RUNNING  # Automatically start the schedule
)

# Combine definitions into a single Definitions object
defs = Definitions(
    assets=all_assets,
    jobs=[duckdb_job],
    schedules=[duckdb_schedule]
)
