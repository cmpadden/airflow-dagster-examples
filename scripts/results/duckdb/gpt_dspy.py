
from dagster import asset, MaterializeResult, MetadataValue, ScheduleDefinition, define_asset_job, repository, AssetSelection, Definitions
import pandas as pd
import duckdb
import os

@asset
def duck_data() -> MaterializeResult:
    data = {
        "name": ["Mallard", "Pekin", "Muscovy", "Rouen", "Indian Runner"],
        "country_of_origin": ["North America", "China", "South America", "France", "India"],
        "job": ["Friend", "Friend, Eggs", "Friend, Pest Control", "Friend, Show", "Eggs, Friend"],
        "num": [3, 5, 6, 2, 3],
        "num_quacks_per_hour": [10, 20, 25, 5, 44],
    }
    df = pd.DataFrame(data)
    return MaterializeResult(
        df,
        metadata={
            "num_rows": MetadataValue.int(len(df)),
            "data_types": MetadataValue.json(df.dtypes.apply(lambda x: str(x)).to_dict())
        }
    )

@asset
def duck_info_parquet(duck_data) -> MaterializeResult:
    file_path = "file://include/duck_info.parquet"
    duck_data.to_parquet(file_path)
    file_size = os.path.getsize(file_path.replace("file://", ""))
    return MaterializeResult(
        file_path,
        metadata={
            "file_path": MetadataValue.path(file_path),
            "file_size": MetadataValue.int(file_size)
        }
    )

@asset(required_resource_keys={"io_manager"})
def duckdb_ingest(duck_info_parquet) -> MaterializeResult:
    conn = duckdb.connect(database="include/duckdb.db")
    result = conn.execute(f"CREATE OR REPLACE TABLE ducks_table AS SELECT * FROM read_parquet('{duck_info_parquet}')")
    conn.close()
    return MaterializeResult(
        None,
        metadata={
            "ingestion_status": MetadataValue.text("Completed" if result.success() else "Failed"),
            "records_ingested": MetadataValue.int(result.row_count())
        }
    )

@asset
def filtered_duck_info() -> MaterializeResult:
    conn = duckdb.connect(database="include/duckdb.db")
    ducks_info = conn.execute("SELECT * FROM ducks_table WHERE num_quacks_per_hour > 15").fetchdf()
    conn.close()
    return MaterializeResult(
        ducks_info,
        metadata={
            "filter_criteria": MetadataValue.text("num_quacks_per_hour > 15"),
            "records_returned": MetadataValue.int(len(ducks_info))
        }
    )

# Define the job that includes all assets
duck_info_job = define_asset_job("duck_info_job", selection=AssetSelection.all())

# Define the schedule using a cron expression that matches the Airflow schedule
duck_info_schedule = ScheduleDefinition(
    job=duck_info_job,
    cron_schedule="0 0 * * 0",  # Every Sunday at midnight
    name="weekly_duck_info_schedule"
)

defs = Definitions(
    assets=[duck_data, duck_info_parquet, duckdb_ingest, filtered_duck_info],
    jobs=[duck_info_job],
    schedules=[duck_info_schedule],
)
