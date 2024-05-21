
from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    DefaultScheduleStatus,
    asset,
    Config,
    MaterializeResult,
    MetadataValue
)
import pandas as pd
import duckdb
from pydantic import Field

# Define the configuration for the DuckInfo asset
class DuckInfoConfig(Config):
    file_path: str = Field(default="file://include/duck_info.parquet", description="Path to save the parquet file")
    database_path: str = Field(default="include/duckdb.db", description="Path to the DuckDB database")

@asset
def duck_info(config: DuckInfoConfig):
    data = {
        "name": ["Mallard", "Pekin", "Muscovy", "Rouen", "Indian Runner"],
        "country_of_origin": ["North America", "China", "South America", "France", "India"],
        "job": ["Friend", "Friend, Eggs", "Friend, Pest Control", "Friend, Show", "Eggs, Friend"],
        "num": [3, 5, 6, 2, 3],
        "num_quacks_per_hour": [10, 20, 25, 5, 44],
    }
    df = pd.DataFrame(data)
    df.to_parquet(config.file_path)
    
    conn = duckdb.connect(database=config.database_path)
    conn.execute(f"CREATE OR REPLACE TABLE ducks_table AS SELECT * FROM read_parquet('{config.file_path}')")
    conn.close()
    return MaterializeResult(
        value=df,
        metadata={
            "file_path": MetadataValue.path(config.file_path),
            "num_rows": MetadataValue.int(len(df))
        }
    )

# Define the configuration for the FilteredDuckInfo asset
class FilteredDuckInfoConfig(Config):
    quack_threshold: int = Field(default=15, description="Minimum number of quacks per hour for filtering")

# Asset to filter data from DuckDB
@asset
def filtered_duck_info(duck_info, config: FilteredDuckInfoConfig):
    query = f"SELECT * FROM ducks_table WHERE num_quacks_per_hour > {config.quack_threshold}"
    conn = duckdb.connect(database="include/duckdb.db")
    ducks_info = conn.execute(query).fetchdf()
    conn.close()
    
    # Add metadata
    metadata = {
        "database": MetadataValue.path(config.database_path),
        "table": MetadataValue.text(config.table_name)
    }
    
    context.log.info(f"Data ingested into DuckDB table {config.table_name} from {duck_info_parquet}.")
    return metadata

# Load assets from the current module
all_assets = load_assets_from_modules([__name__])

# Define a job that will materialize the assets
duck_job = define_asset_job("duck_job", selection=AssetSelection.all())

# Define a schedule for the job, matching the Airflow schedule of "0 0 * * 0"
duckdb_schedule = ScheduleDefinition(
    job=duckdb_job,
    cron_schedule="0 0 * * 0",  # every Sunday at midnight
    default_status=DefaultScheduleStatus.RUNNING  # Automatically start the schedule when deployed
)

# Combine all definitions
defs = Definitions(
    assets=[duck_info_parquet, duckdb_ingestion, filtered_duck_info],
    jobs=[duck_job],
    schedules=[duck_schedule],
)
