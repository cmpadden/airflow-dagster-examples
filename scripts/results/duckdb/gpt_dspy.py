import duckdb
import pandas as pd
from dagster import (
    Config,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from pydantic import Field


class DuckInfoConfig(Config):
    file_path: str = Field(
        default="file://include/duck_info.parquet",
        description="Path to save the parquet file",
    )
    database_path: str = Field(
        default="include/duckdb.db", description="Path to the DuckDB database"
    )


@asset
def duck_info(config: DuckInfoConfig):
    """
    Create a DataFrame with duck information, save it as a parquet file, and ingest it into a DuckDB database.
    """
    data = {
        "name": ["Mallard", "Pekin", "Muscovy", "Rouen", "Indian Runner"],
        "country_of_origin": [
            "North America",
            "China",
            "South America",
            "France",
            "India",
        ],
        "job": [
            "Friend",
            "Friend, Eggs",
            "Friend, Pest Control",
            "Friend, Show",
            "Eggs, Friend",
        ],
        "num": [3, 5, 6, 2, 3],
        "num_quacks_per_hour": [10, 20, 25, 5, 44],
    }
    df = pd.DataFrame(data)
    # Save DataFrame as Parquet
    df.to_parquet(config.file_path)
    # Ingest into DuckDB
    conn = duckdb.connect(database=config.database_path)
    conn.execute(
        f"CREATE OR REPLACE TABLE ducks_table AS SELECT * FROM read_parquet('{config.file_path}')"
    )
    conn.close()
    return MaterializeResult(
        value=df,
        metadata={
            "file_path": MetadataValue.path(config.file_path),
            "num_rows": MetadataValue.int(len(df)),
        },
    )


class FilteredDuckInfoConfig(Config):
    quack_threshold: int = Field(
        default=15, description="Minimum number of quacks per hour for filtering"
    )


@asset
def filtered_duck_info(duck_info, config: FilteredDuckInfoConfig):
    """
    Read and return the DuckDB table with filtering based on the number of quacks per hour.
    """
    query = f"SELECT * FROM ducks_table WHERE num_quacks_per_hour > {config.quack_threshold}"
    conn = duckdb.connect(database="include/duckdb.db")
    ducks_info = conn.execute(query).fetchdf()
    conn.close()
    return MaterializeResult(
        value=ducks_info,
        metadata={
            "query": MetadataValue.text(query),
            "num_rows_filtered": MetadataValue.int(len(ducks_info)),
        },
    )


data_processing_job = define_asset_job(
    "data_processing_job", selection=[duck_info, filtered_duck_info]
)

weekly_schedule = ScheduleDefinition(
    job=data_processing_job,
    cron_schedule="0 0 * * 0",  # Every Sunday at midnight
    name="weekly_data_processing_schedule",
)


defs = Definitions(
    assets=[duck_info, filtered_duck_info],
    jobs=[data_processing_job],
    schedules=[weekly_schedule],
)
