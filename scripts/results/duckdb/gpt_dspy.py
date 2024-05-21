import duckdb
import pandas as pd
from dagster import (
    AssetSelection,
    Config,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from pydantic import Field


class DuckInfoParquetConfig(Config):
    file_path: str = Field(
        default="include/duck_info.parquet", description="Path to save the parquet file"
    )
    database: str = Field(
        default="include/duckdb.db", description="DuckDB database path"
    )


@asset
def duck_info_parquet(config: DuckInfoParquetConfig) -> MaterializeResult:
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
    df.to_parquet(config.file_path)
    conn = duckdb.connect(database=config.database)
    conn.execute(
        f"CREATE OR REPLACE TABLE ducks_table AS SELECT * FROM read_parquet('{config.file_path}')"
    )
    conn.close()
    metadata = {
        "rows": MetadataValue.int(df.shape[0]),
        "columns": MetadataValue.int(df.shape[1]),
        "file_path": MetadataValue.path(config.file_path),
    }
    return MaterializeResult(metadata=metadata)


class FilteredDucksInfoConfig(Config):
    database: str = Field(
        default="include/duckdb.db", description="DuckDB database path"
    )
    quack_threshold: int = Field(
        default=15, description="Minimum number of quacks per hour to filter"
    )


@asset
def filtered_ducks_info(config: FilteredDucksInfoConfig) -> MaterializeResult:
    conn = duckdb.connect(database=config.database)
    ducks_info = conn.execute(
        f"SELECT * FROM ducks_table WHERE num_quacks_per_hour > {config.quack_threshold}"
    ).fetchdf()
    conn.close()
    metadata = {
        "filtered_rows": MetadataValue.int(ducks_info.shape[0]),
        "total_quacks": MetadataValue.int(ducks_info["num_quacks_per_hour"].sum()),
    }
    return MaterializeResult(metadata=metadata)


asset_job = define_asset_job("daily_duck_info_job", AssetSelection.all())

job_schedule = ScheduleDefinition(
    job=asset_job,
    cron_schedule="0 0 * * 0",  # Every Sunday at midnight
)

defs = Definitions(
    assets=[duck_info_parquet, filtered_ducks_info],
    schedules=[job_schedule],
    jobs=[asset_job],
)
