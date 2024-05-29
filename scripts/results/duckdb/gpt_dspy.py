
from dagster import asset, MetadataValue, Config, Definitions, define_asset_job, ScheduleDefinition, AssetSelection, AssetExecutionContext
import pandas as pd
import duckdb
from pathlib import Path
from pydantic import Field

class DuckInfoParquetConfig(Config):
    file_path: str = Field(default="include/duck_info.parquet", description="Path to save the parquet file")

@asset
def duck_info_parquet(context: AssetExecutionContext, config: DuckInfoParquetConfig):
    """Create a table about ducks and save it as a parquet file."""
    data = {
        "name": ["Mallard", "Pekin", "Muscovy", "Rouen", "Indian Runner"],
        "country_of_origin": ["North America", "China", "South America", "France", "India"],
        "job": ["Friend", "Friend, Eggs", "Friend, Pest Control", "Friend, Show", "Eggs, Friend"],
        "num": [3, 5, 6, 2, 3],
        "num_quacks_per_hour": [10, 20, 25, 5, 44],
    }
    df = pd.DataFrame(data)
    file_path = Path(config.file_path)
    df.to_parquet(file_path)
    
    # Add metadata
    metadata = {
        "file_path": MetadataValue.path(file_path),
        "row_count": MetadataValue.int(len(df)),
        "columns": MetadataValue.text(", ".join(df.columns))
    }
    
    context.log.info(f"Parquet file created at {file_path} with {len(df)} rows.")
    return metadata

class DuckDBIngestionConfig(Config):
    database_path: str = Field(default="include/duckdb.db", description="Path to the DuckDB database")
    table_name: str = Field(default="ducks_table", description="Name of the table to create in DuckDB")

@asset
def duckdb_ingestion(context: AssetExecutionContext, config: DuckDBIngestionConfig, duck_info_parquet: Path):
    """Ingest the parquet file into a DuckDB database."""
    conn = duckdb.connect(database=config.database_path)
    conn.execute(f"CREATE OR REPLACE TABLE {config.table_name} AS SELECT * FROM read_parquet('{duck_info_parquet}')")
    conn.close()
    
    # Add metadata
    metadata = {
        "database": MetadataValue.path(config.database_path),
        "table": MetadataValue.text(config.table_name)
    }
    
    context.log.info(f"Data ingested into DuckDB table {config.table_name} from {duck_info_parquet}.")
    return metadata

class FilteredDuckInfoConfig(Config):
    database_path: str = Field(default="include/duckdb.db", description="Path to the DuckDB database")
    filter_condition: str = Field(default="num_quacks_per_hour > 15", description="SQL filter condition")

@asset
def filtered_duck_info(context: AssetExecutionContext, config: FilteredDuckInfoConfig):
    """Read and return the duckdb table with specific filter."""
    conn = duckdb.connect(database=config.database_path)
    ducks_info = conn.execute(f"SELECT * FROM ducks_table WHERE {config.filter_condition}").fetchdf()
    conn.close()
    
    # Add metadata
    metadata = {
        "row_count": MetadataValue.int(len(ducks_info)),
        "columns": MetadataValue.text(", ".join(ducks_info.columns))
    }
    
    context.log.info(f"Filtered data retrieved from DuckDB with condition '{config.filter_condition}'.")
    return metadata

# Define a job that will materialize the assets
duck_job = define_asset_job("duck_job", selection=AssetSelection.all())

# Define a schedule for the job
duck_schedule = ScheduleDefinition(
    job=duck_job,
    cron_schedule="0 0 * * 0",  # every Sunday at midnight
)

# Create the Definitions object
defs = Definitions(
    assets=[duck_info_parquet, duckdb_ingestion, filtered_duck_info],
    jobs=[duck_job],
    schedules=[duck_schedule],
)
