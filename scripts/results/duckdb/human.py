from dagster import asset, MaterializeResult, AssetExecutionContext, MetadataValue
from dagster_duckdb import DuckDBResource

from pathlib import Path

import pandas as pd

BASE_PATH = Path("../data/")

duckdb_resource = DuckDBResource(
    database=BASE_PATH / "duckdb.db"
)


@asset
def parquet_file():
    """A table about ducks, saved as a parquet file."""
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
    df_parquet = df.to_parquet()

    # write the parquet file
    file = BASE_PATH / "duck_info.parquet"
    file.touch()
    file.write_bytes(df_parquet)

@asset
def duckdb_table(duckdb: DuckDBResource):
    """A table about ducks, saved in a DuckDB database."""
    with duckdb.get_connection() as conn:
        conn.execute(
            f"CREATE OR REPLACE TABLE ducks_table AS SELECT * FROM read_parquet('{BASE_PATH / 'duck_info.parquet'}')"
        )
        conn.close()

@asset
def high_quacking_ducks(context: AssetExecutionContext, duckdb: DuckDBResource) -> MaterializeResult:
    """Ducks that quack more than 15 times per hour."""
    
    with duckdb.get_connection() as conn:
        ducks_info = conn.execute(
            f"SELECT * FROM ducks_table WHERE num_quacks_per_hour > 15"
        ).fetchdf()
        conn.close()

    context.log.info(ducks_info)

    return MaterializeResult(
        metadata={
            "ducks_info": MetadataValue.md(ducks_info.head().to_markdown())
        }
    )