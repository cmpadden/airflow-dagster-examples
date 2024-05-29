Here's the Dagster code that mirrors the functionality of the provided Airflow code. This code uses Dagster's asset-based approach and avoids using outdated APIs like `solid` or `ops`.

```python
from dagster import job, asset
import pandas as pd
import duckdb
from pathlib import Path

base_path = Path("include/")

@asset
def create_parquet_file():
    """Create a table about ducks and save it as a parquet file."""
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
    df_parquet = df.to_parquet(index=False)
    file_path = base_path / "duck_info.parquet"
    file_path.write_bytes(df_parquet)
    return file_path

@asset
def ingest_to_duckdb(file_path):
    """Ingest the parquet file into a duckdb database."""
    conn = duckdb.connect(database=str(base_path / "duckdb.db"))
    conn.register_filesystem(file_path.parent)
    conn.execute(
        f"CREATE OR REPLACE TABLE ducks_table AS SELECT * FROM read_parquet('{file_path}')"
    )
    conn.close()

@asset
def read_from_duckdb():
    """Read and return the duckdb table."""
    conn = duckdb.connect(database=str(base_path / "duckdb.db"))
    ducks_info = conn.execute(
        f"SELECT * FROM ducks_table WHERE num_quacks_per_hour > 15"
    ).fetchdf()
    print(ducks_info)
    return ducks_info

@job
def object_storage_load_file_to_duckdb():
    file_path = create_parquet_file()
    ingest_to_duckdb(file_path)
    read_from_duckdb()

```

This code defines three assets in Dagster, each corresponding to a task in the Airflow DAG. The `@job` decorator is used to define the workflow, which specifies the execution order of the assets. The assets interact with each other through their outputs and inputs, similar to how tasks are chained in Airflow. The use of `Path` from the `pathlib` module handles file paths, and the `duckdb` operations are adjusted to work with the local file system.