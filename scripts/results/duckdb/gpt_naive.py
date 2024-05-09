Here's the Dagster code that replicates the functionality of the provided Airflow code using Dagster assets:

```python
from dagster import job, op, AssetIn, AssetOut, AssetMaterialization, IOManager, io_manager
from dagster import repository
import pandas as pd
import duckdb
import os
import pyarrow.parquet as pq

class LocalFileSystemIOManager(IOManager):
    def handle_output(self, context, obj):
        filepath = os.path.join(context.resource_config["base_path"], context.asset_key.path[-1])
        if isinstance(obj, pd.DataFrame):
            obj.to_parquet(filepath)
        elif isinstance(obj, duckdb.DuckDBPyConnection):
            # Assuming obj is a connection, this is a no-op for output handling.
            pass

    def load_input(self, context):
        filepath = os.path.join(context.upstream_output.resource_config["base_path"], context.asset_key.path[-1])
        if 'parquet' in filepath:
            return pd.read_parquet(filepath)
        return duckdb.connect(database=filepath)

@io_manager(config_schema={"base_path": str})
def local_file_system_io_manager(init_context):
    return LocalFileSystemIOManager()

@op(out={"parquet_file": AssetOut(io_manager_key="local_file_system_io_manager")})
def create_parquet_file():
    data = {
        "name": ["Mallard", "Pekin", "Muscovy", "Rouen", "Indian Runner"],
        "country_of_origin": ["North America", "China", "South America", "France", "India"],
        "job": ["Friend", "Friend, Eggs", "Friend, Pest Control", "Friend, Show", "Eggs, Friend"],
        "num": [3, 5, 6, 2, 3],
        "num_quacks_per_hour": [10, 20, 25, 5, 44],
    }
    df = pd.DataFrame(data)
    return df

@op(ins={"parquet_file": AssetIn()}, out={"duckdb_conn": AssetOut(io_manager_key="local_file_system_io_manager")})
def ingest_to_duckdb(parquet_file):
    conn = duckdb.connect(database="include/duckdb.db")
    conn.execute(f"CREATE OR REPLACE TABLE ducks_table AS SELECT * FROM '{parquet_file}'")
    return conn

@op(ins={"duckdb_conn": AssetIn()})
def read_from_duckdb(duckdb_conn):
    ducks_info = duckdb_conn.execute("SELECT * FROM ducks_table WHERE num_quacks_per_hour > 15").fetchdf()
    print(ducks_info)

@job(resource_defs={"local_file_system_io_manager": local_file_system_io_manager})
def object_storage_load_file_to_duckdb():
    parquet_file = create_parquet_file()
    duckdb_conn = ingest_to_duckdb(parquet_file)
    read_from_duckdb(duckdb_conn)

@repository
def my_repository():
    return [object_storage_load_file_to_duckdb]
```

This Dagster code defines a job `object_storage_load_file_to_duckdb` that performs the same operations as the Airflow DAG. It uses an IOManager to handle file operations, ensuring that the parquet file is written and read correctly. The `create_parquet_file` op creates a DataFrame and writes it to a parquet file. The `ingest_to_duckdb` op reads this parquet file and ingests it into a DuckDB database. Finally, the `read_from_duckdb` op reads from the database and prints the results. The repository function wraps the job for Dagster to recognize it.