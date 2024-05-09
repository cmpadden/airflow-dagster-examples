```python
from dagster import job, op, AssetIn, AssetOut, AssetMaterialization, IOManager, io_manager
from dagster import AssetGroup
import pandas as pd
import os
from typing import List

# Define the table and dates as constants
TABLE = "YELLOW_TRIPDATA_WITH_UPLOAD_DATE"
DATES = ["2019-01", "2019-02"]

# Define an IOManager for handling CSV files
class CsvIOManager(IOManager):
    def handle_output(self, context, obj):
        filepath = os.path.join(context.resource_config["base_dir"], context.asset_key.path[-1])
        obj.to_csv(filepath, index=False)

    def load_input(self, context):
        filepath = os.path.join(context.resource_config["base_dir"], context.upstream_output.asset_key.path[-1])
        return pd.read_csv(filepath)

@io_manager(config_schema={"base_dir": str})
def csv_io_manager(init_context):
    return CsvIOManager()

# Define operations for data manipulation
@op(out={"result": AssetOut(io_manager_key="csv_io_manager")})
def add_upload_date(context, file_path: str, upload_date: str):
    trip_dict = pd.read_csv(file_path, header=0, parse_dates=["pickup_datetime"], infer_datetime_format=True)
    trip_dict["upload_date"] = upload_date
    context.log_event(AssetMaterialization(asset_key="add_upload_date", description="Added upload date to CSV."))
    return trip_dict

@op(out={"result": AssetOut(io_manager_key="csv_io_manager")})
def delete_upload_date(context, file_path: str):
    trip_dict = pd.read_csv(file_path, header=0, parse_dates=["pickup_datetime"], infer_datetime_format=True)
    trip_dict.drop(columns="upload_date", inplace=True)
    context.log_event(AssetMaterialization(asset_key="delete_upload_date", description="Removed upload date from CSV."))
    return trip_dict

# Define a job to orchestrate the operations
@job(resource_defs={"csv_io_manager": csv_io_manager})
def taxi_snowflake():
    for date in DATES:
        file_name = f"yellow_tripdata_sample_{date}.csv"
        file_path = f"/usr/local/airflow/include/sample_data/yellow_trip_data/{file_name}"
        upload_date = f"2023-01-01"  # Example static date, replace with dynamic logic if needed

        added_date_df = add_upload_date(file_path, upload_date)
        cleaned_df = delete_upload_date(added_date_df)

# Define an asset group if needed for more complex dependencies
assets = AssetGroup(
    [
        add_upload_date,
        delete_upload_date
    ],
    resource_defs={"csv_io_manager": csv_io_manager}
)

if __name__ == "__main__":
    taxi_snowflake.execute_in_process(
        run_config={
            "resources": {
                "csv_io_manager": {
                    "config": {
                        "base_dir": "/path/to/your/csv/storage"
                    }
                }
            }
        }
    )
```

This Dagster code provides a direct translation of the Airflow tasks into Dagster operations (`@op`) and manages file I/O through a custom `CsvIOManager`. The job `taxi_snowflake` orchestrates these operations. Adjust the file paths and other configurations as necessary to fit your environment.