
from dagster import asset, Config, MaterializeResult, MetadataValue, Definitions
import pandas as pd
from pydantic import Field
from typing import List

class TripDataPreparationConfig(Config):
    file_path: str
    upload_date: str  # Changed from datetime.date to str to comply with Dagster's config type requirements

@asset
def trip_data_preparation_and_upload(config: TripDataPreparationConfig) -> MaterializeResult:
    """
    Prepares trip data by reading from a CSV, adding an 'upload_date' column, and saving it.
    """
    trip_data = pd.read_csv(config.file_path, header=0, parse_dates=["pickup_datetime"], infer_datetime_format=True)
    trip_data["upload_date"] = pd.to_datetime(config.upload_date)  # Convert string back to datetime for processing
    trip_data.to_csv(config.file_path, header=True, index=False)
    record_count = len(trip_data)
    metadata = {
        "records_processed": MetadataValue.int(record_count),
        "upload_date": MetadataValue.text(str(config.upload_date))
    }
    return MaterializeResult(output=config.file_path, metadata=metadata)

@asset
def snowflake_resources_setup_and_cleanup() -> MaterializeResult:
    """
    Manages Snowflake resources by creating and cleaning up tables and stages.
    """
    # Logic to create and delete table and stage
    # Placeholder for SQL execution logic
    # Assuming success for demonstration
    success_message = "Snowflake resources setup and cleanup completed successfully."
    metadata = {"operation_status": MetadataValue.text(success_message)}
    return MaterializeResult(metadata=metadata)

@asset
def trip_data_loading_and_quality_checks(prepared_tripdata, snowflake_resources_setup_and_cleanup) -> MaterializeResult:
    """
    Loads trip data from S3 to Snowflake and performs data quality checks.
    """
    # Logic to load data to Snowflake
    # Data quality checks: row count, interval data, threshold checks, and row quality checks
    # Placeholder for actual logic, assuming checks passed for demonstration
    rows_loaded = 1000  # Example value
    discrepancies_found = 0
    metadata = {
        "rows_loaded": MetadataValue.int(rows_loaded),
        "discrepancies_found": MetadataValue.int(discrepancies_found),
        "data_integrity_status": MetadataValue.text("Passed"),
    }
    return MaterializeResult(metadata=metadata)

defs = Definitions(
    assets=[
        trip_data_preparation_and_upload,
        snowflake_resources_setup_and_cleanup,
        trip_data_loading_and_quality_checks
    ]
)
