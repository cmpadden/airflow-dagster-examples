import pandas as pd
from dagster import Config, Definitions, MaterializeResult, MetadataValue, asset
from pydantic import Field


class TripDataConfig(Config):
    file_path: str = Field(..., description="Path to the CSV file containing trip data")
    upload_date: str = Field(..., description="Upload date for the trip data")


@asset
def trip_data_management(config: TripDataConfig):
    """Manages the entire lifecycle of trip data from preparation, upload, processing in Snowflake, performing data quality checks, and cleanup."""
    # Prepare trip data
    trip_data = pd.read_csv(
        config.file_path,
        header=0,
        parse_dates=["pickup_datetime"],
        infer_datetime_format=True,
    )
    trip_data["upload_date"] = config.upload_date
    trip_data.to_csv(config.file_path, header=True, index=False)

    # Simulate uploading to S3
    s3_path = f"S3://{config.file_path}"
    s3_metadata = MetadataValue.text(s3_path)

    # Simulate SQL execution for creating table and stage in Snowflake
    table_stage_status = "Snowflake Table and Stage Created"
    table_stage_metadata = MetadataValue.text(table_stage_status)

    # Simulate data loading to Snowflake
    data_load_status = "Data Loaded to Snowflake"
    data_load_metadata = MetadataValue.text(data_load_status)

    # Simulate data quality checks
    quality_checks_status = "Data Quality Checks Passed"
    quality_checks_metadata = MetadataValue.text(quality_checks_status)

    # Simulate SQL execution for deleting table in Snowflake
    cleanup_status = "Snowflake Table Deleted"
    cleanup_metadata = MetadataValue.text(cleanup_status)

    # Attach metadata to the asset
    return MaterializeResult(
        metadata={
            "S3 Path": s3_metadata,
            "Table and Stage Creation": table_stage_metadata,
            "Data Load Status": data_load_metadata,
            "Quality Checks Status": quality_checks_metadata,
            "Cleanup Status": cleanup_metadata,
        }
    )


defs = Definitions(
    assets=[trip_data_management],
)
