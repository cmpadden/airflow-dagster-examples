
import pandas as pd
from dagster import asset, MaterializeResult, MetadataValue, Config, define_asset_job, ScheduleDefinition, Definitions, AssetSelection, AssetExecutionContext
from pydantic import Field
from typing import List
from dagster_snowflake import snowflake_resource
from dagster_aws.s3 import s3_resource

# Define resources
snowflake = snowflake_resource.configured({
    "account": "your_account",
    "user": "your_user",
    "password": "your_password",
    "database": "your_database",
    "schema": "your_schema",
    "warehouse": "your_warehouse",
})

s3 = s3_resource.configured({
    "region_name": "your_region",
    "access_key_id": "your_access_key_id",
    "secret_access_key": "your_secret_access_key",
})

# Define configuration classes
class AddUploadDateConfig(Config):
    file_path: str
    upload_date: str

class UploadToS3Config(Config):
    file_path: str

# Define assets
@asset
def create_snowflake_table(context: AssetExecutionContext) -> MaterializeResult:
    # SQL to create the Snowflake table
    # Assuming some SQL execution here
    context.log.info("Snowflake table created.")
    return MaterializeResult(metadata={"description": MetadataValue.text("Snowflake table created.")})

@asset
def create_snowflake_stage(context: AssetExecutionContext) -> MaterializeResult:
    # SQL to create the Snowflake stage
    # Assuming some SQL execution here
    context.log.info("Snowflake stage created.")
    return MaterializeResult(metadata={"description": MetadataValue.text("Snowflake stage created.")})

@asset
def add_upload_date(context: AssetExecutionContext, config: AddUploadDateConfig) -> MaterializeResult:
    trip_dict = pd.read_csv(config.file_path, header=0, parse_dates=["pickup_datetime"], infer_datetime_format=True)
    trip_dict["upload_date"] = config.upload_date
    trip_dict.to_csv(config.file_path, header=True, index=False)
    context.log.info(f"Upload date {config.upload_date} added to file {config.file_path}.")
    return MaterializeResult(output=config.file_path, metadata={"upload_date": MetadataValue.text(config.upload_date)})

@asset
def upload_to_s3(context: AssetExecutionContext, config: UploadToS3Config) -> MaterializeResult:
    file_path = config.file_path
    # Code to upload file to S3
    s3_path = f"s3://your_bucket/{file_path.split('/')[-1]}"
    # Assuming upload code here
    context.log.info(f"File {file_path} uploaded to {s3_path}.")
    return MaterializeResult(output=s3_path, metadata={"s3_path": MetadataValue.text(s3_path)})

@asset
def load_to_snowflake(context: AssetExecutionContext) -> MaterializeResult:
    # Code to load data from S3 to Snowflake
    # Assuming load code here
    context.log.info("Data loaded to Snowflake.")
    return MaterializeResult(metadata={"description": MetadataValue.text("Data loaded to Snowflake.")})

@asset
def delete_upload_date(context: AssetExecutionContext, config: AddUploadDateConfig) -> MaterializeResult:
    trip_dict = pd.read_csv(config.file_path, header=0, parse_dates=["pickup_datetime"], infer_datetime_format=True)
    trip_dict.drop(columns="upload_date", inplace=True)
    trip_dict.to_csv(config.file_path, header=True, index=False)
    context.log.info(f"Upload date deleted from file {config.file_path}.")
    return MaterializeResult(output=config.file_path, metadata={"description": MetadataValue.text("Upload date deleted.")})

@asset
def value_check(context: AssetExecutionContext) -> MaterializeResult:
    # SQL to perform value check
    # Assuming some SQL execution here
    context.log.info("Value check performed.")
    return MaterializeResult(metadata={"description": MetadataValue.text("Value check performed.")})

@asset
def interval_check(context: AssetExecutionContext) -> MaterializeResult:
    # SQL to perform interval check
    # Assuming some SQL execution here
    context.log.info("Interval check performed.")
    return MaterializeResult(metadata={"description": MetadataValue.text("Interval check performed.")})

@asset
def threshold_check(context: AssetExecutionContext) -> MaterializeResult:
    # SQL to perform threshold check
    # Assuming some SQL execution here
    context.log.info("Threshold check performed.")
    return MaterializeResult(metadata={"description": MetadataValue.text("Threshold check performed.")})

@asset
def row_quality_checks(context: AssetExecutionContext) -> MaterializeResult:
    # SQL to perform row quality checks
    # Assuming some SQL execution here
    context.log.info("Row quality checks performed.")
    return MaterializeResult(metadata={"description": MetadataValue.text("Row quality checks performed.")})

@asset
def delete_snowflake_table(context: AssetExecutionContext) -> MaterializeResult:
    # SQL to delete the Snowflake table
    # Assuming some SQL execution here
    context.log.info("Snowflake table deleted.")
    return MaterializeResult(metadata={"description": MetadataValue.text("Snowflake table deleted.")})

# Define the job
hackernews_job = define_asset_job("hackernews_job", selection=AssetSelection.all())

# Define the schedule
hackernews_schedule = ScheduleDefinition(
    job=hackernews_job,
    cron_schedule="0 0 * * *",  # daily at midnight
)

# Create the Definitions object
defs = Definitions(
    assets=[
        create_snowflake_table,
        create_snowflake_stage,
        add_upload_date,
        upload_to_s3,
        load_to_snowflake,
        delete_upload_date,
        value_check,
        interval_check,
        threshold_check,
        row_quality_checks,
        delete_snowflake_table,
    ],
    resources={
        "snowflake": snowflake,
        "s3": s3,
    },
    schedules=[hackernews_schedule],
)
