
from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    DefaultScheduleStatus,
    asset,
    Config,
    MaterializeResult,
    MetadataValue
)
from typing import List
from pathlib import Path
import shutil
import os
from pydantic import Field

# Define the configuration for the asset
class TransferAndReadPoemsConfig(Config):
    object_storage: str = Field(default="s3", description="Type of object storage")
    conn_id: str = Field(default="aws_s3_webinar_conn", description="Connection ID for AWS S3")
    path: str = Field(default="ce-2-8-examples-bucket", description="Bucket path in S3")
    local_storage_path: str = Field(default="/include/poems/", description="Local storage path for poems")

# Define the asset
@asset
def transfer_and_read_poems(config: TransferAndReadPoemsConfig) -> MaterializeResult:
    """
    Transfer poem files from remote S3 storage to local storage and read their contents.
    Returns:
        MaterializeResult: Contains the list of contents of each poem file and metadata.
    """
    remote_path = Path(f"{config.object_storage}://{config.path}/poems")
    local_path = Path(config.local_storage_path)
    local_path.mkdir(parents=True, exist_ok=True)
    contents = []
    poem_names = []
    for file in os.listdir(remote_path):
        local_file_path = local_path / file
        shutil.copy(remote_path / file, local_file_path)
        with open(local_file_path, 'r', encoding='utf-8') as f:
            contents.append(f.read())
        poem_names.append(file)
    # Metadata about the number of poems and their names
    metadata = {
        "number_of_poems": MetadataValue.int(len(poem_names)),
        "poem_names": MetadataValue.json(poem_names)
    }
    return MaterializeResult(contents, metadata)

# Load assets from the current module
all_assets = load_assets_from_modules([__name__])

# Define a job that will materialize the assets
transfer_and_read_poems_job = define_asset_job(
    "transfer_and_read_poems_job",
    selection=AssetSelection.all()  # Selects all assets, adjust if necessary
)

# Define a schedule for the job with a cron expression for weekly execution on Sunday at midnight
transfer_and_read_poems_schedule = ScheduleDefinition(
    job=transfer_and_read_poems_job,
    cron_schedule="0 0 * * 0",  # every Sunday at midnight
    default_status=DefaultScheduleStatus.RUNNING  # Automatically start the schedule
)

# Update the Definitions object to include the job and schedule
defs = Definitions(
    assets=all_assets,
    jobs=[transfer_and_read_poems_job],
    schedules=[transfer_and_read_poems_schedule]
)
