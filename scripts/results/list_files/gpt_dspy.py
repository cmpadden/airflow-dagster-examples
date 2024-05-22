import os
import shutil
from pathlib import Path

from dagster import (
    Config,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from pydantic import Field


class TransferAndReadPoemsConfig(Config):
    object_storage: str = Field(default="s3", description="Type of object storage")
    conn_id: str = Field(
        default="aws_s3_webinar_conn", description="Connection ID for AWS S3"
    )
    path: str = Field(default="ce-2-8-examples-bucket", description="Bucket path in S3")
    local_storage_path: str = Field(
        default="/include/poems/", description="Local storage path for poems"
    )


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
        with open(local_file_path, "r", encoding="utf-8") as f:
            contents.append(f.read())
            poem_names.append(file)
    # Metadata about the number of poems and their names
    metadata = {
        "number_of_poems": MetadataValue.int(len(poem_names)),
        "poem_names": MetadataValue.json(poem_names),
    }
    return MaterializeResult(contents, metadata)


poem_transfer_job = define_asset_job(
    "poem_transfer_job", selection=[transfer_and_read_poems]
)

poem_transfer_schedule = ScheduleDefinition(
    job=poem_transfer_job,
    cron_schedule="0 0 * * 0",  # every Sunday at midnight
    name="weekly_poem_transfer_schedule",
)


defs = Definitions(
    assets=[transfer_and_read_poems],
    jobs=[poem_transfer_job],
    schedules=[poem_transfer_schedule],
)
