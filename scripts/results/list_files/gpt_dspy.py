
from dagster import asset, MaterializeResult, MetadataValue, define_asset_job, ScheduleDefinition, Definitions
from typing import List
from pathlib import Path
import shutil
import os

OBJECT_STORAGE = "s3"
CONN_ID = "aws_s3_webinar_conn"
PATH = "ce-2-8-examples-bucket"
LOCAL_STORAGE_PATH = "/include/poems/"

@asset
def transfer_and_read_poems() -> MaterializeResult:
    """
    Transfer poem files from remote S3 storage to local storage and read their contents.
    Returns:
        MaterializeResult: Contains the list of contents of each poem file and metadata.
    """
    remote_path = Path(f"{OBJECT_STORAGE}://{PATH}/poems")
    local_path = Path(LOCAL_STORAGE_PATH)
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

# Define the job that includes the asset
poem_job = define_asset_job("poem_transfer_job", selection=["transfer_and_read_poems"])

# Define the schedule for the job
poem_schedule = ScheduleDefinition(
    job=poem_job,
    cron_schedule="0 0 * * 0",  # Every Sunday at midnight
)

# Combine everything in a Definitions object
defs = Definitions(
    assets=[transfer_and_read_poems],
    jobs=[poem_job],
    schedules=[poem_schedule],
)
