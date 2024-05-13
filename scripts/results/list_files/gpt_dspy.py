
from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    asset,
    MaterializeResult,
    MetadataValue
)
from dagster_aws.s3 import S3Resource
from dagster.core.storage.file_manager import LocalFileManager
from pathlib import Path
import shutil
import os

# Define the asset
@asset(required_resource_keys={"s3_resource", "local_fs"})
def transfer_and_read_poems(context):
    """
    Transfer poem files from remote S3 storage to local storage and read their contents.
    Returns:
        MaterializeResult: Contains the list of contents of each poem file and metadata.
    """
    OBJECT_STORAGE = "s3"
    CONN_ID = "aws_s3_webinar_conn"
    PATH = "ce-2-8-examples-bucket"
    LOCAL_STORAGE_PATH = "/include/poems/"

    remote_path = Path(f"{OBJECT_STORAGE}://{PATH}/poems")
    local_path = Path(LOCAL_STORAGE_PATH)
    local_path.mkdir(parents=True, exist_ok=True)

    contents = []
    poem_names = []

    # Simulating listdir on S3 for example purposes
    # In a real scenario, use boto3 or similar library to list and download files from S3
    for file in context.resources.s3_resource.listdir(remote_path):
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

# Load all assets
all_assets = [transfer_and_read_poems]

# Define a job that will materialize the assets
poems_job = define_asset_job(
    "poems_job",
    selection=AssetSelection.all()  # Selects all assets, adjust if needed
)

# Define a schedule for the job
poems_schedule = ScheduleDefinition(
    job=poems_job,
    cron_schedule="0 0 * * 0",  # Every Sunday at midnight
)

# Define resources required by the assets
resources = {
    "s3_resource": S3Resource(),
    "local_fs": LocalFileManager(base_dir="/include/poems/")
}

# Update the Definitions object to include the job and schedule
defs = Definitions(
    assets=all_assets,
    jobs=[poems_job],
    schedules=[poems_schedule],
    resources=resources
)
