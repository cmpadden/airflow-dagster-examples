from airflow.io.path import ObjectStoragePath
from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from pydantic import Field


class ListedFilesConfig:
    base_path: str = Field(
        default="file://data/poems/", description="Base path for listing files"
    )


class CopiedFilesConfig:
    destination_path: str = Field(
        default="file://include/poems/",
        description="Destination path for copying files",
    )


@asset
def listed_files(config: ListedFilesConfig) -> list[ObjectStoragePath]:
    """Asset representing the list of files in remote object storage."""
    base_path = ObjectStoragePath(config.base_path)
    path = base_path / "poems/"
    files = [f for f in path.iterdir() if f.is_file()]
    return files


@asset
def copied_files(files: list[ObjectStoragePath], config: CopiedFilesConfig):
    """Asset representing files copied from remote to local storage."""
    destination_path = ObjectStoragePath(config.destination_path)
    for file in files:
        file.copy(dst=destination_path)


@asset
def file_contents(files: list[ObjectStoragePath]) -> list[str]:
    """Asset representing the content of files read from remote storage."""
    contents = []
    for file in files:
        bytes = file.read_block(offset=0, length=None)
        text = bytes.decode("utf-8")
        contents.append(text)
    return contents


# Define dependencies between assets
copied_files(listed_files(ListedFilesConfig()), CopiedFilesConfig())
file_contents(listed_files(ListedFilesConfig()))

# Define the asset job
asset_job = define_asset_job(
    "object_storage_job",
    selection=AssetSelection.assets(listed_files, copied_files, file_contents),
)

# Define the schedule for the job
job_schedule = ScheduleDefinition(
    job=asset_job,
    cron_schedule="0 0 * * 0",  # Every Sunday at midnight
)

# Create Definitions object with assets, job, and schedule
defs = Definitions(
    assets=[listed_files, copied_files, file_contents],
    schedules=[job_schedule],
    jobs=[asset_job],
)
