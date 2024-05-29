
from dagster import asset, MaterializeResult, AssetIn, MetadataValue, Config, Definitions, define_asset_job, ScheduleDefinition, AssetSelection, AssetExecutionContext
from pydantic import Field
from airflow.io.path import ObjectStoragePath

class RemoteFilesConfig(Config):
    object_storage: str = Field(default="s3", description="Object storage type")
    conn_id: str = Field(default="aws_s3_webinar_conn", description="Connection ID for object storage")
    path: str = Field(default="ce-2-8-examples-bucket", description="Path in the object storage")

class LocalFilesConfig(Config):
    local_path: str = Field(default="file://include/poems/", description="Local path to copy files to")

@asset
def remote_files(context: AssetExecutionContext, config: RemoteFilesConfig) -> MaterializeResult:
    """List files in remote object storage."""
    base_src = ObjectStoragePath(f"{config.object_storage}://{config.path}", conn_id=config.conn_id)
    path = base_src / "poems/"
    files = [f for f in path.iterdir() if f.is_file()]
    metadata = {"file_count": MetadataValue.int(len(files))}
    context.log.info(f"Found {len(files)} files in remote storage.")
    return MaterializeResult(output=files, metadata=metadata)

@asset(ins={"remote_files": AssetIn()})
def local_files(context: AssetExecutionContext, remote_files: list[ObjectStoragePath], config: LocalFilesConfig) -> MaterializeResult:
    """Copy files from remote to local storage."""
    base_dst = ObjectStoragePath(config.local_path)
    copied_files = []
    for file in remote_files:
        file.copy(dst=base_dst)
        copied_files.append(base_dst / file.name)
    metadata = {"copied_file_count": MetadataValue.int(len(copied_files))}
    context.log.info(f"Copied {len(copied_files)} files to local storage.")
    return MaterializeResult(output=copied_files, metadata=metadata)

@asset(ins={"local_files": AssetIn()})
def file_contents(context: AssetExecutionContext, local_files: list[ObjectStoragePath]) -> MaterializeResult:
    """Read the content of the files."""
    contents = []
    for file in local_files:
        bytes = file.read_block(offset=0, length=None)
        text = bytes.decode("utf-8")
        contents.append(text)
    metadata = {"file_count": MetadataValue.int(len(contents))}
    context.log.info(f"Read contents of {len(contents)} files.")
    return MaterializeResult(output=contents, metadata=metadata)

# Define a job that will materialize the assets
object_storage_job = define_asset_job("object_storage_job", selection=AssetSelection.all())

# Define a schedule for the job
object_storage_schedule = ScheduleDefinition(
    job=object_storage_job,
    cron_schedule="0 0 * * 0",  # every Sunday at midnight
)

# Create the Definitions object
defs = Definitions(
    assets=[remote_files, local_files, file_contents],
    jobs=[object_storage_job],
    schedules=[object_storage_schedule],
)
