Here's the Dagster equivalent of the provided Airflow code. This code uses Dagster's asset-based approach to replicate the functionality of listing, copying, and reading files from object storage:

```python
from dagster import job, asset, AssetIn, AssetOut, AssetMaterialization, IOManager, io_manager
from dagster.io.path import ObjectStoragePath

OBJECT_STORAGE = "s3"
CONN_ID = "aws_s3_webinar_conn"
PATH = "ce-2-8-examples-bucket"

base_src = ObjectStoragePath(f"{OBJECT_STORAGE}://{PATH}", conn_id=CONN_ID)
base_dst = ObjectStoragePath(f"file://include/poems/")

class ObjectStorageIOManager(IOManager):
    def handle_output(self, context, obj):
        if isinstance(obj, ObjectStoragePath):
            obj.copy(dst=context.asset_key.path)

    def load_input(self, context):
        return context.upstream_output

@io_manager
def object_storage_io_manager():
    return ObjectStorageIOManager()

@asset(io_manager_key="object_storage_io_manager", required_resource_keys={"object_storage_io_manager"})
def list_files() -> list[ObjectStoragePath]:
    """List files in remote object storage."""
    path = base_src / "poems/"
    files = [f for f in path.iterdir() if f.is_file()]
    return files

@asset(io_manager_key="object_storage_io_manager", required_resource_keys={"object_storage_io_manager"})
def copy_file_to_local(file: AssetIn) -> AssetOut:
    """Copy a file from remote to local storage."""
    dst_path = base_dst / file.path.name
    file.copy(dst=dst_path)
    return dst_path

@asset(io_manager_key="object_storage_io_manager", required_resource_keys={"object_storage_io_manager"})
def read_file_content(file: AssetIn) -> str:
    """Read a file from remote object storage and return utf-8 decoded text."""
    bytes = file.read_block(offset=0, length=None)
    text = bytes.decode("utf-8")
    return text

@job
def object_storage_list_read_remote():
    files_s3 = list_files()
    copied_files = copy_file_to_local.map(files_s3)
    file_contents = read_file_content.map(copied_files)
```

This code defines three assets in Dagster:
1. `list_files`: Lists files in the specified remote object storage path.
2. `copy_file_to_local`: Copies each file from the remote storage to a local path.
3. `read_file_content`: Reads the content of each file.

The `@job` decorator is used to define the workflow, where each asset is computed based on the outputs of the previous assets. The `ObjectStorageIOManager` class is a custom IO manager that handles the copying of files between object storage locations, which is used by the assets to manage their outputs.