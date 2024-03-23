from dagster import asset, sensor, SensorResult, RunRequest,RunConfig, SensorEvaluationContext, MaterializeResult, AssetExecutionContext
from pydantic import BaseModel
from boto3 import client
import os

BUCKET_PATH = "ce-2-8-examples-bucket"
LOCAL_PATH = "data/poems/"

s3_client = client("s3")

class Poem(BaseModel):
    filename: str

@asset
def local_poems(context: AssetExecutionContext, poem_config: Poem) -> MaterializeResult:
    # download the poem from the bucket
    os.makedirs(LOCAL_PATH, exist_ok=True)
    s3_client.download_file(BUCKET_PATH, f"poems/{poem_config.filename}", f"{LOCAL_PATH}{poem_config.filename}")

    # read the poem into memory
    with open(f"{LOCAL_PATH}{poem_config.filename}", "r") as f:
        poem = f.read()

    # log the poem to the console
    context.log.info(poem)

    return MaterializeResult(
        metadata={
            "filename": poem_config.filename,
            "poem_snippet": poem[:100] + "...",
            "poem_length": len(poem),
        }
    )


@sensor(
    asset_selection=local_poems
)
def list_files_sensor(context: SensorEvaluationContext):
    poems = set(context.cursor) if context.cursor else set()

    run_requests = []

    # list the files in the poems directory in the bucket
    response = s3_client.list_objects_v2(Bucket=BUCKET_PATH, Prefix="poems/")

    # iterate through the file names in the response
    for obj in response.get("Contents", []):
        filename = obj["Key"].split("/")[-1]

        # don't download a poem if we've already logged it
        if filename in poems:
            continue
        
        # make a run request to download the poem, if its new
        run_requests.append(RunRequest(
            run_key=filename,
            run_config=RunConfig(
                ops={
                    "local_poems": Poem(filename=filename)
                }
            )
        ))

        poems.add(filename)

    return SensorResult(
        run_requests=run_requests,
        cursor=str(list(poems))
    )