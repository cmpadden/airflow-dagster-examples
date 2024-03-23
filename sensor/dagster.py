from dagster import asset, MaterializeResult, AssetExecutionContext, sensor, SensorEvaluationContext, SensorResult, RunRequest, RunConfig, MetadataValue
from pydantic import BaseModel

import requests


class ShibePictureConfig(BaseModel):
    url: str

@asset
def shibe_picture(context: AssetExecutionContext, shibe_config: ShibePictureConfig) -> MaterializeResult:
    context.log.info(f"Shibe picture URL: {shibe_config.url}")

    # get the base64 encoded image
    response = requests.get(shibe_config.url)
    md_content = f"![img](data:image/png;base64,{response.content.decode('base64')})"

    return MaterializeResult(
        metadata={
            "url": shibe_config.url,
            "image": MetadataValue.md(md_content)
        }
    )

@sensor
def shibe_sensor(context: SensorEvaluationContext) -> SensorResult:
    r = requests.get("http://shibe.online/api/shibes?count=1&urls=true")
    context.log.info(f"Shibe URL returned the status code {r.status_code}")

    run_requests = []
    skip_reason = None

    if r.status_code == 200:
        run_requests.append(RunRequest(
            run_config=RunConfig(
                ops={
                    "shibe_picture": ShibePictureConfig(url=r.json()[0])
                }
            )
        ))
        
    else:
        skip_reason = f"Shibe URL returned the status code {r.status_code}"

    return SensorResult(run_requests=run_requests, skip_reason=skip_reason)