from dagster import asset, Config, MaterializeResult, AssetExecutionContext, sensor, SensorEvaluationContext, SensorResult, RunRequest, RunConfig, MetadataValue, Definitions

import requests
import base64


class ShibePictureConfig(Config):
    url: str

@asset
def shibe_picture(context: AssetExecutionContext, config: ShibePictureConfig) -> MaterializeResult:
    context.log.info(f"Shibe picture URL: {config.url}")

    # get the base64 encoded image
    response = requests.get(config.url)
    
    with open("../data/shibe.jpg", "wb") as f:
        f.write(response.content)

    md_content = f"![img](data:image/png;base64,{base64.b64encode(response.content)})"

    return MaterializeResult(
        metadata={
            "url": config.url,
            "image": MetadataValue.md(md_content)
        }
    )

@sensor(
    asset_selection=[shibe_picture]
)
def shibe_sensor(context: SensorEvaluationContext) -> SensorResult:
    r = requests.get("http://shibe.online/api/shibes?count=1&urls=true")
    context.log.info(f"Shibe URL returned the status code {r.status_code}")

    run_requests = []
    skip_reason = None

    if r.status_code == 200:
        url = r.json()[0]
        run_requests.append(RunRequest(
            run_key=url,
            run_config={
                "ops": {
                    "shibe_picture": {
                        "config": {
                            "url": url
                        }
                    }
                }
            }
        ))
        
    else:
        skip_reason = f"Shibe URL returned the status code {r.status_code}"

    return SensorResult(run_requests=run_requests, skip_reason=skip_reason)


defs = Definitions(sensors=[shibe_sensor], assets=[shibe_picture])