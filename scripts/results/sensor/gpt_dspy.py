import logging

import requests
from dagster import (
    Config,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RunRequest,
    asset,
    job,
    sensor,
)
from pydantic import Field

logger = logging.getLogger(__name__)


class ShibePictureConfig(Config):
    api_url: str = Field(
        default="http://shibe.online/api/shibes", description="API base URL"
    )
    count: int = Field(default=1, description="Number of Shibe pictures to fetch")
    urls: bool = Field(default=True, description="Whether to fetch URLs only")


@asset(name="shibe_picture")
def shibe_picture_url(config: ShibePictureConfig) -> MaterializeResult:
    try:
        response = requests.get(
            f"{config.api_url}?count={config.count}&urls={config.urls}"
        )
        response.raise_for_status()  # Raises an HTTPError for bad responses
        shibe_url = response.json()[0]  # Assuming the API returns a list of URLs
        return MaterializeResult(
            value=shibe_url, metadata={"shibe_url": MetadataValue.url(shibe_url)}
        )
    except requests.RequestException as e:
        logger.error(f"Failed to retrieve Shibe picture: {e}")
        return MaterializeResult(
            value=None, metadata={"error": MetadataValue.text(str(e))}
        )


@job
def shibe_picture_job():
    shibe_picture_url()


@sensor(job=shibe_picture_job)
def shibe_api_availability_sensor():
    response = requests.get("http://shibe.online/api/shibes?count=1&urls=true")
    if response.status_code == 200:
        yield RunRequest(
            run_key="shibe_api_check",
            run_config={
                "ops": {
                    "shibe_picture_url": {
                        "config": {
                            "api_url": "http://shibe.online/api/shibes",
                            "count": 1,
                            "urls": True,
                        }
                    }
                }
            },
        )


defs = Definitions(assets=[shibe_picture_url], sensors=[shibe_api_availability_sensor])
