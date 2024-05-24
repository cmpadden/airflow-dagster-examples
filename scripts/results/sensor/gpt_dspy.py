import logging

import requests
from dagster import (
    Config,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RunRequest,
    SensorEvaluationContext,
    asset,
    sensor,
)
from pydantic import Field


class ShibeImageDataConfig(Config):
    api_url: str = Field(
        default="http://shibe.online/api/shibes?count=1&urls=true",
        description="API URL to fetch shibe images",
    )


@asset
def shibe_image_data(config: ShibeImageDataConfig) -> MaterializeResult:
    """
    Fetches shibe image data from the API and returns it with metadata.
    """
    try:
        response = requests.get(config.api_url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        data = response.json()
        return MaterializeResult(
            value=data,
            metadata={
                "status_code": MetadataValue.int(response.status_code),
                "url_requested": MetadataValue.text(response.url),
            },
        )
    except requests.RequestException as e:
        logging.error(f"Failed to fetch shibe image data: {e}")
        return MaterializeResult(
            value=None, metadata={"error": MetadataValue.text(str(e))}
        )


@asset
def shibe_picture_url(shibe_image_data) -> MaterializeResult:
    """
    Extracts and returns the shibe picture URL from the image data with metadata.
    """
    if shibe_image_data:
        url = shibe_image_data[0]
        return MaterializeResult(
            value=url, metadata={"picture_url": MetadataValue.url(url)}
        )
    return MaterializeResult(
        value=None, metadata={"error": MetadataValue.text("No image data available")}
    )


@sensor(asset_selection=[shibe_image_data])
def shibe_api_availability_sensor(context: SensorEvaluationContext):
    """
    Sensor to check the availability of the Shibe API and trigger the shibe_image_data asset.
    """
    api_url = "http://shibe.online/api/shibes?count=1&urls=true"
    response = requests.get(api_url)
    run_key = f"shibe-api-status-{response.status_code}"

    if response.status_code == 200:
        yield RunRequest(
            run_key=run_key,
            run_config={"ops": {"shibe_image_data": {"config": {"api_url": api_url}}}},
        )
    else:
        context.update_cursor(
            str(response.status_code)
        )  # Optionally update cursor to log last status code


defs = Definitions(
    assets=[shibe_image_data, shibe_picture_url],
    sensors=[shibe_api_availability_sensor],
)
