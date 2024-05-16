
from dagster import Definitions, asset, MaterializeResult, MetadataValue
import requests
import logging

@asset
def shibe_image_data() -> MaterializeResult:
    """
    Fetches shibe image data from the API and returns it with metadata.
    """
    try:
        response = requests.get("http://shibe.online/api/shibes?count=1&urls=true")
        response.raise_for_status()  # Raises an HTTPError for bad responses
        data = response.json()
        return MaterializeResult(
            value=data,
            metadata={
                "status_code": MetadataValue.int(response.status_code),
                "url_requested": MetadataValue.text(response.url)
            }
        )
    except requests.RequestException as e:
        logging.error(f"Failed to fetch shibe image data: {e}")
        return MaterializeResult(
            value=None,
            metadata={
                "error": MetadataValue.text(str(e))
            }
        )

@asset
def shibe_picture_url(shibe_image_data) -> MaterializeResult:
    """
    Extracts and returns the shibe picture URL from the image data with metadata.
    """
    if shibe_image_data:
        url = shibe_image_data[0]
        return MaterializeResult(
            value=url,
            metadata={
                "picture_url": MetadataValue.url(url)
            }
        )
    return MaterializeResult(
        value=None,
        metadata={
            "error": MetadataValue.text("No image data available")
        }
    )

defs = Definitions(
    assets=[shibe_image_data, shibe_picture_url]
)
