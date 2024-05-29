
from dagster import asset, MaterializeResult, MetadataValue, AssetIn, Config, AssetExecutionContext
import requests
from pydantic import Field

class ShibePictureUrlConfig(Config):
    api_url: str = Field(default="http://shibe.online/api/shibes", description="The API URL to fetch shibe pictures.")
    count: int = Field(default=1, description="The number of shibe pictures to fetch.")

@asset
def shibe_picture_url(context: AssetExecutionContext, config: ShibePictureUrlConfig) -> MaterializeResult:
    """Fetches a Shibe picture URL from the shibe.online API."""
    response = requests.get(f"{config.api_url}?count={config.count}&urls=true")
    context.log.info(f"Response status code: {response.status_code}")
    
    response.raise_for_status()  # Let exceptions propagate to Dagster
    
    shibe_url = response.json()
    condition_met = True
    
    context.log.info(f"Shibe URL: {shibe_url}")
    return MaterializeResult(
        value=shibe_url,
        metadata={"condition_met": MetadataValue.bool(condition_met)}
    )

@asset(ins={"shibe_picture_url": AssetIn()})
def display_shibe_picture_url(context: AssetExecutionContext, shibe_picture_url):
    """Logs the Shibe picture URL if available."""
    if shibe_picture_url:
        context.log.info(f"Shibe picture URL: {shibe_picture_url[0]}")
    else:
        context.log.warning("No URL available")

from dagster import Definitions

defs = Definitions(
    assets=[shibe_picture_url, display_shibe_picture_url],
)
