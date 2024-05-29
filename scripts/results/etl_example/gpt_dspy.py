
from dagster import asset, MaterializeResult, MetadataValue, Config, define_asset_job, ScheduleDefinition, Definitions, AssetExecutionContext, RetryPolicy
import requests
from typing import Dict
from pydantic import Field

class BitcoinDataConfig(Config):
    api_url: str = Field(
        default="https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true",
        description="The API URL to fetch Bitcoin data"
    )

@asset(retry_policy=RetryPolicy(max_retries=2))
def bitcoin_data(context: AssetExecutionContext, config: BitcoinDataConfig) -> MaterializeResult:
    """Extracts and processes the Bitcoin price from the API."""
    response = requests.get(config.api_url)
    response.raise_for_status()
    bitcoin_price = response.json()["bitcoin"]

    # Process the data
    data = {
        "usd": bitcoin_price["usd"],
        "change": bitcoin_price["usd_24h_change"]
    }

    # Log the processed data
    context.log.info(f"Bitcoin Price: {data['usd']} with 24h change: {data['change']}")

    # Attach metadata to the asset
    metadata = {
        "usd": MetadataValue.float(data["usd"]),
        "change": MetadataValue.float(data["change"]),
        "source": MetadataValue.url(config.api_url)
    }

    return MaterializeResult(metadata=metadata)

# Define a job that will materialize the bitcoin_data asset
bitcoin_data_job = define_asset_job("bitcoin_data_job", selection=[bitcoin_data])

# Define a schedule that runs the job daily
bitcoin_data_schedule = ScheduleDefinition(
    job=bitcoin_data_job,
    cron_schedule="0 0 * * *",  # daily at midnight
)

# Create the Definitions object
defs = Definitions(
    assets=[bitcoin_data],
    jobs=[bitcoin_data_job],
    schedules=[bitcoin_data_schedule],
)
