
from dagster import asset, Config, MaterializeResult, MetadataValue, Definitions, ScheduleDefinition, define_asset_job, RetryPolicy
import requests
import logging
import json
from pydantic import Field

class BitcoinMarketDataConfig(Config):
    api_url: str = Field(
        default="https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24h_change=true&include_last_updated_at=true",
        description="API URL to fetch Bitcoin market data"
    )

def fetch_bitcoin_data(api_url: str):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()["bitcoin"]
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        return None

@asset(name="bitcoin_market_data")
def bitcoin_market_data(config: BitcoinMarketDataConfig) -> MaterializeResult:
    bitcoin_data = fetch_bitcoin_data(config.api_url)
    if bitcoin_data is None:
        return MaterializeResult(metadata={"error": MetadataValue.text("Failed to fetch data")})
    processed_data = {
        "usd": bitcoin_data["usd"],
        "change": bitcoin_data["usd_24h_change"]
    }

# Define the retry policy
retry_policy = RetryPolicy(max_retries=2)

# Define the job with retry policy
bitcoin_job = define_asset_job(
    "bitcoin_job",
    selection=AssetSelection.assets(bitcoin_market_data),
    op_retry_policy=retry_policy
)

# Define the schedule
bitcoin_schedule = ScheduleDefinition(
    job=bitcoin_job,
    cron_schedule="0 0 * * *",  # every day at midnight
)

# Update Definitions
defs = Definitions(
    assets=[bitcoin_data],
    jobs=[bitcoin_data_job],
    schedules=[bitcoin_data_schedule],
)
