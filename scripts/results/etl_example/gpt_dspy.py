
from dagster import (
    asset, MaterializeResult, MetadataValue, Definitions, define_asset_job,
    ScheduleDefinition, DefaultScheduleStatus, AssetSelection, RetryPolicy
)
import requests
import logging
import json

API_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

def fetch_bitcoin_data():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()["bitcoin"]
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data: {e}")
        return None

@asset(name="bitcoin_market_data")
def bitcoin_market_data() -> MaterializeResult:
    bitcoin_data = fetch_bitcoin_data()
    if bitcoin_data is None:
        return MaterializeResult(metadata={"error": MetadataValue.text("Failed to fetch data")})
    processed_data = {
        "usd": bitcoin_data["usd"],
        "change": bitcoin_data["usd_24h_change"]
    }
    logging.info(f"Bitcoin Market Data: USD {processed_data['usd']} with change {processed_data['change']}")
    # Convert the raw data to JSON for metadata
    raw_data_json = json.dumps(bitcoin_data, indent=2)
    # Create metadata entries
    metadata_entries = {
        "raw_data": MetadataValue.json(raw_data_json),
        "log": MetadataValue.text(f"USD {processed_data['usd']} with change {processed_data['change']}")
    }
    return MaterializeResult(output=processed_data, metadata=metadata_entries)

# Define the job with retry policy
retry_policy = RetryPolicy(max_retries=2)
bitcoin_job = define_asset_job(
    "bitcoin_job",
    selection=AssetSelection.assets(bitcoin_market_data),
    op_retry_policy=retry_policy
)

# Define the schedule
bitcoin_schedule = ScheduleDefinition(
    job=bitcoin_job,
    cron_schedule="0 0 * * *",  # Daily at midnight
    default_status=DefaultScheduleStatus.RUNNING
)

# Add to Definitions
defs = Definitions(
    assets=[bitcoin_market_data],
    jobs=[bitcoin_job],
    schedules=[bitcoin_schedule]
)
