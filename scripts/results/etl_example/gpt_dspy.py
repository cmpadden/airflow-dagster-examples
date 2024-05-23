import json
import logging

import requests
from dagster import (
    Config,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from pydantic import Field


class BitcoinMarketDataConfig(Config):
    api_url: str = Field(
        default="https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true",
        description="API URL to fetch Bitcoin market data",
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
        return MaterializeResult(
            metadata={"error": MetadataValue.text("Failed to fetch data")}
        )

    processed_data = {
        "usd": bitcoin_data["usd"],
        "change": bitcoin_data["usd_24h_change"],
    }

    logging.info(
        f"Bitcoin Market Data: USD {processed_data['usd']} with change {processed_data['change']}"
    )

    # Convert the raw data to JSON for metadata
    raw_data_json = json.dumps(bitcoin_data, indent=2)

    # Create metadata entries
    metadata_entries = {
        "raw_data": MetadataValue.json(raw_data_json),
        "log": MetadataValue.text(
            f"USD {processed_data['usd']} with change {processed_data['change']}"
        ),
    }

    return MaterializeResult(output=processed_data, metadata=metadata_entries)


bitcoin_data_job = define_asset_job("bitcoin_data_job", selection=[bitcoin_market_data])

bitcoin_data_schedule = ScheduleDefinition(
    job=bitcoin_data_job,
    cron_schedule="0 0 * * *",  # Daily at midnight
)


defs = Definitions(
    assets=[bitcoin_market_data],
    jobs=[bitcoin_data_job],
    schedules=[bitcoin_data_schedule],
)
