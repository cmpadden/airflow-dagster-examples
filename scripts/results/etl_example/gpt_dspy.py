import logging

import requests
from dagster import (
    AssetSelection,
    Config,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from pydantic import Field

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class BitcoinPriceConfig(Config):
    api_url: str = Field(
        default="https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true",
        description="API URL to fetch Bitcoin price data",
    )
    log_level: str = Field(default="INFO", description="Logging level")


@asset(name="manage_bitcoin_price")
def bitcoin_price_management(config: BitcoinPriceConfig) -> MaterializeResult:
    logging.getLogger().setLevel(config.log_level)
    try:
        response = requests.get(config.api_url)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        bitcoin_data = response.json()["bitcoin"]
        logging.info(f"Fetched data: {bitcoin_data}")
    except requests.RequestException as e:
        logging.error(f"Failed to fetch data from API: {e}")
        return MaterializeResult(
            metadata={"error": MetadataValue.text(f"Failed to fetch data: {str(e)}")}
        )
    if bitcoin_data is None:
        logging.warning("No data to process.")
        return MaterializeResult(
            metadata={"warning": MetadataValue.text("No data to process.")}
        )
    processed_data = {
        "usd": bitcoin_data.get("usd", 0),
        "change": bitcoin_data.get("usd_24h_change", 0),
    }
    logging.info(
        f"Processed data: USD {processed_data['usd']} with change {processed_data['change']}"
    )
    return MaterializeResult(
        output=processed_data,
        metadata={
            "raw_data": MetadataValue.json(bitcoin_data),
            "processed_data": MetadataValue.json(processed_data),
            "log": MetadataValue.text(
                f"Processed data: USD {processed_data['usd']} with change {processed_data['change']}"
            ),
        },
    )


retry_policy = RetryPolicy(max_retries=2)
asset_job = define_asset_job(
    "bitcoin_price_job",
    AssetSelection.assets(bitcoin_price_management),
    op_retry_policy=retry_policy,
)

job_schedule = ScheduleDefinition(
    job=asset_job,
    cron_schedule="0 0 * * *",  # Daily at midnight
)

defs = Definitions(
    assets=[bitcoin_price_management], schedules=[job_schedule], jobs=[asset_job]
)
