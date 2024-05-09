from dagster import asset, RetryPolicy, MaterializeResult, AssetExecutionContext
import requests

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

BITCOIN_PRICE_FILE_PATH = "../data/bitcoin_price.txt"

@asset(
    retry_policy=RetryPolicy(max_retries=2)
)
def bitcoin_prices(context: AssetExecutionContext) -> MaterializeResult:
    # equivalent to extract_bitcoin_price
    response = requests.get(API).json()["bitcoin"]
    
    # equivalent to process_data
    context.log.info(response)

    # equivalent to store_data
    with open(BITCOIN_PRICE_FILE_PATH, "w") as f:
        f.write(str(response))

    context.log.info(f"Store: {response['usd']} with change {response['usd_24h_change']}")

    # unique to Dagster: storing metadata in the asset
    return MaterializeResult(
        metadata={
            "usd": response["usd"],
            "change": response["usd_24h_change"],
            "file_path": BITCOIN_PRICE_FILE_PATH,
        }
    )