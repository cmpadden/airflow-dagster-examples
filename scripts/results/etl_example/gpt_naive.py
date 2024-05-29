```python
"""
Dagster ETL Example

This example shows how to use Dagster to build a pipeline with three tasks.
The first task extracts the Bitcoin price from an API, the second task processes
the data and the third task prints the result to the logs.
"""

from dagster import job, op, AssetMaterialization, AssetKey
import requests
import logging

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

@op
def extract_bitcoin_price(context) -> dict:
    response = requests.get(API)
    if response.status_code == 200:
        return response.json()["bitcoin"]
    else:
        context.log.error("Failed to fetch data")
        return {}

@op
def process_data(context, response: dict) -> dict:
    if response:
        context.log.info(f"Processing data: {response}")
        return {"usd": response["usd"], "change": response["usd_24h_change"]}
    else:
        return {"usd": 0, "change": 0}

@op
def store_data(context, data: dict):
    context.log.info(f"Store: {data['usd']} with change {data['change']}")
    yield AssetMaterialization(
        asset_key=AssetKey(["bitcoin_price"]),
        description="Bitcoin price and change",
        metadata={
            "USD": data["usd"],
            "Change": data["change"]
        }
    )

@job
def dagster_etl_job():
    store_data(process_data(extract_bitcoin_price()))

```

This Dagster code defines a job `dagster_etl_job` that performs the same ETL process as the Airflow DAG. It extracts Bitcoin prices, processes the data, and logs the results. The `store_data` op also materializes the result as an asset for better observability and tracking within the Dagster environment.