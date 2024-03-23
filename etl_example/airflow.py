"""
## TaskFlow API ETL Example DAG

This example shows how to use the TaskFlow API to build a DAG with three tasks.
The first task extracts the Bitcoin price from an API, the second task processes
the data and the third task prints the result to the logs.
"""

from airflow.decorators import dag, task
from datetime import datetime
from typing import Dict
import requests
import logging

API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"


@dag(
    schedule="@daily",
    start_date=datetime(2023, 12, 19),
    catchup=False,
    tags=["TaskFlow", "Tutorial Part 1"],
)
def taskflow_api_example_dag():
    @task(task_id="extract", retries=2)
    def extract_bitcoin_price() -> Dict[str, float]:
        return requests.get(API).json()["bitcoin"]

    @task(multiple_outputs=True)
    def process_data(response: Dict[str, float]) -> Dict[str, float]:
        logging.info(response)
        return {"usd": response["usd"], "change": response["usd_24h_change"]}

    @task
    def store_data(data: Dict[str, float]):
        logging.info(f"Store: {data['usd']} with change {data['change']}")

    store_data(process_data(extract_bitcoin_price()))


taskflow_api_example_dag()
