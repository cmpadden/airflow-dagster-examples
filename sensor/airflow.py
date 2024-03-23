"""
### Create a custom sensor using the @task.sensor decorator

This DAG showcases how to create a custom sensor using the @task.sensor decorator to check the availability of an API.
"""

from airflow.decorators import dag, task
from pendulum import datetime
from airflow.sensors.base import PokeReturnValue
import requests


@dag(
    start_date=datetime(2022, 12, 1),
    schedule=None,
    catchup=False,
    tags=["sensor"],
)
def sensor_decorator():
    # turn any Python function into a sensor
    @task.sensor(poke_interval=10, timeout=3600, mode="reschedule")
    def check_shibe_availability() -> PokeReturnValue:
        r = requests.get("http://shibe.online/api/shibes?count=1&urls=true")
        print(r.status_code)

        # set the condition to True if the API response was 200
        if r.status_code == 200:
            condition_met = True
            operator_return_value = r.json()
        else:
            condition_met = False
            operator_return_value = None
            print(f"Shibe URL returned the status code {r.status_code}")

        # the function has to return a PokeReturnValue
        # if is_done = True the sensor will exit successfully, if is_done=False, the sensor will either poke or be rescheduled
        return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)

    # click the link in the logs for a cute picture :)
    @task
    def print_shibe_picture_url(url):
        print(url)

    print_shibe_picture_url(check_shibe_availability())


sensor_decorator()
