
from dagster import asset, MaterializeResult, MetadataValue, Config, Definitions, define_asset_job, ScheduleDefinition, AssetExecutionContext, RetryPolicy
import requests
from pydantic import Field

@asset(retry_policy=RetryPolicy(max_retries=3))
def current_astronauts(context: AssetExecutionContext) -> MaterializeResult:
    """This asset uses the requests library to retrieve a list of Astronauts currently in space. The results are returned as a list of dictionaries."""
    response = requests.get("http://api.open-notify.org/astros.json")
    response.raise_for_status()
    list_of_people_in_space = response.json()["people"]
    # Adding metadata
    metadata = {
        "number_of_astronauts": MetadataValue.int(len(list_of_people_in_space)),
        "source": MetadataValue.url("http://api.open-notify.org/astros.json")
    }
    return MaterializeResult(output=list_of_people_in_space, metadata=metadata)

class AstronautCraftsConfig(Config):
    greeting: str = Field(default="Hello! :)", description="Greeting message for the astronauts")

@asset(retry_policy=RetryPolicy(max_retries=3))
def astronaut_crafts(context: AssetExecutionContext, current_astronauts: list[dict], config: AstronautCraftsConfig) -> MaterializeResult:
    """This asset prints the name of each Astronaut in space and the craft they are flying on, using the data from the `current_astronauts` asset."""
    greeting = config.greeting
    crafts = []
    for person_in_space in current_astronauts:
        craft = person_in_space["craft"]
        name = person_in_space["name"]
        crafts.append(f"{name} is currently in space flying on the {craft}! {greeting}")
        context.log.info(crafts[-1])
    # Adding metadata
    metadata = {
        "number_of_crafts": MetadataValue.int(len(set(person["craft"] for person in current_astronauts))),
        "greeting_message": MetadataValue.text(greeting)
    }
    return MaterializeResult(output=None, metadata=metadata)

# Define the job that will materialize the assets
astronauts_job = define_asset_job("astronauts_job", selection=[current_astronauts, astronaut_crafts])

# Define the schedule to run the job daily
astronauts_schedule = ScheduleDefinition(
    job=astronauts_job,
    cron_schedule="0 0 * * *",  # daily at midnight
)

# Create the Definitions object
defs = Definitions(
    assets=[current_astronauts, astronaut_crafts],
    jobs=[astronauts_job],
    schedules=[astronauts_schedule],
)
