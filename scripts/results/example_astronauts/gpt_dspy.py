
from dagster import asset, Config, MaterializeResult, MetadataValue, Definitions, define_asset_job, ScheduleDefinition, RetryPolicy
import requests
from pydantic import Field

class AstronautMessagesConfig(Config):
    api_url: str = Field(default="http://api.open-notify.org/astros.json", description="API URL for fetching astronaut data")

@asset
def astronaut_messages(config: AstronautMessagesConfig) -> MaterializeResult:
    """
    Fetches data about astronauts currently in space from the Open Notify API and creates a descriptive message for each astronaut.
    Returns:
        MaterializeResult: Descriptive messages about each astronaut with metadata.
    """
    response = requests.get(config.api_url)
    response.raise_for_status()  # Proper error handling
    astronauts = response.json()["people"]
    messages = []
    for astronaut in astronauts:
        name = astronaut["name"]
        craft = astronaut["craft"]
        message = f"{name} is currently in space flying on the {craft}! Hello! :)"
        messages.append(message)
        print(message)  # Print each message as a side effect

    # Metadata to include the number of astronauts and a sample message
    metadata = {
        "number_of_astronauts": MetadataValue.int(len(list_of_people_in_space)),
        "source": MetadataValue.url("http://api.open-notify.org/astros.json")
    }
    return MaterializeResult(output=list_of_people_in_space, metadata=metadata)

# Define the job that includes the astronaut_messages asset with a retry policy
retry_policy = RetryPolicy(max_retries=3)
astronaut_job = define_asset_job(
    "astronaut_job",
    selection=AssetSelection.assets(astronaut_messages),
    op_retry_policy=retry_policy
)

# Define the schedule for the job
astronaut_schedule = ScheduleDefinition(
    job=astronaut_job,
    cron_schedule="0 0 * * *",  # Run daily at midnight
    name="daily_astronaut_update"
)

# Update the Definitions object to include the job and schedule
defs = Definitions(
    assets=[current_astronauts, astronaut_crafts],
    jobs=[astronauts_job],
    schedules=[astronauts_schedule],
)
