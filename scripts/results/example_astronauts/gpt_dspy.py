
from dagster import (
    asset, MaterializeResult, MetadataValue, define_asset_job, ScheduleDefinition,
    Definitions, DefaultScheduleStatus, RetryPolicy
)
import requests

@asset
def astronaut_messages() -> MaterializeResult:
    """
    Fetches data about astronauts currently in space from the Open Notify API and creates a descriptive message for each astronaut.
    Returns:
        MaterializeResult: Descriptive messages about each astronaut with metadata.
    """
    response = requests.get("http://api.open-notify.org/astros.json")
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
        "total_astronauts": MetadataValue.int(len(astronauts)),
        "sample_message": MetadataValue.text(messages[0] if messages else "No astronauts currently in space.")
    }
    return MaterializeResult(output=messages, metadata=metadata)

# Define the retry policy
retry_policy = RetryPolicy(max_retries=3)

# Define the job that includes the astronaut_messages asset with retry policy
astronaut_job = define_asset_job(
    "astronaut_job",
    selection=[astronaut_messages],
    op_retry_policy=retry_policy
)

# Define the schedule for the job
astronaut_schedule = ScheduleDefinition(
    job=astronaut_job,
    cron_schedule="0 0 * * *",  # At 00:00 (midnight) every day
    default_status=DefaultScheduleStatus.RUNNING
)

# Update the Definitions object to include the job and schedule
defs = Definitions(
    assets=[astronaut_messages],
    jobs=[astronaut_job],
    schedules=[astronaut_schedule]
)
