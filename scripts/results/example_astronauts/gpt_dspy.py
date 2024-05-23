import requests
from dagster import (
    Config,
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from pydantic import Field


class AstronautMessagesConfig(Config):
    api_url: str = Field(
        default="http://api.open-notify.org/astros.json",
        description="API URL for fetching astronaut data",
    )


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
        "total_astronauts": MetadataValue.int(len(astronauts)),
        "sample_message": MetadataValue.text(
            messages[0] if messages else "No astronauts currently in space."
        ),
    }
    return MaterializeResult(output=messages, metadata=metadata)


astronaut_job = define_asset_job("astronaut_job", selection=[astronaut_messages])

daily_astronaut_schedule = ScheduleDefinition(
    job=astronaut_job,
    cron_schedule="0 0 * * *",  # Run daily at midnight
    default_status=DefaultScheduleStatus.RUNNING,  # Schedule is active by default
)


defs = Definitions(
    assets=[astronaut_messages],
    jobs=[astronaut_job],
    schedules=[daily_astronaut_schedule],
)
