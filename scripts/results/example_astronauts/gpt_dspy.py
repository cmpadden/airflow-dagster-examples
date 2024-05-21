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


class AstronautsConfig(Config):
    api_url: str = Field(
        default="http://api.open-notify.org/astros.json",
        description="API URL for fetching astronauts currently in space",
    )


@asset
def astronauts_in_space(config: AstronautsConfig) -> MaterializeResult:
    """
    This asset fetches the list of astronauts currently in space from the Open Notify API, raises an error for bad responses, and attaches metadata including the total number of astronauts and their respective crafts.
    """
    response = requests.get(config.api_url)
    response.raise_for_status()  # Raises an HTTPError for bad responses
    astronauts = response.json()["people"]
    astronaut_count = len(astronauts)
    crafts = {astronaut["name"]: astronaut["craft"] for astronaut in astronauts}

    # Print each astronaut's name and their flying craft
    for name, craft in crafts.items():
        print(f"{name} is currently in space flying on the {craft}! Hello! :)")

    # Create metadata entries
    metadata_entries = {
        "total_astronauts": MetadataValue.int(astronaut_count),
        "crafts": MetadataValue.json(crafts),
    }

    # Return the result with metadata
    return MaterializeResult(metadata=metadata_entries)


# Define the retry policy
retry_policy = RetryPolicy(max_retries=3)

# Define the asset job with retry policy
asset_job = define_asset_job(
    "daily_astronauts_job",
    selection=AssetSelection.assets(astronauts_in_space),
    op_retry_policy=retry_policy,
)

# Define the schedule for the job
job_schedule = ScheduleDefinition(
    job=asset_job,
    cron_schedule="0 0 * * *",  # Daily at midnight
    name="daily_astronauts_schedule",
)

# Define the Dagster definitions including assets, jobs, and schedules
definitions = Definitions(
    assets=[astronauts_in_space], jobs=[asset_job], schedules=[job_schedule]
)
