
from dagster import Definitions, asset, define_asset_job, ScheduleDefinition, RetryPolicy
from dagster_aws.s3 import S3PickleIOManager, S3Resource

# Define assets
@asset
def dagster_github_issues():
    # Logic to fetch and process GitHub issues
    pass

@asset
def dagster_github_pull_requests():
    # Logic to fetch and process GitHub pull requests
    pass

# Define resources
s3_resource = S3Resource()
s3_pickle_io_manager = S3PickleIOManager(s3_resource=s3_resource, s3_bucket="my-dagster-bucket")

# Define a job that includes all assets with a retry policy
retry_policy = RetryPolicy(max_retries=2)  # Retry up to 2 times
all_assets_job = define_asset_job(
    "all_assets_job",
    selection=[dagster_github_issues, dagster_github_pull_requests],
    op_retry_policy=retry_policy
)

# Define a schedule for the job
daily_schedule = ScheduleDefinition(
    job=all_assets_job,
    cron_schedule="0 0 * * *",  # Run daily at midnight
    name="daily_asset_refresh"
)

# Combine all assets, resources, job, and schedule into a Definitions object
defs = Definitions(
    assets=[dagster_github_issues, dagster_github_pull_requests],
    resources={"s3_io_manager": s3_pickle_io_manager},
    jobs=[all_assets_job],
    schedules=[daily_schedule]
)
