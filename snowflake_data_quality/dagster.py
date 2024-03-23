from dagster import asset, AssetExecutionContext, MaterializeResult, EnvVar, AssetCheckSpec, AssetCheckResult, asset_check, StaticPartitionsDefinition
from dagster_snowflake import SnowflakeResource
from dagster_aws.s3 import S3Resource
import pandas as pd

TABLE = "YELLOW_TRIPDATA_WITH_UPLOAD_DATE"
DATES = ["2019-01", "2019-02"]
STAGING_DIR_NAME = "tripdata_to_snowflake"

s3_resource = S3Resource(
    bucket=EnvVar("S3_BUCKET"),
    key=EnvVar("S3_KEY"),
    secret=EnvVar("S3_SECRET"),
    aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS"),
)

snowflake_resource = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
    schema=EnvVar("SNOWFLAKE_SCHEMA"),
)

file_partitions = StaticPartitionsDefinition(DATES)

@asset(
    partitions_def=file_partitions,
    compute_kind="S3"
)
def staged_yellow_tripdata(context: AssetExecutionContext, s3: S3Resource) -> MaterializeResult
    date = context.partition_key

    # not included
    file_name = f"yellow_tripdata_sample_{date}.csv"
    file_path = f"sample_data/yellow_trip_data/{file_name}"
    destination = f"{STAGING_DIR_NAME}/{file_name}"

    # untested
    with s3.get_client() as client:
        client.upload_file(destination, file_path)

    return MaterializeResult(
        metadata={
            "file_name": file_name
        }
    )

@asset(
    deps=[staged_yellow_tripdata],
)
def yellow_tripdata(context: AssetExecutionContext, snowflake: SnowflakeResource) -> MaterializeResult:

    # create a Snowflake stage called {TABLE}_STAGE for all of the files in the STAGING_DIR_NAME directory, with 
    # copy all files that match the pattern {STAGING_DIR_NAME}/yellow_tripdata_sample_*.csv to the {TABLE}_STAGE stage
    # copy the files from the {TABLE}_STAGE stage to the {TABLE} table

    # TODO: Make an easy way to make a stage
    with snowflake.connect() as connection:
        connection.execute(f"CREATE OR REPLACE STAGE {TABLE}_STAGE
            # TODO: This stage should be created with the correct file format
        ;")
        connection.execute(f"PUT file://{STAGING_DIR_NAME}/yellow_tripdata_sample_*.csv @{TABLE}_STAGE;")
        connection.execute(f"COPY INTO {TABLE} FROM @{TABLE}_STAGE;")


@asset_check(
    asset=yellow_tripdata
)
def check_row_count(context: AssetExecutionContext, snowflake: SnowflakeResource) -> AssetCheckResult:
    query = "SELECT COUNT(*) FROM {TABLE};"
    expected = 20000

    with snowflake.connect() as connection:
        result = connection.execute(query)
        row_count = result.fetchone()[0]

    return AssetCheckResult(
        passed=row_count == expected,
        metadata={
            "row_count": row_count
        }
    )

@asset_check(
    asset=yellow_tripdata
)
def check_interval_data(context: AssetExecutionContext, snowflake: SnowflakeResource) -> AssetCheckResult:
    """
    #### Run Interval Check
    Check that the average trip distance today is within a desirable threshold
    compared to the average trip distance yesterday.
    """
    with snowflake.connect() as connection:
        query = f"""
        SELECT AVG(trip_distance) FROM {TABLE}
        WHERE upload_date = CURRENT_DATE - 1;
        """
        result = connection.execute(query)
        yesterday_avg = result.fetchone()[0]

        query = f"""
        SELECT AVG(trip_distance) FROM {TABLE}
        WHERE upload_date = CURRENT_DATE;
        """
        result = connection.execute(query)
        today_avg = result.fetchone()[0]

    metrics_threshold = 1.5

    passed = today_avg <= yesterday_avg * metrics_threshold and today_avg >= yesterday_avg / metrics_threshold

    return AssetCheckResult(
        passed=passed,
        metadata={
            "yesterday_avg": yesterday_avg,
            "today_avg": today_avg
        }
    )

@asset_check(
    asset=yellow_tripdata
)
def check_threshold(context: AssetExecutionContext, snowflake: SnowflakeResource) -> AssetCheckResult:
    query = "SELECT MAX(passenger_count) FROM {TABLE};"
    min_threshold = 1
    max_threshold = 8

    with snowflake.connect() as connection:
        result = connection.execute(query)
        passenger_count = result.fetchone()[0]
        passed = passenger_count >= min_threshold and passenger_count <= max_threshold

    return AssetCheckResult(
        passed=passed,
        metadata={
            "max_passenger_count": passenger_count
        }
    )

row_checks = [
    {
        "check_name": "date_check",
        "check_statement": "dropoff_datetime > pickup_datetime",
        "table": TABLE
    },
    {
        "check_name": "passenger_count_check",
        "check_statement": "passenger_count >= 0",
        "table": TABLE
    },
    {
        "check_name": "trip_distance_check",
        "check_statement": "trip_distance >= 0 AND trip_distance <= 100",
        "table": TABLE
    },
    {
        "check_name": "fare_check",
        "check_statement": "ROUND((fare_amount + extra + mta_tax + tip_amount + improvement_surcharge + COALESCE(congestion_surcharge, 0)), 1) = ROUND(total_amount, 1) THEN 1 WHEN ROUND(fare_amount + extra + mta_tax + tip_amount + improvement_surcharge, 1) = ROUND(total_amount, 1)",
        "table": TABLE
    }
]

# TODO: Make a factory to create row-level checks
def make_row_check(target_asset, check_name, check_statement, table):

    @asset_check(
        name=check_name,
        asset=target_asset,
    )
    def row_check(snowflake: SnowflakeResource) -> AssetCheckResult:
        query = f"SELECT COUNT(*) FROM {table} WHERE NOT ({check_statement});"
        expected = 0
        result = snowflake.query(query)
        return AssetCheckResult(
            passed=result == expected,
            metadata={
                "result": result
            }
        )
    return row_check

row_checks = [make_row_check(staged_yellow_tripdata, **check) for check in row_checks]