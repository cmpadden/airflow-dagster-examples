import logging

import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

docs = """
# Sensors

## Defining a sensor

To write a sensor that materializes assets, you can [build a job that materializes assets](/concepts/assets/software-defined-assets#building-jobs-that-materialize-assets):

```python file=concepts/partitions_schedules_sensors/sensors/sensors.py startafter=start_asset_job_sensor_marker endbefore=end_asset_job_sensor_marker
asset_job = define_asset_job("asset_job", "*")


@sensor(job=asset_job)
def materializes_asset_sensor():
    yield RunRequest(...)
```

If no asset jobs are present, you can link a sensor directly to the asset(s) using the `asset_selection` syntax:
```python
@asset
def my_asset(config: MyAssetConfig):
    ...


@sensor(asset_selection=[my_asset])
def my_sensor(context: SensorEvaluationContext):
    ...
    yield RunRequest(run_config={"ops": {"my_asset": {"config": {"...": "..."}}})
```

---

## Idempotence and cursors

When instigating runs based on external events, you usually want to run exactly one job run for each event. There are two ways to define your sensors to avoid creating duplicate runs for your events:

- [Using a `run_key`](#idempotence-using-run-keys)
- [Using cursors](#sensor-optimizations-using-cursors)

### Idempotence using run keys

In the example sensor [in the previous section](#defining-a-sensor), the <PyObject object="RunRequest"/> is constructed with a `run_key`:

```python
yield RunRequest(
    run_key=filename,
    run_config={"ops": {"process_file": {"config": {"filename": filename}}}},
)
```

Dagster guarantees that for a given sensor, at most one run is created for each <PyObject object="RunRequest"/> with a unique `run_key`. If a sensor yields a new run request with a previously used `run_key`, Dagster skips processing the new run request.

In the example, a <PyObject object="RunRequest"/> is requested for each file during every sensor evaluation. Therefore, for a given sensor evaluation, there already exists a `RunRequest` with a `run_key` for any file that existed during the previous sensor evaluation. Dagster skips processing duplicate run requests, so Dagster launches runs for only the files added since the last sensor evaluation. The result is exactly one run per file.

Run keys allow you to write sensor evaluation functions that declaratively describe what job runs should exist, and helps you avoid the need for more complex logic that manages state. However, when dealing with high-volume external events, some state-tracking optimizations might be necessary.

### Sensor optimizations using cursors

When writing a sensor that deals with high-volume events, it might not be feasible to yield a <PyObject object="RunRequest"/> during every sensor evaluation. For example, you may have an `s3` storage bucket that contains thousands of files.

When writing a sensor for such event sources, you can maintain a cursor that limits the number of yielded run requests for previously processed events. The sensor context, provided to every sensor evaluation function, has a `cursor` property and a `update_cursor` method for sensors to track state across evaluations:

- `cursor` - A cursor field on <PyObject object="SensorEvaluationContext"/> that returns the last persisted cursor value from a previous evaluation
- `update_cursor` - A method on <PyObject object="SensorEvaluationContext"/> that takes a string to persist and make available to future evaluations

Here is a somewhat contrived example of our directory file sensor using a cursor for updated files:

```python file=concepts/partitions_schedules_sensors/sensors/sensors.py startafter=start_cursor_sensors_marker endbefore=end_cursor_sensors_marker
@sensor(job=asset_job)
def my_directory_sensor_cursor(context):
    last_mtime = float(context.cursor) if context.cursor else 0

    max_mtime = last_mtime
    for filename in os.listdir(MY_DIRECTORY):
        filepath = os.path.join(MY_DIRECTORY, filename)
        if os.path.isfile(filepath):
            fstats = os.stat(filepath)
            file_mtime = fstats.st_mtime
            if file_mtime <= last_mtime:
                continue

            # the run key should include mtime if we want to kick off new runs based on file modifications
            run_key = f"{filename}:{file_mtime}"
            run_config = {"ops": {"process_file": {"config": {"filename": filename}}}}
            yield RunRequest(run_key=run_key, run_config=run_config)
            max_mtime = max(max_mtime, file_mtime)

    context.update_cursor(str(max_mtime))
```

For sensors that consume multiple event streams, you may need to serialize and deserialize a more complex data structure in and out of the cursor string to keep track of the sensor's progress over the multiple streams.

Note also that in the example above, both a `run_key` _and_ cursor are being used. This means that if the cursor is reset but the target files don't change, new runs will not be launched because the run keys have not changed. If you want the ability to fully reset the state of your sensor by resetting the cursor, then you should not set `run_key` on `RunRequest`.

---

## Evaluation interval

By default, the Dagster daemon runs a sensor 30 seconds after that sensor's previous evaluation finishes executing. You can configure the interval using the `minimum_interval_seconds` argument on the <PyObject object="sensor" decorator/> decorator.

It's important to note that this interval represents a minimum interval between runs of the sensor and not the exact frequency the sensor runs. If you have a sensor that takes 30 seconds to complete, but the `minimum_interval_seconds` is `5` seconds, the fastest Dagster daemon will run the sensor is every 35 seconds. The `minimum_interval_seconds` only guarantees that the sensor is not evaluated more frequently than the given interval.

For example, here are two sensors that specify two different minimum intervals:

```python file=concepts/partitions_schedules_sensors/sensors/sensors.py startafter=start_interval_sensors_maker endbefore=end_interval_sensors_maker
@sensor(job=my_job, minimum_interval_seconds=30)
def sensor_A():
    yield RunRequest(run_key=None, run_config={})


@sensor(job=my_job, minimum_interval_seconds=45)
def sensor_B():
    yield RunRequest(run_key=None, run_config={})
```

These sensor definitions are short, so they run in less than a second. Therefore, you can expect these sensors to run consistently around every 30 and 45 seconds, respectively.

If a sensor evaluation function takes more than 60 seconds to return its results, the sensor evaluation will time out and the Dagster daemon will move on to the next sensor without submitting any runs. This 60 second timeout only applies to the time it takes to run the sensor function, not to the execution time of the runs submitted by the sensor. To avoid timeouts, slower sensors can break up their work into chunks, using [cursors](/concepts/partitions-schedules-sensors/sensors#sensor-optimizations-using-cursors) to let subsequent sensor calls pick up where the previous call left off.

---

## Skipping sensor evaluations

For debugging purposes, it's often useful to describe why a sensor might not yield any runs for a given evaluation. The sensor evaluation function can yield a <PyObject object="SkipReason" /> with a string description that will be displayed in the UI.

For example, here is our directory sensor that now provides a `SkipReason` when no files are encountered:

```python file=concepts/partitions_schedules_sensors/sensors/sensors.py startafter=start_skip_sensors_marker endbefore=end_skip_sensors_marker
@sensor(job=asset_job)
def my_directory_sensor_with_skip_reasons():
    has_files = False
    for filename in os.listdir(MY_DIRECTORY):
        filepath = os.path.join(MY_DIRECTORY, filename)
        if os.path.isfile(filepath):
            yield RunRequest(
                run_key=filename,
                run_config={
                    "ops": {"process_file": {"config": {"filename": filename}}}
                },
            )
            has_files = True
    if not has_files:
        yield SkipReason(f"No files found in {MY_DIRECTORY}.")
```

"""


class AddSensorSignature(dspy.Signature):
    """
    Translate sensors in the Airflow code and add it to the Dagster code's `Definitions`.

    Keep the input Dagster code intact — only translate and add the sensor(s) to the `Definitions` object.
    """

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    airflow_code = dspy.InputField(desc="Airflow code containing a sensor")
    input_dagster_code = dspy.InputField(desc="Equivalent Dagster code without a sensor")
    dagster_code = dspy.OutputField(
        desc="Input Dagster code with similar sensor behaviour to Airflow code"
    )


class Output(BaseModel):
    has_sensor: bool = Field(description="Whether the code has any sensors")


class DetectSensorSignature(dspy.Signature):
    """Does the following Airflow code contain sensors?"""

    airflow_code = dspy.InputField()
    output: Output = dspy.OutputField()


class AddSensorModule(dspy.Module):
    def __init__(self):
        self.detect_sensor = dspy.TypedPredictor(DetectSensorSignature)
        self.add_sensor = dspy.ChainOfThought(AddSensorSignature)

    def forward(self, airflow_code: str, input_dagster_code: str) -> dspy.Prediction:
        has_sensor = self.detect_sensor(airflow_code=airflow_code).output.has_sensor
        if not has_sensor:
            logger.info(
                "No sensors detected in the Airflow code. Skipping adding sensors."
            )
            return dspy.Prediction(dagster_code=input_dagster_code)
        pred = self.add_sensor(
            context=docs,
            airflow_code=airflow_code,
            input_dagster_code=input_dagster_code,
        )
        pred.dagster_code = extract_code_block_from_markdown(pred.dagster_code)

        dspy.Assert(
            "@asset" in pred.dagster_code,
            "Do not forget to include the input Dagster code in the final code.",
        )
        dspy.Suggest(
            *is_runnable(pred.dagster_code, verbose=True)
        )  # Some code cannot be run without external dependencies

        return dspy.Prediction(dagster_code=pred.dagster_code)
