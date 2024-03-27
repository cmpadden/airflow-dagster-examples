# airflow-dagster-examples
A compilation of common Airflow DAGs and their Dagster equivalent

## To run

To run the Dagster examples, install Dagster and its UI:

```bash
pip install dagster dagster-webserver
```

Then cd into any of the directories and run:

```bash
dagster dev -f dagster_version.py
```

# STABLE
* `elt_example`
* `sensor`
* `dbt`
* `duckdb`
* `example_astronauts`

# WIP
* `list_files` -> why: it shows dynamic partitions really well
* `snowflake_data_quality` -> why: it shows asset checks really well
* 