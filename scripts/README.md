# Script Usage

> **Notes**:
> - Set your working directory here to run these scripts
> - An `OPENAI_API_KEY` is required to run the scripts below.
Please add it to your `.env` file.

## Generate translation for a single example using DSPy
```sh
poetry run python -m airflow2dagster --airflow-filepath "../<path-to-airflow-file>"
```

## Update the leaderboard
```sh
make leaderboard
```

This command first re-generates translations for all examples.
After that, the translations are evaluated and the leaderboard is updated.


## How to add more solution:
```py
def your_amazing_gpt5_function(airflow_code: str) -> str:
    pass
```

Define your funtion like this and make sure include it into the generate and evaluate scripts.


## (Optional) Update documentation in the vector store
An in-file vectorstore has been included at `chroma.db`. This is used for the DSPy implementation.

You can tweak the script below to add more documentation to the vector store, if needed.

```sh
poetry run python vectorize_dagster_docs.py
```
