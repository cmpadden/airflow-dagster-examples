# Script Usage

> **Notes**:
> - Set your working directory here to run these scripts
> - An `OPENAI_API_KEY` is required to run the scripts below.
Please add it to your `.env` file.


## Generate
```sh
poetry run python generate.py cli-generate
```


## Evaluate
```sh
poetry run python evaluate_results.py build-supervised-leaderboard
```


## How to add more solution:
```py
def your_amazing_gpt5_function(airflow_code: str) -> str:
    pass
```

Define your funtion like this and make sure include it into the generate and evaluate scripts.


## Add documents to vector store (for DSPy implementation)
```sh
poetry run python vectorize_dagster_docs.py
```
