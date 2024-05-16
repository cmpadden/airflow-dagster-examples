import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable

# TODO: Prompt the model to not translate Airflow code if it's a sensor
class TranslateCoreLogicSignature(dspy.Signature):
    """
    Translate the core logic in the Airflow code into Dagster code.

    Requirements:
        - Use `@asset` instead of `@op`, `@job`
        - Assets should be descriptive nouns, ideally related to the domain
        - Use as few assets as possible, combining assets which are too low-level where appropriate
        - When an asset relies on another asset, it should use the name of the upstream asset as input

    Out of scope:
        - Asset materialization metadata
        - Data quality checks
        - Schedule, sensors, resources, io managers, definitions
        - Skip translating any Airflow sensors
    """

    airflow_code = dspy.InputField()
    dagster_code = dspy.OutputField()


class ConsolidateAssets(dspy.Signature):
    """
    Reflect on the given Dagster code, and suggest potential improvements.

    Improvements could include consolidating the number of assets, ensuring the names of assets are nouns, etc.
    """

    input_dagster_code = dspy.InputField(desc="The input Dagster code")
    dagster_code = dspy.OutputField(desc="The improved Dagster code")


def fewer_assets_than_tasks(airflow_code: str, dagster_code: str) -> bool:
    task_count = airflow_code.count("@task")
    asset_count = dagster_code.count("@asset")

    # Can't have fewer assets than tasks if there is only one or no tasks
    if task_count <= 1:
        return True

    return asset_count < task_count


class TranslateCoreLogicModule(dspy.Module):
    def __init__(self):
        self.code_translator = dspy.ChainOfThought(TranslateCoreLogicSignature)
        self.asset_consolidator = dspy.ChainOfThought(ConsolidateAssets)

    def forward(self, airflow_code: str) -> dspy.Prediction:
        pred = self.code_translator(airflow_code=airflow_code)
        pred.dagster_code = extract_code_block_from_markdown(pred.dagster_code)

        dspy.Assert(
            "@job" not in pred.dagster_code and "@op" not in pred.dagster_code,
            "The code should not contain @job or @op",
        )

        pred = self.asset_consolidator(input_dagster_code=pred.dagster_code)
        pred.dagster_code = extract_code_block_from_markdown(pred.dagster_code)

        dspy.Suggest(
            fewer_assets_than_tasks(airflow_code, pred.dagster_code),
            "The number of Dagster assets should be fewer than the number of Airflow tasks",
        )

        dspy.Suggest(
            *is_runnable(pred.dagster_code, verbose=True)
        )  # Some code cannot be run without external dependencies

        return dspy.Prediction(dagster_code=pred.dagster_code)
