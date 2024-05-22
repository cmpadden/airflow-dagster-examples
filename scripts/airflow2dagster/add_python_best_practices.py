import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable

relevant_documentation = """
### Dagster Error Handling

Dagster assets typically handle their own error handling. Unless it is an exception that can be caught and handled within the asset, it is best to let the error propagate up to the Dagster pipeline. This way, the error can be logged and handled by Dagster.

Do NOT return MaterializeResult if you encounter an error. Instead, raise an exception and let Dagster handle it.

Instead of doing this:
```
import requests

try:
    response = requests.get(API)
    response.raise_for_status()
    return MaterializeResult()
except requests.RequestException as e:
    return MaterializeResult(metadata={"error": MetadataValue.text(str(e))})
```

Do this:
```
import requests

response = requests.get(API)
response.raise_for_status()
```

### Dagster Logging
Including logs and debugging information in your Dagster code can be very helpful. Dagster has its own logging system that is integrated with the rest of the Dagster ecosystem. This should be done using the `context.log` object, and not by using `print` statements or the `logging` module. The `context.log` object is available from a magic parameter in an asset-decorated function. The parent `context` argument is typed to AssetExecutionContext.

It can be used like this:
```
from dagster import asset, AssetExecutionContext

@asset
def my_asset(context: AssetExecutionContext):
    context.log.info("This is an info message")
    context.log.warning("This is a warning message")
    context.log.error("This is an error message")
```

DO NOT remove any existing `MaterializeResult` objects that are being returned. Logging and returning metadata are not mutually exclusive and are complementary to each other.
"""


class AddPythonBestPractices(dspy.Signature):
    """
    Override generic Python best practices with Dagster-specific best practices.

    Do not catch most exceptions and instead let them propagate up to Dagster.

    Do not use `print` statements or the `logging` module. Use `context.log` instead.
    """

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    input_dagster_code = dspy.InputField()
    dagster_code = dspy.OutputField(
        desc="Dagster code that is following Pythonic best practices"
            "while also adhering to Dagster best practices"
    )


class AddPythonBestPracticesModule(dspy.Module):
    def __init__(self):
        self.retrieve = dspy.Retrieve(k=3)
        self.add_best_practices = dspy.ChainOfThought(
            AddPythonBestPractices
        )

    def forward(self, input_dagster_code: str) -> dspy.Prediction:
        # context = self.retrieve(self.add_best_practices.signature.__doc__).passages
        context = [relevant_documentation]
        pred = self.add_best_practices(
            context="\n".join(context), input_dagster_code=input_dagster_code
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
