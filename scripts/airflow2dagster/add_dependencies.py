import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable

relevant_documentation = """
### 
```
Requirements:
    - Asset functions should manage reading and writing data themselves, such as reading from a file to disk or writing to a database
    - Use the `@asset` decorator's `deps` argument to specify dependencies
    - Pass the dependencies as a list of asset-decorated functions
    - Out of convention, avoid using IO managers
    - Do not use `ins` and `outs` arguments, and thus, don't use AssetIn or AssetOut
    - Do not pass the dependencies as arguments to the decorated function
```
"""


class AddDependenciesSignature(dspy.Signature):
    """
    Surface relevant metadata using `MaterializeResult` instead of `AssetMaterialization`.

    Ensure that metadata fields are added using `MetadataValue`.
    """

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    input_dagster_code = dspy.InputField()
    dagster_code = dspy.OutputField(
        desc="Dagster code with `MaterializeResult` surfacing asset metadata where relevant"
    )


class AddDependenciesModule(dspy.Module):
    def __init__(self):
        self.retrieve = dspy.Retrieve(k=3)
        self.add_dependencies = dspy.ChainOfThought(
            AddDependenciesSignature
        )

    def forward(self, input_dagster_code: str) -> dspy.Prediction:
        # context = self.retrieve(self.add_dependencies.signature.__doc__).passages
        context = [relevant_documentation]
        pred = self.add_dependencies(
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
