import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable

relevant_documentation = """
### Add Metadata when running

Prioritize returning metadata using `MaterializeResult`. This is a best practice in Dagster to ensure that metadata made at runtime is tracked asset definition functions. Metadata can be added to an asset returning the `MaterializeResult` object.

Metadata is important for high-level information about an asset. It can include information like the asset's schema, summary statistics, or even visualizations. In Dagster, metadata can be added to an asset using the `MaterializeResult` object.

It is a Dagster best practice to track metadata fo any assets that are materialized. If you are not using IO managers, the recommended way to do this is to use the `MaterializeResult` object to return metadata at the end of the asset function.

In the below example, we are adding metadata to the asset using the `MaterializeResult` object. The metadata is a dictionary with a single key, `plot`, that contains a Markdown image of the top 25 words in Hacker News titles.

```
from dagster import asset, MetadataValue, MaterializeResult
import pandas as pd
import os

@asset
def data_asset(context) -> MaterializeResult:
    # Sample data
    data = {
        'col1': [1, 2, 3, 4, 5],
        'col2': [10, 20, 30, 40, 50]
    }
    df = pd.DataFrame(data)

    # Write data to local storage
    file_path = "data.csv"
    df.to_csv(file_path, index=False)

    # Calculate metadata
    file_size = os.path.getsize(file_path)
    row_count = len(df)
    averages = df.mean().to_dict()

    # Return MaterializeResult
    return MaterializeResult(
        metadata={
            "file_path": MetadataValue.path(file_path),
            "file_size": MetadataValue.int(file_size),
            "row_count": MetadataValue.int(row_count),
            "averages": MetadataValue.json(averages)
        }
    )
```

DO NOT just blindly return the metadata as a dictionary without wrapping it in a `MaterializeResult`.

### Definition-time metadata

In all cases, you should add static metadata to an asset when it's defined. This can be done using some arguments in the `@asset` decorator. For example, you should categorize your assets with the `group_name` argument, indicate the main compute/storage system with compute_kind (like `Python`), add asset owners' emails with `owners`, or do custom and domain-specific categorization with the tags argument. You can add a description using the Python docsrting.

Here's an example:

```
from dagster import asset

@asset(
    group_name="finance",
    compute_kind="pandas",
    owners=["hello@dagsterlabs.com", "team:finance"]
)
def finance_data():
    \"""
    This asset contains financial data for the company.
    \"""
    pass
```
"""


class AddMaterializationResultSignature(dspy.Signature):
    """
    Surface relevant metadata using `MaterializeResult` instead of `AssetMaterialization`.

    Ensure that metadata fields are added using `MetadataValue`.
    """

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    input_dagster_code = dspy.InputField()
    dagster_code = dspy.OutputField(
        desc="Dagster code with `MaterializeResult` surfacing asset metadata where relevant"
    )


class AddMaterializationResultModule(dspy.Module):
    def __init__(self):
        self.retrieve = dspy.Retrieve(k=3)
        self.add_materialization = dspy.ChainOfThought(
            AddMaterializationResultSignature
        )

    def forward(self, input_dagster_code: str) -> dspy.Prediction:
        # context = self.retrieve(self.add_materialization.signature.__doc__).passages
        context = [relevant_documentation]
        pred = self.add_materialization(
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
