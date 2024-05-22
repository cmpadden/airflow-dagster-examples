import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable

relevant_documentation = """
### When to return data

In Dagster, returning data in an asset function uses a concept called an IO manager. An IO manager is a way to manage the input and output of data in a Dagster pipeline. It is used to handle the data that is passed between assets in a pipeline.

While there are valid use cases for IO managers, it is an advanced concept and we typically don't recommend leveraging them to early Dagster users. Instead, we recommend that users manage writing and reading data themselves in their assets directly.

You can tell an IO manager is being used in an asset if:
* the arguments of the asset function include the names of other assets, as that loads the data from the other asset using the IO manager
* the asset function returns an `Output` object or the data outright.
* if the `@asset` decorator uses the `ins` argument to specify the inputs to the asset using `AssetIn` or `outs` to specify the outputs using `AssetOut`.

Instead of doing the following in Dagster, assuming that `other_asset` is stored in the local filesystem:
```
from dagster import asset

@asset
def my_asset(other_asset):
    return other_asset + "!!!"
```

Do this:
```
from dagster import asset

@asset
def my_asset():
    with open("other_asset.txt", "r") as f:
        other_asset = f.read()

    with open("my_asset.txt", "w") as f:
        f.write(other_asset + "!!!")
```

Another example with DataFrames:

```
from dagster import asset
import pandas as pd

@asset
def my_asset(other_asset):
    return other_asset.assign(new_column=other_asset["old_column"] + 1)

```

Do this:

```
from dagster import asset
import pandas as pd

@asset
def my_asset():
    other_asset = pd.read_csv("other_asset.csv")
    other_asset["new_column"] = other_asset["old_column"] + 1
    other_asset.to_csv("my_asset.csv", index=False)
```

You can, and should, still return small volumes of metadata by returning MaterializeResult, such as:

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
"""


class AddDisincentivizeIOManagers(dspy.Signature):
    """
    Disincentivize the use of IO managers in Dagster assets.

    IO managers are a powerful feature in Dagster that allow for the management of data between assets in a pipeline.
    
    However, they are an advanced feature and are not recommended for early Dagster users. 
    
    Instead, we recommend that users manage reading and writing data themselves in their assets directly.
    """

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    input_dagster_code = dspy.InputField()
    dagster_code = dspy.OutputField(
        desc="Dagster code that avoids IO managers"
    )


class AddDisincentivizeIOManagersModule(dspy.Module):
    def __init__(self):
        self.retrieve = dspy.Retrieve(k=3)
        self.add_best_practices = dspy.ChainOfThought(
            AddDisincentivizeIOManagers
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
