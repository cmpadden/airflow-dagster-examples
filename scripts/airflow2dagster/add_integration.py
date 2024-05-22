import dspy
from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable

relevant_documentation = """
### Using Python SDKs over Airflow Operators

Airflow is known for its suite of operators to interact with various services, such as database like Snowflake or an API like Discord. However, when migrating to Dagster, it is recommended to use the Python SDKs provided by these services instead of a native Dagster integration.

## Using resources to manage connections and SDKs
In Dagster, resources are used to manage connections, create one instance of the Python SDK's client, and create a single source of truth for the connection. This is a best practice in Dagster.

Here's an example of how to use a resource to manage a connection to Snowflake:

```
from dagster import Definitions, asset, AssetExecutionContext, ResourceParam
from github import Github, Auth
import os

github_client = Github(auth=Auth(os.getenv("GITHUB_TOKEN")))

@asset
def user_github_repos(context: AssetExecutionContext, github: ResourceParam[GitHub]):
    repos = github.get_user().get_repos()
    with open("repos.txt", "w") as f:
        for repo in repos:
            f.write(repo.full_name + "\\n")

defs = Definitions(
    assets=[user_github_repos],
    resources={
        "github": github_client,
)

## Tips and Best Practices

Avoid Dagster resources that use the legacy resource API. You can tell it's the legacy API if the out-of-the box resource has an underscore in its name or has a `.configured` method. Instead, use the new resource API, which is more flexible and powerful. For example, in the `dagster-snowflake` library, don't use `snowflake_resource`, use `SnowflakeResource`.

Do not use Airflow-specific utilities like `ObjectStoragePath` in Dagster. Instead, understand the source of the data (such as either locally or in a cloud storage bucket like S3) and make a resource out of the appropriate Python SDK to interact with the data.
"""


class AddDagsterIntegrationSignature(dspy.Signature):
    """
    Surface relevant metadata using `MaterializeResult` instead of `AssetMaterialization`.

    Ensure that metadata fields are added using `MetadataValue`.
    """

    context = dspy.InputField(desc="Potentially relevant Dagster documentation")
    input_dagster_code = dspy.InputField()
    dagster_code = dspy.OutputField(
        desc="Dagster code with `MaterializeResult` surfacing asset metadata where relevant"
    )


class AddDagsterIntegrationModule(dspy.Module):
    def __init__(self):
        self.retrieve = dspy.Retrieve(k=3)
        self.add_materialization = dspy.ChainOfThought(
            AddDagsterIntegrationSignature
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
