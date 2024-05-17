import dspy

from airflow2dagster.utils import extract_code_block_from_markdown
from metrics.run_validity import is_runnable


class AddConfigSignature(dspy.Signature):
    """
    Identify constants and parameters of the following Dagster assets, and parameterize them using `dagster.Config`.

    Configs must inherit from `dagster.Config` instead of Pydantic's `BaseModel`.
    You can use `pydantic.Field` for additional configuration of the config fields.

    Requirements:
    - The field types must be a primitive type (int, float, bool, str) or a list of primitive types
    - Do not use `config_schema`
    - Do not use the legacy `dagster.Field`. Use `pydantic.Field` instead
    - Do not add an empty config to an asset if it doesn't require one
    - Do not use required_resource_keys to add a Config to the asset

    Example of adding a config to an asset:

        from dagster import asset, Config

        class MyAssetConfig(Config):
            my_str: str = "my_default_string"
            my_int_list: List[int]
            my_bool_with_metadata: bool = Field(default=False, description="A bool field")

        @asset
        def asset_with_config(config: MyAssetConfig):
            assert config.my_str == "my_default_string"
            assert config.my_int_list == [1, 2, 3]
            assert config.my_bool_with_metadata == False
    """

    input_code = dspy.InputField(desc="Input Dagster code")
    dagster_code = dspy.OutputField(
        desc="Dagster code where assets are parameterized using `dagster.Config`s."
    )


class AddConfigModule(dspy.Module):
    def __init__(self):
        self.add_config = dspy.ChainOfThought(AddConfigSignature)

    def forward(self, input_code: str) -> dspy.Prediction:
        pred = self.add_config(input_code=input_code)
        pred.dagster_code = extract_code_block_from_markdown(pred.dagster_code)

        dspy.Assert(
            "@asset" in pred.dagster_code,
            "Do not forget to include the input Dagster code in the final code.",
        )
        dspy.Assert(
            "(BaseModel)" not in pred.dagster_code,
            "Config must inherit from `dagster.Config` instead of `pydantic.BaseModel`.",
        )
        dspy.Assert(
            "config_schema" not in pred.dagster_code,
            "Do not use `config_schema`. Add the config directly as a type-hinted input to the asset.`",
        )
        dspy.Assert(
            "required_resource_keys" not in pred.dagster_code,
            "Do not add a config using `required_resource_keys`. Add the config directly as a type-hinted input to the asset.",
        )
        dspy.Suggest(
            *is_runnable(pred.dagster_code, verbose=True)
        )  # Some code cannot be run without external dependencies

        return dspy.Prediction(dagster_code=pred.dagster_code)
