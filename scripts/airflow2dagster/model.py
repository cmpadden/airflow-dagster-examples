from functools import partial

import dspy
from airflow2dagster.add_asset_checks import AddAssetCheckModule
from airflow2dagster.add_config import AddConfigModule
from airflow2dagster.add_definitions import AddDefinitionsModule
from airflow2dagster.add_integration import AddDagsterIntegrationModule
from airflow2dagster.add_materialization_results import AddMaterializationResultModule
from airflow2dagster.add_retry_policy import AddRetryPolicyModule
from airflow2dagster.add_sensor import AddSensorModule
from airflow2dagster.add_schedule import AddScheduleModule
from airflow2dagster.translate_core_logic import TranslateCoreLogicModule
from airflow2dagster.utils import format_code
from dspy.primitives.assertions import backtrack_handler


class Model(dspy.Module):
    def __init__(self):
        self.translate_core_logic = TranslateCoreLogicModule()
        self.add_materialization_result = AddMaterializationResultModule()
        self.add_config = AddConfigModule()
        self.add_definitions = AddDefinitionsModule()
        self.add_schedule = AddScheduleModule()
        self.add_retry_policy = AddRetryPolicyModule()
        self.add_sensor = AddSensorModule()
        self.add_asset_check = AddAssetCheckModule()

    def forward(self, airflow_code: str) -> dspy.Prediction:
        pred = self.translate_core_logic(airflow_code)
        pred = self.add_materialization_result(pred.dagster_code)

        # NOTE: From experiments, configs should be generated *after* materialization results.
        # Otherwise, the `AddMaterializationModule`'s output
        # fails to retain the correct, original Config code and removes it
        pred = self.add_config(pred.dagster_code)

        pred = self.add_definitions(pred.dagster_code)
        pred = self.add_schedule(airflow_code, pred.dagster_code)
        pred = self.add_sensor(airflow_code, pred.dagster_code)
        if "retries" in airflow_code.lower():
            pred = self.add_retry_policy(airflow_code, pred.dagster_code)
        # if "check" in airflow_code.lower():
        #     pred = self.add_asset_check(airflow_code, pred.dagster_code)

        return dspy.Prediction(dagster_code=format_code(pred.dagster_code))


def get_translator(retries: int | None) -> Model:
    model = Model()
    if retries:
        for name, submodule in model.named_sub_modules():
            # Only wrap direct submodules of the `Model` with assertions
            # There'll be a deadlock if (sub)modules of different levels are wrapped
            # NOTE: Submodules are named as `self.<submodule_name>.<subsubmodule_name>`
            if len(name.split(".")) != 2:
                continue
            submodule.activate_assertions(
                partial(backtrack_handler, max_backtracks=retries)
            )
    return model
