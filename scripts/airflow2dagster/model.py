from functools import partial

import dspy
from airflow2dagster.add_asset_checks import AddAssetCheckModule
from airflow2dagster.add_definitions import AddDefinitionsModule
from airflow2dagster.add_integration import AddDagsterIntegrationModule
from airflow2dagster.add_materialization_results import AddMaterializationResultModule
from airflow2dagster.add_retry_policy import AddRetryPolicyModule
from airflow2dagster.add_schedule import AddScheduleModule
from airflow2dagster.add_python_best_practices import AddPythonBestPracticesModule
from airflow2dagster.translate_core_logic import TranslateCoreLogicModule
from dspy.primitives.assertions import backtrack_handler


class Model(dspy.Module):
    def __init__(self):
        self.translate_core_logic = TranslateCoreLogicModule()
        self.add_materialization_result = AddMaterializationResultModule()
        self.add_definitions = AddDefinitionsModule()
        self.add_schedule = AddScheduleModule()
        self.add_retry_policy = AddRetryPolicyModule()
        self.add_best_practices = AddPythonBestPracticesModule()
        self.add_asset_check = AddAssetCheckModule()

    def forward(self, airflow_code: str) -> dspy.Prediction:
        pred = self.translate_core_logic(airflow_code)

        pred = self.add_materialization_result(pred.dagster_code)
        pred = self.add_definitions(pred.dagster_code)
        pred = self.add_schedule(airflow_code, pred.dagster_code)
        pred = self.add_best_practices(pred.dagster_code)
        if "retries" in airflow_code.lower():
            pred = self.add_retry_policy(airflow_code, pred.dagster_code)
        # if "check" in airflow_code.lower():
        #     pred = self.add_asset_check(airflow_code, pred.dagster_code)

        return dspy.Prediction(dagster_code=pred.dagster_code)


def get_translator(retries: int | None) -> Model:
    model = Model()
    if retries:
        for name, submodule in model.named_sub_modules():
            # Do not wrap main module with assertions, as it'll encounter a deadlock
            if name == "self":
                continue
            submodule.activate_assertions(
                partial(backtrack_handler, max_backtracks=retries)
            )
    return model
