from dagster import asset, AutoMaterializePolicy, AutoMaterializeRule, MaterializeResult, AssetExecutionContext, sensor, SensorResult, RunRequest, DynamicPartitionsDefinition, RunConfig, SensorEvaluationContext
from pydantic import BaseModel
import requests
import os
import json

LIST_OF_PEOPLE_IN_SPACE_FILE = "data/previous_astronauts.txt"

# used to make a partition per astronaut
astronauts_in_space_partition = DynamicPartitionsDefinition(
    name="astronauts_in_space_partition"
)

class AstronautConfig(BaseModel):
    craft: str
    name: str
    greeting: str = "Hello! :)"

@asset(
    auto_materialize_policy=AutoMaterializePolicy.eager().with_rules(
        # cron for daily materialization
        AutoMaterializeRule.materialize_on_cron("0 0 * * *")
    )
)
def previous_astronauts() -> MaterializeResult:
    """
    This asset is a list of Astronauts currently in space. The data is retrieved with the requests library and the list is saved to a file in local storage. The asset returns the number of people in space, the list of people in space, and the file path to the list.
    """
    r = requests.get("http://api.open-notify.org/astros.json")
    number_of_people_in_space = r.json()["number"]
    list_of_people_in_space = r.json()["people"]

    # if the directory does not exist, create it
    os.makedirs(os.path.dirname(LIST_OF_PEOPLE_IN_SPACE_FILE), exist_ok=True)

    # write the list of astronauts to a file
    with open(LIST_OF_PEOPLE_IN_SPACE_FILE, "w") as f:
        f.write(str(list_of_people_in_space))

    return MaterializeResult(
        metadata={
            "number_of_people_in_space": number_of_people_in_space,
            "list_of_people_in_space": list_of_people_in_space,
            "list_file_path": LIST_OF_PEOPLE_IN_SPACE_FILE,
        }
    )

def astronaut_log(context: AssetExecutionContext, astronaut_config: AstronautConfig) -> MaterializeResult:
    """
    This function logs the number of people in space and the list of people in space to the console.
    """
    context.log.info(f"{astronaut_config.name} is currently in space flying on the {astronaut_config.craft}! {astronaut_config.greeting}")



#TODO: Redo without dynamic partitions!!

@sensor(
    asset_selection=astronaut_log
)
def astronaut_sensor(context: SensorEvaluationContext) -> SensorResult:
    """
    This sensor logs the number of people in space and the list of people in space to the console.
    """
    previous_astronauts = set(context.cursor) if context.cursor else set()
    current_astronauts = set()

    with open(LIST_OF_PEOPLE_IN_SPACE_FILE, "r") as f:
        list_of_people_in_space = json.load(f)

        run_requests = []

        for astronaut in list_of_people_in_space:

            current_astronauts.add(astronaut["name"])

            # don't log an astronaut if we've already logged them
            if astronaut["name"] in previous_astronauts:
                continue

            astronaut_config = AstronautConfig(**astronaut)
            run_requests.append(RunRequest(
                run_key=f"{astronaut_config.name}_{astronaut_config.craft}",
                run_config=RunConfig(
                    ops={
                        "astronaut_log": astronaut_config
                    }),
            ))

    # if an astronaut is no longer in space, remove them from the partition
    delete_requests = []
    for astronaut in previous_astronauts - current_astronauts:
        delete_requests.append(astronauts_in_space_partition.build_delete_request(astronaut["name"]))

    # if a new astronaut is in space, add them to the partition
    add_requests = []
    for astronaut in current_astronauts:
        add_requests.append(astronauts_in_space_partition.build_add_request(astronaut))

    # update the previous_astronauts with the new astronauts plus the astronauts that are already in space
    
    return SensorResult(
        run_requests=run_requests,
        dynamic_partitions_requests=[*delete_requests, *add_requests],
        cursor=json.dumps(list(current_astronauts | previous_astronauts))
    )