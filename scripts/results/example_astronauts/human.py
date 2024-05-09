from dagster import asset, Config, AutoMaterializePolicy, AutoMaterializeRule, MaterializeResult, AssetExecutionContext
import requests
import os

LIST_OF_PEOPLE_IN_SPACE_FILE = "../data/current_astronauts.txt"

class GreetingConfig(Config):
    greeting: str = "Hello! :)"

@asset(
    auto_materialize_policy=AutoMaterializePolicy.eager().with_rules(
        # cron for daily materialization
        AutoMaterializeRule.materialize_on_cron("0 0 * * *")
    )
)
def current_astronauts(context: AssetExecutionContext, config: GreetingConfig) -> MaterializeResult:
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

    for astronaut in list_of_people_in_space:
        context.log.info(f"{astronaut["name"]} is currently in space flying on the {astronaut["craft"]}! {config.greeting}")

    return MaterializeResult(
        metadata={
            "number_of_people_in_space": number_of_people_in_space,
            "list_of_people_in_space": list_of_people_in_space,
            "list_file_path": LIST_OF_PEOPLE_IN_SPACE_FILE,
        }
    )