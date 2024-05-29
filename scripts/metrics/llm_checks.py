import os

from openai import OpenAI

from metrics.utils import persistent_cache


@persistent_cache
def check_assets_only(dagster_code: str) -> bool:
    base_model = "gpt-4o-2024-05-13"

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    prompt = f"""

    Based on the following code, on whether it meets the functional requirements.
    Answer with “True”/”False” only for each requirement.

    ```py
    {dagster_code}
    ```

    Functional requirements:

    ###
    This code use Dagster assets.
    ###

    """

    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": "You are an experienced data engineer, well-versed in both Dagster and Airflow. Your job is to review Dagster codebase.",
            },
            {"role": "user", "content": prompt},
        ],
        model=base_model,
    )
    script = chat_completion.choices[0].message.content
    return "True" in script


@persistent_cache
def check_scheduling_definition(dagster_code: str) -> bool:
    base_model = "gpt-4o-2024-05-13"

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    prompt = f"""

    Based on the following code, on whether it meets the functional requirements.
    Answer with “True”/”False” only for each requirement.

    ```py
    {dagster_code}
    ```

    Functional requirements:

    ###
    This code have Dagster scheduling definition.
    ###

    """

    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": "You are an experienced data engineer, well-versed in both Dagster and Airflow. Your job is to review Dagster codebase.",
            },
            {"role": "user", "content": prompt},
        ],
        model=base_model,
    )
    script = chat_completion.choices[0].message.content
    return "True" in script
