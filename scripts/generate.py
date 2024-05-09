import os
from pathlib import Path

import typer
from openai import OpenAI
from tqdm import tqdm

import dspy
from airflow2dagster.__main__ import translate_airflow_code

from metrics.utils import persistent_cache

app = typer.Typer()


@persistent_cache
def generate_gpt_naive(airflow_code: str) -> str:
    # 4k tokens
    base_model = "gpt-4-turbo"

    # 32k tokens
    # base_model = "gpt-4-32k"

    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

    prompt = f"""
    Write a Dagster alternative to the following Airflow code:

    - Use only Dagster assets.
    - Avoid using outdated Dagster APIs such as solid, ops, etc.
    - Ensure that the Dagster code performs the same functions as the Airflow code.
    - Ensure that the Dagster code is executable.
    - Return only code which I can write in .py file right away.

    Airflow Code:
    ###
    {airflow_code}
    ###
    """

    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": "You are an experienced data engineer, well-versed in both Dagster and Airflow. Your job is to migrate the Airflow codebase to Dagster.",
            },
            {"role": "user", "content": prompt},
        ],
        model=base_model,
        # max_tokens=16000,
    )
    script = chat_completion.choices[0].message.content
    return script


@app.command()
def cli_generate():
    subfolders = [
        folder
        for folder in Path("..").iterdir()
        if folder.is_dir() and folder.name not in ("data", ".git", "scripts")
    ]

    result_folder_identical = Path("./results/just-copy-airflow")
    result_folder_identical.mkdir(exist_ok=True, parents=True)
    result_folder_human = Path("./results/human")
    result_folder_human.mkdir(exist_ok=True, parents=True)
    result_folder_gpt_naive = Path("./results/gpt_naive")
    result_folder_gpt_naive.mkdir(exist_ok=True, parents=True)
    result_folder_gpt_dspy = Path("./results/gpt_dspy")
    result_folder_gpt_dspy.mkdir(exist_ok=True, parents=True)

    for folder in tqdm(subfolders):
        airflow_filepath = folder / "airflow.py"
        airflow_code = airflow_filepath.read_text()

        print(f"--- Generating `{folder}` ---")

        print("--- Just copy airflow ---")
        # Just same airflow code
        (result_folder_identical / folder.name).mkdir(exist_ok=True)
        (result_folder_identical / folder.name / "dagster_version.py").write_text(
            airflow_code
        )

        print("--- Human ---")
        # Human crafter output
        (result_folder_human / folder.name).mkdir(exist_ok=True)
        (result_folder_human / folder.name / "dagster_version.py").write_text(
            (folder / "dagster_version.py").read_text()
        )

        print("--- GPT Naive ---")
        # Just ask GPT
        (result_folder_gpt_naive / folder.name).mkdir(exist_ok=True)
        (result_folder_gpt_naive / folder.name / "dagster_version.py").write_text(
            generate_gpt_naive(airflow_code=airflow_code)
        )

        print("--- GPT DSPy ---")
        # Using DSPy
        (result_folder_gpt_dspy / folder.name).mkdir(exist_ok=True)
        try:
            code = translate_airflow_code(airflow_code=airflow_code, retries=6)
        except dspy.DSPyAssertionError as e:
            code = f"Failed to generate Dagster code with error: {e}"

        (result_folder_gpt_dspy / folder.name / "dagster_version.py").write_text(code)


@app.command()
def help():
    print("NO!")


if __name__ == "__main__":
    app()
