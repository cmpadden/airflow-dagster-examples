from functools import partial
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

import dspy
from airflow2dagster.model import Model
from airflow2dagster.utils import configure_dspy
from dspy.primitives.assertions import backtrack_handler

load_dotenv()
configure_dspy()

app = typer.Typer()


def _get_translator(retries: int | None) -> Model:
    model = Model()
    if retries:
        model = model.activate_assertions(
            partial(backtrack_handler, max_backtracks=retries)
        )
    return model


def translate_airflow_code(airflow_code: str, retries: int | None) -> str:
    translator = _get_translator(retries=retries)
    return translator(airflow_code).dagster_code


@app.command()
def translate(
    airflow_filepath: Path = typer.Option(
        ..., help="Path to airflow code (ASSUMPTION: single file)"
    ),
    dst: Optional[Path] = typer.Option(
        None, help="If provided, write to file. Otherwise, print to stdout."
    ),
    retries: Optional[int] = typer.Option(
        4, help="Number of retries for DSPy assertions"
    ),
) -> None:
    airflow_code = airflow_filepath.read_text()

    try:
        translated_dagster_code = translate_airflow_code(airflow_code, retries=retries)
    except dspy.DSPyAssertionError as e:
        raise typer.Exit(f"Error: {e}")

    # Print to stdout, if `dst` is not specified
    if dst:
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(translated_dagster_code)
    else:
        typer.echo(translated_dagster_code)


if __name__ == "__main__":
    app()


# TODO: Handle resources and IO managers
# TODO: Handle inputs using Configs
