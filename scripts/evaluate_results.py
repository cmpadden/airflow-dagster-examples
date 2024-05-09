from pathlib import Path
from typing import Any, List

import evaluate
import pandas as pd
import typer

from metrics.functional_requirements import (
    check_functional_requirements,
    create_chain,
    get_functional_requirements,
)
from metrics.llm_checks import check_assets_only, check_scheduling_definition
from metrics.run_validity import is_runnable

app = typer.Typer()


def _functional_requirements(
    candidate_code: str, functional_requirements: Path
) -> List[bool]:
    functional_requirements_result = check_functional_requirements(
        create_chain(),
        code=candidate_code,
        reqs=get_functional_requirements(functional_requirements),
    )
    functional_requirements_met = sum(
        [x.result for x in functional_requirements_result]
    ) / len(functional_requirements_result)
    return functional_requirements_met


def _evaluate(references: List, candidates: List) -> pd.DataFrame:
    rouge = evaluate.load("rouge")
    bleu = evaluate.load("bleu")

    assert len(references) == len(candidates)
    assert all([r["name"] == c["name"] for (r, c) in zip(references, candidates)])

    references_code = [x["filepath"].read_text() for x in references]
    candidates_code = [x["filepath"].read_text() for x in candidates]

    is_runnable_result = [is_runnable(x) for x in candidates_code]

    results_bleu = [
        bleu.compute(predictions=[candidates_code[i]], references=[references_code[i]])[
            "bleu"
        ]
        for i in range(len(references_code))
    ]
    results_rouge = [
        rouge.compute(
            predictions=[candidates_code[i]], references=[references_code[i]]
        )["rouge1"]
        for i in range(len(references_code))
    ]

    functional_requirements = [
        _functional_requirements(
            candidate_code=candidates_code[i],
            functional_requirements=references[i]["functional_requirements"],
        )
        for i in range(len(references))
    ]

    assets_only_ = [check_assets_only(dagster_code=x) for x in candidates_code]
    scheduling_ = [check_scheduling_definition(dagster_code=x) for x in candidates_code]
    metrics_result = pd.DataFrame(
        {
            "name": [x["name"] for x in candidates],
            "code_is_runnable": is_runnable_result,
            "bleu": results_bleu,
            "rouge": results_rouge,
            "functional_requirements": functional_requirements,
            "assets_only": assets_only_,
            "scheduling": scheduling_,
        }
    )

    metrics_result["code_is_runnable"] = metrics_result["code_is_runnable"].astype(int)
    metrics_result["assets_only"] = metrics_result["assets_only"].astype(int)
    metrics_result["scheduling"] = metrics_result["scheduling"].astype(int)

    mean_values = {
        "name": "AVG",
        "code_is_runnable": metrics_result["code_is_runnable"].mean(),
        "bleu": metrics_result["bleu"].mean(),
        "rouge": metrics_result["rouge"].mean(),
        "functional_requirements": metrics_result["functional_requirements"].mean(),
        "assets_only": metrics_result["assets_only"].mean(),
        "scheduling": metrics_result["scheduling"].mean(),
    }

    metrics_result = pd.concat(
        [metrics_result, pd.DataFrame([mean_values])], ignore_index=True
    )
    return metrics_result


def read_files(result_folder: Path) -> list[dict[str, Any]]:
    subfolders = [
        folder
        for folder in result_folder.iterdir()
        if folder.is_dir() and folder.name not in ("data", ".git")
    ]

    samples = []
    for folder in subfolders:
        reference = folder / "dagster_version.py"

        samples.append(
            {
                "name": folder.name,
                "filepath": reference,
            }
        )

        if (folder / "functional_requirements.txt").exists():
            samples[-1]["functional_requirements"] = (
                folder / "functional_requirements.txt"
            )
    return samples


@app.command()
def build_supervised_leaderboard():
    references = read_files(result_folder=Path('.'))
    just_copy_airflow = read_files(result_folder=Path("results/just-copy-airflow/"))
    human = read_files(result_folder=Path("results/human/"))
    gpt_naive = read_files(result_folder=Path("results/gpt_naive/"))
    gpt_dspy = read_files(result_folder=Path("results/gpt_dspy/"))

    just_copy_airflow_metrics = _evaluate(
        references=references, candidates=just_copy_airflow
    )
    just_copy_airflow_metrics["source"] = "just_copy_airflow"

    human_metrics = _evaluate(references=references, candidates=human)
    human_metrics["source"] = "human"

    gpt_naive_metrics = _evaluate(references=references, candidates=gpt_naive)
    gpt_naive_metrics["source"] = "gpt_naive"

    gpt_dspy_metrics = _evaluate(references=references, candidates=gpt_dspy)
    gpt_dspy_metrics["source"] = "gpt_dspy"

    supervised_leaderboard = pd.concat(
        [just_copy_airflow_metrics, human_metrics, gpt_naive_metrics, gpt_dspy_metrics],
        ignore_index=True,
    )
    supervised_leaderboard.to_csv("./results/supervised_leaderboard.csv", index=False)


@app.command()
def help():
    print("OK")


if __name__ == "__main__":
    app()
