# Adopted from CodeXGLUE

# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

# -*- coding:utf-8 -*-
import warnings
from enum import Enum
from pathlib import Path

import typer
from pydantic import BaseModel

from CodeBLEU import bleu, dataflow_match, syntax_match, weighted_ngram_match

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

app = typer.Typer()


class SupportedLanguage(Enum):
    java = "java"
    js = "js"
    c_sharp = "c_sharp"
    php = "php"
    go = "go"
    python = "python"
    ruby = "ruby"


class CodeBLEU(BaseModel):
    score: float
    ngram_match_score: float
    weighted_ngram_match_score: float
    syntax_match_score: float
    dataflow_match_score: float
    alpha: float
    beta: float
    gamma: float
    theta: float


@app.command()
def code_bleu(
    refs: list[Path],
    hyp: Path,
    lang: SupportedLanguage = SupportedLanguage.python,
    params: tuple[float, float, float, float] = (0.25, 0.25, 0.25, 0.25),
) -> CodeBLEU:
    lang = lang.value
    alpha, beta, gamma, theta = params

    # preprocess inputs
    pre_references = [
        [x.strip() for x in open(file, encoding="utf-8").readlines()] for file in refs
    ]
    hypothesis = [x.strip() for x in open(hyp, encoding="utf-8").readlines()]

    for i in range(len(pre_references)):
        assert len(hypothesis) == len(pre_references[i])

    references = []
    for i in range(len(hypothesis)):
        ref_for_instance = []
        for j in range(len(pre_references)):
            ref_for_instance.append(pre_references[j][i])
        references.append(ref_for_instance)
    assert len(references) == len(pre_references) * len(hypothesis)

    # calculate ngram match (BLEU)
    tokenized_hyps = [x.split() for x in hypothesis]
    tokenized_refs = [[x.split() for x in reference] for reference in references]

    ngram_match_score = bleu.corpus_bleu(tokenized_refs, tokenized_hyps)

    # calculate weighted ngram match
    keywords = [
        x.strip()
        for x in open(
            "CodeBLEU/keywords/" + lang + ".txt", encoding="utf-8"
        ).readlines()
    ]

    def make_weights(reference_tokens, key_word_list):
        return {
            token: 1 if token in key_word_list else 0.2 for token in reference_tokens
        }

    tokenized_refs_with_weights = [
        [
            [reference_tokens, make_weights(reference_tokens, keywords)]
            for reference_tokens in reference
        ]
        for reference in tokenized_refs
    ]

    weighted_ngram_match_score = weighted_ngram_match.corpus_bleu(
        tokenized_refs_with_weights, tokenized_hyps
    )

    # calculate syntax match
    syntax_match_score = syntax_match.corpus_syntax_match(references, hypothesis, lang)

    # calculate dataflow match
    dataflow_match_score = dataflow_match.corpus_dataflow_match(
        references, hypothesis, lang
    )

    code_bleu_score = (
        alpha * ngram_match_score
        + beta * weighted_ngram_match_score
        + gamma * syntax_match_score
        + theta * dataflow_match_score
    )

    result = CodeBLEU(
        score=code_bleu_score,
        ngram_match_score=ngram_match_score,
        weighted_ngram_match_score=weighted_ngram_match_score,
        syntax_match_score=syntax_match_score,
        dataflow_match_score=dataflow_match_score,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        theta=theta,
    )
    return result


if __name__ == "__main__":
    app()
