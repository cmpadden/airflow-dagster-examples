import os
import subprocess
import tempfile
from pathlib import Path

import dspy
from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction
from dspy.retrieve.chromadb_rm import ChromadbRM


def configure_dspy() -> None:
    turbo = dspy.OpenAI(model="gpt-4-turbo-2024-04-09", max_tokens=2048)

    embedding_function = OpenAIEmbeddingFunction(api_key=os.getenv("OPENAI_API_KEY"))
    retriever_model = ChromadbRM(
        collection_name="langchain",
        persist_directory="chroma.db",
        embedding_function=embedding_function,
    )

    dspy.settings.configure(lm=turbo, rm=retriever_model)


def extract_code_block_from_markdown(markdown_string: str) -> str:
    """Assumes that there is only one Markdown codeblock."""

    if "```" not in markdown_string:
        return markdown_string
    return markdown_string.split("```")[1].removeprefix("python")


def format_code(code: str) -> str:
    """Formats the code using Ruff."""

    with tempfile.NamedTemporaryFile() as fp:
        Path(fp.name).write_text(code)

        # Run Ruff via the CLI
        subprocess.run(  # Remove unused imports
            ["ruff", "check", "--fix", fp.name], capture_output=True, text=True, check=True
        )
        subprocess.run(
            ["ruff", "format", fp.name], capture_output=True, text=True, check=True
        )

        formatted_code = Path(fp.name).read_text()

    return formatted_code
