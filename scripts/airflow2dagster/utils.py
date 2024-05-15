import os

from chromadb.utils.embedding_functions import OpenAIEmbeddingFunction

import dspy
from dspy.retrieve.chromadb_rm import ChromadbRM


def configure_dspy() -> None:
    turbo = dspy.OpenAI(model="gpt-4o", max_tokens=2048)

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
