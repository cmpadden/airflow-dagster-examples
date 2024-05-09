"""
Quick script to create a vectorstore from the Dagster (1.7.2) documentation.

This aspect of RAG is unoptimized. Better splitting heuristics can be used.
"""

from langchain_chroma import Chroma
from langchain_community.document_loaders import DirectoryLoader, TextLoader
from langchain_openai import OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter

rst_loader = DirectoryLoader(
    "dagster/docs/content", glob="**/*.rst", loader_cls=TextLoader
)
mdx_loader = DirectoryLoader(
    "dagster/docs/content", glob="**/*.mdx", loader_cls=TextLoader
)
docs = rst_loader.load() + mdx_loader.load()

text_splitter = RecursiveCharacterTextSplitter()
splits = text_splitter.split_documents(docs)

PERSIST_DIR = "chroma.db"
vectorstore = Chroma(
    persist_directory=PERSIST_DIR, embedding_function=OpenAIEmbeddings()
)
