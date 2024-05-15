from pathlib import Path


import dotenv
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnableSerializable
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from pydantic import BaseModel

dotenv.load_dotenv()


class FunctionalRequirementCheck(BaseModel):
    requirement: str
    result: bool


def create_chain() -> RunnableSerializable[dict, str]:
    template = """
    Based on the following code, on whether it meets the functional requirements.
    Answer with “True”/”False” only for each requirement.

    Ensure that there are as many outputs as there are inputs.

    ```py
    {code}
    ```

    Functional requirements:
    {functional_requirements}
    """

    prompt = PromptTemplate.from_template(template=template)
    model = ChatOpenAI(model="gpt-4o", temperature=0)
    chain = prompt | model | StrOutputParser()
    return chain


def get_functional_requirements(path: Path) -> list[str]:
    return [line.removeprefix("- ") for line in path.read_text().strip().split("\n")]


def check_functional_requirements(
    chain: RunnableSerializable[dict, str], code: str, reqs: list[str]
) -> list[FunctionalRequirementCheck]:
    def format_requirements(reqs: list[str]) -> str:
        return "- " + "\n- ".join(f"- {req}" for req in reqs)

    llm_output = chain.invoke(
        {"functional_requirements": format_requirements(reqs), "code": code}
    )
    results = [ans == "True" for ans in llm_output.split("\n")]

    assert len(reqs) == len(results)

    ret_vals = [
        FunctionalRequirementCheck(requirement=req, result=response)
        for req, response in zip(reqs, results)
    ]
    return ret_vals
