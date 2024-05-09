from pydantic import BaseModel, Field, field_validator
import logging

logger = logging.getLogger(__name__)


class MarkdownWithCodeOutput(BaseModel):
    """Assumes that there is a single Markdown code block."""

    code: str

    @field_validator("code")
    def validate_code(cls, value: str):
        logger.warning(value)
        if "```" not in value:
            logger.warning("Expecting code in Markdown")
            return value
        return value.split("```")[1].removeprefix("python")
