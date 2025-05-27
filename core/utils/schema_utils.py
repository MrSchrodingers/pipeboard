from typing import Any, Dict
from pydantic import BaseModel, model_validator
import re

def to_snake(s: str) -> str:
    """Converte camelCase ou PascalCase para snake_case."""
    return re.sub(r'(?<!^)(?=[A-Z])', '_', s).lower()

class PDBaseModel(BaseModel):
    """
    BaseModel padrão para Pipedrive:
    - Converte aliases de camelCase para snake_case.
    - Permite campos extras, armazenando-os em `custom_fields`.
    """
    custom_fields: Dict[str, Any] = {}

    model_config = {
        "alias_generator": to_snake,
        "populate_by_name": True,
        "extra": "allow",
    }
