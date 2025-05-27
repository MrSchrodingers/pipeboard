from typing import Any, Dict, Optional
from pydantic import model_validator
from core.utils.schema_utils import PDBaseModel

class ContactDetail(PDBaseModel):
    value:   Optional[str]  = None
    primary: Optional[bool] = False
    label:   Optional[str]  = None

    # herdamos alias_generator e extra=allow de PDBaseModel

class Address(PDBaseModel):
    value:              Optional[str] = None
    country:            Optional[str] = None
    admin_area_level_1: Optional[str] = None
    admin_area_level_2: Optional[str] = None
    locality:           Optional[str] = None
    sublocality:        Optional[str] = None
    route:              Optional[str] = None
    street_number:      Optional[str] = None
    postal_code:        Optional[str] = None

    @model_validator(mode="before")
    def _wrap_str(cls, v: Any) -> Any:
        """
        Some address fields may arrive as plain strings.
        If so, wrap into a dict so that PDBaseModel can map it.
        """
        if isinstance(v, str):
            return {"value": v}
        return v

class Money(PDBaseModel):
    amount:   Optional[float] = None
    currency: Optional[str]   = None

    @model_validator(mode="before")
    def _normalize(cls, v: Any) -> Any:
        """
        Pipedrive returns monetary fields as {value: float, currency: str}.
        Normalize keys to match our model.
        """
        if isinstance(v, dict):
            return {"amount": v.get("value"), "currency": v.get("currency")}
        return v
