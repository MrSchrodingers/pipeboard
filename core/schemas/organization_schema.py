from datetime import datetime
from typing import Any, Dict, List, Optional
from pydantic import model_validator
from core.schemas.shared_schema import Address
from core.utils.schema_utils import PDBaseModel

class Organization(PDBaseModel):
    id:             int
    name:           Optional[str]       = None
    owner_id:       Optional[int]       = None
    org_id:         Optional[int]       = None
    add_time:       Optional[datetime]  = None
    update_time:    Optional[datetime]  = None
    address:        Optional[Address]   = None
    is_deleted:     Optional[bool]      = False
    visible_to:     Optional[int]       = None
    label_ids:      Optional[List[int]] = None
    custom_fields:  Dict[str, Any]      = None

    model_config = {"extra": "allow"}

    @model_validator(mode="before")
    def _normalize_fks(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        for fk in ("owner_id", "org_id"):
            v = values.get(fk)
            if isinstance(v, dict):
                values[fk] = v.get("value") or v.get("id")
        return values
