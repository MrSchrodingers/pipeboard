from datetime import datetime, date
from typing import Any, Dict, List, Optional

from pydantic import Field, field_validator
from core.schemas.shared_schema import Address, ContactDetail
from core.utils.schema_utils import PDBaseModel


class Person(PDBaseModel):
    id: int

    custom_fields: Optional[Dict[str, Any]] = Field(default_factory=dict)
    
    name:        Optional[str] = None
    first_name:  Optional[str] = None
    last_name:   Optional[str] = None

    owner_id:    Optional[int] = None
    org_id:      Optional[int] = None

    add_time:    Optional[datetime] = None
    update_time: Optional[datetime] = None

    emails:      Optional[List[ContactDetail]] = None
    phones:      Optional[List[ContactDetail]] = None
    im:          Optional[List[ContactDetail]] = None

    is_deleted:  Optional[bool] = False
    visible_to:  Optional[int]  = None
    label_ids:   Optional[List[int]] = None
    picture_id:  Optional[int] = None

    notes:          Optional[str] = None
    birthday:       Optional[date] = None
    job_title:      Optional[str] = None
    postal_address: Optional[Address] = None
    
    @field_validator("org_id", "picture_id", mode="before")
    @classmethod
    def nan_to_none_days(cls, v):
        import numpy as np
        if v is not None and isinstance(v, float) and np.isnan(v):
            return None
        return v