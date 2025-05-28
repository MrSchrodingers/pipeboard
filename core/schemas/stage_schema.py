from datetime import datetime
from typing import Optional

from pydantic import field_validator
from core.utils.schema_utils import PDBaseModel

class Stage(PDBaseModel):
    id:                     int
    order_nr:               Optional[int]       = None
    name:                   Optional[str]       = None
    is_deleted:             Optional[bool]      = False
    deal_probability:       Optional[int]       = None
    pipeline_id:            Optional[int]       = None
    is_deal_rot_enabled:    Optional[bool]      = False
    days_to_rotten:         Optional[int]       = None
    add_time:               Optional[datetime]  = None
    update_time:            Optional[datetime]  = None

    model_config = {"extra": "allow"}

    @field_validator("days_to_rotten", mode="before")
    @classmethod
    def nan_to_none_days(cls, v):
        import numpy as np
        if v is not None and isinstance(v, float) and np.isnan(v):
            return None
        return v