from __future__ import annotations

from typing import Optional
from datetime import datetime
from core.utils.schema_utils import PDBaseModel 

class ActivityType(PDBaseModel):
    id: int
    name: Optional[str] = None
    key_string: Optional[str] = None
    icon_key: Optional[str] = None
    active_flag: Optional[bool] = True
    color: Optional[str] = None
    is_custom_flag: Optional[bool] = None
    order_nr: Optional[int] = None
    add_time: Optional[datetime] = None
    update_time: Optional[datetime] = None