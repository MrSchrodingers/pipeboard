from __future__ import annotations

from typing import Any, List, Optional
from datetime import datetime

from pydantic import BaseModel
from core.utils.schema_utils import PDBaseModel 


class ActivityFieldOptions(BaseModel):
    id:         Optional[Any]   = None 
    label:      Optional[str]   = None
    
    model_config = {"extra": "allow"}

class ActivityField(PDBaseModel):
    id: Optional[int]   = None 
    key: Optional[str]  = None
    name: Optional[str] = None
    field_type: Optional[str] = None
    order_nr: Optional[int] = None
    add_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    active_flag: Optional[bool] = True
    edit_flag: Optional[bool] = True
    mandatory_flag: Optional[Any] = None
    is_subfield: Optional[bool] = False
    options: Optional[List[ActivityFieldOptions]] = None
    bulk_edit_allowed: Optional[bool] = True
    filtering_allowed: Optional[bool] = True
    sortable_flag: Optional[bool] = True
    searchable_flag: Optional[bool] = False
    important_flag: Optional[bool] = False 
