from datetime import datetime
from typing import Optional
from core.utils.schema_utils import PDBaseModel

class Filter(PDBaseModel):
    id:                 int
    name:               Optional[str]       = None
    type:               Optional[str]       = None
    active_flag:        Optional[bool]      = False
    temporary_flag:     Optional[bool]      = None
    user_id:            Optional[int]       = None
    visible_to:         Optional[int]       = None
    custom_view_id:     Optional[int]       = None
    add_time:           Optional[datetime]  = None
    update_time:        Optional[datetime]  = None

    model_config = {"extra": "allow"}