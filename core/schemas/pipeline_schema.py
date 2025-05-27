from datetime import datetime
from typing import Optional
from core.utils.schema_utils import PDBaseModel

class Pipeline(PDBaseModel):
    id:                                 int
    order_nr:                           Optional[int]       = None
    name:                               Optional[str]       = None
    is_selected:                        Optional[bool]      = False
    is_deleted:                         Optional[bool]      = False
    is_deal_probability_enabled:        Optional[bool]      = False
    add_time:                           Optional[datetime]  = None
    update_time:                        Optional[datetime]  = None

    model_config = {"extra": "allow"}
