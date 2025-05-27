from datetime import datetime
from typing import Optional
from core.utils.schema_utils import PDBaseModel

class LeadLabel(PDBaseModel):
    id:             str
    name:           Optional[str]       = None
    color:          Optional[str]       = None
    add_time:       Optional[datetime]  = None
    update_time:    Optional[datetime]  = None

    model_config = {"extra": "allow"}