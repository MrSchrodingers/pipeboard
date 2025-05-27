from typing import Optional
from core.utils.schema_utils import PDBaseModel

class Currency(PDBaseModel):
    id:             int
    code:           Optional[str]   = None
    name:           Optional[str]   = None
    decimal_points: Optional[int]   = None
    symbol:         Optional[str]   = None
    active_flag:    Optional[bool]  = False
    is_custom_flag: Optional[bool]  = False

    model_config = {"extra": "allow"}
