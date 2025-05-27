from typing import List, Optional
from pydantic import Field
from datetime import datetime, date
from core.schemas.shared_schema import Money
from core.utils.schema_utils import PDBaseModel

class Lead(PDBaseModel):
    id:                  int
    title:               Optional[str]       = None
    owner_id:            Optional[int]       = None
    creator_id:          Optional[int]       = Field(default=None, alias='creator_id')
    label_ids:           Optional[List[int]] = None
    person_id:           Optional[int]       = None
    organization_id:     Optional[int]       = None
    source_name:         Optional[str]       = None
    origin:              Optional[str]       = None
    origin_id:           Optional[str]       = None
    channel:             Optional[int]       = None
    channel_id:          Optional[str]       = None
    is_archived:         Optional[bool]      = False
    was_seen:            Optional[bool]      = False
    value:               Money
    expected_close_date: Optional[date]      = None
    next_activity_id:    Optional[int]       = None
    add_time:            Optional[datetime]  = None
    update_time:         Optional[datetime]  = None
    visible_to:          Optional[int]       = None
    cc_email:            Optional[str]       = None

    model_config = {"extra": "allow"}
