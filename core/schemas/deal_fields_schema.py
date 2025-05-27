from datetime import datetime
from typing import Any, List, Optional

from core.utils.schema_utils import PDBaseModel

class DealFieldOption(PDBaseModel):
    id:     Optional[Any]   = None
    label:  Optional[str]   = None
    color:  Optional[str]   = None
    alt_id: Optional[str]   = None

    model_config = {"extra": "allow"}

class DealField(PDBaseModel):
    id:                         Optional[int]       = None
    key:                        Optional[str]       = None
    name:                       Optional[str]       = None
    order_nr:                   Optional[int]       = None
    field_type:                 Optional[str]       = None
    add_time:                   Optional[datetime]  = None
    update_time:                Optional[datetime]  = None
    last_updated_by_user_id:    Optional[int]       = None
    created_by_user_id:         Optional[int]       = None
    active_flag:                Optional[bool]      = False
    edit_flag:                  Optional[bool]      = False
    index_visible_flag:         Optional[bool]      = False
    details_visible_flag:       Optional[bool]      = False
    add_visible_flag:           Optional[bool]      = False
    important_flag:             Optional[bool]      = False
    bulk_edit_allowed:          Optional[bool]      = False
    searchable_flag:            Optional[bool]      = False
    filtering_allowed:          Optional[bool]      = False
    sortable_flag:              Optional[bool]      = False
    options:                    Optional[List[DealFieldOption]] = None
    mandatory_flag:             Optional[Any]       = None
    is_subfield:                Optional[bool]      = False
    related_object_type:        Optional[str]       = None 
    related_object_label:       Optional[str]       = None

    model_config = {"extra": "allow"}


