from datetime import datetime
from typing import Any, Optional, List
from core.utils.schema_utils import PDBaseModel

class PersonFieldOption(PDBaseModel):
    id:    Optional[int]    = None
    label: Optional[str]    = None
    color: Optional[str]    = None

    model_config = {"extra": "allow"}

class PersonField(PDBaseModel):
    # campos “core”
    id:                       Optional[int]    = None
    key:                      Optional[str]    = None
    name:                     Optional[str]    = None
    group_id:                 Optional[int]    = None
    order_nr:                 Optional[int]    = None
    field_type:               Optional[str]    = None
    json_column_flag:         Optional[bool]   = False
    add_time:                 Optional[datetime]= None
    update_time:              Optional[datetime]= None
    last_updated_by_user_id:  Optional[int]    = None
    created_by_user_id:       Optional[int]    = None

    # flags
    active_flag:              Optional[bool]   = False
    edit_flag:                Optional[bool]   = False
    details_visible_flag:     Optional[bool]   = False
    add_visible_flag:         Optional[bool]   = False
    important_flag:           Optional[bool]   = False
    bulk_edit_allowed:        Optional[bool]   = False
    filtering_allowed:        Optional[bool]   = False
    sortable_flag:            Optional[bool]   = False
    mandatory_flag:           Optional[Any]    = None
    searchable_flag:          Optional[bool]   = False
    index_visible_flag:       Optional[bool]   = False

    # descrição / links / autocomplete
    description:              Optional[str]    = None
    use_field:                Optional[str]    = None
    link:                     Optional[str]    = None
    autocomplete:             Optional[str]    = None
    display_field:            Optional[str]    = None

    # opções de enum
    options:                  Optional[List[PersonFieldOption]] = None

    # sub-fields (ex.: address_lat, monetary_currency…)
    is_subfield:              Optional[bool]   = False
    parent_id:                Optional[int]    = None
    id_suffix:                Optional[str]    = None

    model_config = {"extra": "allow"}
