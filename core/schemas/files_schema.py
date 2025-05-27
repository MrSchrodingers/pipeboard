from typing import Optional
from datetime import datetime
from core.utils.schema_utils import PDBaseModel

class File(PDBaseModel):
    id:                 int
    user_id:            Optional[int]       = None
    deal_id:            Optional[int]       = None
    person_id:          Optional[int]       = None
    org_id:             Optional[int]       = None
    product_id:         Optional[int]       = None
    activity_id:        Optional[int]       = None
    lead_id:            Optional[str]       = None
    log_id:             Optional[int]       = None
    add_time:           Optional[datetime]  = None
    update_time:        Optional[datetime]  = None
    file_name:          Optional[str]       = None
    file_type:          Optional[str]       = None
    file_size:          Optional[int]       = None
    active_flag:        Optional[bool]      = False
    inline_flag:        Optional[bool]      = False
    remote_location:    Optional[str]       = None
    remote_id:          Optional[int]       = None
    cid:                Optional[str]       = None
    s3_bucket:          Optional[str]       = None
    mail_message_id:    Optional[int]       = None
    mail_template_id:   Optional[int]       = None
    deal_name:          Optional[str]       = None
    person_name:        Optional[str]       = None
    lead_name:          Optional[str]       = None
    org_name:           Optional[str]       = None
    product_name:       Optional[str]       = None
    url:                Optional[str]       = None
    name:               Optional[str]       = None
    description:        Optional[str]       = None

    model_config = {"extra": "allow"}