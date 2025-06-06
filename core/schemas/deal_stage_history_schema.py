from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import Field

from core.utils.schema_utils import PDBaseModel

class DealStageHistory(PDBaseModel):
    deal_id: int = Field(alias="deal_id")
    stage_id: Optional[int] = Field(default=None, alias="stage_id")
    change_time: Optional[datetime] = Field(default=None, alias="timestamp")
    user_id: Optional[int] = Field(default=None, alias="user_id")

    model_config = {"extra": "allow"}