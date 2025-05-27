from typing import List, Optional, Any
from datetime import datetime, date
from pydantic import Field, field_validator 
from core.utils.schema_utils import PDBaseModel

class Deal(PDBaseModel):
    id: int
    title: Optional[str] = None
    creator_user_id: Optional[int] = None
    user_id: Optional[int] = None
    owner_id: Optional[int] = None
    person_id: Optional[int] = None
    org_id: Optional[int] = None
    stage_id: Optional[int] = None
    pipeline_id: Optional[int] = None

    value: Optional[float] = None
    currency: Optional[str] = None

    add_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    stage_change_time: Optional[datetime] = None # Hora da última mudança de etapa
    close_time: Optional[datetime] = None # Hora que foi fechado (ganho ou perdido)
    won_time: Optional[datetime] = None
    lost_time: Optional[datetime] = None
    expected_close_date: Optional[date] = None

    status: Optional[str] = None # open, won, lost
    probability: Optional[float] = None # Probabilidade pode ser float
    lost_reason: Optional[str] = None
    visible_to: Optional[int] = None # Geralmente é uma string '1', '3', '5', '7'

    label_ids: Optional[List[int]] = Field(default_factory=list)

    @field_validator(
        "expected_close_date", 
        mode="before"
    )
    @classmethod
    def clean_invalid_date_formats(cls, v: Any) -> Optional[str]:
        if isinstance(v, str):
            stripped_v = v.strip()
            if not stripped_v or stripped_v == "0000-00-00" or stripped_v.startswith("-0001"):
                return None
        return v

    @field_validator(
        "add_time", "update_time", "stage_change_time", 
        "close_time", "won_time", "lost_time",
        mode="before"
    )
    @classmethod
    def clean_invalid_datetime_formats(cls, v: Any) -> Optional[str]:
        if isinstance(v, str):
            stripped_v = v.strip()
            if not stripped_v or \
               stripped_v.startswith("0000-00-00") or \
               stripped_v.startswith("-0001-11-30T00:00:00"):
                return None
        return v

    model_config = {"extra": "allow"}