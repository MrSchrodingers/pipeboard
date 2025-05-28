from __future__ import annotations

from typing import Any, List, Optional
from datetime import datetime, date
from pydantic import BaseModel, Field, field_validator
from core.utils.schema_utils import PDBaseModel

class ActivityParticipant(BaseModel):
    person_id: Optional[int] = None
    primary: Optional[bool] = False

class Activity(PDBaseModel):
    id: Optional[int] = None
    company_id: Optional[int] = None
    user_id: Optional[int] = None 
    done: Optional[bool] = False
    type: Optional[str] = None 
    subject: Optional[str] = None
    
    due_date: Optional[date] = None
    due_time: Optional[str] = None # HH:MM format
    duration: Optional[str] = None # HH:MM format
    
    add_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    marked_as_done_time: Optional[datetime] = None
    
    deal_id: Optional[int] = None
    person_id: Optional[int] = None
    org_id: Optional[int] = None
    
    project_id: Optional[int] = None
    
    note: Optional[str] = None 
    
    location: Optional[str] = None 
    location_subpremise: Optional[str] = None
    location_street_number: Optional[str] = None
    location_route: Optional[str] = None
    location_sublocality: Optional[str] = None
    location_locality: Optional[str] = None
    location_admin_area_level_1: Optional[str] = None
    location_admin_area_level_2: Optional[str] = None
    location_country: Optional[str] = None
    location_postal_code: Optional[str] = None
    location_formatted_address: Optional[str] = None
    
    public_description: Optional[str] = None 
    
    active_flag: Optional[bool] = True
    update_user_id: Optional[int] = None 
    
    gcal_event_id: Optional[str] = None
    google_calendar_id: Optional[str] = None
    google_calendar_etag: Optional[str] = None

    conference_meeting_client: Optional[str] = None
    conference_meeting_url: Optional[str] = None
    conference_meeting_id: Optional[str] = None
    
    created_by_user_id: Optional[int] = None
    assigned_to_user_id: Optional[int] = None 

    participants: Optional[List[ActivityParticipant]] = Field(default_factory=list)
    attendees: Optional[List[ActivityParticipant]] = Field(default_factory=list) 

    owner_name: Optional[str] = None
    person_name: Optional[str] = None
    org_name: Optional[str] = None
    deal_title: Optional[str] = None
    person_dropbox_bcc: Optional[str] = None
    deal_dropbox_bcc: Optional[str] = None
    busy_flag: Optional[bool] = None
    
    @field_validator("add_time", "update_time", "marked_as_done_time", mode="before")
    @classmethod
    def clean_datetime_fields(cls, v: Any) -> Optional[str]:
        if isinstance(v, str):
            stripped_v = v.strip()
            if not stripped_v or \
               stripped_v.startswith("0000-00-00") or \
               stripped_v.startswith("-0001-11-30T00:00:00"):
                return None
        return v

    @field_validator("due_date", mode="before")
    @classmethod
    def clean_date_fields(cls, v: Any) -> Optional[str]:
        if isinstance(v, str):
            stripped_v = v.strip()
            if not stripped_v or stripped_v == "0000-00-00" or stripped_v.startswith("-0001"):
                return None
        return v
    
    @field_validator("org_id", mode="before")
    @classmethod
    def nan_to_none_days(cls, v):
        import numpy as np
        if v is not None and isinstance(v, float) and np.isnan(v):
            return None
        return v
        
    model_config = {"extra": "allow"}