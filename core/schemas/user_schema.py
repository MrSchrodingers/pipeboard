from typing import Optional, List, Any 
from datetime import datetime
from pydantic import field_validator, Field 
from core.utils.schema_utils import PDBaseModel

class AccessItem(PDBaseModel):
    app: Optional[str] = None
    admin: Optional[bool] = False
    permission_set_id: Optional[str] = None

class User(PDBaseModel):
    id: int
    name: Optional[str] = None
    email: Optional[str] = None
    lang: Optional[str] = None
    locale: Optional[str] = None
    timezone_name: Optional[str] = None
    timezone_offset: Optional[int] = None 
    default_currency: Optional[str] = None
    icon_url: Optional[str] = None
    active_flag: Optional[bool] = None
    is_deleted: Optional[bool] = None
    is_admin: Optional[bool] = None  
    role_id: Optional[int] = None
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
    last_login: Optional[datetime] = None
    phone: Optional[str] = None 
    company_id: Optional[str] = None 
    company_name: Optional[str] = None
    company_domain: Optional[str] = None
    activated: Optional[bool] = None 
    
    has_created_company: Optional[bool] = None
    is_you: Optional[bool] = None
    access: Optional[List[AccessItem]] = Field(default_factory=list)


    model_config = {
        "extra": "allow"
    }

    @field_validator("timezone_offset", mode="before")
    @classmethod
    def parse_offset_to_int_minutes(cls, v: Any) -> Optional[int]:
        if isinstance(v, int):
            return v
        if isinstance(v, str):
            stripped_v = v.strip()
            if not stripped_v:
                return None
            if ":" in stripped_v: # Formato como "-03:00" ou "+02:00"
                try:
                    sign = -1 if stripped_v.startswith("-") else 1
                    parts = stripped_v.lstrip("+-").split(":")
                    if len(parts) == 2:
                        h, m = int(parts[0]), int(parts[1])
                        return sign * (h * 60 + m)
                    else:
                        return None
                except ValueError:
                    return None
            else:
                try:
                    return int(stripped_v)
                except ValueError:
                    return None
        return v 
    
    @field_validator("lang", mode="before")
    @classmethod
    def convert_lang_id_to_code(cls, v: Any) -> Optional[str]:
        if isinstance(v, int):
            return str(v)
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @field_validator("is_admin", "active_flag", "is_deleted", "has_created_company", "is_you", "activated", mode="before")
    @classmethod
    def convert_int_to_bool(cls, v: Any) -> Optional[bool]:
        if v == 1 or v == "1" or (isinstance(v, str) and v.lower() == 'true'):
            return True
        if v == 0 or v == "0" or (isinstance(v, str) and v.lower() == 'false'):
            return False
        if v is None:
            return None
        return bool(v) 
    
    @field_validator("phone", mode="before")
    @classmethod
    def nan_to_none_phone(cls, v):
        import numpy as np
        if v is not None and isinstance(v, float) and np.isnan(v):
            return None
        return v