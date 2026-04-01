import math
import re
from typing import Any

from pydantic import BaseModel, model_validator, field_validator

_email_re = re.compile(r"^[^@]+@[^@]+\.[^@]+$")
_phone_re = re.compile(r"^[\d\s+\-()/]{1,32}$")
_VALID_USER_TYPES = {1, 2, 3, 7}


class UserRowIn(BaseModel):
    username: str
    first_name: str | None = None
    last_name: str | None = None
    email: str
    user_type: int | None = 1
    phone_number: str | None = None
    supervisor_username: str | None = None
    is_active: bool | int | str | None = True

    @field_validator("username", mode="before")
    @classmethod
    def username_strip(cls, v: Any) -> str:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return ""
        return str(v).strip()

    @field_validator("email", mode="before")
    @classmethod
    def email_strip(cls, v: Any) -> str:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return ""
        return str(v).strip()

    @field_validator("first_name", "last_name", "phone_number", "supervisor_username", mode="before")
    @classmethod
    def optional_strip(cls, v: Any) -> str | None:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        s = str(v).strip()
        return s if s else None

    @field_validator("user_type", mode="before")
    @classmethod
    def coerce_user_type(cls, v: Any) -> int:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return 1
        return int(v)

    @field_validator("is_active", mode="before")
    @classmethod
    def coerce_bool(cls, v: Any) -> bool:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return True
        if isinstance(v, bool):
            return v
        if isinstance(v, int | float):
            return bool(int(v))
        s = str(v).strip().lower()
        if s in ("true",  "yes"):
            return True
        if s in ("false", "no"):
            return False
        return True

    @field_validator("username")
    @classmethod
    def username_rules(cls, v: str) -> str:
        if not v:
            raise ValueError("username is required")
        if len(v) > 150:
            raise ValueError("username exceeds 150 characters")
        return v

    @field_validator("email")
    @classmethod
    def email_rules(cls, v: str) -> str:
        if not v:
            raise ValueError("email is required")
        if len(v) > 254:
            raise ValueError("email exceeds 254 characters")
        if not _email_re.match(v):
            raise ValueError("Invalid email format")
        return v

    @field_validator("first_name", "last_name")
    @classmethod
    def len_names(cls, v: str | None) -> str | None:
        if v is not None and len(v) > 150:
            raise ValueError("exceeds 150 characters")
        return v

    @field_validator("user_type")
    @classmethod
    def user_type_allowed(cls, v: int) -> int:
        if v not in _VALID_USER_TYPES:
            raise ValueError("user_type must be one of 1, 2, 3, 7")
        return v

    @field_validator("phone_number")
    @classmethod
    def phone_rules(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if len(v) > 32:
            raise ValueError("phone_number exceeds 32 characters")
        if not _phone_re.match(v):
            raise ValueError("Invalid phone format")
        return v

    @model_validator(mode="after")
    def not_self_supervisor(self) -> "UserRowIn":
        if self.supervisor_username and self.username:
            nu = self.username.strip().lower()
            ns = self.supervisor_username.strip().lower()
            if nu == ns:
                raise ValueError("User cannot be their own supervisor")
        return self
