import math
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, field_validator
from validation_utils import strip_val, validate_is_active, parse_date,validate_iso_date

class MappingRowIn(BaseModel):
    username: str
    store_id: str
    date: date | str
    is_active: bool | int | str | None = True

    @field_validator("username", "store_id", mode="before")
    @classmethod
    def strip_ids(cls, v: Any) -> str:
        return strip_val(v)

    @field_validator("date", mode="before")
    @classmethod
    def parse_date(cls, v: Any) -> date | str:
        return parse_date(v)

    @field_validator("is_active", mode="before")
    @classmethod
    def coerce_bool(cls, v: Any) -> bool:
        return validate_is_active(v)

    @field_validator("username")
    @classmethod
    def username_required(cls, v: str) -> str:
        if not v:
            raise ValueError("username is required")
        if len(v) > 150:
            raise ValueError("username exceeds 150 characters")
        return v

    @field_validator("store_id")
    @classmethod
    def store_id_required(cls, v: str) -> str:
        if not v:
            raise ValueError("store_id is required")
        if not v.startswith("STR"):
            raise ValueError("store_id must start with STR")
        if len(v) > 255:
            raise ValueError("store_id exceeds 255 characters")
        return v

    @field_validator("date")
    @classmethod
    def date_iso(cls, v: date | str) -> date:
        return validate_iso_date(v)