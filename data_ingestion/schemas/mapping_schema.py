import math
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, field_validator


class MappingRowIn(BaseModel):
    username: str
    store_id: str
    date: date | str
    is_active: bool | int | str | None = True

    @field_validator("username", "store_id", mode="before")
    @classmethod
    def strip_ids(cls, v: Any) -> str:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return ""
        return str(v).strip()

    @field_validator("date", mode="before")
    @classmethod
    def parse_date(cls, v: Any) -> date | str:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return ""
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v
        s = str(v).strip()
        return s

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
        if s in ("true", "yes"):
            return True
        if s in ("false", "no"):
            return False
        return True

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
        if isinstance(v, date):
            return v
        if not v:
            raise ValueError("date is required")
        s = str(v).strip()
        parts = s.split("-")
        if len(parts) != 3:
            raise ValueError("date must be YYYY-MM-DD")
        try:
            y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
            return date(y, m, d)
        except ValueError as e:
            raise ValueError("Invalid date") from e
