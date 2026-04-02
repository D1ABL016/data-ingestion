import math
from typing import Any

from pydantic import BaseModel, field_validator
from validation_utils import strip_val, validate_is_active


class StoreRowIn(BaseModel):
    store_id: str
    store_external_id: str | None = ""
    name: str
    title: str
    store_brand: str | None = None
    store_type: str | None = None
    city: str | None = None
    state: str | None = None
    country: str | None = None
    region: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    is_active: bool | int | str | None = True

    @field_validator("store_id", mode="before")
    @classmethod
    def store_id_non_empty(cls, v: Any) -> str:
        return strip_val(v)

    @field_validator("name", "title", mode="before")
    @classmethod
    def strip_required_strings(cls, v: Any) -> str:
        return strip_val(v)
    
    @field_validator("store_external_id", "store_brand", "store_type", "city", "state", "country", "region", mode="before")
    @classmethod
    def optional_strings(cls, v: Any) -> str | None:
        s = strip_val(v)
        return s if s!="" else None

    @field_validator("latitude", "longitude", mode="before")
    @classmethod
    def coerce_float(cls, v: Any) -> float | None:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        if v == "":
            return None
        return float(v)

    @field_validator("is_active", mode="before")
    @classmethod
    def coerce_bool(cls, v: Any) -> bool:
        return validate_is_active(v)

    @field_validator("store_id")
    @classmethod
    def len_store_id(cls, v: str) -> str:
        if not v:
            raise ValueError("store_id is required")
        if not v.startswith("STR"):
            raise ValueError("store_id must start with STR")
        if len(v) > 255:
            raise ValueError("store_id exceeds 255 characters")
        return v

    @field_validator("name", "title")
    @classmethod
    def len_name_title(cls, v: str) -> str:
        if not v:
            raise ValueError("must be non-empty")
        if len(v) > 255:
            raise ValueError("exceeds 255 characters")
        return v

    @field_validator("store_external_id")
    @classmethod
    def len_ext(cls, v: str | None) -> str | None:
        if v is not None and len(v) > 255:
            raise ValueError("exceeds 255 characters")
        if v is not None and not v.startswith("EXT"):
            raise ValueError("store_external_id must start with EXT")
        return v

    @field_validator("store_brand", "store_type", "city", "state", "country", "region")
    @classmethod
    def len_optional_255(cls, v: str | None) -> str | None:
        if v is not None and len(v) > 255:
            raise ValueError("exceeds 255 characters")
        return v
