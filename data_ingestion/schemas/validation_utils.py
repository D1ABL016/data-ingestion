from datetime import date, datetime
import math
from typing import Any


def strip_val(v: Any) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    return str(v).strip()

def validate_is_active(v: Any) -> bool:
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

def validate_iso_date(v: Any) -> date:
    if not v:
        raise ValueError("date is required")
    if isinstance(v, date):
        return v
    s = str(v).strip()
    parts = s.split("-")
    if len(parts) != 3:
        raise ValueError("date must be YYYY-MM-DD")
    try:
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    except Exception as e:
        raise ValueError("invalid date format") from e


def parse_date(v: Any) -> date | str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    return s