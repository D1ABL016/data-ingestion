import re


_ws_re = re.compile(r"\s+")


def normalize_string(value: str | None) -> str | None:
    if value is None:
        return None
    s = value.strip().lower()
    if not s:
        return None
    return _ws_re.sub(" ", s)


def normalize_optional_lookup(value: str | None) -> str | None:
    """Normalize for lookup get-or-create; empty after normalize -> None."""
    n = normalize_string(value)
    return n if n else None
