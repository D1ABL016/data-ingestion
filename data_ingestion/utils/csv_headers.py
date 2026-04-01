import io
from pathlib import Path
from typing import Literal

import pandas as pd

FileKind = Literal["stores", "users", "mappings"]

REQUIRED_HEADERS: dict[FileKind, set[str]] = {
    "stores": {"store_id", "store_external_id", "name", "title", "store_brand", "store_type", "city", "state", "country", "region", "latitude", "longitude"},
    "users": {"username", "first_name", "last_name", "email", "user_type", "phone_number", "supervisor_username", "is_active"},
    "mappings": {"username", "store_id", "date", "is_active"},
}


def get_header_row_index(filepath: str | Path) -> int:
    path = Path(filepath)
    with path.open(encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if line.strip():
                return i
    raise ValueError("File is completely empty")


def count_data_rows_after_header(filepath: str | Path, header_idx: int) -> int:
    path = Path(filepath)
    n = 0
    with path.open(encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f):
            if i <= header_idx:
                continue
            if line.strip():
                n += 1
    return n


def validate_headers_bytes(content: bytes, file_kind: FileKind) -> int:
    text = content.decode("utf-8", errors="replace")
    lines = text.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        if line.strip():
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("File is completely empty")
    buf = io.StringIO(text)
    df0 = pd.read_csv(buf, skiprows=header_idx, nrows=0, header=0)
    actual = {c.strip().lower() for c in df0.columns}
    missing = REQUIRED_HEADERS[file_kind] - actual
    print("actual", actual)
    print("missing", missing)
    if missing:
        raise ValueError(f"Invalid or missing headers: {sorted(missing)}")
    return header_idx


def validate_headers_only(filepath: str | Path, file_kind: FileKind) -> int:
    """Return header row index after validating required columns exist."""
    header_idx = get_header_row_index(filepath)
    path = str(filepath)
    df0 = pd.read_csv(path, skiprows=header_idx, nrows=0, header=0)
    actual = {c.strip().lower() for c in df0.columns}
    missing = REQUIRED_HEADERS[file_kind] - actual
    if missing:
        raise ValueError(f"Invalid or missing headers: {sorted(missing)}")
    return header_idx


def read_csv_with_strict_headers(
    filepath: str | Path,
    file_kind: FileKind,
    chunksize: int | None = None,
):
    """
    Validate first non-empty line headers, then return iterator of chunks or full frame.
    """
    header_idx = get_header_row_index(filepath)
    path = str(filepath)

    def _validate_columns(columns: list[str]) -> None:
        actual = {c.strip().lower() for c in columns}
        required = REQUIRED_HEADERS[file_kind]
        missing = required - actual
        if missing:
            raise ValueError(f"Invalid or missing headers: {sorted(missing)}")

    if chunksize:
        first = pd.read_csv(path, skiprows=header_idx, nrows=0, header=0)
        _validate_columns(list(first.columns))
        return pd.read_csv(path, skiprows=header_idx, chunksize=chunksize, header=0)

    df = pd.read_csv(path, skiprows=header_idx, header=0)
    _validate_columns(list(df.columns))
    return df
