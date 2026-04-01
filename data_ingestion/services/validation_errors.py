from typing import Any

from pydantic import ValidationError


def pydantic_errors_to_records(
    exc: ValidationError,
    row: int,
    default_column: str | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for err in exc.errors():
        loc = err.get("loc") or ()
        col = default_column
        if loc:
            last = loc[-1]
            if isinstance(last, str):
                col = last
            elif isinstance(last, int):
                col = str(last)
        if col is None:
            col = "row"
        msg = err.get("msg", "validation error")
        if isinstance(msg, str) and msg.startswith("Value error, "):
            msg = msg[len("Value error, ") :]
        inp = err.get("input")
        out.append(
            {
                "row": row,
                "column": col,
                "value": str(inp) if inp is not None else "",
                "reason": msg,
            }
        )
    return out
