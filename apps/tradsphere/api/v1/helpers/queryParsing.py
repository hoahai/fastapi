from __future__ import annotations

from datetime import date

from fastapi import HTTPException


def _coerce_iterable(values: object) -> list[str]:
    if values is None:
        return []
    if isinstance(values, (list, tuple, set)):
        return [str(item or "") for item in values]
    return [str(values)]


def parse_csv_values(
    *values: object,
    uppercase: bool = False,
    lowercase: bool = False,
) -> list[str]:
    parsed: list[str] = []
    seen: set[str] = set()

    for raw_group in values:
        for raw in _coerce_iterable(raw_group):
            for chunk in str(raw).split(","):
                value = chunk.strip()
                if not value:
                    continue
                if uppercase:
                    value = value.upper()
                elif lowercase:
                    value = value.lower()
                if value in seen:
                    continue
                seen.add(value)
                parsed.append(value)
    return parsed


def parse_int_list(*values: object) -> list[int]:
    parsed = parse_csv_values(*values)
    out: list[int] = []
    for value in parsed:
        try:
            out.append(int(value))
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid integer value: {value}",
            ) from exc
    return out


def parse_optional_date(value: str | None, *, field: str) -> date | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{field} must be ISO date YYYY-MM-DD",
        ) from exc
