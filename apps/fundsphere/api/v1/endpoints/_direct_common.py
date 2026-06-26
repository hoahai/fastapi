from __future__ import annotations

import mysql.connector
from fastapi import HTTPException


def parse_optional_int_query(value: object, *, field: str) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=f"{field} must be an integer") from exc


def translate_mysql_error(exc: mysql.connector.Error) -> HTTPException:
    errno = int(getattr(exc, "errno", 0) or 0)
    if errno == 1062:
        return HTTPException(status_code=409, detail=str(exc))
    if errno in {1048, 1364, 1451, 1452}:
        return HTTPException(status_code=400, detail=str(exc))
    return HTTPException(status_code=500, detail="Database operation failed")
