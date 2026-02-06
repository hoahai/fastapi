import calendar
from datetime import date

from fastapi import APIRouter, HTTPException, Query

from apps.spendsphere.api.v2.helpers.db_queries import get_accelerations
router = APIRouter()


# ============================================================
# ACCELERATIONS
# ============================================================


@router.get("/accelerations")
def get_accelerations_route(
    account_codes: list[str] | None = Query(None, alias="accountCodes"),
    account_code: str | None = Query(None, alias="accountCode"),
    month: int | None = Query(None, description="Month (1-12)."),
    year: int | None = Query(None, description="Year (e.g., 2026)."),
):
    def _normalize_codes(values: list[str] | None) -> list[str]:
        if not values:
            return []
        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            if not isinstance(value, str):
                continue
            for chunk in value.split(","):
                code = chunk.strip().upper()
                if not code or code in seen:
                    continue
                seen.add(code)
                normalized.append(code)
        return normalized

    requested_codes = _normalize_codes(account_codes)
    if not requested_codes and isinstance(account_code, str) and account_code.strip():
        requested_codes = _normalize_codes([account_code])
    if not requested_codes:
        raise HTTPException(
            status_code=400,
            detail="accountCodes is required",
        )

    if (month is None) != (year is None):
        raise HTTPException(
            status_code=400,
            detail="month and year must be provided together",
        )
    if month is not None and not 1 <= month <= 12:
        raise HTTPException(
            status_code=400,
            detail="month must be between 1 and 12",
        )
    if year is not None and not 2000 <= year <= 2100:
        raise HTTPException(
            status_code=400,
            detail="year must be between 2000 and 2100",
        )

    if month is not None:
        start_date = date(year, month, 1)
        end_date = date(year, month, calendar.monthrange(year, month)[1])
    else:
        start_date = None
        end_date = None

    data = get_accelerations(
        requested_codes,
        start_date=start_date,
        end_date=end_date,
    )
    if not data:
        raise HTTPException(
            status_code=404,
            detail="No accelerations found for requested account codes",
        )

    sanitized = []
    for row in data:
        if isinstance(row, dict):
            sanitized.append(
                {
                    k: v
                    for k, v in row.items()
                    if k not in {"dateCreated", "dateUpdated"}
                }
            )
        else:
            sanitized.append(row)

    return sanitized
