from fastapi import APIRouter, HTTPException, Query

from apps.spendsphere.api.v1.helpers.db_queries import get_rollbreakdowns
from apps.spendsphere.api.v1.helpers.ggSheet import get_rollovers
router = APIRouter()


# ============================================================
# ROLLOVERS
# ============================================================


@router.get("/rollovers")
def get_rollovers_route(
    account_codes: list[str] | None = Query(None, alias="accountCodes"),
    account_code: str | None = Query(None, alias="accountCode"),
    month: int | None = Query(None, description="Month (1-12)."),
    year: int | None = Query(None, description="Year (e.g., 2026)."),
    include_unrollable: bool = Query(False, alias="includeUnrollable"),
):
    """
    Example request:
        GET /api/spendsphere/v1/rollovers?accountCodes=TAAA

    Example request (specific period):
        GET /api/spendsphere/v1/rollovers?accountCodes=TAAA&month=1&year=2026

    Example request (multiple accounts, include unrollable):
        GET /api/spendsphere/v1/rollovers?accountCodes=TAAA,LACS&includeUnrollable=true

    Example response:
        [
          {
            "accountCode": "TAAA",
            "adTypeCode": "SEM",
            "amount": 250.0,
            "rollable": 1
          }
        ]
    """
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

    data = get_rollovers(
        requested_codes,
        month,
        year,
        include_unrollable=include_unrollable,
    )
    if not data:
        raise HTTPException(
            status_code=404,
            detail="No rollovers found for requested account codes",
        )

    return data


@router.get("/rollovers/breakdown/{account_code}")
def get_rollovers_breakdown(account_code: str):
    """
    Example request:
        GET /api/spendsphere/v1/rollovers/breakdown/TAAA

    Example response:
        [
          {
            "id": 1,
            "accountCode": "TAAA",
            "adTypeCode": "SEM",
            "amount": 250.0
          }
        ]
    """
    account_code = require_account_code(account_code)

    data = get_rollbreakdowns(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No rollovers breakdown found for account_code '{account_code}'",
        )

    return data
