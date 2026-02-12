from fastapi import APIRouter, HTTPException, Query

from apps.spendsphere.api.v1.helpers.db_queries import get_masterbudgets
router = APIRouter()


# ============================================================
# BUDGETS
# ============================================================


@router.get("/budgets")
def get_budgets(
    account_codes: list[str] | None = Query(None, alias="accountCodes"),
    month: int | None = Query(None, description="Month (1-12)."),
    year: int | None = Query(None, description="Year (e.g., 2026)."),
):
    """
    Example request:
        GET /api/spendsphere/v1/budgets?accountCodes=TAAA&accountCodes=TBBB

    Example request (specific period):
        GET /api/spendsphere/v1/budgets?accountCodes=TAAA&accountCodes=TBBB&month=1&year=2026

    Example response:
        [
          {
            "accountCode": "TAAA",
            "serviceId": "c6ac34bc-0fc0-46a6-9723-e83780ebb938",
            "serviceName": "Search Engine Marketing",
            "subService": null,
            "month": 1,
            "year": 2026,
            "netAmount": 1500.0
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

    data = get_masterbudgets(requested_codes, month, year)
    if not data:
        raise HTTPException(
            status_code=404,
            detail="No budgets found for requested account codes",
        )

    return data
