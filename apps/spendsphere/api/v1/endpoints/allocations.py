from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict

from apps.spendsphere.api.v1.helpers.db_queries import (
    duplicate_allocations,
    get_allocations,
)
router = APIRouter()


# ============================================================
# ALLOCATIONS
# ============================================================


@router.get("/allocations")
def get_allocations_route(
    account_codes: list[str] | None = Query(None, alias="accountCodes"),
    account_code: str | None = Query(None, alias="accountCode"),
    month: int | None = Query(None, description="Month (1-12)."),
    year: int | None = Query(None, description="Year (e.g., 2026)."),
):
    """
    Example request:
        GET /api/spendsphere/v1/allocations?accountCodes=TAAA

    Example request (specific period):
        GET /api/spendsphere/v1/allocations?accountCodes=TAAA&month=1&year=2026

    Example response:
        [
          {
            "id": 1,
            "accountCode": "TAAA",
            "ggBudgetId": "15264548297",
            "allocation": 60.0
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

    data = get_allocations(requested_codes, month, year)
    if not data:
        raise HTTPException(
            status_code=404,
            detail="No allocations found for requested account codes",
        )

    return data


class AllocationDuplicateRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    fromMonth: int
    fromYear: int
    toMonth: int
    toYear: int
    overwrite: bool = False


@router.post("/allocations/duplicate")
def duplicate_allocations_route(request_payload: AllocationDuplicateRequest):
    """
    Example request:
        POST /api/spendsphere/v1/allocations/duplicate
        {
          "fromMonth": 12,
          "fromYear": 2025,
          "toMonth": 1,
          "toYear": 2026,
          "overwrite": false
        }

    Example response:
        {
          "meta": {"timestamp": "2026-01-20T10:00:00-05:00", "duration_ms": 2},
          "data": {"inserted": 42}
        }
    """
    if request_payload.fromMonth < 1 or request_payload.fromMonth > 12:
        raise HTTPException(status_code=400, detail="fromMonth must be 1-12")
    if request_payload.toMonth < 1 or request_payload.toMonth > 12:
        raise HTTPException(status_code=400, detail="toMonth must be 1-12")
    if request_payload.fromYear < 2000 or request_payload.fromYear > 2100:
        raise HTTPException(status_code=400, detail="fromYear must be 2000-2100")
    if request_payload.toYear < 2000 or request_payload.toYear > 2100:
        raise HTTPException(status_code=400, detail="toYear must be 2000-2100")

    inserted = duplicate_allocations(
        from_month=request_payload.fromMonth,
        from_year=request_payload.fromYear,
        to_month=request_payload.toMonth,
        to_year=request_payload.toYear,
        overwrite=request_payload.overwrite,
    )
    return {"inserted": inserted}
