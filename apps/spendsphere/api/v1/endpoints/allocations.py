from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict

from apps.spendsphere.api.v1.helpers.db_queries import (
    duplicate_allocations,
    get_allocations,
)
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
    normalize_account_codes,
    validate_account_codes,
)
from shared.logger import get_logger

router = APIRouter()
_API_LOGGER = get_logger("SpendSphere API")


# ============================================================
# ALLOCATIONS
# ============================================================


@router.get("/allocations")
def get_allocations_route(
    account_codes: list[str] | None = Query(None, alias="accountCodes"),
    month: int | None = Query(None, description="Month (1-12)."),
    year: int | None = Query(None, description="Year (e.g., 2026)."),
):
    """
    Example request:
        GET /api/spendsphere/v1/allocations?accountCodes=TAAA&accountCodes=TBBB

    Example request (specific period):
        GET /api/spendsphere/v1/allocations?accountCodes=TAAA&accountCodes=TBBB&month=1&year=2026

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
    requested_codes = normalize_account_codes(account_codes)
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

    validate_account_codes(
        requested_codes,
        month=month,
        year=year,
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
    accountCodes: list[str] | None = None
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
          "accountCodes": ["TAAA", "TBBB"],
          "fromMonth": 12,
          "fromYear": 2025,
          "toMonth": 1,
          "toYear": 2026,
          "overwrite": false
        }
        - accountCodes is required and must not be null or [].
        - allocations with value 0 are skipped during duplication.

    Example response:
        {
          "inserted": 42
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

    normalized_codes = normalize_account_codes(request_payload.accountCodes)
    if not normalized_codes:
        raise HTTPException(status_code=400, detail="accountCodes is required")

    validate_account_codes(
        normalized_codes,
        month=request_payload.toMonth,
        year=request_payload.toYear,
    )

    inserted = duplicate_allocations(
        from_month=request_payload.fromMonth,
        from_year=request_payload.fromYear,
        to_month=request_payload.toMonth,
        to_year=request_payload.toYear,
        account_codes=normalized_codes,
        overwrite=request_payload.overwrite,
    )

    _API_LOGGER.info(
        "duplicate allocations route result",
        extra={
            "extra_fields": {
                "event": "duplicate_allocations_route_result",
                "inserted": inserted,
            }
        },
    )

    return {"inserted": inserted}
