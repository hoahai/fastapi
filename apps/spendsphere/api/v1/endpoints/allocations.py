from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict

from apps.spendsphere.api.v1.helpers.db_queries import (
    duplicate_allocations,
    get_allocations,
)
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import require_account_code

router = APIRouter()


# ============================================================
# ALLOCATIONS
# ============================================================


@router.get("/allocations/{account_code}")
def get_allocations_route(account_code: str):
    """
    Example request:
    GET /spendsphere/api/v1/allocations/TAAA

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
    account_code = require_account_code(account_code)

    data = get_allocations(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No allocations found for account_code '{account_code}'",
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
