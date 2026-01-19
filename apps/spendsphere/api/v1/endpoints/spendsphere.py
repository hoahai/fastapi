from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
    validate_account_codes,
    require_account_code,
)
from apps.spendsphere.api.v1.helpers.db_queries import (
    get_masterbudgets,
    get_allocations,
    get_rollbreakdowns,
)
from apps.spendsphere.api.v1.helpers.ggSheet import get_rollovers
from apps.spendsphere.api.v1.helpers.pipeline import run_google_ads_budget_pipeline
from shared.utils import with_meta, get_current_period

router = APIRouter()


@router.get("/current-period")
def getCurrentPeriod(request: Request):
    return with_meta(
        data=get_current_period(),
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.get("/budgets/{account_code}")
def getBudgets(account_code: str, request: Request):
    account_code = require_account_code(account_code)

    data = get_masterbudgets(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No budgets found for account_code '{account_code}'",
        )

    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.get("/allocations/{account_code}")
def getAllocations(account_code: str, request: Request):
    account_code = require_account_code(account_code)

    data = get_allocations(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No allocations found for account_code '{account_code}'",
        )

    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.get("/rollovers/{account_code}")
def getRollovers(account_code: str, request: Request):
    account_code = require_account_code(account_code)

    data = get_rollovers(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No rollovers found for account_code '{account_code}'",
        )

    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.get("/rollovers/breakdown/{account_code}")
def getRolloversBreakDown(account_code: str, request: Request):
    account_code = require_account_code(account_code)

    data = get_rollbreakdowns(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No rollovers breakdown found for account_code '{account_code}'",
        )

    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


class GoogleAdsUpdateRequest(BaseModel):
    accountCodes: str | list[str] | None = None
    dryRun: bool = False


@router.post("/update-budget")
def update_google_ads(payload: GoogleAdsUpdateRequest, request: Request):
    validate_account_codes(payload.accountCodes)

    result = run_google_ads_budget_pipeline(
        account_codes=payload.accountCodes,
        dry_run=payload.dryRun,
    )

    return with_meta(
        data=result,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )
