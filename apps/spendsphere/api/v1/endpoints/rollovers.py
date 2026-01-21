from fastapi import APIRouter, HTTPException, Request

from apps.spendsphere.api.v1.helpers.db_queries import get_rollbreakdowns
from apps.spendsphere.api.v1.helpers.ggSheet import get_rollovers
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import require_account_code
from shared.utils import with_meta

router = APIRouter()


# ============================================================
# ROLLOVERS
# ============================================================


@router.get("/rollovers/{account_code}")
def get_rollovers_route(account_code: str, request: Request):
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
def get_rollovers_breakdown(account_code: str, request: Request):
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
