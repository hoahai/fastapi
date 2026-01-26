from fastapi import APIRouter, HTTPException

from apps.spendsphere.api.v1.helpers.db_queries import get_rollbreakdowns
from apps.spendsphere.api.v1.helpers.ggSheet import get_rollovers
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import require_account_code

router = APIRouter()


# ============================================================
# ROLLOVERS
# ============================================================


@router.get("/rollovers/{account_code}")
def get_rollovers_route(account_code: str):
    account_code = require_account_code(account_code)

    data = get_rollovers(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No rollovers found for account_code '{account_code}'",
        )

    return data


@router.get("/rollovers/breakdown/{account_code}")
def get_rollovers_breakdown(account_code: str):
    account_code = require_account_code(account_code)

    data = get_rollbreakdowns(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No rollovers breakdown found for account_code '{account_code}'",
        )

    return data
