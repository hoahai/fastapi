from fastapi import APIRouter, HTTPException, Request

from apps.spendsphere.api.v1.helpers.db_queries import get_masterbudgets
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import require_account_code
from shared.utils import with_meta

router = APIRouter()


# ============================================================
# BUDGETS
# ============================================================


@router.get("/budgets/{account_code}")
def get_budgets(account_code: str, request: Request):
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
