from fastapi import APIRouter, HTTPException

from apps.spendsphere.api.v2.helpers.db_queries import get_masterbudgets
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import require_account_code

router = APIRouter()


# ============================================================
# BUDGETS
# ============================================================


@router.get("/budgets/{account_code}")
def get_budgets(account_code: str):
    account_code = require_account_code(account_code)

    data = get_masterbudgets(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No budgets found for account_code '{account_code}'",
        )

    return data
