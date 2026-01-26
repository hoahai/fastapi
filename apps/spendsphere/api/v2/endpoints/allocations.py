from fastapi import APIRouter, HTTPException

from apps.spendsphere.api.v2.helpers.db_queries import get_allocations
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import require_account_code

router = APIRouter()


# ============================================================
# ALLOCATIONS
# ============================================================


@router.get("/allocations/{account_code}")
def get_allocations_route(account_code: str):
    account_code = require_account_code(account_code)

    data = get_allocations(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No allocations found for account_code '{account_code}'",
        )

    return data
