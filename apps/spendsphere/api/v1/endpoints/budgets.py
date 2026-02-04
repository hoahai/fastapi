from fastapi import APIRouter, HTTPException

from apps.spendsphere.api.v1.helpers.db_queries import get_masterbudgets
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import require_account_code

router = APIRouter()


# ============================================================
# BUDGETS
# ============================================================


@router.get("/budgets/{account_code}")
def get_budgets(account_code: str):
    """
    Example request:
        GET /api/spendsphere/v1/budgets/TAAA

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
    account_code = require_account_code(account_code)

    data = get_masterbudgets(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No budgets found for account_code '{account_code}'",
        )

    return data
