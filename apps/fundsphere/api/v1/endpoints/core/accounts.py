from fastapi import APIRouter

from apps.fundsphere.api.v1.helpers.db_queries import get_accounts

router = APIRouter()


# ============================================================
# ACCOUNTS
# ============================================================


@router.get("/accounts")
def list_accounts():
    """
    Return all account rows from database sorted by active status and code.

    Example request:
        GET /api/fundsphere/v1/accounts

    Example response:
        [
          {
            "code": "TAAA",
            "name": "Toyota",
            "active": 1
          }
        ]

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
    """
    return get_accounts()
