from fastapi import APIRouter

from shared.utils import get_current_period

router = APIRouter()


# ============================================================
# CURRENT PERIOD
# ============================================================


@router.get("/current-period")
def get_current_period_route():
    """
    Example request:
    GET /spendsphere/api/v1/current-period

    Example response:
    {
      "year": 2026,
      "month": 1,
      "start_date": "2026-01-01",
      "end_date": "2026-01-31"
    }
    """
    return get_current_period()
