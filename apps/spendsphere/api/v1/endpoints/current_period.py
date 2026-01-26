from fastapi import APIRouter

from shared.utils import get_current_period

router = APIRouter()


# ============================================================
# CURRENT PERIOD
# ============================================================


@router.get("/current-period")
def get_current_period_route():
    return get_current_period()
