from fastapi import APIRouter, Request

from shared.utils import get_current_period, with_meta

router = APIRouter()


# ============================================================
# CURRENT PERIOD
# ============================================================


@router.get("/current-period")
def get_current_period_route(request: Request):
    return with_meta(
        data=get_current_period(),
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )
