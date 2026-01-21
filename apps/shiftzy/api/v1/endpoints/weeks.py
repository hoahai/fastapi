from datetime import date as DateType

from fastapi import APIRouter, Query, Request

from apps.shiftzy.api.v1.helpers.weeks import (
    build_week_info,
    get_week_no_for_date,
    list_weeks,
)
from shared.utils import with_meta

router = APIRouter()


# ============================================================
# WEEKS
# ============================================================

@router.get("/weeks")
def get_weeks(
    request: Request,
    week_before: int | None = Query(None, ge=0),
    week_after: int | None = Query(None, ge=0),
):
    data = list_weeks(week_before=week_before, week_after=week_after)
    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.get("/weeks/by-date")
def get_week_by_date(request: Request, date: DateType = Query(...)):
    week_no = get_week_no_for_date(date)
    data = build_week_info(week_no)
    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.get("/weeks/by-week-no")
def get_week_by_no(request: Request, week_no: int = Query(...)):
    data = build_week_info(week_no)
    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )
