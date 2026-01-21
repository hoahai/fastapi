from datetime import date as DateType

from fastapi import APIRouter, HTTPException, Query, Request

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
    date_value: DateType | None = Query(None, alias="date"),
    week_no: int | None = Query(None, ge=0),
):
    has_list_params = week_before is not None or week_after is not None
    has_date = date_value is not None
    has_week_no = week_no is not None

    if sum([has_list_params, has_date, has_week_no]) > 1:
        raise HTTPException(
            status_code=400,
            detail="Use only one of week_before and/or week_after, date, or week_no",
        )

    if has_date:
        computed_week_no = get_week_no_for_date(date_value)
        data = build_week_info(computed_week_no)
    elif has_week_no:
        data = build_week_info(week_no)
    else:
        data = list_weeks(week_before=week_before, week_after=week_after)
    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )
