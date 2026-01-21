from __future__ import annotations

from datetime import date as DateType, time as TimeType

from fastapi import APIRouter, Body, HTTPException, Query, Request

from apps.shiftzy.api.v1.helpers.db_queries import get_schedules, insert_schedules
from shared.utils import with_meta

router = APIRouter()


# ============================================================
# SCHEDULES
# ============================================================


@router.get("/schedules")
def list_schedules(
    request: Request,
    schedule_id: str | None = Query(None, alias="id"),
    employee_id: str | None = Query(None),
    date_value: DateType | None = Query(None, alias="date"),
    start_date: DateType | None = Query(None),
    end_date: DateType | None = Query(None),
    start_time: TimeType | None = Query(None),
    end_time: TimeType | None = Query(None),
    week_no: int | None = Query(None),
):
    try:
        data = get_schedules(
            schedule_id=schedule_id,
            employee_id=employee_id,
            date_value=date_value,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time,
            week_no=week_no,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.post("/schedules")
def create_schedules(
    request: Request,
    payload: list[dict] | dict = Body(...),
):
    try:
        inserted = insert_schedules(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return with_meta(
        data={"inserted": inserted},
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )
