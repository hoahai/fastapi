from __future__ import annotations

from datetime import date as DateType, time as TimeType

from fastapi import APIRouter, Body, HTTPException, Query, Request, Response
from pydantic import BaseModel

from apps.shiftzy.api.v1.helpers.db_queries import (
    apply_schedule_changes,
    delete_schedules as delete_schedules_db,
    duplicate_week_schedules as duplicate_week_schedules_db,
    get_schedules,
    insert_schedules,
    update_schedules as update_schedules_db,
)
from apps.shiftzy.api.v1.helpers.schedule_pdf import build_schedule_pdf
from apps.shiftzy.api.v1.helpers.weeks import build_week_info
from shared.utils import with_meta

router = APIRouter()


# ============================================================
# SCHEDULES
# ============================================================

try:
    from pydantic import ConfigDict

    class _ShiftzyModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

except ImportError:

    class _ShiftzyModel(BaseModel):
        class Config:
            extra = "ignore"


class ScheduleCreate(_ShiftzyModel):
    id: str | None = None
    employee_id: str
    position_code: str
    shift_id: str | None = None
    date: DateType
    start_time: TimeType
    end_time: TimeType
    note: str | None = None


class ScheduleUpdate(_ShiftzyModel):
    id: str
    employee_id: str | None = None
    position_code: str | None = None
    shift_id: str | None = None
    date: DateType | None = None
    start_time: TimeType | None = None
    end_time: TimeType | None = None
    note: str | None = None


class ScheduleDeleteItem(_ShiftzyModel):
    id: str


class ScheduleBatchRequest(_ShiftzyModel):
    toCreate: list[ScheduleCreate] | ScheduleCreate | None = None
    toUpdate: list[ScheduleUpdate] | ScheduleUpdate | None = None
    toDelete: list[ScheduleDeleteItem | str] | ScheduleDeleteItem | None = None


def _dump_model(item: BaseModel) -> dict:
    if hasattr(item, "model_dump"):
        return item.model_dump(exclude_unset=True)
    return item.dict(exclude_unset=True)


def _normalize_payload(payload):
    if isinstance(payload, list):
        normalized = []
        for item in payload:
            normalized.append(
                _dump_model(item) if isinstance(item, BaseModel) else item
            )
        return normalized
    if isinstance(payload, BaseModel):
        return _dump_model(payload)
    return payload


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


@router.get("/schedules/pdf")
def download_schedule_pdf(
    week_no: int = Query(...),
    orientation: str = Query("landscape"),
):
    try:
        week_info = build_week_info(week_no)
        schedules = get_schedules(week_no=week_no)
        pdf_bytes = build_schedule_pdf(
            schedules=schedules,
            week_info=week_info,
            orientation=orientation,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    filename = f"schedule-week-{week_no}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/schedules")
def create_schedules(
    request: Request,
    payload: list[ScheduleCreate] | ScheduleCreate = Body(...),
):
    """
    Example request:
    [
      {
        "employee_id": "948f09c9-f6b9-11f0-b7f6-5a4783e25118",
        "position_code": "FR-CASH",
        "shift_id": "9941aa49-f6b9-11f0-b7f6-5a4783e25118",
        "date": "2026-01-20",
        "start_time": "10:00",
        "end_time": "14:00",
        "note": null
      }
    ]

    Example response:
    {
      "meta": {"timestamp": "2026-01-20T10:00:00-05:00", "duration_ms": 3},
      "data": {"inserted": 1}
    }
    """
    normalized = _normalize_payload(payload)
    try:
        inserted = insert_schedules(normalized)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return with_meta(
        data={"inserted": inserted},
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.put("/schedules")
def update_schedules(
    request: Request,
    payload: list[ScheduleUpdate] | ScheduleUpdate = Body(...),
):
    normalized = _normalize_payload(payload)
    try:
        updated = update_schedules_db(normalized)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return with_meta(
        data={"updated": updated},
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.delete("/schedules")
def delete_schedules(
    request: Request,
    payload: list[ScheduleDeleteItem | str] | ScheduleDeleteItem = Body(...),
):
    normalized = _normalize_payload(payload)
    try:
        deleted = delete_schedules_db(normalized)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return with_meta(
        data={"deleted": deleted},
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.post("/schedules/batchUpdates")
def batch_schedules(
    request: Request,
    payload: ScheduleBatchRequest = Body(...),
):
    """
    Example request:
    {
      "toCreate": [],
      "toUpdate": [
        {
          "id": "66495150-aada-4a11-bffa-e90e84d90662",
          "employee_id": "948f09c9-f6b9-11f0-b7f6-5a4783e25118",
          "position_code": "FR-CASH",
          "shift_id": "9941aa49-f6b9-11f0-b7f6-5a4783e25118",
          "date": "2026-01-20",
          "start_time": "10:00",
          "end_time": "14:00",
          "note": null
        }
      ],
      "toDelete": ["50e20973-9584-4d2c-a4a7-0977e8e4d80a"]
    }

    Example response:
    {
      "meta": {"timestamp": "2026-01-20T10:00:00-05:00", "duration_ms": 5},
      "data": {"inserted": 0, "updated": 1, "deleted": 1}
    }
    """
    normalized = _normalize_payload(payload)
    try:
        results = apply_schedule_changes(normalized)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return with_meta(
        data=results,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.post("/schedules/duplicate")
def duplicate_week_schedules(
    request: Request,
    week_start: int = Query(...),
    week_end: int = Query(...),
    overwrite: bool = Query(False),
    return_schedules: bool = Query(False),
):
    try:
        if week_start == week_end:
            raise ValueError("week_start and week_end must be different")

        source_week = build_week_info(week_start)
        target_week = build_week_info(week_end)
        source_start = DateType.fromisoformat(source_week["start_date"])
        source_end = DateType.fromisoformat(source_week["end_date"])
        target_start = DateType.fromisoformat(target_week["start_date"])
        target_end = DateType.fromisoformat(target_week["end_date"])
        delta_days = (target_start - source_start).days

        inserted = duplicate_week_schedules_db(
            source_start=source_start,
            source_end=source_end,
            target_start=target_start,
            target_end=target_end,
            delta_days=delta_days,
            overwrite=overwrite,
        )
        schedules = get_schedules(week_no=week_end) if return_schedules else None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    data = {
        "inserted": inserted,
        "source_week": week_start,
        "target_week": week_end,
        "overwrite": overwrite,
    }
    if return_schedules:
        data["schedules"] = schedules

    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )
