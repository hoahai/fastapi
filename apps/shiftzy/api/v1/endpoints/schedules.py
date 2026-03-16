from __future__ import annotations

from datetime import date as DateType, time as TimeType

from fastapi import APIRouter, Body, HTTPException, Query, Response
from pydantic import BaseModel

from apps.shiftzy.api.v1.helpers.db_queries import (
    apply_schedule_changes,
    delete_schedules as delete_schedules_db,
    delete_schedules_by_week as delete_schedules_by_week_db,
    duplicate_week_schedules as duplicate_week_schedules_db,
    get_schedules,
    insert_schedules,
    update_schedules as update_schedules_db,
)
from shared.utils import normalize_payload, normalize_payload_list
from apps.shiftzy.api.v1.helpers.schedule_pdf import build_schedule_pdf
from apps.shiftzy.api.v1.helpers.weeks import build_week_info

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


@router.get("/schedules")
def list_schedules(
    schedule_id: str | None = Query(None, alias="id"),
    employee_id: str | None = Query(None),
    date_value: DateType | None = Query(None, alias="date"),
    start_date: DateType | None = Query(None),
    end_date: DateType | None = Query(None),
    start_time: TimeType | None = Query(None),
    end_time: TimeType | None = Query(None),
    week_no: int | None = Query(None),
    include_all: bool = Query(False, alias="all"),
):
    """
    List schedules with optional filters by id, employee, date/date-range, time, or week.

    Example request:
        GET /api/shiftzy/v1/schedules

    Example request (week filter):
        GET /api/shiftzy/v1/schedules?week_no=120

    Example request (date range + employee):
        GET /api/shiftzy/v1/schedules?employee_id=948f09c9-f6b9-11f0-b7f6-5a4783e25118&start_date=2026-03-16&end_date=2026-03-22

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 4},
          "data": [
            {
              "id": "66495150-aada-4a11-bffa-e90e84d90662",
              "employee_id": "948f09c9-f6b9-11f0-b7f6-5a4783e25118",
              "position_code": "FR-CASH",
              "shift_id": "9941aa49-f6b9-11f0-b7f6-5a4783e25118",
              "date": "2026-03-16",
              "start_time": "10:00",
              "end_time": "14:00",
              "note": null,
              "employee_name": "Taylor Reed",
              "schedule_section": "Front",
              "position_name": "Cashier",
              "shift_name": "Morning"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Use week_no OR date/date-range filters, not both
        - Use date OR start_date/end_date, not both
        - Query param `all` is accepted but currently has no effect on this endpoint
    """
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
    return data


@router.get("/schedules/pdf")
def download_schedule_pdf(
    week_no: int = Query(...),
    orientation: str = Query("landscape"),
    include_all: bool = Query(False, alias="all"),
):
    """
    Generate and download a schedule PDF for a given week number.

    Example request:
        GET /api/shiftzy/v1/schedules/pdf?week_no=120

    Example request (portrait):
        GET /api/shiftzy/v1/schedules/pdf?week_no=120&orientation=portrait

    Example response:
        HTTP 200
        Content-Type: application/pdf
        Content-Disposition: attachment; filename="schedule-week-120.pdf"

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - week_no is required
        - Query param `all` is accepted but currently has no effect on this endpoint
    """
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
    payload: list[ScheduleCreate] | ScheduleCreate = Body(...),
):
    """
    Create one or more schedule rows.

    Example request:
        POST /api/shiftzy/v1/schedules
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
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 3},
          "data": {"inserted": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts an object or array
        - employee_id, position_code, date, start_time, and end_time are required
    """
    normalized = normalize_payload_list(payload, name="schedules")
    try:
        inserted = insert_schedules(normalized)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"inserted": inserted}


@router.put("/schedules")
def update_schedules(
    payload: list[ScheduleUpdate] | ScheduleUpdate = Body(...),
):
    """
    Update one or more existing schedule rows.

    Example request:
        PUT /api/shiftzy/v1/schedules
        [
          {
            "id": "66495150-aada-4a11-bffa-e90e84d90662",
            "start_time": "09:00",
            "end_time": "13:30",
            "note": "Cover shift"
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 3},
          "data": {"updated": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts an object or array
        - id is required for each update item
        - At least one updatable field is required per item
    """
    normalized = normalize_payload_list(payload, name="schedules")
    try:
        updated = update_schedules_db(normalized)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"updated": updated}


@router.delete("/schedules")
def delete_schedules(
    payload: list[ScheduleDeleteItem | str] | ScheduleDeleteItem = Body(...),
):
    """
    Delete schedule rows by id.

    Example request:
        DELETE /api/shiftzy/v1/schedules
        ["50e20973-9584-4d2c-a4a7-0977e8e4d80a"]

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 2},
          "data": {"deleted": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts an id string/object or an array of ids/objects
        - id is required for each delete item
    """
    normalized = normalize_payload_list(payload, name="schedules")
    try:
        deleted = delete_schedules_db(normalized)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"deleted": deleted}


@router.delete("/schedules/week")
def delete_schedules_by_week(
    week_no: int = Query(...),
    include_all: bool = Query(False, alias="all"),
):
    """
    Delete all schedules for a specific week number.

    Example request:
        DELETE /api/shiftzy/v1/schedules/week?week_no=12

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 4},
          "data": {"deleted": 7, "week_no": 12}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - week_no is required
        - Query param `all` is accepted but currently has no effect on this endpoint
    """
    try:
        deleted = delete_schedules_by_week_db(week_no)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"deleted": deleted, "week_no": week_no}


@router.post("/schedules/batchUpdates")
def batch_schedules(
    payload: ScheduleBatchRequest = Body(...),
):
    """
    Apply schedule creates, updates, and deletes in a single transaction.

    Example request:
        POST /api/shiftzy/v1/schedules/batchUpdates
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
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 5},
          "data": {"inserted": 0, "updated": 1, "deleted": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - At least one of toCreate, toUpdate, toDelete must be provided
        - The same schedule id cannot appear across multiple operation groups
    """
    normalized = normalize_payload(
        payload,
        require_any=("toCreate", "toUpdate", "toDelete"),
        name="payload",
    )
    try:
        results = apply_schedule_changes(normalized)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return results


@router.post("/schedules/duplicate")
def duplicate_week_schedules(
    week_start: int = Query(...),
    week_end: int = Query(...),
    overwrite: bool = Query(False),
    return_schedules: bool = Query(False),
):
    """
    Copy schedules from one week to another week.

    Example request:
        POST /api/shiftzy/v1/schedules/duplicate?week_start=120&week_end=121

    Example request (overwrite + return copied rows):
        POST /api/shiftzy/v1/schedules/duplicate?week_start=120&week_end=121&overwrite=true&return_schedules=true

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 6},
          "data": {
            "inserted": 7,
            "source_week": 120,
            "target_week": 121,
            "overwrite": false
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - week_start and week_end are required and must be different
        - overwrite=true replaces target-week rows before copy
        - return_schedules=true includes copied target-week schedules in response
    """
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

    return data
