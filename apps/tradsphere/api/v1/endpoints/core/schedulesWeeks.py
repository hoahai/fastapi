from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.queryParsing import (
    parse_csv_values,
    parse_int_list,
    parse_optional_date,
)
from apps.tradsphere.api.v1.helpers.schedules import (
    create_schedule_weeks_data,
    list_schedule_weeks_data,
    modify_schedule_weeks_data,
)

router = APIRouter(prefix="/schedules/weeks")


@router.get("")
def get_schedule_weeks_route(
    ids: list[str] | None = Query(None, alias="ids"),
    row_id: list[str] | None = Query(None, alias="id"),
    schedule_ids: list[str] | None = Query(None, alias="scheduleIds"),
    schedule_id: list[str] | None = Query(None, alias="scheduleId"),
    week_start_from: str | None = Query(None, alias="weekStartFrom"),
    week_start_to: str | None = Query(None, alias="weekStartTo"),
    week_end_from: str | None = Query(None, alias="weekEndFrom"),
    week_end_to: str | None = Query(None, alias="weekEndTo"),
):
    """
    Return schedule-weeks rows with optional week-range filters.

    Example request:
        GET /api/tradsphere/v1/schedules/weeks

    Example request (filtered):
        GET /api/tradsphere/v1/schedules/weeks?scheduleIds=6a2f88e6-bad0-4ac8-b321-a70f64ca693d&weekStartFrom=2026-04-01

    Example response:
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 3},
          "data": [
            {
              "id": 101,
              "scheduleId": "6a2f88e6-bad0-4ac8-b321-a70f64ca693d",
              "weekStart": "2026-04-07",
              "weekEnd": "2026-04-13",
              "spots": 4
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - ids/id accepts comma-separated integers
        - weekStartFrom/weekStartTo/weekEndFrom/weekEndTo must be ISO date YYYY-MM-DD when provided
        - weekStartFrom must be on or before weekStartTo when both are provided
        - weekEndFrom must be on or before weekEndTo when both are provided
    """
    normalized_ids = parse_int_list(ids, row_id)
    normalized_schedule_ids = parse_csv_values(schedule_ids, schedule_id)
    parsed_week_start_from = parse_optional_date(week_start_from, field="weekStartFrom")
    parsed_week_start_to = parse_optional_date(week_start_to, field="weekStartTo")
    parsed_week_end_from = parse_optional_date(week_end_from, field="weekEndFrom")
    parsed_week_end_to = parse_optional_date(week_end_to, field="weekEndTo")
    try:
        return list_schedule_weeks_data(
            ids=normalized_ids,
            schedule_ids=normalized_schedule_ids,
            week_start_from=parsed_week_start_from.isoformat()
            if parsed_week_start_from
            else None,
            week_start_to=parsed_week_start_to.isoformat() if parsed_week_start_to else None,
            week_end_from=parsed_week_end_from.isoformat() if parsed_week_end_from else None,
            week_end_to=parsed_week_end_to.isoformat() if parsed_week_end_to else None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise


@router.post("")
def create_schedule_weeks_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Create one or many schedule-weeks rows.

    Example request:
        POST /api/tradsphere/v1/schedules/weeks
        [
          {
            "scheduleId": "6a2f88e6-bad0-4ac8-b321-a70f64ca693d",
            "weekStart": "2026-04-07",
            "weekEnd": "2026-04-13",
            "spots": 4
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 2},
          "data": {"inserted": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - scheduleId, weekStart, weekEnd are required
        - scheduleId must exist
        - weekStart must be on or before weekEnd
    """
    try:
        return create_schedule_weeks_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("")
def update_schedule_weeks_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Update schedule-weeks rows by id.

    Example request:
        PUT /api/tradsphere/v1/schedules/weeks
        [
          {
            "id": 101,
            "spots": 5
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 2},
          "data": {"updated": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - id is required per item
        - scheduleId update is validated when provided
        - weekStart must be on or before weekEnd after update
    """
    try:
        return modify_schedule_weeks_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
