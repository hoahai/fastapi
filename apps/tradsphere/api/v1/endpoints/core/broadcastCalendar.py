from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query

from apps.tradsphere.api.v1.helpers.broadcastCalendar import (
    get_broadcast_calendar_info,
    get_broadcast_calendar_value,
)
from apps.tradsphere.api.v1.helpers.queryParsing import parse_optional_date
from shared.tenant import get_timezone

router = APIRouter(prefix="/broadcastCalendar")


def _serialize_value(value: object) -> object:
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            return value
    return value


@router.get("")
def get_broadcast_calendar_route(
    given_date: str | None = Query(None, alias="givenDate"),
    result_type: str | None = Query(None, alias="resultType"),
):
    """
    Compute broadcast-calendar fields for a given date or today.

    Example request:
        GET /api/tradsphere/v1/broadcastCalendar

    Example request (full object for specific date):
        GET /api/tradsphere/v1/broadcastCalendar?givenDate=2026-04-22

    Example request (single mapped value):
        GET /api/tradsphere/v1/broadcastCalendar?givenDate=2026-04-22&resultType=week_num_of_month

    Example response (full):
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 1},
          "data": {
            "broadcastMonth": 4,
            "broadcastYear": 2026,
            "beginBroadcastMonth": "2026-03-30",
            "endBroadcastMonth": "2026-05-03",
            "numberOfBroadcastWeek": 5,
            "firstDayOfWeek": "2026-04-20",
            "lastDateOfWeek": "2026-04-26",
            "weekNumofMonth": 4,
            "weekNumofYear": 17
          }
        }

    Example response (single value):
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 1},
          "data": {
            "resultType": "week_num_of_month",
            "value": 4
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - givenDate is optional and must be ISO YYYY-MM-DD when provided
        - resultType is optional and supports:
          month, year, start_date, end_date, num_of_week, firstdate_of_week,
          lastdate_of_week, week_num_of_month, week_num_of_year
    """
    try:
        resolved_date = parse_optional_date(given_date, field="givenDate")
        if resolved_date is None:
            tz = ZoneInfo(get_timezone())
            resolved_date = datetime.now(tz).date()

        if result_type is None or not str(result_type).strip():
            info = get_broadcast_calendar_info(resolved_date)
            return {key: _serialize_value(value) for key, value in info.items()}

        value = get_broadcast_calendar_value(resolved_date, result_type)
        return {
            "resultType": str(result_type).strip().lower(),
            "value": _serialize_value(value),
        }
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
