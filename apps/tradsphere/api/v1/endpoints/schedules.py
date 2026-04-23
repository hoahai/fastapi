from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.queryParsing import (
    parse_csv_values,
    parse_int_list,
    parse_optional_date,
)
from apps.tradsphere.api.v1.helpers.schedules import (
    create_schedules_data,
    list_schedules_data,
    modify_schedules_data,
)

router = APIRouter(prefix="/schedules")


@router.get("")
def get_schedules_route(
    ids: list[str] | None = Query(None, alias="ids"),
    row_id: list[str] | None = Query(None, alias="id"),
    schedule_ids: list[str] | None = Query(None, alias="scheduleIds"),
    schedule_id: list[str] | None = Query(None, alias="scheduleId"),
    est_nums: list[str] | None = Query(None, alias="estNums"),
    est_num: list[str] | None = Query(None, alias="estNum"),
    billing_codes: list[str] | None = Query(None, alias="billingCodes"),
    billing_code: list[str] | None = Query(None, alias="billingCode"),
    media_types: list[str] | None = Query(None, alias="mediaTypes"),
    media_type: list[str] | None = Query(None, alias="mediaType"),
    station_codes: list[str] | None = Query(None, alias="stationCodes"),
    station_code: list[str] | None = Query(None, alias="stationCode"),
    broadcast_month: int | None = Query(None, alias="broadcastMonth"),
    broadcast_year: int | None = Query(None, alias="broadcastYear"),
    start_date_from: str | None = Query(None, alias="startDateFrom"),
    start_date_to: str | None = Query(None, alias="startDateTo"),
    end_date_from: str | None = Query(None, alias="endDateFrom"),
    end_date_to: str | None = Query(None, alias="endDateTo"),
):
    """
    Return schedules rows with optional ids/period/station/media filters.

    Example request:
        GET /api/tradsphere/v1/schedules

    Example request (filtered):
        GET /api/tradsphere/v1/schedules?estNums=1001&stationCodes=KABC&broadcastMonth=4&broadcastYear=2026

    Example request (date range):
        GET /api/tradsphere/v1/schedules?startDateFrom=2026-04-01&startDateTo=2026-04-30

    Example response:
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 4},
          "data": [
            {
              "id": "6a2f88e6-bad0-4ac8-b321-a70f64ca693d",
              "scheduleId": "SCH-001",
              "lineNum": 1,
              "estNum": 1001,
              "billingCode": "NAT",
              "mediaType": "TV",
              "stationCode": "KABC",
              "broadcastMonth": 4,
              "broadcastYear": 2026,
              "startDate": "2026-04-07",
              "endDate": "2026-04-13",
              "totalSpot": 12,
              "totalGross": "12000.000",
              "rateGross": "1000.000",
              "length": 30,
              "runtime": "00:30",
              "programName": "Morning News",
              "days": "MTWTF",
              "daypart": "AM",
              "rtg": "1.25",
              "matchKey": "d9f6e0..."
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - ids/id, estNums/estNum accept comma-separated values
        - scheduleIds/scheduleId filters schedule business ids (varchar(20))
        - broadcastMonth must be 1-12 when provided
        - broadcastYear must be 1901-2155 when provided
        - startDateFrom/startDateTo/endDateFrom/endDateTo must be ISO date YYYY-MM-DD when provided
        - startDateFrom must be on or before startDateTo when both are provided
        - endDateFrom must be on or before endDateTo when both are provided
        - Blank list filters are treated as no filter
    """
    normalized_ids = parse_csv_values(ids, row_id)
    normalized_schedule_ids = parse_csv_values(schedule_ids, schedule_id)
    normalized_est_nums = parse_int_list(est_nums, est_num)
    normalized_billing_codes = parse_csv_values(billing_codes, billing_code)
    normalized_media_types = parse_csv_values(media_types, media_type, uppercase=True)
    normalized_station_codes = parse_csv_values(station_codes, station_code, uppercase=True)
    parsed_start_date_from = parse_optional_date(start_date_from, field="startDateFrom")
    parsed_start_date_to = parse_optional_date(start_date_to, field="startDateTo")
    parsed_end_date_from = parse_optional_date(end_date_from, field="endDateFrom")
    parsed_end_date_to = parse_optional_date(end_date_to, field="endDateTo")
    try:
        return list_schedules_data(
            ids=normalized_ids,
            schedule_ids=normalized_schedule_ids,
            est_nums=normalized_est_nums,
            billing_codes=normalized_billing_codes,
            media_types=normalized_media_types,
            station_codes=normalized_station_codes,
            broadcast_month=broadcast_month,
            broadcast_year=broadcast_year,
            start_date_from=parsed_start_date_from.isoformat()
            if parsed_start_date_from
            else None,
            start_date_to=parsed_start_date_to.isoformat() if parsed_start_date_to else None,
            end_date_from=parsed_end_date_from.isoformat() if parsed_end_date_from else None,
            end_date_to=parsed_end_date_to.isoformat() if parsed_end_date_to else None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise


@router.post("")
def create_schedules_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Create one or many schedules rows.

    Example request:
        POST /api/tradsphere/v1/schedules
        [
          {
            "id": "6a2f88e6-bad0-4ac8-b321-a70f64ca693d",
            "scheduleId": "SCH-001",
            "lineNum": 1,
            "estNum": 1001,
            "billingCode": "NAT",
            "mediaType": "TV",
            "stationCode": "KABC",
            "broadcastMonth": 4,
            "broadcastYear": 2026,
            "startDate": "2026-04-07",
            "endDate": "2026-04-13",
            "totalSpot": 12,
            "totalGross": 12000,
            "rateGross": 1000,
            "length": 30,
            "runtime": "00:30",
            "programName": "Morning News",
            "days": "MTWTF",
            "daypart": "AM",
            "rtg": 1.25,
            "w1": 3,
            "w2": 4,
            "w3": 2,
            "w4": 3
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 3},
          "data": {"inserted": 1, "scheduleWeeksUpserted": 4}
        }

    Example error response (week validation):
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 1},
          "error": {
            "message": "Bad Request",
            "detail": "item 1: w fields must be consecutive and complete for this date range: requires 4 week field(s): w1, w2, w3, w4 (missing: w2)"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts object or array of objects
        - scheduleId, lineNum, estNum, billingCode, mediaType, stationCode, broadcastMonth, broadcastYear, startDate, endDate, length, runtime, days, daypart are required
        - id is optional; if omitted, UUID is auto-generated
        - w1..w5 are optional; when provided they are upserted into /schedules/weeks using startDate-based weekly buckets
        - If any w field is provided, w fields must be complete and consecutive for the date range (for example: 4 broadcast weeks requires w1,w2,w3,w4)
        - Validation errors include item context (item N: <detail>)
        - estNum and stationCode must exist
        - matchKey is auto-generated as SHA-256(scheduleId|lineNum|estNum|startDate|endDate)
        - Duplicate matchKey rows within the same payload are deduped with last row values and a single canonical schedule id
    """
    try:
        return create_schedules_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("")
def update_schedules_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Update schedules rows by id.

    Example request:
        PUT /api/tradsphere/v1/schedules
        [
          {
            "id": "6a2f88e6-bad0-4ac8-b321-a70f64ca693d",
            "stationCode": "WXYZ",
            "totalSpot": 14,
            "rateGross": 1100
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 3},
          "data": {"updated": 1}
        }

    Example error response (immutable matchKey fields):
        {
          "meta": {"timestamp": "2026-04-23T10:00:00+07:00", "duration_ms": 1},
          "error": {
            "message": "Bad Request",
            "detail": "item 1: Cannot update matchKey source fields on schedules PUT: startDate"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - id is required per item
        - stationCode updates are validated when provided
        - scheduleId, lineNum, estNum, startDate, endDate are immutable in PUT
        - matchKey is system-generated and cannot be updated
        - Validation errors include item context (item N: <detail>)
    """
    try:
        return modify_schedules_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
