from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.endpoints.core.traffic import (
    parse_unsigned_int_query,
    require_query_value,
)
from apps.tradsphere.api.v1.helpers.queryParsing import parse_csv_values, parse_int_list
from apps.tradsphere.api.v1.helpers.traffic import (
    ConflictError,
    InvalidReferenceError,
    NotFoundError,
    SafeDatabaseError,
    create_traffic_station_data,
    delete_traffic_station_data,
    list_station_candidates_for_flight_range_data,
    update_traffic_station_data,
)

router = APIRouter(prefix="/traffic")


@router.get("/station-candidates")
def get_traffic_station_candidates_route(
    account_code: str | None = Query(None, alias="accountCode"),
    flight_start: str | None = Query(None, alias="flightStart"),
    flight_end: str | None = Query(None, alias="flightEnd"),
    est_nums: list[str] | None = Query(None, alias="estNums"),
    est_num: list[str] | None = Query(None, alias="estNum"),
    languages: list[str] | None = Query(None, alias="languages"),
    language: list[str] | None = Query(None, alias="language"),
    media_types: list[str] | None = Query(None, alias="mediaTypes"),
    media_type: list[str] | None = Query(None, alias="mediaType"),
):
    """
    Return schedule station candidates for one account within a flight date range.

    Example request:
        GET /api/tradsphere/v1/traffic/station-candidates?accountCode=TAAA&flightStart=2026-06-01&flightEnd=2026-06-30

    Example request (filtered by estimate numbers):
        GET /api/tradsphere/v1/traffic/station-candidates?accountCode=TAAA&flightStart=2026-06-01&flightEnd=2026-06-30&estNums=26001,26002

    Example request (filtered by flight languages):
        GET /api/tradsphere/v1/traffic/station-candidates?accountCode=TAAA&flightStart=2026-06-01&flightEnd=2026-06-30&languages=English,Spanish

    Example request (filtered by media types):
        GET /api/tradsphere/v1/traffic/station-candidates?accountCode=TAAA&flightStart=2026-06-01&flightEnd=2026-06-30&mediaTypes=TV,RA

    Example response:
        {
          "meta": {"timestamp": "2026-05-22T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "accountCode": "TAAA",
            "flightStart": "2026-06-01",
            "flightEnd": "2026-06-30",
            "estNums": [
              {"estNum": 26001, "note": "June TV Push", "medium": "TV", "stationCount": 2}
            ],
            "stations": [
              {
                "stationCode": "KABC",
                "stationName": "ABC Affiliate",
                "deliveryMethod": "Station Portal",
                "contactsSnapshot": {
                  "TRAFFIC": ["traffic@kabc.com"],
                  "REP": [{"id": 88, "name": "Mina Tran", "email": "rep@kabc.com"}]
                }
              }
            ],
            "summary": {
              "candidateCount": 1,
              "estNumCount": 1,
              "months": [
                {"monthKey": "2026-06", "year": 2026, "month": 6, "label": "JUN'26"}
              ],
              "rows": [
                {
                  "estNum": 26001,
                  "stationCode": "KABC",
                  "stationName": "ABC Affiliate",
                  "monthCells": [
                    {
                      "monthKey": "2026-06",
                      "year": 2026,
                      "month": 6,
                      "label": "JUN'26",
                      "hasSchedule": true,
                      "hasSpot": true,
                      "scheduleCount": 2,
                      "totalSpot": 8,
                      "totalGrossText": "$125.00"
                    }
                  ]
                }
              ]
            }
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - accountCode, flightStart, and flightEnd query params are required
        - Optional estNums/estNum accepts comma-separated integer values
        - Optional languages/language accepts comma-separated values: English, Spanish
        - Optional mediaTypes/mediaType accepts comma-separated values: TV, RA, CA, OD, NP, CINE
        - flightStart/flightEnd must be ISO dates and flightStart <= flightEnd
        - summary.months and summary.rows describe the overlapping schedule matrix used by the station-sync modal
        - the matrix groups rows by the broadcast month of the Monday at the start of each schedule week
        - Unknown query params are rejected (400)
    """
    account_code_value = require_query_value(account_code, field="accountCode")
    flight_start_value = require_query_value(flight_start, field="flightStart")
    flight_end_value = require_query_value(flight_end, field="flightEnd")
    normalized_est_nums = parse_int_list(est_nums, est_num)
    normalized_languages = parse_csv_values(languages, language)
    normalized_media_types = parse_csv_values(media_types, media_type, uppercase=True)
    try:
        return list_station_candidates_for_flight_range_data(
            account_code=account_code_value,
            flight_start=flight_start_value,
            flight_end=flight_end_value,
            est_nums=normalized_est_nums,
            languages=normalized_languages,
            media_types=normalized_media_types,
        )
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to load station candidates")


@router.post("/station")
def create_traffic_station_route(
    traffic_id: str | None = Query(None, alias="trafficId"),
    payload: dict = Body(...),
):
    """
    Create one station row under a traffic header.

    Example request:
        POST /api/tradsphere/v1/traffic/station?trafficId=d9c98f56-54da-4688-bc96-3d3cb6388f5d

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": 501,
            "trafficId": "d9c98f56-54da-4688-bc96-3d3cb6388f5d",
            "stationCode": "KABC"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - trafficId query param is required and must be UUID v4
        - stationCode is required and must exist in TradSphere_Stations
        - Archived traffic cannot be modified
    """
    traffic_id_value = require_query_value(traffic_id, field="trafficId")
    try:
        return create_traffic_station_data(traffic_id=traffic_id_value, payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to create station")


@router.put("/station")
def update_traffic_station_route(
    traffic_id: str | None = Query(None, alias="trafficId"),
    station_id: str | None = Query(None, alias="stationId"),
    payload: dict = Body(...),
):
    """
    Update one station row under a traffic header.

    Example request:
        PUT /api/tradsphere/v1/traffic/station?trafficId=d9c98f56-54da-4688-bc96-3d3cb6388f5d&stationId=501

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": 501,
            "deliveryStatus": "ready_to_email",
            "confirmedStatus": "pending"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - trafficId and stationId query params are required
        - stationId must be unsigned integer
        - Archived traffic cannot be modified
    """
    traffic_id_value = require_query_value(traffic_id, field="trafficId")
    station_id_value = parse_unsigned_int_query(station_id, field="stationId")
    try:
        return update_traffic_station_data(
            traffic_id=traffic_id_value,
            station_id=station_id_value,
            payload=payload,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to update station")


@router.delete("/station")
def delete_traffic_station_route(
    traffic_id: str | None = Query(None, alias="trafficId"),
    station_id: str | None = Query(None, alias="stationId"),
):
    """
    Delete one station row under a traffic header.

    Example request:
        DELETE /api/tradsphere/v1/traffic/station?trafficId=d9c98f56-54da-4688-bc96-3d3cb6388f5d&stationId=501

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 1},
          "data": {"deleted": 1, "id": 501}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - trafficId and stationId query params are required
        - stationId must be unsigned integer
        - Archived traffic cannot be modified
    """
    traffic_id_value = require_query_value(traffic_id, field="trafficId")
    station_id_value = parse_unsigned_int_query(station_id, field="stationId")
    try:
        return delete_traffic_station_data(
            traffic_id=traffic_id_value,
            station_id=station_id_value,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to delete station")
