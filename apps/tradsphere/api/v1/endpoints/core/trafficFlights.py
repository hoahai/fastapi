from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.endpoints.core.traffic import (
    parse_unsigned_int_query,
    require_query_value,
)
from apps.tradsphere.api.v1.helpers.traffic import (
    ConflictError,
    InvalidReferenceError,
    NotFoundError,
    SafeDatabaseError,
    create_traffic_flight_data,
    delete_traffic_flight_data,
    update_traffic_flight_data,
)

router = APIRouter(prefix="/traffic")


@router.post("/flight")
def create_traffic_flight_route(
    traffic_id: str | None = Query(None, alias="trafficId"),
    payload: dict = Body(...),
):
    """
    Create one flight row under a traffic header.

    Example request:
        POST /api/tradsphere/v1/traffic/flight?trafficId=d9c98f56-54da-4688-bc96-3d3cb6388f5d

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": 101,
            "trafficId": "d9c98f56-54da-4688-bc96-3d3cb6388f5d",
            "rotation": 50.0
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - trafficId query param is required and must be UUID v4
        - flightStart/flightEnd are required ISO dates and flightStart <= flightEnd
        - medium supports: TV, RA, CA, OD, NP, CINE
        - language supports: English, Spanish (defaults to English)
        - rotation must be between 0 and 100
        - fileUrl is optional
        - Archived traffic cannot be modified
    """
    traffic_id_value = require_query_value(traffic_id, field="trafficId")
    try:
        return create_traffic_flight_data(traffic_id=traffic_id_value, payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to create flight")


@router.put("/flight")
def update_traffic_flight_route(
    traffic_id: str | None = Query(None, alias="trafficId"),
    flight_id: str | None = Query(None, alias="flightId"),
    payload: dict = Body(...),
):
    """
    Update one flight row under a traffic header.

    Example request:
        PUT /api/tradsphere/v1/traffic/flight?trafficId=d9c98f56-54da-4688-bc96-3d3cb6388f5d&flightId=101

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": 101,
            "rotation": 55.0,
            "note": "Updated split"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - trafficId and flightId query params are required
        - flightId must be unsigned integer
        - Archived traffic cannot be modified
    """
    traffic_id_value = require_query_value(traffic_id, field="trafficId")
    flight_id_value = parse_unsigned_int_query(flight_id, field="flightId")
    try:
        return update_traffic_flight_data(
            traffic_id=traffic_id_value,
            flight_id=flight_id_value,
            payload=payload,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to update flight")


@router.delete("/flight")
def delete_traffic_flight_route(
    traffic_id: str | None = Query(None, alias="trafficId"),
    flight_id: str | None = Query(None, alias="flightId"),
):
    """
    Delete one flight row under a traffic header.

    Example request:
        DELETE /api/tradsphere/v1/traffic/flight?trafficId=d9c98f56-54da-4688-bc96-3d3cb6388f5d&flightId=101

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 1},
          "data": {"deleted": 1, "id": 101}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - trafficId and flightId query params are required
        - flightId must be unsigned integer
        - Archived traffic cannot be modified
    """
    traffic_id_value = require_query_value(traffic_id, field="trafficId")
    flight_id_value = parse_unsigned_int_query(flight_id, field="flightId")
    try:
        return delete_traffic_flight_data(
            traffic_id=traffic_id_value,
            flight_id=flight_id_value,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to delete flight")
