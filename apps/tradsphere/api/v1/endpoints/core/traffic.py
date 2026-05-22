from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.traffic import (
    ConflictError,
    InvalidReferenceError,
    NotFoundError,
    SafeDatabaseError,
    archive_traffic_data,
    create_traffic_data,
    get_traffic_detail_data,
    list_traffic_for_account_data,
    mark_traffic_ready_data,
    update_traffic_data,
)

router = APIRouter(prefix="/traffic")


def require_query_value(value: str | None, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail=f"{field} is required")
    return text


def parse_unsigned_int_query(value: str | None, *, field: str) -> int:
    text = require_query_value(value, field=field)
    try:
        parsed = int(text)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{field} must be an unsigned integer",
        ) from exc
    if parsed < 0:
        raise HTTPException(
            status_code=400,
            detail=f"{field} must be an unsigned integer",
        )
    return parsed


@router.get("/account")
def get_account_traffic_route(
    code: str | None = Query(None, alias="code"),
):
    """
    Return a lightweight account-centered list of traffic records for one account.

    Example request:
        GET /api/tradsphere/v1/traffic/account?code=TAAA

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 3},
          "data": [
            {
              "id": "d9c98f56-54da-4688-bc96-3d3cb6388f5d",
              "accountCode": "TAAA",
              "campaign": "Spring Retail Push",
              "status": "draft",
              "flightCount": 2,
              "stationCount": 6,
              "summary": {
                "totalRotation": 92.5,
                "rotationWarning": true,
                "rotationWarningMessage": "Total rotation is 92.50%; expected 100.00%"
              }
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - code query param is required
        - Archived records are excluded by default on this account load route
        - Unknown query params are rejected (400)
    """
    code_value = require_query_value(code, field="code")
    try:
        return list_traffic_for_account_data(account_code=code_value)
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to load account traffic")


@router.get("")
def get_traffic_detail_route(
    traffic_id: str | None = Query(None, alias="id"),
):
    """
    Return one full traffic detail payload including header, flights, stations, email, and summary warnings.

    Example request:
        GET /api/tradsphere/v1/traffic?id=d9c98f56-54da-4688-bc96-3d3cb6388f5d

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 5},
          "data": {
            "traffic": {
              "id": "d9c98f56-54da-4688-bc96-3d3cb6388f5d",
              "accountCode": "TAAA",
              "campaign": "Spring Retail Push",
              "status": "draft"
            },
            "flights": [],
            "stations": [],
            "email": null,
            "summary": {
              "totalRotation": 0.0,
              "rotationWarning": true,
              "rotationWarningMessage": "Total rotation is 0.00%; expected 100.00%"
            }
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - id query param is required and must be UUID v4
        - Unknown id returns 404
        - Unknown query params are rejected (400)
    """
    traffic_id_value = require_query_value(traffic_id, field="id")
    try:
        return get_traffic_detail_data(traffic_id=traffic_id_value)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to load traffic detail")


@router.post("")
def create_traffic_route(
    payload: dict = Body(...),
):
    """
    Create one traffic header row for an account and generate UUID v4 id.

    Example request:
        POST /api/tradsphere/v1/traffic
        {
          "accountCode": "TAAA",
          "campaign": "Spring Retail Push",
          "status": "draft",
          "note": "First wave"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": "d9c98f56-54da-4688-bc96-3d3cb6388f5d",
            "accountCode": "TAAA",
            "campaign": "Spring Retail Push",
            "status": "draft"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - accountCode is required and must exist in TradSphere accounts
        - campaign is required, max length 255
        - status supports: draft, ready, sent, confirmed, archived
        - note max length is 2048
    """
    try:
        return create_traffic_data(payload=payload)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to create traffic")


@router.put("")
def update_traffic_route(
    traffic_id: str | None = Query(None, alias="id"),
    payload: dict = Body(...),
):
    """
    Update one traffic header row (campaign/status/note).

    Example request:
        PUT /api/tradsphere/v1/traffic?id=d9c98f56-54da-4688-bc96-3d3cb6388f5d
        {
          "campaign": "Spring Retail Push - Rev A",
          "status": "draft",
          "note": "Updated after client review"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": "d9c98f56-54da-4688-bc96-3d3cb6388f5d",
            "campaign": "Spring Retail Push - Rev A",
            "status": "draft"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - id query param is required and must be UUID v4
        - At least one field is required: campaign/status/note
        - Archived traffic cannot be modified
    """
    traffic_id_value = require_query_value(traffic_id, field="id")
    try:
        return update_traffic_data(traffic_id=traffic_id_value, payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to update traffic")


@router.post("/ready")
def mark_traffic_ready_route(
    traffic_id: str | None = Query(None, alias="id"),
):
    """
    Mark a traffic header as ready and return rotation warning metadata if total rotation is not 100.00.

    Example request:
        POST /api/tradsphere/v1/traffic/ready?id=d9c98f56-54da-4688-bc96-3d3cb6388f5d

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": "d9c98f56-54da-4688-bc96-3d3cb6388f5d",
            "status": "ready",
            "summary": {
              "totalRotation": 92.5,
              "rotationWarning": true
            }
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - id query param is required and must be UUID v4
        - Rotation warning does not block ready transition
    """
    traffic_id_value = require_query_value(traffic_id, field="id")
    try:
        return mark_traffic_ready_data(traffic_id=traffic_id_value)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to set traffic ready")


@router.post("/archive")
def archive_traffic_route(
    traffic_id: str | None = Query(None, alias="id"),
):
    """
    Archive one traffic header by setting status to archived.

    Example request:
        POST /api/tradsphere/v1/traffic/archive?id=d9c98f56-54da-4688-bc96-3d3cb6388f5d

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": "d9c98f56-54da-4688-bc96-3d3cb6388f5d",
            "status": "archived"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - id query param is required and must be UUID v4
    """
    traffic_id_value = require_query_value(traffic_id, field="id")
    try:
        return archive_traffic_data(traffic_id=traffic_id_value)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to archive traffic")
