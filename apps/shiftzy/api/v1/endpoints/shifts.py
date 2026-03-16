from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.shiftzy.api.v1.helpers.db_queries import (
    delete_shifts,
    get_shifts,
    insert_shifts,
    update_shifts,
)

router = APIRouter()


# ============================================================
# SHIFTS
# ============================================================


@router.get("/shifts")
def list_shifts(
    shift_id: int | None = Query(None, alias="id"),
    include_all: bool = Query(False, alias="all"),
):
    """
    List shifts, optionally filtered by shift id.

    Example request:
        GET /api/shiftzy/v1/shifts

    Example request (include inactive):
        GET /api/shiftzy/v1/shifts?all=true

    Example request (specific id):
        GET /api/shiftzy/v1/shifts?id=3&all=true

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 2},
          "data": [
            {
              "id": 3,
              "name": "Morning",
              "start_time": "08:00",
              "end_time": "12:00",
              "active": 1,
              "duration": "04:00"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Query param `all` maps to `include_all`
        - all=false (default) returns active rows only
        - all=true includes inactive rows
    """
    data = get_shifts(shift_id, include_all=include_all)
    return data


@router.post("/shifts")
def create_shifts(
    payload: list[dict] | dict = Body(...),
):
    """
    Create one or more shifts.

    Example request:
        POST /api/shiftzy/v1/shifts
        [
          {
            "name": "Morning",
            "start_time": "08:00",
            "end_time": "12:00",
            "active": 1
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
        - Payload must be a dict or list[dict]
        - name, start_time, and end_time are required
    """
    try:
        inserted = insert_shifts(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"inserted": inserted}


@router.put("/shifts")
def update_shifts_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Update one or more existing shifts.

    Example request:
        PUT /api/shiftzy/v1/shifts
        [
          {
            "id": 3,
            "name": "Morning",
            "start_time": "08:00",
            "end_time": "12:00",
            "active": 1
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 2},
          "data": {"updated": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload must be a dict or list[dict]
        - id is required for each update item
        - At least one updatable field is required per item
    """
    try:
        updated = update_shifts(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"updated": updated}


@router.delete("/shifts")
def delete_shifts_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Soft-delete shifts by setting active = 0.

    Example request:
        DELETE /api/shiftzy/v1/shifts
        [3, 4]

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 2},
          "data": {"deleted": 2}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts list of ids or list[{"id": ...}]
        - Empty ids are rejected
    """
    try:
        deleted = delete_shifts(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"deleted": deleted}
