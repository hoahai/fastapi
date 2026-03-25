from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.shiftzy.api.v1.helpers.dbQueries import (
    delete_positions,
    get_positions,
    insert_positions,
    update_positions,
)

router = APIRouter()


# ============================================================
# POSITIONS
# ============================================================


@router.get("/positions")
def list_positions(
    position_id: str | None = Query(None, alias="id"),
    code: str | None = Query(None),
    include_all: bool = Query(False, alias="all"),
):
    """
    List positions, optionally filtered by position code.

    Example request:
        GET /api/shiftzy/v1/positions

    Example request (include inactive):
        GET /api/shiftzy/v1/positions?all=true

    Example request (specific id):
        GET /api/shiftzy/v1/positions?id=FR-CASH&all=true

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 2},
          "data": [
            {
              "code": "FR-CASH",
              "name": "Cashier",
              "icon": "cashier.svg",
              "active": 1
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
    code_value = position_id or code
    data = get_positions(code_value, include_all=include_all)
    return data


@router.post("/positions")
def create_positions(
    payload: list[dict] | dict = Body(...),
):
    """
    Create one or more positions.

    Example request:
        POST /api/shiftzy/v1/positions
        [
          {
            "code": "FR-HOST",
            "name": "Host",
            "icon": "host.svg",
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
        - code and name are required
    """
    try:
        inserted = insert_positions(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"inserted": inserted}


@router.put("/positions")
def update_positions_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Update one or more existing positions.

    Example request:
        PUT /api/shiftzy/v1/positions
        [
          {
            "code": "FR-CASH",
            "name": "Cashier",
            "icon": "cashier.svg",
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
        - code is required for each update item
        - At least one updatable field is required per item
    """
    try:
        updated = update_positions(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"updated": updated}


@router.delete("/positions")
def delete_positions_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Soft-delete positions by setting active = 0.

    Example request:
        DELETE /api/shiftzy/v1/positions
        ["FR-CASH", "FR-HOST"]

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 2},
          "data": {"deleted": 2}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts list of codes or list[{"code": "..."}]
        - Empty/blank codes are rejected
    """
    try:
        deleted = delete_positions(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"deleted": deleted}
