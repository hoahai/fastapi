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
    data = get_shifts(shift_id, include_all=include_all)
    return data


@router.post("/shifts")
def create_shifts(
    payload: list[dict] | dict = Body(...),
):
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
    Example request:
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
      "meta": {"timestamp": "2026-01-20T10:00:00-05:00", "duration_ms": 2},
      "data": {"updated": 1}
    }
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
    Example request:
    [3, 4]

    Example response:
    {
      "meta": {"timestamp": "2026-01-20T10:00:00-05:00", "duration_ms": 2},
      "data": {"deleted": 2}
    }
    """
    try:
        deleted = delete_shifts(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"deleted": deleted}
