from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.shiftzy.api.v1.helpers.db_queries import (
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
    code_value = position_id or code
    data = get_positions(code_value, include_all=include_all)
    return data


@router.post("/positions")
def create_positions(
    payload: list[dict] | dict = Body(...),
):
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
    Example request:
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
      "meta": {"timestamp": "2026-01-20T10:00:00-05:00", "duration_ms": 2},
      "data": {"updated": 1}
    }
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
    Example request:
    ["FR-CASH", "FR-HOST"]

    Example response:
    {
      "meta": {"timestamp": "2026-01-20T10:00:00-05:00", "duration_ms": 2},
      "data": {"deleted": 2}
    }
    """
    try:
        deleted = delete_positions(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"deleted": deleted}
