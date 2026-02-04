from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.shiftzy.api.v1.helpers.db_queries import (
    delete_employees,
    get_employees,
    insert_employees,
    update_employees,
)

router = APIRouter()


# ============================================================
# EMPLOYEES
# ============================================================


@router.get("/employees")
def list_employees(
    employee_id: str | None = Query(None, alias="id"),
    include_all: bool = Query(False, alias="all"),
):
    data = get_employees(employee_id, include_all=include_all)
    return data


@router.post("/employees")
def create_employees(
    payload: list[dict] | dict = Body(...),
):
    try:
        inserted = insert_employees(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"inserted": inserted}


@router.put("/employees")
def update_employees_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Example request:
        PUT /api/shiftzy/v1/employees
        [
          {
            "id": "948f09c9-f6b9-11f0-b7f6-5a4783e25118",
            "name": "Taylor Reed",
            "schedule_section": "Front",
            "ref_positionCode": "FR-CASH",
            "note": "Prefers mornings",
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
        updated = update_employees(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"updated": updated}


@router.delete("/employees")
def delete_employees_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Example request:
        DELETE /api/shiftzy/v1/employees
        ["948f09c9-f6b9-11f0-b7f6-5a4783e25118"]

    Example response:
        {
          "meta": {"timestamp": "2026-01-20T10:00:00-05:00", "duration_ms": 2},
          "data": {"deleted": 1}
        }
    """
    try:
        deleted = delete_employees(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"deleted": deleted}
