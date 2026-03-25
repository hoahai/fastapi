from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel

from apps.shiftzy.api.v1.helpers.dbQueries import (
    apply_employee_changes,
    delete_employees,
    get_employees,
    insert_employees,
    update_employees,
)
from shared.utils import normalize_payload

router = APIRouter()


# ============================================================
# EMPLOYEES
# ============================================================

try:
    from pydantic import ConfigDict

    class _ShiftzyModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

except ImportError:

    class _ShiftzyModel(BaseModel):
        class Config:
            extra = "ignore"


class EmployeeCreate(_ShiftzyModel):
    id: str | None = None
    name: str
    schedule_section: str
    note: str | None = None
    ref_positionCode: str | None = None
    active: bool | int | str | None = None


class EmployeeUpdate(_ShiftzyModel):
    id: str
    name: str | None = None
    schedule_section: str | None = None
    note: str | None = None
    ref_positionCode: str | None = None
    active: bool | int | str | None = None


class EmployeeDeleteItem(_ShiftzyModel):
    id: str


class EmployeeBatchRequest(_ShiftzyModel):
    toCreate: list[EmployeeCreate] | EmployeeCreate | None = None
    toUpdate: list[EmployeeUpdate] | EmployeeUpdate | None = None
    toDelete: list[EmployeeDeleteItem | str] | EmployeeDeleteItem | None = None


@router.get("/employees")
def list_employees(
    employee_id: str | None = Query(None, alias="id"),
    include_all: bool = Query(False, alias="all"),
):
    """
    List employees, optionally filtered by employee id.

    Example request:
        GET /api/shiftzy/v1/employees

    Example request (include inactive):
        GET /api/shiftzy/v1/employees?all=true

    Example request (specific id):
        GET /api/shiftzy/v1/employees?id=948f09c9-f6b9-11f0-b7f6-5a4783e25118&all=true

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 2},
          "data": [
            {
              "id": "948f09c9-f6b9-11f0-b7f6-5a4783e25118",
              "name": "Taylor Reed",
              "schedule_section": "Front",
              "note": "Prefers mornings",
              "ref_positionCode": "FR-CASH",
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
    data = get_employees(employee_id, include_all=include_all)
    return data


@router.post("/employees")
def create_employees(
    payload: list[dict] | dict = Body(...),
):
    """
    Create one or more employees.

    Example request:
        POST /api/shiftzy/v1/employees
        [
          {
            "name": "Taylor Reed",
            "schedule_section": "Front",
            "ref_positionCode": "FR-CASH",
            "note": "Prefers mornings",
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
        - name and schedule_section are required
    """
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
    Update one or more existing employees.

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
        updated = update_employees(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"updated": updated}


@router.delete("/employees")
def delete_employees_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Soft-delete employees by setting active = 0.

    Example request:
        DELETE /api/shiftzy/v1/employees
        ["948f09c9-f6b9-11f0-b7f6-5a4783e25118"]

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 2},
          "data": {"deleted": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload accepts list of ids or list[{"id": "..."}]
        - Empty/blank ids are rejected
    """
    try:
        deleted = delete_employees(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"deleted": deleted}


@router.post("/employees/batchUpdates")
def batch_employees(
    payload: EmployeeBatchRequest = Body(...),
):
    """
    Apply employee creates, updates, and deletes in a single transaction.

    Example request:
        POST /api/shiftzy/v1/employees/batchUpdates
        {
          "toCreate": [
            {
              "name": "Alex Vo",
              "schedule_section": "Front",
              "ref_positionCode": "FR-CASH",
              "note": null,
              "active": 1
            }
          ],
          "toUpdate": [
            {
              "id": "948f09c9-f6b9-11f0-b7f6-5a4783e25118",
              "name": "Taylor Reed",
              "schedule_section": "Front",
              "note": "Prefers mornings"
            }
          ],
          "toDelete": ["5fc17dda-f6b9-11f0-b7f6-5a4783e25118"]
        }

    Example response:
        {
          "meta": {"timestamp": "2026-03-16T10:00:00-05:00", "duration_ms": 5},
          "data": {"inserted": 1, "updated": 1, "deleted": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - At least one of toCreate, toUpdate, toDelete must be provided
        - The same employee id cannot appear across multiple operation groups
    """
    normalized = normalize_payload(
        payload,
        require_any=("toCreate", "toUpdate", "toDelete"),
        name="payload",
    )
    try:
        results = apply_employee_changes(normalized)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return results
