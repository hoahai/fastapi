from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from pydantic import BaseModel

from apps.leavesphere.api.v1.helpers.employees import (
    activate_employee,
    create_employee,
    deactivate_employee,
    get_employee,
    list_employees,
    modify_employee,
)
from apps.leavesphere.api.v1.helpers.workspaceCache import invalidate_leave_sphere_workspace_caches
from apps.leavesphere.api.v1.permissions import require_leavesphere_admin

router = APIRouter(prefix="/employees")


try:
    from pydantic import ConfigDict

    class _LeaveSphereModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

except ImportError:

    class _LeaveSphereModel(BaseModel):
        class Config:
            extra = "ignore"


class EmployeeCreateRequest(_LeaveSphereModel):
    id: str | None = None
    identityKey: str | None = None
    firstName: str
    lastName: str
    email: str
    phone: str | None = None
    dob: str | None = None
    pictureUrl: str | None = None
    region: str | None = None
    startDate: str | None = None
    title: str | None = None
    isAE: bool | int | str | None = None
    active: bool | int | str | None = None


class EmployeeUpdateRequest(_LeaveSphereModel):
    identityKey: str | None = None
    firstName: str | None = None
    lastName: str | None = None
    email: str | None = None
    phone: str | None = None
    dob: str | None = None
    pictureUrl: str | None = None
    region: str | None = None
    startDate: str | None = None
    title: str | None = None
    isAE: bool | int | str | None = None
    active: bool | int | str | None = None


@router.get("")
def get_employees_route():
    """
    List LeaveSphere employees.

    Example request:
        GET /api/leavesphere/v1/employees

    Example response:
        {
          "meta": {"timestamp": "2026-05-28T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
              "firstName": "Alex",
              "lastName": "Chen",
              "email": "alex@example.com",
              "active": 1
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
    """
    return list_employees()


@router.get("/{employee_id}")
def get_employee_route(employee_id: str):
    """
    Fetch one employee by id.

    Example request:
        GET /api/leavesphere/v1/employees/13f6b22f-0a86-43b8-946d-cbba67642e8b

    Example response:
        {
          "meta": {"timestamp": "2026-05-28T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
            "identityKey": "218f1519-f95d-4d88-ac4c-df6a8cbf45c1",
            "firstName": "Alex",
            "lastName": "Chen",
            "email": "alex@example.com"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - Returns 404 when employee is not found
    """
    employee = get_employee(employee_id)
    if employee is None:
        raise HTTPException(status_code=404, detail="Employee not found")
    return employee


@router.post("")
def create_employee_route(
    request: Request,
    payload: EmployeeCreateRequest = Body(...),
    _: None = Depends(require_leavesphere_admin),
):
    """
    Create one employee row for LeaveSphere setup.

    Example request:
        POST /api/leavesphere/v1/employees
        {
          "firstName": "Alex",
          "lastName": "Chen",
          "email": "alex@example.com",
          "region": "US",
          "active": true
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {"id": "13f6b22f-0a86-43b8-946d-cbba67642e8b", "inserted": 1}
        }

    Requirements:
        - Requires leavesphere.admin permission (or workspace.super_admin)
        - Legacy API key compat behavior remains unchanged
        - identityKey is generated automatically when omitted
    """
    try:
        result = create_employee(payload.model_dump() if hasattr(payload, "model_dump") else payload.dict())
        invalidate_leave_sphere_workspace_caches()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/{employee_id}")
def update_employee_route(
    employee_id: str,
    request: Request,
    payload: EmployeeUpdateRequest = Body(...),
    _: None = Depends(require_leavesphere_admin),
):
    """
    Update one employee row for LeaveSphere setup.

    Example request:
        PUT /api/leavesphere/v1/employees/13f6b22f-0a86-43b8-946d-cbba67642e8b
        {"title": "Senior AE", "active": false}

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {"updated": 1}
        }

    Requirements:
        - Requires leavesphere.admin permission (or workspace.super_admin)
        - Legacy API key compat behavior remains unchanged
    """
    try:
        result = modify_employee(
            employee_id=employee_id,
            payload=payload.model_dump(exclude_unset=True) if hasattr(payload, "model_dump") else payload.dict(exclude_unset=True),
        )
        invalidate_leave_sphere_workspace_caches()
        return result
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if detail == "Employee not found" else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/{employee_id}/activate")
def activate_employee_route(
    employee_id: str,
    _: None = Depends(require_leavesphere_admin),
):
    """
    Activate one employee row.

    Example request:
        POST /api/leavesphere/v1/employees/13f6b22f-0a86-43b8-946d-cbba67642e8b/activate

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "updated": 1,
            "employee": {
              "id": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
              "firstName": "Alex",
              "lastName": "Chen",
              "email": "alex@example.com",
              "active": 1
            }
          }
        }

    Requirements:
        - Requires leavesphere.admin permission (or workspace.super_admin)
        - Sets `active` to `1`
    """
    try:
        result = activate_employee(employee_id)
        invalidate_leave_sphere_workspace_caches()
        return result
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if detail == "Employee not found" else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/{employee_id}/deactivate")
def deactivate_employee_route(
    employee_id: str,
    _: None = Depends(require_leavesphere_admin),
):
    """
    Deactivate one employee row.

    Example request:
        POST /api/leavesphere/v1/employees/13f6b22f-0a86-43b8-946d-cbba67642e8b/deactivate

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "updated": 1,
            "employee": {
              "id": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
              "firstName": "Alex",
              "lastName": "Chen",
              "email": "alex@example.com",
              "active": 0
            }
          }
        }

    Requirements:
        - Requires leavesphere.admin permission (or workspace.super_admin)
        - Sets `active` to `0`
    """
    try:
        result = deactivate_employee(employee_id)
        invalidate_leave_sphere_workspace_caches()
        return result
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if detail == "Employee not found" else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
