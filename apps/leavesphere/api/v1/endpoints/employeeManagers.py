from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from apps.leavesphere.api.v1.helpers.employeeManagers import (
    create_employee_manager,
    get_employee_manager,
    list_employee_managers,
    remove_employee_manager,
)
from apps.leavesphere.api.v1.helpers.workspaceCache import invalidate_leave_sphere_workspace_caches
from apps.leavesphere.api.v1.permissions import require_leavesphere_admin

router = APIRouter(prefix="/employeeManagers")


try:
    from pydantic import ConfigDict

    class _LeaveSphereModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

except ImportError:

    class _LeaveSphereModel(BaseModel):
        class Config:
            extra = "ignore"


class EmployeeManagerCreateRequest(_LeaveSphereModel):
    id: str | None = None
    employeeId: str
    managerId: str


@router.get("")
def get_employee_managers_route(
    employee_id: str | None = Query(None, alias="employeeId"),
    manager_id: str | None = Query(None, alias="managerId"),
):
    """
    List employee-manager relationships with optional filters.

    Example request:
        GET /api/leavesphere/v1/employeeManagers

    Example request (filtered):
        GET /api/leavesphere/v1/employeeManagers?employeeId=13f6b22f-0a86-43b8-946d-cbba67642e8b

    Example response:
        {
          "meta": {"timestamp": "2026-05-28T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": "d10d5fb0-ccf0-4f2f-a7fd-59d79aad17c5",
              "employeeId": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
              "managerId": "d33c8657-c3dd-4901-a06b-aac77f59be58"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - Optional filters: employeeId, managerId
    """
    return list_employee_managers(employee_id=employee_id, manager_id=manager_id)


@router.get("/{id}")
def get_employee_manager_route(id: str):
    """
    Fetch one employee-manager relationship by id.

    Example request:
        GET /api/leavesphere/v1/employeeManagers/d10d5fb0-ccf0-4f2f-a7fd-59d79aad17c5

    Example response:
        {
          "meta": {"timestamp": "2026-05-28T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": "d10d5fb0-ccf0-4f2f-a7fd-59d79aad17c5",
            "employeeId": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
            "managerId": "d33c8657-c3dd-4901-a06b-aac77f59be58"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - Returns 404 when mapping is not found
    """
    mapping = get_employee_manager(id)
    if mapping is None:
        raise HTTPException(status_code=404, detail="Employee manager mapping not found")
    return mapping


@router.post("")
def create_employee_manager_route(
    request: Request,
    payload: EmployeeManagerCreateRequest = Body(...),
    _: None = Depends(require_leavesphere_admin),
):
    """
    Create one employee-manager relationship row.

    Example request:
        POST /api/leavesphere/v1/employeeManagers
        {
          "employeeId": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
          "managerId": "d33c8657-c3dd-4901-a06b-aac77f59be58"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {"id": "d10d5fb0-ccf0-4f2f-a7fd-59d79aad17c5", "inserted": 1}
        }

    Requirements:
        - Requires leavesphere.admin permission (or workspace.super_admin)
        - Legacy API key compat behavior remains unchanged
    """
    try:
        result = create_employee_manager(payload.model_dump() if hasattr(payload, "model_dump") else payload.dict())
        invalidate_leave_sphere_workspace_caches()
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/{id}")
def delete_employee_manager_route(
    id: str,
    request: Request,
    _: None = Depends(require_leavesphere_admin),
):
    """
    Delete one employee-manager relationship row by id.

    Example request:
        DELETE /api/leavesphere/v1/employeeManagers/d10d5fb0-ccf0-4f2f-a7fd-59d79aad17c5

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {"deleted": 1}
        }

    Requirements:
        - Requires leavesphere.admin permission (or workspace.super_admin)
        - Legacy API key compat behavior remains unchanged
    """
    try:
        result = remove_employee_manager(id)
        invalidate_leave_sphere_workspace_caches()
        return result
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if detail == "Employee manager mapping not found" else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
