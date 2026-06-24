from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from apps.leavesphere.api.v1.helpers.employees import load_employee_management_workspace
from apps.leavesphere.api.v1.permissions import require_leavesphere_admin

router = APIRouter(
    prefix="/ui/employees",
    dependencies=[Depends(require_leavesphere_admin)],
)


@router.get("/load")
def get_employee_management_load_route(
    fresh_data: bool = Query(False, alias="fresh_data"),
):
    """
    Load the LeaveSphere Employee Management workspace.

    Example request:
        GET /api/leavesphere/v1/ui/employees/load

    Example request (hard refresh):
        GET /api/leavesphere/v1/ui/employees/load?fresh_data=true

    Example response:
        {
          "meta": {"timestamp": "2026-06-24T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "pageCode": "employee-management",
            "pageTitle": "Employee Management",
            "summary": {
              "totalEmployees": 2,
              "activeEmployees": 1,
              "inactiveEmployees": 1
            },
            "capabilities": {
              "canCreate": true,
              "canUpdate": true,
              "canActivate": true,
              "canDeactivate": true,
              "canArchive": false
            },
            "employees": [
              {
                "id": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
                "firstName": "Alex",
                "lastName": "Chen",
                "email": "alex@example.com",
                "active": 1
              }
            ]
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires leavesphere.admin permission or workspace.super_admin
        - Requires valid API key or bearer token in compat mode
        - `fresh_data=true` bypasses the cached employee workspace snapshot and refreshes the employee list from the database
    """
    try:
        return load_employee_management_workspace(force_refresh=fresh_data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
