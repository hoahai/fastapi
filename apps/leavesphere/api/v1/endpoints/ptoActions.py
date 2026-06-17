from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from pydantic import BaseModel

from apps.leavesphere.api.v1.helpers.ptoActions import (
    create_pto_action,
    get_pto_action,
    list_pto_actions,
    modify_pto_action,
)
from apps.leavesphere.api.v1.helpers.workspaceCache import invalidate_leave_sphere_workspace_caches
from apps.leavesphere.api.v1.permissions import require_leavesphere_admin

router = APIRouter(prefix="/ptoActions")


try:
    from pydantic import ConfigDict

    class _LeaveSphereModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

except ImportError:

    class _LeaveSphereModel(BaseModel):
        class Config:
            extra = "ignore"


class PTOActionCreateRequest(_LeaveSphereModel):
    code: str
    name: str
    color: str | None = None


class PTOActionUpdateRequest(_LeaveSphereModel):
    name: str | None = None
    color: str | None = None


@router.get("")
def get_pto_actions_route():
    """
    List PTO action definitions.

    Example request:
        GET /api/leavesphere/v1/ptoActions

    Example response:
        {
          "meta": {"timestamp": "2026-05-28T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "code": "REQUEST",
              "name": "Request",
              "color": "#F59E0B"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
    """
    return list_pto_actions()


@router.get("/{code}")
def get_pto_action_route(code: str):
    """
    Fetch one PTO action by code.

    Example request:
        GET /api/leavesphere/v1/ptoActions/REQUEST

    Example response:
        {
          "meta": {"timestamp": "2026-05-28T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "code": "REQUEST",
            "name": "Request",
            "color": "#F59E0B"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - Returns 404 when PTO action code is not found
    """
    item = get_pto_action(code.strip().upper())
    if item is None:
        raise HTTPException(status_code=404, detail="PTO action not found")
    return item


@router.post("")
def create_pto_action_route(
    request: Request,
    payload: PTOActionCreateRequest = Body(...),
    _: None = Depends(require_leavesphere_admin),
):
    """
    Create one PTO action configuration row.

    Example request:
        POST /api/leavesphere/v1/ptoActions
        {"code": "LOAD", "name": "Load", "color": "#16A34A"}

    Requirements:
        - Requires leavesphere.admin permission (or workspace.super_admin)
        - Legacy API key compat behavior remains unchanged
    """
    try:
        result = create_pto_action(payload.model_dump() if hasattr(payload, "model_dump") else payload.dict())
        invalidate_leave_sphere_workspace_caches(include_catalogs=True)
        return result
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/{code}")
def update_pto_action_route(
    code: str,
    request: Request,
    payload: PTOActionUpdateRequest = Body(...),
    _: None = Depends(require_leavesphere_admin),
):
    """
    Update one PTO action configuration row by code.

    Example request:
        PUT /api/leavesphere/v1/ptoActions/LOAD
        {"name": "Admin Load", "color": "#0EA5E9"}

    Requirements:
        - Requires leavesphere.admin permission (or workspace.super_admin)
        - Legacy API key compat behavior remains unchanged
    """
    try:
        result = modify_pto_action(
            code=code,
            payload=payload.model_dump(exclude_unset=True) if hasattr(payload, "model_dump") else payload.dict(exclude_unset=True),
        )
        invalidate_leave_sphere_workspace_caches(include_catalogs=True)
        return result
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if detail == "PTO action not found" else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
