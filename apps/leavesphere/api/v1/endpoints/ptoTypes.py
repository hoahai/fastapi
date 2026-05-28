from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException, Request
from pydantic import BaseModel

from apps.leavesphere.api.v1.helpers.ptoTypes import (
    create_pto_type,
    get_pto_type,
    list_pto_types,
    modify_pto_type,
)
from apps.leavesphere.api.v1.permissions import require_leavesphere_admin

router = APIRouter(prefix="/ptoTypes")


try:
    from pydantic import ConfigDict

    class _LeaveSphereModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

except ImportError:

    class _LeaveSphereModel(BaseModel):
        class Config:
            extra = "ignore"


class PTOTypeCreateRequest(_LeaveSphereModel):
    code: str
    name: str
    rolloverable: bool | int | str | None = None
    payoutable: bool | int | str | None = None
    listingOrder: int | None = None
    usaDefaultHour: int | None = None
    phlDefaultHour: int | None = None


class PTOTypeUpdateRequest(_LeaveSphereModel):
    name: str | None = None
    rolloverable: bool | int | str | None = None
    payoutable: bool | int | str | None = None
    listingOrder: int | None = None
    usaDefaultHour: int | None = None
    phlDefaultHour: int | None = None


@router.get("")
def get_pto_types_route():
    """
    List PTO type definitions.

    Example request:
        GET /api/leavesphere/v1/ptoTypes

    Example response:
        {
          "meta": {"timestamp": "2026-05-28T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "code": "VAC",
              "name": "Vacation",
              "rolloverable": 1,
              "payoutable": 1
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
    """
    return list_pto_types()


@router.get("/{code}")
def get_pto_type_route(code: str):
    """
    Fetch one PTO type by code.

    Example request:
        GET /api/leavesphere/v1/ptoTypes/VAC

    Example response:
        {
          "meta": {"timestamp": "2026-05-28T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "code": "VAC",
            "name": "Vacation",
            "rolloverable": 1,
            "payoutable": 1
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - Returns 404 when PTO type code is not found
    """
    item = get_pto_type(code.strip().upper())
    if item is None:
        raise HTTPException(status_code=404, detail="PTO type not found")
    return item


@router.post("")
def create_pto_type_route(
    request: Request,
    payload: PTOTypeCreateRequest = Body(...),
    _: None = Depends(require_leavesphere_admin),
):
    """
    Create one PTO type configuration row.

    Example request:
        POST /api/leavesphere/v1/ptoTypes
        {
          "code": "VAC",
          "name": "Vacation",
          "rolloverable": true,
          "payoutable": true,
          "usaDefaultHour": 80,
          "phlDefaultHour": 80
        }

    Requirements:
        - Requires leavesphere.admin permission (or workspace.super_admin)
        - Legacy API key compat behavior remains unchanged
    """
    try:
        return create_pto_type(payload.model_dump() if hasattr(payload, "model_dump") else payload.dict())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/{code}")
def update_pto_type_route(
    code: str,
    request: Request,
    payload: PTOTypeUpdateRequest = Body(...),
    _: None = Depends(require_leavesphere_admin),
):
    """
    Update one PTO type configuration row by code.

    Example request:
        PUT /api/leavesphere/v1/ptoTypes/VAC
        {"listingOrder": 1, "payoutable": false}

    Requirements:
        - Requires leavesphere.admin permission (or workspace.super_admin)
        - Legacy API key compat behavior remains unchanged
    """
    try:
        return modify_pto_type(
            code=code,
            payload=payload.model_dump(exclude_unset=True) if hasattr(payload, "model_dump") else payload.dict(exclude_unset=True),
        )
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if detail == "PTO type not found" else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
