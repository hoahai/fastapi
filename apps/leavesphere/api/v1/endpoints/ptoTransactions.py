from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from apps.leavesphere.api.v1.helpers.ptoTransactions import (
    approve_request,
    cancel_request,
    create_adjustment,
    create_request,
    get_pto_transaction,
    list_pto_balances,
    list_pto_transactions,
    reject_request,
)
from apps.leavesphere.api.v1.permissions import require_leavesphere_admin, require_leavesphere_editor

router = APIRouter(prefix="/ptoTransactions")


try:
    from pydantic import ConfigDict

    class _LeaveSphereModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

except ImportError:

    class _LeaveSphereModel(BaseModel):
        class Config:
            extra = "ignore"


class PTOAdjustmentCreateRequest(_LeaveSphereModel):
    employeeId: str
    ptoTypeCode: str
    ptoActionCode: str
    hours: float | int | str
    year: int | str
    description: str | None = None
    approverNote: str | None = None
    startDate: str | None = None
    endDate: str | None = None
    calendarId: str | None = None


class PTORequestCreateRequest(_LeaveSphereModel):
    employeeId: str
    ptoTypeCode: str
    ptoActionCode: str
    hours: float | int | str
    year: int | str
    startDate: str | None = None
    endDate: str | None = None
    description: str | None = None
    approverNote: str | None = None
    calendarId: str | None = None


class PTORequestDecisionRequest(_LeaveSphereModel):
    approverNote: str | None = None


@router.get("/balances")
def get_pto_balances_route(
    employee_id: str | None = Query(None, alias="employeeId"),
    pto_type_code: str | None = Query(None, alias="ptoTypeCode"),
    year: int | None = Query(None, ge=1901, le=2155),
):
    """
    Return PTO balances with approved totals and pending request reservations.

    Example request:
        GET /api/leavesphere/v1/ptoTransactions/balances

    Example request (filtered):
        GET /api/leavesphere/v1/ptoTransactions/balances?employeeId=13f6b22f-0a86-43b8-946d-cbba67642e8b&ptoTypeCode=VAC&year=2026

    Example response:
        {
          "meta": {"timestamp": "2026-05-28T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "employeeId": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
              "ptoTypeCode": "VAC",
              "year": 2026,
              "approvedBalanceHours": 48.00,
              "pendingRequestHours": 8.00,
              "availableBalanceHours": 40.00
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - approvedBalanceHours subtracts request rows and adds other approved transactions
        - pendingRequestHours uses pending request rows as positive reservations
        - availableBalanceHours = approvedBalanceHours - pendingRequestHours
    """
    try:
        return list_pto_balances(
            employee_id=employee_id,
            pto_type_code=pto_type_code,
            year=year,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/adjustments")
def create_adjustment_route(
    request: Request,
    payload: PTOAdjustmentCreateRequest = Body(...),
    _: None = Depends(require_leavesphere_admin),
):
    """
    Create an approved admin PTO adjustment transaction.

    Example request:
        POST /api/leavesphere/v1/ptoTransactions/adjustments
        {
          "employeeId": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
          "ptoTypeCode": "VAC",
          "ptoActionCode": "LOAD",
          "hours": 8,
          "year": 2026,
          "description": "Annual top-up"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {"id": "0d5da2e2-0769-44c3-ac8c-bebf6b7f2f2f", "status": "Approved", "inserted": 1}
        }

    Requirements:
        - Requires leavesphere.admin permission (or workspace.super_admin)
        - status is always Approved
        - hours must be non-zero and can be positive or negative
        - Legacy API key compat behavior remains unchanged
    """
    try:
        return create_adjustment(payload.model_dump() if hasattr(payload, "model_dump") else payload.dict())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/requests")
def create_request_route(
    request: Request,
    payload: PTORequestCreateRequest = Body(...),
    _: None = Depends(require_leavesphere_editor),
):
    """
    Submit an employee PTO request as a pending transaction.

    Example request:
        POST /api/leavesphere/v1/ptoTransactions/requests
        {
          "employeeId": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
          "ptoTypeCode": "VAC",
          "ptoActionCode": "REQUEST",
          "hours": 8,
          "year": 2026,
          "startDate": "2026-06-10T00:00:00",
          "endDate": "2026-06-10T23:59:59",
          "description": "Family leave"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {"id": "0d5da2e2-0769-44c3-ac8c-bebf6b7f2f2f", "status": "Pending", "inserted": 1}
        }

    Requirements:
        - Requires leavesphere.editor permission (or higher)
        - Input hours must be greater than zero; stored value remains positive
        - Request is rejected when available balance would go below zero
        - Legacy API key compat behavior remains unchanged
    """
    try:
        body = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        return create_request(body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{transaction_id}/cancel")
def cancel_request_route(
    transaction_id: str,
    request: Request,
    _: None = Depends(require_leavesphere_editor),
):
    """
    Cancel one pending PTO request transaction.

    Example request:
        POST /api/leavesphere/v1/ptoTransactions/0d5da2e2-0769-44c3-ac8c-bebf6b7f2f2f/cancel

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {"id": "0d5da2e2-0769-44c3-ac8c-bebf6b7f2f2f", "status": "Canceled", "updated": 1}
        }

    Requirements:
        - Requires leavesphere.editor permission (or higher)
        - Only Pending transactions can be canceled
        - Approved/Rejected/Canceled transactions cannot be canceled
        - Legacy API key compat behavior remains unchanged
    """
    try:
        return cancel_request(transaction_id=transaction_id)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if detail == "PTO transaction not found" else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/{transaction_id}/approve")
def approve_request_route(
    transaction_id: str,
    request: Request,
    payload: PTORequestDecisionRequest | None = Body(default=None),
    _: None = Depends(require_leavesphere_editor),
):
    """
    Approve one pending PTO request transaction.

    Example request:
        POST /api/leavesphere/v1/ptoTransactions/0d5da2e2-0769-44c3-ac8c-bebf6b7f2f2f/approve
        {"approverNote": "Approved for upcoming schedule"}

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {"id": "0d5da2e2-0769-44c3-ac8c-bebf6b7f2f2f", "status": "Approved", "updated": 1}
        }

    Requirements:
        - Requires leavesphere.editor permission (or higher)
        - Direct manager is required unless leavesphere.admin/workspace.super_admin override applies
        - Only Pending request transactions can be approved
        - Legacy API key compat behavior remains unchanged
    """
    try:
        payload_obj = payload or PTORequestDecisionRequest()
        body = payload_obj.model_dump() if hasattr(payload_obj, "model_dump") else payload_obj.dict()
        return approve_request(request=request, transaction_id=transaction_id, approverNote=body.get("approverNote"))
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if detail == "PTO transaction not found" else 400
        if "Only a direct manager" in detail:
            status_code = 403
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/{transaction_id}/reject")
def reject_request_route(
    transaction_id: str,
    request: Request,
    payload: PTORequestDecisionRequest | None = Body(default=None),
    _: None = Depends(require_leavesphere_editor),
):
    """
    Reject one pending PTO request transaction.

    Example request:
        POST /api/leavesphere/v1/ptoTransactions/0d5da2e2-0769-44c3-ac8c-bebf6b7f2f2f/reject
        {"approverNote": "Insufficient staffing coverage"}

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {"id": "0d5da2e2-0769-44c3-ac8c-bebf6b7f2f2f", "status": "Rejected", "updated": 1}
        }

    Requirements:
        - Requires leavesphere.editor permission (or higher)
        - Direct manager is required unless leavesphere.admin/workspace.super_admin override applies
        - Only Pending request transactions can be rejected
        - Legacy API key compat behavior remains unchanged
    """
    try:
        payload_obj = payload or PTORequestDecisionRequest()
        body = payload_obj.model_dump() if hasattr(payload_obj, "model_dump") else payload_obj.dict()
        return reject_request(request=request, transaction_id=transaction_id, approverNote=body.get("approverNote"))
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if detail == "PTO transaction not found" else 400
        if "Only a direct manager" in detail:
            status_code = 403
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.get("")
def get_pto_transactions_route(
    employee_id: str | None = Query(None, alias="employeeId"),
    pto_type_code: str | None = Query(None, alias="ptoTypeCode"),
    year: int | None = Query(None, ge=1901, le=2155),
    status: str | None = Query(None),
):
    """
    List PTO transactions.

    Example request:
        GET /api/leavesphere/v1/ptoTransactions

    Example request (filtered):
        GET /api/leavesphere/v1/ptoTransactions?employeeId=13f6b22f-0a86-43b8-946d-cbba67642e8b&ptoTypeCode=VAC&year=2026&status=Pending

    Example response:
        {
          "meta": {"timestamp": "2026-05-28T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": "0d5da2e2-0769-44c3-ac8c-bebf6b7f2f2f",
              "employeeId": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
              "ptoTypeCode": "VAC",
              "ptoActionCode": "REQUEST",
              "hours": 8.00,
              "year": 2026,
              "status": "Pending"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - Optional filters: employeeId, ptoTypeCode, year, status
    """
    try:
        return list_pto_transactions(
            employee_id=employee_id,
            pto_type_code=pto_type_code,
            year=year,
            status=status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{transaction_id}")
def get_pto_transaction_route(transaction_id: str):
    """
    Fetch one PTO transaction by id.

    Example request:
        GET /api/leavesphere/v1/ptoTransactions/0d5da2e2-0769-44c3-ac8c-bebf6b7f2f2f

    Example response:
        {
          "meta": {"timestamp": "2026-05-28T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": "0d5da2e2-0769-44c3-ac8c-bebf6b7f2f2f",
            "employeeId": "13f6b22f-0a86-43b8-946d-cbba67642e8b",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": 8.00,
            "status": "Pending"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - Returns 404 when transaction is not found
    """
    item = get_pto_transaction(transaction_id)
    if item is None:
        raise HTTPException(status_code=404, detail="PTO transaction not found")
    return item
