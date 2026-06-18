from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from apps.leavesphere.api.v1.helpers.leaveManagement import (
    adjust_leave_management_balance,
    create_leave_management_request,
    load_leave_management_workspace,
    review_leave_management_request,
    update_leave_management_request,
    update_leave_management_setup_data,
)
from apps.leavesphere.api.v1.permissions import require_leavesphere_admin

router = APIRouter(
    prefix="/admin/pto",
    dependencies=[Depends(require_leavesphere_admin)],
)


try:
    from pydantic import ConfigDict

    class _LeaveSphereModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

except ImportError:

    class _LeaveSphereModel(BaseModel):
        class Config:
            extra = "ignore"


class LeaveManagementRequestCreateRequest(_LeaveSphereModel):
    employeeId: str | None = None
    type: str
    startDate: str
    endDate: str
    hours: float | int | str
    description: str | None = None
    year: int | str | None = None
    ptoTypeCode: str | None = None
    calendarId: str | None = None
    approveImmediately: bool | None = None


class LeaveManagementRequestUpdateRequest(_LeaveSphereModel):
    transactionId: str
    type: str
    startDate: str
    endDate: str
    hours: float | int | str
    description: str | None = None
    year: int | str | None = None
    ptoTypeCode: str | None = None
    calendarId: str | None = None


class LeaveManagementReviewRequest(_LeaveSphereModel):
    requestId: str
    action: str
    approverNote: str | None = None


class LeaveManagementAdjustRequest(_LeaveSphereModel):
    employeeId: str
    ptoTypeCode: str
    ptoActionCode: str
    transactionId: str | None = None
    hours: float | int | str
    year: int | str
    status: str
    description: str | None = None
    approverNote: str | None = None


class LeaveManagementSetupRequest(_LeaveSphereModel):
    kind: str
    code: str | None = None
    label: str | None = None
    detail: str | None = None
    employeeName: str | None = None
    title: str | None = None
    managerId: str | None = None
    employeeId: str | None = None
    date: str | None = None
    teamRegion: str | None = None
    active: bool | int | str | None = None


@router.get("/workspace")
def get_leave_management_workspace_route(
    request: Request,
    year: int | None = Query(None, ge=1901, le=2155),
    include_pending: bool = Query(True),
    history_start_date: str | None = Query(None),
    history_end_date: str | None = Query(None),
    overlap_month: str | None = Query(None),
    fresh_data: bool = Query(False, alias="fresh_data"),
):
    """
    Load the Leave Management workspace for the selected year.

    Example request:
        GET /api/leavesphere/v1/admin/pto/workspace?year=2026

    Example request (expanded history):
        GET /api/leavesphere/v1/admin/pto/workspace?year=2026&include_pending=true&history_start_date=2026-05-01&history_end_date=2026-12-31&overlap_month=2026-06

    Example request (hard refresh):
        GET /api/leavesphere/v1/admin/pto/workspace?year=2026&fresh_data=true

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "currentUserId": "emp-123",
            "currentUserName": "Alex Chen",
            "currentUserEmail": "alex@example.com",
            "managerId": "mgr-9",
            "currentUserTeamRegion": "US",
            "isManager": true,
            "ptoTypes": [],
            "ptoActions": [],
            "employeeBalances": [],
            "balanceTransactions": [],
            "requests": [
              {
                "id": "pto-1",
                "employeeId": "emp-123",
                "managerId": "mgr-9",
                "type": "vacation",
                "startDate": "2026-06-10",
                "endDate": "2026-06-10",
                "hours": 8,
                "description": "Family trip",
                "status": "pending",
                "submittedAt": "2026-05-29"
              }
            ],
            "holidays": [],
            "employees": []
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires leavesphere.admin permission or workspace.super_admin
        - Requires valid API key or bearer token in compat mode
        - Requests are loaded from the provided history/overlap window, not the full year
        - Balances remain year-scoped for the selected year and are computed from full-year transaction data
        - `history_start_date` and `history_end_date` must use YYYY-MM-DD format when provided
        - `overlap_month` must use YYYY-MM format when provided
        - `fresh_data=true` bypasses the backend workspace cache and rebuilds the workspace from source data
    """
    try:
        return load_leave_management_workspace(
            request=request,
            year=year,
            include_pending=include_pending,
            history_start_date=history_start_date,
            history_end_date=history_end_date,
            overlap_month=overlap_month,
            force_refresh=fresh_data,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/requests")
def create_leave_management_request_route(
    request: Request,
    payload: LeaveManagementRequestCreateRequest = Body(...),
):
    """
    Create a PTO request on behalf of an employee.

    Example request:
        POST /api/leavesphere/v1/admin/pto/requests
        {
          "employeeId": "emp-123",
          "type": "vacation",
          "startDate": "2026-06-10",
          "endDate": "2026-06-10",
          "hours": 8,
          "description": "Family trip"
        }

    Example request (approve immediately):
        POST /api/leavesphere/v1/admin/pto/requests
        {
          "employeeId": "emp-123",
          "type": "vacation",
          "startDate": "2026-06-10",
          "endDate": "2026-06-10",
          "hours": 8,
          "description": "Family trip",
          "approveImmediately": true
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "workspacePatch": { "...": "changed Leave Management rows only" },
            "source": "network",
            "createdRequestId": "pto-1",
            "inserted": 1,
            "status": "Pending"
          }
        }

    Requirements:
        - Requires leavesphere.admin permission or workspace.super_admin
        - employeeId is required and must identify an active employee
        - `hours` must be greater than zero
        - The request is stored as a pending debit transaction unless `approveImmediately=true`
        - `approveImmediately=true` inserts the request directly in `Approved` state with the admin as approver
        - Mutation responses may return `workspacePatch` instead of a full `workspace` when the server can patch a cached snapshot
    """
    try:
        body = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        return create_leave_management_request(request=request, payload=body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/requests")
def update_leave_management_request_route(
    request: Request,
    payload: LeaveManagementRequestUpdateRequest = Body(...),
):
    """
    Update an existing PTO request transaction.

    Example request:
        PUT /api/leavesphere/v1/admin/pto/requests
        {
          "transactionId": "pto-1",
          "type": "vacation",
          "startDate": "2026-06-11",
          "endDate": "2026-06-11",
          "hours": 8,
          "description": "Updated family trip"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "workspacePatch": { "...": "changed Leave Management rows only" },
            "source": "network",
            "updated": 1
          }
        }

    Requirements:
        - Requires leavesphere.admin permission or workspace.super_admin
        - The transaction must already exist
        - Request hours are stored as positive values
        - Mutation responses may return `workspacePatch` instead of a full `workspace` when the server can patch a cached snapshot
    """
    try:
        body = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        return update_leave_management_request(request=request, payload=body)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if detail == "PTO transaction not found" else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/review")
def review_leave_management_request_route(
    request: Request,
    payload: LeaveManagementReviewRequest = Body(...),
):
    """
    Approve or reject a PTO request transaction.

    Example request:
        POST /api/leavesphere/v1/admin/pto/review
        {"requestId": "pto-1", "action": "approve", "approverNote": "Approved"}

    Example request (cancel/revert):
        POST /api/leavesphere/v1/admin/pto/review
        {"requestId": "pto-1", "action": "cancel", "approverNote": "Canceling"}

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "workspacePatch": { "...": "changed Leave Management rows only" },
            "source": "network",
            "updated": 1,
            "status": "Approved"
          }
        }

    Requirements:
        - Requires leavesphere.admin permission or workspace.super_admin
        - `action` must be `approve`, `reject`, `cancel`, or `revert`
        - The transaction must already exist
        - Admin review bypasses direct-manager validation
        - Mutation responses may return `workspacePatch` instead of a full `workspace` when the server can patch a cached snapshot
    """
    try:
        body = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        return review_leave_management_request(request=request, payload=body)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if detail == "PTO transaction not found" else 400
        if "Only a direct manager" in detail:
            status_code = 403
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/balances/adjust")
def adjust_leave_management_balance_route(
    request: Request,
    payload: LeaveManagementAdjustRequest = Body(...),
):
    """
    Record an approved PTO balance transaction.

    Example request:
        POST /api/leavesphere/v1/admin/pto/balances/adjust
        {
          "employeeId": "emp-123",
          "ptoTypeCode": "VAC",
          "ptoActionCode": "load_grant",
          "hours": 8,
          "year": 2026,
          "status": "Approved",
          "description": "Opening balance load",
          "approverNote": "Opening balance"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "workspacePatch": { "...": "changed Leave Management rows only" },
            "source": "network",
            "updated": 1,
            "status": "Approved"
          }
        }

    Requirements:
        - Requires leavesphere.admin permission or workspace.super_admin
        - `hours` must be zero or greater
        - `status` must be `Approved`
        - `description` is stored with the load transaction
        - `approverNote` is stored in the `approverNote` database column
        - `approverId` is set from the authenticated admin employee
        - Mutation responses may return `workspacePatch` instead of a full `workspace` when the server can patch a cached snapshot
    """
    try:
        body = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        return adjust_leave_management_balance(request=request, payload=body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/setup")
def update_leave_management_setup_route(
    request: Request,
    payload: LeaveManagementSetupRequest = Body(...),
):
    """
    Update Leave Management setup data for PTO types, actions, employees, managers, and holidays.

    Example request:
        POST /api/leavesphere/v1/admin/pto/setup
        {"kind": "holiday", "name": "Labor Day", "date": "2026-09-07", "teamRegion": "US"}

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "workspacePatch": { "...": "changed Leave Management rows only" },
            "source": "network"
          }
        }

    Requirements:
        - Requires leavesphere.admin permission or workspace.super_admin
        - `kind` must be one of `pto_type`, `pto_action`, `employee`, `employee_manager`, or `holiday`
        - Employee setup may generate synthetic identity values when the UI does not provide them
        - Mutation responses may return `workspacePatch` instead of a full `workspace` when the server can patch a cached snapshot
    """
    try:
        body = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        return update_leave_management_setup_data(request=request, payload=body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
