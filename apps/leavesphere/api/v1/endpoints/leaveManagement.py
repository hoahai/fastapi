from __future__ import annotations

from fastapi import APIRouter, Body, Depends, Form, HTTPException, Query, Request
from pydantic import BaseModel

from apps.leavesphere.api.v1.helpers.leaveManagement import (
    adjust_leave_management_balance,
    create_leave_management_request,
    duplicate_leave_management_load_transactions,
    load_leave_management_workspace,
    review_leave_management_request,
    send_leave_management_pending_approval_reminders,
    update_leave_management_request,
    update_leave_management_setup_data,
)
from apps.leavesphere.api.v1.helpers.notification_emails import (
    LEAVESPHERE_EMAIL_PREVIEW_TEST_RECIPIENT,
    send_leave_sphere_status_preview_email,
)
from apps.leavesphere.api.v1.permissions import require_leavesphere_admin
from shared.smtp import SmtpSendError

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


class LeaveManagementDuplicateBalancesRequest(_LeaveSphereModel):
    yearFrom: int | str
    yearTo: int | str
    employeeIds: list[str] | None = None


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


@router.post("/email-preview/test-email")
def send_leave_management_email_preview_test_route(
    request: Request,
    toEmail: str = Form(LEAVESPHERE_EMAIL_PREVIEW_TEST_RECIPIENT),
):
    """
    Send a LeaveSphere status preview email to a test recipient.

    Example request:
        POST /api/leavesphere/v1/admin/pto/email-preview/test-email
        toEmail=hai@theautoadagency.com

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "status": "sent",
            "recipient_email": "hai@theautoadagency.com",
            "subject": "PTO request approved for Alex Chen | 06/10/2026 - 06/12/2026"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires leavesphere.admin permission or workspace.super_admin
        - Requires valid API key or bearer token in compat mode
        - Requires `leavesphere.smtp` to be configured
        - Sends the approved LeaveSphere status template sample
        - `toEmail` defaults to `hai@theautoadagency.com`
    """
    try:
        recipient_email = str(toEmail or "").strip() or LEAVESPHERE_EMAIL_PREVIEW_TEST_RECIPIENT
        if "@" not in recipient_email:
            raise ValueError("toEmail must be a valid email address")
        return send_leave_sphere_status_preview_email(recipient_email=recipient_email)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except (SmtpSendError, OSError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/reminders/pending-approvals")
def send_leave_management_pending_approvals_reminder_route(
    request: Request,
    year: int | None = Query(None, ge=1901, le=2155),
    coming_days: int = Query(7, alias="comingDays", ge=0),
    test_email: str | None = Query(None, alias="testEmail"),
    send_email: bool = Query(True, alias="sendEmail"),
    debug: bool = Query(False),
):
    """
    Send reminder emails for pending PTO approvals to every manager of each employee in scope.

    Example request:
        POST /api/leavesphere/v1/admin/pto/reminders/pending-approvals?year=2026&comingDays=7

    Example request (defaults to current year and 7-day window):
        POST /api/leavesphere/v1/admin/pto/reminders/pending-approvals

    Example request (test mode):
        POST /api/leavesphere/v1/admin/pto/reminders/pending-approvals?testEmail=hai@theautoadagency.com

    Example request (debug mode):
        POST /api/leavesphere/v1/admin/pto/reminders/pending-approvals?testEmail=hai@theautoadagency.com&debug=true

    Example request (dry run, no email send):
        POST /api/leavesphere/v1/admin/pto/reminders/pending-approvals?sendEmail=false&debug=true

    Example response:
        {
          "meta": {"timestamp": "2026-06-22T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "source": "network",
            "year": 2026,
            "comingDays": 7,
            "windowStart": "2026-01-01",
            "windowEnd": "2026-06-29",
            "requestsFound": 2,
            "managersFound": 2,
            "emailsSent": 2,
            "emailsFailed": 0,
            "skippedRequests": 0,
            "skippedManagerContacts": 0,
            "managers": [
              {
                "managerId": "mgr-1",
                "managerName": "Jordan Lee",
                "managerEmail": "jordan@example.com",
                "requestCount": 1,
                "sent": true,
                "deliveryMode": "smtp"
              }
            ],
            "sendEmail": true,
            "emailsSkippedDryRun": 0,
            "debugQuickApprovalUrls": [
              {
                "managerId": "mgr-1",
                "managerEmail": "jordan@example.com",
                "managerName": "Jordan Lee",
                "requestId": "pto-123",
                "employeeId": "emp-9",
                "url": "http://127.0.0.1:8000/leavesphere/quick-approval/v1.eyJ..."
              }
            ]
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires leavesphere.admin permission or workspace.super_admin
        - Requires valid API key or bearer token in compat mode
        - `year` defaults to the current year when omitted
        - `comingDays` defaults to 7 when omitted
        - Only pending PTO requests in the selected year with start dates up to `today + comingDays` are included
        - Each employee's requests are fanned out to all of that employee's managers
        - When `testEmail` is provided, all reminder emails are sent only to that address and CC recipients are suppressed
        - When `sendEmail=false`, the route performs a dry run and does not send SMTP mail
        - When `debug=true`, the response includes generated quick-approval URLs for inspection
    """
    try:
        return send_leave_management_pending_approval_reminders(
            request=request,
            year=year,
            coming_days=coming_days,
            test_email=test_email,
            send_email=send_email,
            include_debug_quick_approval_urls=debug,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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
            "lastSubmissionDate": "10-30",
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
    Approve, reject, cancel, or revert a PTO request transaction.

    Example request:
        POST /api/leavesphere/v1/admin/pto/review
        {"requestId": "pto-1", "action": "approve", "approverNote": "Approved"}

    Example request (reject):
        POST /api/leavesphere/v1/admin/pto/review
        {"requestId": "pto-1", "action": "reject", "approverNote": "Insufficient staffing coverage"}

    Example request (cancel):
        POST /api/leavesphere/v1/admin/pto/review
        {"requestId": "pto-1", "action": "cancel", "approverNote": "Employee requested cancellation"}

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
        - `approverNote` is required when `action` is `reject` or `cancel`
        - When `leavesphere.smtp` is configured, the affected employee receives a status email using the LeaveSphere template
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


@router.post("/balances/duplicate")
def duplicate_leave_management_load_transactions_route(
    request: Request,
    payload: LeaveManagementDuplicateBalancesRequest = Body(...),
):
    """
    Duplicate approved LOAD transactions from one year into another year.

    Example request:
        POST /api/leavesphere/v1/admin/pto/balances/duplicate
        {
          "yearFrom": 2025,
          "yearTo": 2026,
          "employeeIds": ["emp-123", "emp-456"]
        }

    Example request (all employees):
        POST /api/leavesphere/v1/admin/pto/balances/duplicate
        {
          "yearFrom": 2025,
          "yearTo": 2026,
          "employeeIds": []
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "workspacePatch": { "...": "changed Leave Management rows only" },
            "source": "network",
            "yearFrom": 2025,
            "yearTo": 2026,
            "employeeIds": ["emp-123", "emp-456"],
            "matchedTransactions": 4,
            "duplicated": 4,
            "inserted": 4,
            "skipped": 0
          }
        }

    Requirements:
        - Requires leavesphere.admin permission or workspace.super_admin
        - `yearFrom` and `yearTo` are required and must be valid years
        - `employeeIds` is optional; omit it or pass an empty list to duplicate all employees
        - Only approved LOAD transactions are duplicated
        - Mutation responses may return `workspacePatch` instead of a full `workspace` when the server can patch a cached snapshot
    """
    try:
        body = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        return duplicate_leave_management_load_transactions(request=request, payload=body)
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
