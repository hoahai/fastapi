from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query, Request
from pydantic import BaseModel

from apps.leavesphere.api.v1.helpers.myPto import (
    cancel_my_pto_request,
    create_my_pto_request,
    load_my_pto_workspace,
    review_my_pto_request,
    update_my_pto_request,
)

router = APIRouter(prefix="/ui/my-pto")


try:
    from pydantic import ConfigDict

    class _LeaveSphereModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

except ImportError:

    class _LeaveSphereModel(BaseModel):
        class Config:
            extra = "ignore"


class MyPtoRequestCreateRequest(_LeaveSphereModel):
    type: str
    startDate: str
    endDate: str
    hours: float | int | str
    description: str | None = None
    year: int | str
    ptoTypeCode: str | None = None
    transactionId: str | None = None


class MyPtoRequestUpdateRequest(_LeaveSphereModel):
    transactionId: str
    type: str
    startDate: str
    endDate: str
    hours: float | int | str
    description: str | None = None
    year: int | str
    ptoTypeCode: str | None = None


class MyPtoRequestActionRequest(_LeaveSphereModel):
    transactionId: str


class MyPtoReviewRequest(_LeaveSphereModel):
    transactionId: str | None = None
    requestId: str | None = None
    action: str
    approverNote: str | None = None


@router.get("/load")
def get_my_pto_load_route(
    request: Request,
    year: int | None = Query(None, ge=1901, le=2155),
    fresh_data: bool = Query(False, alias="fresh_data"),
):
    """
    Load the signed-in user's PTO workspace for the My PTO page.

    Example request:
        GET /api/leavesphere/v1/ui/my-pto/load?year=2026

    Example request (hard refresh):
        GET /api/leavesphere/v1/ui/my-pto/load?year=2026&fresh_data=true

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
            "ptoTypes": [
              {"code": "VAC", "type": "vacation", "label": "Vacation", "listingOrder": 1}
            ],
            "ptoActions": [
              {"code": "REQUEST", "name": "Request"}
            ],
            "defaultRequestActionCode": "REQUEST",
            "balances": [
              {"type": "vacation", "code": "VAC", "label": "Vacation", "totalHours": 120, "usedHours": 40, "scheduledHours": 8, "remainingHours": 72}
            ],
            "requests": [
              {
                "id": "pto-1",
                "employeeId": "emp-123",
                "managerId": "mgr-9",
                "type": "vacation",
                "ptoTypeCode": "VAC",
                "startDate": "2026-06-10",
                "endDate": "2026-06-10",
                "hours": 8,
                "status": "pending"
              }
            ],
            "holidays": [
              {"id": "2026-us-new-year", "name": "New Year's Day", "date": "2026-01-01", "teamRegion": "US"}
            ],
            "directReports": [
              {"employeeId": "emp-456", "employeeName": "Jamie Lee", "title": "Coordinator"}
            ],
            "employees": [
              {"employeeId": "emp-123", "employeeName": "Alex Chen", "pictureUrl": "https://example.com/avatars/alex-chen.jpg"},
              {"employeeId": "emp-456", "employeeName": "Jamie Lee", "pictureUrl": "https://example.com/avatars/jamie-lee.jpg", "title": "Coordinator"}
            ]
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - Resolves the employee from the signed-in user's login email
        - Returns one year of dashboard data for the My PTO page
        - When `lastSubmissionDate` is configured, it is returned as `MM-DD` in the workspace payload
        - `fresh_data=true` bypasses the backend workspace cache and rebuilds the workspace from source data
    """
    try:
        return load_my_pto_workspace(request=request, year=year, force_refresh=fresh_data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/requests")
def create_my_pto_request_route(
    request: Request,
    payload: MyPtoRequestCreateRequest = Body(...),
):
    """
    Create a PTO request for the signed-in user.

    Example request:
        POST /api/leavesphere/v1/ui/my-pto/requests
        {
          "type": "pto",
          "ptoTypeCode": "PTO",
          "startDate": "2026-06-10",
          "endDate": "2026-06-10",
          "hours": 8,
          "description": "Family trip",
          "year": 2026
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "workspacePatch": { "...": "changed My PTO rows only" },
            "source": "network",
            "createdRequestId": "pto-1",
            "inserted": 1,
            "status": "Pending"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - The employee is resolved from the signed-in user's login email
        - `hours` must be greater than zero
        - `ptoTypeCode` is preferred and must match a row from the PTO type table; `type` is accepted as a fallback alias
        - When `lastSubmissionDate` is configured, new requests for the current year are rejected after that MM-DD cutoff
        - Future-year requests remain allowed even after the current-year cutoff has passed
        - Mutation responses may return `workspacePatch` instead of a full `workspace` when the server can patch a cached snapshot
    """
    try:
        body = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        return create_my_pto_request(request=request, payload=body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/requests")
def update_my_pto_request_route(
    request: Request,
    payload: MyPtoRequestUpdateRequest = Body(...),
):
    """
    Update one pending PTO request that belongs to the signed-in user.

    Example request:
        PUT /api/leavesphere/v1/ui/my-pto/requests
        {
          "transactionId": "pto-1",
          "type": "vacation",
          "startDate": "2026-06-11",
          "endDate": "2026-06-11",
          "hours": 8,
          "description": "Family trip",
          "year": 2026
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "workspacePatch": { "...": "changed My PTO rows only" },
            "source": "network",
            "updated": 1
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - Request must still be pending and before its start date
        - The employee is resolved from the signed-in user's login email
        - Mutation responses may return `workspacePatch` instead of a full `workspace` when the server can patch a cached snapshot
    """
    try:
        body = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        return update_my_pto_request(request=request, payload=body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/requests")
def cancel_my_pto_request_route(
    request: Request,
    payload: MyPtoRequestActionRequest = Body(...),
):
    """
    Cancel one PTO request that belongs to the signed-in user.

    Example request:
        DELETE /api/leavesphere/v1/ui/my-pto/requests
        {"transactionId": "pto-1"}

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "workspacePatch": { "...": "changed My PTO rows only" },
            "source": "network",
            "id": "pto-1",
            "status": "Canceled",
            "updated": 1
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - The employee is resolved from the signed-in user's login email
        - Only future PTO requests can be canceled
        - Mutation responses may return `workspacePatch` instead of a full `workspace` when the server can patch a cached snapshot
    """
    try:
        body = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        return cancel_my_pto_request(request=request, transaction_id=body["transactionId"])
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/review")
def review_my_pto_request_route(
    request: Request,
    payload: MyPtoReviewRequest = Body(...),
):
    """
    Review a direct-report PTO request from the My PTO page manager panel.

    Example request:
        POST /api/leavesphere/v1/ui/my-pto/review
        {"requestId": "pto-1", "transactionId": "pto-1", "action": "approve", "approverNote": "Approved for travel"}

    Example request (reject):
        POST /api/leavesphere/v1/ui/my-pto/review
        {"requestId": "pto-1", "transactionId": "pto-1", "action": "reject", "approverNote": "Insufficient staffing coverage"}

    Example request (cancel):
        POST /api/leavesphere/v1/ui/my-pto/review
        {"requestId": "pto-1", "transactionId": "pto-1", "action": "cancel", "approverNote": "Employee requested cancellation"}

    Example response:
        {
          "meta": {"timestamp": "2026-05-29T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "workspacePatch": { "...": "changed My PTO rows only" },
            "source": "network",
            "id": "pto-1",
            "status": "Approved",
            "updated": 1
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - Requires leavesphere.editor permission or higher
        - The manager is resolved from the signed-in user's login email
        - `requestId` and `transactionId` are both accepted for compatibility
        - `approverNote` is required when `action` is `reject` or `cancel`
        - When `leavesphere.smtp` is configured, the affected employee receives a status email using the LeaveSphere template
        - Mutation responses may return `workspacePatch` instead of a full `workspace` when the server can patch a cached snapshot
    """
    try:
        body = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        return review_my_pto_request(request=request, payload=body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
