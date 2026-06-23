from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Request
from pydantic import BaseModel

from apps.leavesphere.api.v1.helpers.quickApproval import (
    load_quick_approval_context,
    submit_quick_approval_decision,
)

router = APIRouter(prefix="/v1/public/approval")


try:
    from pydantic import ConfigDict

    class _LeaveSphereModel(BaseModel):
        model_config = ConfigDict(extra="ignore")

except ImportError:

    class _LeaveSphereModel(BaseModel):
        class Config:
            extra = "ignore"


class QuickApprovalDecisionRequest(_LeaveSphereModel):
    reason: str | None = None
    approverNote: str | None = None


@router.get("/{token}")
def load_quick_approval_route(token: str, debug: bool = False, refresh: bool = False):
    """
    Load the public quick-approval preview for one signed approval token.

    Example request:
        GET /api/leavesphere/v1/public/approval/v1.eyJ...token...

    Example request (live refresh):
        GET /api/leavesphere/v1/public/approval/v1.eyJ...token...?refresh=true

    Example response:
        {
          "meta": {"timestamp": "2026-06-22T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "state": "ready",
            "preview": {
              "requestId": "pto-123",
              "employeeName": "Alex Chen",
              "pictureUrl": "https://example.com/avatars/alex-chen.jpg",
              "ptoTypeCode": "VAC",
              "ptoTypeLabel": "Vacation",
            "startDate": "2026-06-10",
            "endDate": "2026-06-12",
            "hoursRequested": 24,
            "reason": "Family trip",
            "currentStatus": "pending"
          },
            "handledDecision": null,
            "message": null,
            "recipientRole": "manager",
            "recipientName": "Marco Camacho",
            "recipientPictureUrl": "https://example.com/avatars/marco-camacho.jpg"
          }
        }

    Requirements:
        - Public route, no login required
        - Token must be valid, signed, and unexpired
        - Token is bound to a specific tenant, request, and recipient
        - `refresh=true` re-checks the live request after the initial snapshot render
        - Background refresh responses may be served from a tenant-scoped cache for up to 15 minutes
        - When `debug=true`, the response includes a `debugTrace` object
    """
    return load_quick_approval_context(token=token, debug=debug, refresh=refresh)


@router.post("/{token}/approve")
def approve_quick_approval_route(
    token: str,
    request: Request,
    payload: QuickApprovalDecisionRequest | None = Body(default=None),
):
    """
    Approve one pending PTO request through the public quick-approval link.

    Example request:
        POST /api/leavesphere/v1/public/approval/v1.eyJ...token.../approve
        {"reason": "Approved after reviewing coverage"}

    Example response:
        {
          "meta": {"timestamp": "2026-06-22T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "state": "success",
            "decision": "approved",
            "message": null
          }
        }

    Requirements:
        - Public route, no login required
        - Token must be valid, signed, and unexpired
        - The approval grant can only be consumed once
    """
    try:
        payload_obj = payload or QuickApprovalDecisionRequest()
        body = payload_obj.model_dump() if hasattr(payload_obj, "model_dump") else payload_obj.dict()
        reason = str(body.get("reason") or body.get("approverNote") or "").strip() or None
        result = submit_quick_approval_decision(token=token, decision="approved", reason=reason)
        if result.get("state") != "success":
            status_map = {"invalid": 404, "expired": 410, "already_handled": 409}
            raise HTTPException(status_code=status_map.get(str(result.get("state") or ""), 400), detail=result.get("message") or "Quick approval failed")
        return result
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() or "invalid" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/{token}/reject")
def reject_quick_approval_route(
    token: str,
    request: Request,
    payload: QuickApprovalDecisionRequest | None = Body(default=None),
):
    """
    Reject one pending PTO request through the public quick-approval link.

    Example request:
        POST /api/leavesphere/v1/public/approval/v1.eyJ...token.../reject
        {"reason": "Coverage is tight during this period"}

    Example response:
        {
          "meta": {"timestamp": "2026-06-22T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "state": "success",
            "decision": "rejected",
            "message": null
          }
        }

    Requirements:
        - Public route, no login required
        - Token must be valid, signed, and unexpired
        - The approval grant can only be consumed once
    """
    try:
        payload_obj = payload or QuickApprovalDecisionRequest()
        body = payload_obj.model_dump() if hasattr(payload_obj, "model_dump") else payload_obj.dict()
        reason = str(body.get("reason") or body.get("approverNote") or "").strip() or None
        result = submit_quick_approval_decision(token=token, decision="rejected", reason=reason)
        if result.get("state") != "success":
            status_map = {"invalid": 404, "expired": 410, "already_handled": 409}
            raise HTTPException(status_code=status_map.get(str(result.get("state") or ""), 400), detail=result.get("message") or "Quick approval failed")
        return result
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() or "invalid" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
