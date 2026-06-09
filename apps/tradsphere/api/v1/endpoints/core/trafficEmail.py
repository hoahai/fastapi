from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query, Request

from apps.tradsphere.api.v1.endpoints.core.traffic import require_query_value
from apps.tradsphere.api.v1.helpers.traffic import (
    ConflictError,
    EmailSendError,
    InvalidReferenceError,
    NotFoundError,
    SafeDatabaseError,
    send_traffic_email_data,
    send_traffic_email_test_data,
    upsert_traffic_email_data,
)

router = APIRouter(prefix="/traffic")


@router.put("/email")
def upsert_traffic_email_route(
    request: Request,
    traffic_id: str | None = Query(None, alias="trafficId"),
    payload: dict = Body(...),
):
    """
    Upsert the single email payload row for one traffic record.

    Example request:
        PUT /api/tradsphere/v1/traffic/email?trafficId=d9c98f56-54da-4688-bc96-3d3cb6388f5d

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "trafficId": "d9c98f56-54da-4688-bc96-3d3cb6388f5d",
            "toEmails": ["traffic@example.com"],
            "sentStatus": "ready"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - trafficId query param is required and must be UUID v4
        - sentStatus supports: draft, ready, sent, failed
        - When sentStatus=ready, toEmails + subject + body are required
        - Archived traffic cannot be modified
    """
    traffic_id_value = require_query_value(traffic_id, field="trafficId")

    principal = getattr(request.state, "auth_principal", None)
    sent_by_user_id = None
    if principal is not None:
        sent_by_user_id = str(
            getattr(principal, "user_id", "") or getattr(principal, "email", "") or ""
        ).strip() or None

    try:
        return upsert_traffic_email_data(
            traffic_id=traffic_id_value,
            payload=payload,
            sent_by_user_id=sent_by_user_id,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to upsert traffic email")


@router.post("/email/send")
def send_traffic_email_route(
    request: Request,
    traffic_id: str | None = Query(None, alias="trafficId"),
    payload: dict = Body(...),
):
    """
    Send one traffic email via tenant-scoped SMTP config and persist send status.

    Example request:
        POST /api/tradsphere/v1/traffic/email/send?trafficId=d9c98f56-54da-4688-bc96-3d3cb6388f5d
        {
          "toEmails": ["traffic@example.com"],
          "ccEmails": ["manager@example.com"],
          "subject": "May 2026 Traffic",
          "body": "<p>Hello team, please see attached traffic links.</p>",
          "markSentAfterSend": true
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-26T10:00:00+07:00", "duration_ms": 5},
          "data": {
            "trafficId": "d9c98f56-54da-4688-bc96-3d3cb6388f5d",
            "email": {
              "sentStatus": "sent",
              "smtpMessageId": "1748219640.123456@example.com"
            },
            "detail": {
              "traffic": {
                "id": "d9c98f56-54da-4688-bc96-3d3cb6388f5d",
                "accountCode": "TAAA",
                "campaign": "Spring Retail Push",
                "status": "sent"
              },
              "flights": [],
              "stations": [],
              "email": {
                "sentStatus": "sent"
              },
              "summary": {
                "totalRotation": 0.0,
                "rotationWarning": true
              }
            }
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - trafficId query param is required and must be UUID v4
        - toEmails + subject + body are required
        - markSentAfterSend=true also persists traffic status as sent and returns refreshed detail
        - Tenant config must include valid tradsphere.smtp settings
        - Archived traffic cannot be modified
    """
    traffic_id_value = require_query_value(traffic_id, field="trafficId")

    principal = getattr(request.state, "auth_principal", None)
    sent_by_user_id = None
    if principal is not None:
        sent_by_user_id = str(
            getattr(principal, "user_id", "") or getattr(principal, "email", "") or ""
        ).strip() or None

    try:
        return send_traffic_email_data(
            traffic_id=traffic_id_value,
            payload=payload,
            sent_by_user_id=sent_by_user_id,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except EmailSendError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to send traffic email")


@router.post("/email/send-test")
def send_traffic_email_test_route(
    request: Request,
    traffic_id: str | None = Query(None, alias="trafficId"),
    payload: dict = Body(...),
):
    """
    Send a one-off test copy of the traffic email to a single recipient.

    Example request:
        POST /api/tradsphere/v1/traffic/email/send-test?trafficId=d9c98f56-54da-4688-bc96-3d3cb6388f5d
        {
          "toEmail": "test@example.com",
          "subject": "May 2026 Traffic",
          "body": "<p>Hello team, please see attached traffic links.</p>"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-26T10:00:00+07:00", "duration_ms": 5},
          "data": {
            "trafficId": "d9c98f56-54da-4688-bc96-3d3cb6388f5d",
            "testEmail": {
              "toEmail": "test@example.com",
              "smtpMessageId": "1748219640.123456@example.com"
            }
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - trafficId query param is required and must be UUID v4
        - toEmail + subject + body are required
        - Outbound subject is prefixed with "[Test]"
        - The sent email includes a visible test-email notice
        - This route does not persist a traffic email send status
        - Archived traffic cannot be modified
    """
    traffic_id_value = require_query_value(traffic_id, field="trafficId")

    try:
        return send_traffic_email_test_data(
            traffic_id=traffic_id_value,
            payload=payload,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except EmailSendError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to send test traffic email")
