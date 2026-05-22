from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query, Request

from apps.tradsphere.api.v1.endpoints.core.traffic import require_query_value
from apps.tradsphere.api.v1.helpers.traffic import (
    ConflictError,
    InvalidReferenceError,
    NotFoundError,
    SafeDatabaseError,
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
        - SMTP preview/send routes are intentionally not implemented in this phase
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
