from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

from apps.leavesphere.api.v1.helpers.googleCalendarOAuth import (
    build_google_calendar_oauth_authorization_url,
    get_google_calendar_connection_status,
    revoke_google_calendar_connection,
)
from apps.leavesphere.api.v1.permissions import require_leavesphere_admin

router = APIRouter(
    prefix="/admin/google-calendar",
)


@router.get("/oauth/status", dependencies=[Depends(require_leavesphere_admin)])
def get_google_calendar_oauth_status_route(request: Request):
    """
    Inspect the current Google Calendar OAuth connection for the tenant.

    Example request:
        GET /api/leavesphere/v1/admin/google-calendar/oauth/status

    Example response:
        {
          "meta": {"timestamp": "2026-06-23T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "connected": true,
            "connectedAt": "2026-06-23T09:45:00+07:00",
            "scopes": [
              "https://www.googleapis.com/auth/calendar.events.owned"
            ]
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires leavesphere.admin permission or workspace.super_admin
        - Requires valid API key or bearer token in compat mode
        - Returns only connection metadata; the refresh token is never exposed
    """
    return get_google_calendar_connection_status(tenant_id=getattr(request.state, "tenant_id", None))


@router.get("/oauth/url", dependencies=[Depends(require_leavesphere_admin)])
def get_google_calendar_oauth_url_route(request: Request):
    """
    Create a Google OAuth consent URL for the tenant's LeaveSphere calendar connection.

    Example request:
        GET /api/leavesphere/v1/admin/google-calendar/oauth/url

    Example response:
        {
          "meta": {"timestamp": "2026-06-23T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "authorizationUrl": "https://accounts.google.com/o/oauth2/v2/auth?...",
            "redirectUri": "http://localhost:8000/api/leavesphere/v1/public/google-calendar/oauth/callback",
            "scopes": [
              "https://www.googleapis.com/auth/calendar.events.owned"
            ],
            "connection": {
              "connected": false
            }
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires leavesphere.admin permission or workspace.super_admin
        - Requires valid API key or bearer token in compat mode
        - `leavesphere.google_calendar.ggCalendarId` must be configured
        - `leavesphere.google_calendar.redirect_uri` should be set to the exact Google OAuth callback URI registered in Google Cloud Console, for example `http://localhost:8000/api/leavesphere/v1/public/google-calendar/oauth/callback`
        - `leavesphere.google_calendar.json_key_file_path` must point to the downloaded OAuth client JSON file, or `LEAVESPHERE_GOOGLE_CALENDAR_CLIENT_SECRET_FILE_PATH` may be used as a fallback
        - The returned `authorizationUrl` must be opened by the browser that will complete consent
    """
    try:
        return build_google_calendar_oauth_authorization_url(
            request=request,
            tenant_id=str(getattr(request.state, "tenant_id", "") or "").strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/oauth/connection", dependencies=[Depends(require_leavesphere_admin)])
def delete_google_calendar_oauth_connection_route(request: Request):
    """
    Disconnect LeaveSphere from Google Calendar and revoke the stored refresh token.

    Example request:
        DELETE /api/leavesphere/v1/admin/google-calendar/oauth/connection

    Example response:
        {
          "meta": {"timestamp": "2026-06-23T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "connected": false,
            "revokedRemote": true,
            "removed": 1,
            "revocationError": null
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires leavesphere.admin permission or workspace.super_admin
        - Requires valid API key or bearer token in compat mode
        - Clears the tenant refresh token from local shared storage
        - Best-effort revokes the Google refresh token remotely when possible
        - Leaves existing PTO transaction history intact; future approvals will create fresh events on the connected Google Calendar
    """
    try:
        return revoke_google_calendar_connection(
            tenant_id=str(getattr(request.state, "tenant_id", "") or "").strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
