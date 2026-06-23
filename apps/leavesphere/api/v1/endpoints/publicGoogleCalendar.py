from __future__ import annotations

import json

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from apps.leavesphere.api.v1.helpers.googleCalendarOAuth import (
    handle_google_calendar_oauth_callback,
)
from shared.requestValidation import allow_unknown_query_params

router = APIRouter(prefix="/v1/public/google-calendar/oauth")


def _build_callback_popup_response(*, status: str, message: str, detail: str | None = None) -> HTMLResponse:
    payload: dict[str, object] = {
        "type": "leavesphere-google-calendar-oauth",
        "status": status,
        "message": message,
    }
    if detail:
        payload["detail"] = detail

    payload_json = json.dumps(payload, ensure_ascii=False)
    html = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>LeaveSphere Google Calendar</title>
    <style>
      body {{
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        background: #f8fafc;
        color: #0f172a;
        font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }}
      .card {{
        width: min(92vw, 34rem);
        border: 1px solid #dbeafe;
        border-radius: 1rem;
        background: white;
        box-shadow: 0 12px 32px rgba(15, 23, 42, 0.08);
        padding: 1.25rem 1.5rem;
      }}
      .title {{
        font-size: 1.1rem;
        font-weight: 700;
        margin: 0 0 0.5rem;
      }}
      .message {{
        margin: 0;
        color: #334155;
        line-height: 1.5;
      }}
      .actions {{
        margin-top: 1rem;
        display: flex;
        flex-wrap: wrap;
        gap: 0.75rem;
      }}
      a {{
        color: #1d4ed8;
        text-decoration: none;
        font-weight: 600;
      }}
    </style>
  </head>
  <body>
    <div class="card">
      <h1 class="title">LeaveSphere Google Calendar</h1>
      <p id="message" class="message">Completing connection...</p>
      <div class="actions">
        <a href="/leavesphere/leave-management">Back to Leave Management</a>
      </div>
    </div>
    <script>
      (function () {{
        const payload = {payload_json};
        const messageNode = document.getElementById("message");
        if (messageNode) {{
          messageNode.textContent = payload.message || "Completed.";
        }}
        try {{
          if (window.opener && !window.opener.closed) {{
            window.opener.postMessage(payload, window.location.origin);
          }}
        }} catch (error) {{}}
        window.setTimeout(function () {{
          try {{
            window.close();
          }} catch (error) {{}}
        }}, 150);
      }})();
    </script>
  </body>
</html>
"""
    return HTMLResponse(content=html, status_code=200)


@router.get("/callback")
@allow_unknown_query_params
def handle_google_calendar_oauth_callback_route(
    request: Request,
    code: str | None = Query(None),
    state: str | None = Query(None),
    error: str | None = Query(None),
    error_description: str | None = Query(None, alias="error_description"),
):
    """
    Finish the Google OAuth callback and persist the tenant refresh token.

    Example request:
        GET /api/leavesphere/v1/public/google-calendar/oauth/callback?code=4/0Acv...&state=v1.eyJ...token...

    Example request (authorization denied):
        GET /api/leavesphere/v1/public/google-calendar/oauth/callback?error=access_denied&error_description=The%20user%20denied%20access

    Example response:
        Returns a short-lived popup page that notifies the opener and closes itself.

    Requirements:
        - Public route, no login required
        - The `state` parameter must be the signed value created by the admin OAuth start route
        - The callback is one-time use; replayed states are rejected
        - A refresh token must be returned by Google on the initial consent, or the app must already have a stored refresh token for the tenant
        - Google may append extra query params such as `iss` and `scope`; those are ignored by the route
        - The page is intended to run in a popup or new tab opened by the LeaveSphere admin UI
    """
    try:
        handle_google_calendar_oauth_callback(
            request=request,
            code=str(code or "").strip(),
            state=str(state or "").strip(),
            error=str(error or "").strip() or None,
            error_description=str(error_description or "").strip() or None,
        )
        return _build_callback_popup_response(
            status="connected",
            message="Google Calendar connected. You can close this tab.",
        )
    except ValueError as exc:
        return _build_callback_popup_response(
            status="error",
            message=str(exc),
            detail=str(exc),
        )
