from __future__ import annotations

from typing import Any

from googleapiclient.errors import HttpError

from shared.google_oauth import GoogleOAuthConfig, build_google_service

DEFAULT_CALENDAR_ID = "primary"
DEFAULT_SEND_UPDATES = "none"


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _normalize_calendar_id(calendar_id: str | None) -> str:
    normalized = _normalize_text(calendar_id)
    return normalized or DEFAULT_CALENDAR_ID


def _normalize_event_payload(event: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise ValueError("event must be an object")
    return dict(event)


def _execute_request(request) -> dict[str, Any]:
    try:
        response = request.execute()
    except HttpError as exc:
        raise ValueError(f"Google Calendar API request failed: {exc}") from exc
    if not isinstance(response, dict):
        return {}
    return response


def _build_events_resource(*, tenant_id: str | None, config: GoogleOAuthConfig):
    service = build_google_service(tenant_id=tenant_id, config=config)
    return service.events()


def get_google_calendar_event(
    *,
    tenant_id: str | None,
    config: GoogleOAuthConfig,
    calendar_id: str | None,
    event_id: str,
) -> dict[str, Any]:
    normalized_event_id = _normalize_text(event_id)
    if not normalized_event_id:
        raise ValueError("event_id is required")
    events = _build_events_resource(tenant_id=tenant_id, config=config)
    request = events.get(
        calendarId=_normalize_calendar_id(calendar_id),
        eventId=normalized_event_id,
    )
    return _execute_request(request)


def list_google_calendar_events(
    *,
    tenant_id: str | None,
    config: GoogleOAuthConfig,
    calendar_id: str | None,
    time_min: str | None = None,
    time_max: str | None = None,
    q: str | None = None,
    max_results: int | None = None,
) -> dict[str, Any]:
    events = _build_events_resource(tenant_id=tenant_id, config=config)
    params: dict[str, Any] = {"calendarId": _normalize_calendar_id(calendar_id)}
    if _normalize_text(time_min):
        params["timeMin"] = _normalize_text(time_min)
    if _normalize_text(time_max):
        params["timeMax"] = _normalize_text(time_max)
    if _normalize_text(q):
        params["q"] = _normalize_text(q)
    if isinstance(max_results, int) and max_results > 0:
        params["maxResults"] = max_results
    request = events.list(**params)
    return _execute_request(request)


def create_google_calendar_event(
    *,
    tenant_id: str | None,
    config: GoogleOAuthConfig,
    calendar_id: str | None,
    event: dict[str, Any],
    send_updates: str | None = None,
) -> dict[str, Any]:
    events = _build_events_resource(tenant_id=tenant_id, config=config)
    params: dict[str, Any] = {
        "calendarId": _normalize_calendar_id(calendar_id),
        "body": _normalize_event_payload(event),
    }
    normalized_send_updates = _normalize_text(send_updates)
    if normalized_send_updates:
        params["sendUpdates"] = normalized_send_updates
    request = events.insert(**params)
    return _execute_request(request)


def update_google_calendar_event(
    *,
    tenant_id: str | None,
    config: GoogleOAuthConfig,
    calendar_id: str | None,
    event_id: str,
    event: dict[str, Any],
    send_updates: str | None = None,
) -> dict[str, Any]:
    normalized_event_id = _normalize_text(event_id)
    if not normalized_event_id:
        raise ValueError("event_id is required")
    events = _build_events_resource(tenant_id=tenant_id, config=config)
    params: dict[str, Any] = {
        "calendarId": _normalize_calendar_id(calendar_id),
        "eventId": normalized_event_id,
        "body": _normalize_event_payload(event),
    }
    normalized_send_updates = _normalize_text(send_updates)
    if normalized_send_updates:
        params["sendUpdates"] = normalized_send_updates
    request = events.update(**params)
    return _execute_request(request)


def patch_google_calendar_event(
    *,
    tenant_id: str | None,
    config: GoogleOAuthConfig,
    calendar_id: str | None,
    event_id: str,
    event: dict[str, Any],
    send_updates: str | None = None,
) -> dict[str, Any]:
    normalized_event_id = _normalize_text(event_id)
    if not normalized_event_id:
        raise ValueError("event_id is required")
    events = _build_events_resource(tenant_id=tenant_id, config=config)
    params: dict[str, Any] = {
        "calendarId": _normalize_calendar_id(calendar_id),
        "eventId": normalized_event_id,
        "body": _normalize_event_payload(event),
    }
    normalized_send_updates = _normalize_text(send_updates)
    if normalized_send_updates:
        params["sendUpdates"] = normalized_send_updates
    request = events.patch(**params)
    return _execute_request(request)


def delete_google_calendar_event(
    *,
    tenant_id: str | None,
    config: GoogleOAuthConfig,
    calendar_id: str | None,
    event_id: str,
    send_updates: str | None = None,
) -> dict[str, Any]:
    normalized_event_id = _normalize_text(event_id)
    if not normalized_event_id:
        raise ValueError("event_id is required")
    events = _build_events_resource(tenant_id=tenant_id, config=config)
    params: dict[str, Any] = {
        "calendarId": _normalize_calendar_id(calendar_id),
        "eventId": normalized_event_id,
    }
    normalized_send_updates = _normalize_text(send_updates)
    if normalized_send_updates:
        params["sendUpdates"] = normalized_send_updates
    request = events.delete(**params)
    return _execute_request(request)
