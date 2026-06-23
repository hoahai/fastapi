from __future__ import annotations

import ast
import json
import os
from pathlib import Path

from shared.auth.signed_token import decode_json_token_payload_unverified
from shared.tenant import TenantConfigError, get_app_scoped_env, get_env, reset_tenant_context, set_tenant_context
from shared.google_oauth import (
    GoogleOAuthConfig,
    build_google_service,
)
from shared.google_calendar import (
    create_google_calendar_event as _create_google_calendar_event,
    delete_google_calendar_event as _delete_google_calendar_event,
    get_google_calendar_event as _get_google_calendar_event,
    list_google_calendar_events as _list_google_calendar_events,
    patch_google_calendar_event as _patch_google_calendar_event,
    update_google_calendar_event as _update_google_calendar_event,
)
from shared.google_oauth import (
    build_google_oauth_authorization_url as _build_google_oauth_authorization_url,
    clear_google_oauth_connection as _clear_google_oauth_connection,
    get_google_oauth_connection_status as _get_google_oauth_connection_status,
    get_google_oauth_credentials as _get_google_oauth_credentials,
    handle_google_oauth_callback as _handle_google_oauth_callback,
    revoke_google_oauth_connection as _revoke_google_oauth_connection,
)

GOOGLE_CALENDAR_CALLBACK_PATH = "/api/leavesphere/v1/public/google-calendar/oauth/callback"
GOOGLE_CALENDAR_STATE_BUCKET = "leavesphere_google_calendar_state"
GOOGLE_CALENDAR_CONNECTION_BUCKET = "leavesphere_google_calendar"
GOOGLE_CALENDAR_CONNECTION_TABLE = "google_oauth_connections"
GOOGLE_CALENDAR_CONNECTION_KEY = "leavesphere_google_calendar"
GOOGLE_CALENDAR_ID_ENV_KEY = "ggCalendarId"
DEFAULT_GOOGLE_CALENDAR_SCOPES = (
    "https://www.googleapis.com/auth/calendar.events.owned",
)
_REPO_ROOT = Path(__file__).resolve().parents[5]


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _parse_google_calendar_config(raw: object | None) -> dict[str, object]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw

    text = _normalize_text(raw)
    if not text:
        return {}

    try:
        parsed = json.loads(text)
    except Exception:
        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            return {}

    return parsed if isinstance(parsed, dict) else {}


def _get_tenant_google_calendar_block() -> dict[str, object]:
    tenant_block = _parse_google_calendar_config(
        get_app_scoped_env("leavesphere", "GOOGLE_CALENDAR")
        or os.getenv("LEAVESPHERE_GOOGLE_CALENDAR")
        or get_env("LEAVESPHERE_GOOGLE_CALENDAR")
    )
    if not tenant_block:
        return {}
    google_calendar = tenant_block.get("google_calendar")
    return google_calendar if isinstance(google_calendar, dict) else tenant_block


def _resolve_local_secret_path(raw_path: object | None) -> str:
    text = _normalize_text(raw_path)
    if not text:
        return ""

    candidate = Path(text).expanduser()
    if candidate.is_file():
        return str(candidate)

    if not candidate.is_absolute():
        for base in (Path("/etc/secrets"), _REPO_ROOT / "etc" / "secrets"):
            resolved = base / candidate
            if resolved.is_file():
                return str(resolved)

    return ""


def _load_google_calendar_client_file_config() -> dict[str, str]:
    tenant_config = _get_tenant_google_calendar_block()
    raw_file_path = (
        tenant_config.get("json_key_file_path")
        or tenant_config.get("client_secret_file_path")
        or tenant_config.get("credentials_file_path")
        or tenant_config.get("GOOGLE_APPLICATION_CREDENTIALS")
        or os.getenv("LEAVESPHERE_GOOGLE_CALENDAR_CLIENT_SECRET_FILE_PATH")
        or os.getenv("GOOGLE_CALENDAR_CLIENT_SECRET_FILE_PATH")
        or os.getenv("LEAVESPHERE_GOOGLE_CALENDAR_CREDENTIALS_FILE_PATH")
        or os.getenv("GOOGLE_CALENDAR_CREDENTIALS_FILE_PATH")
    )
    file_path = _resolve_local_secret_path(raw_file_path)
    if not file_path:
        if _normalize_text(raw_file_path):
            raise ValueError(f"Google Calendar client secret file not found: {raw_file_path}")
        return {}

    with open(file_path, encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Google Calendar client secret file must contain an object: {file_path}")

    raw_client = payload.get("web") if isinstance(payload.get("web"), dict) else payload.get("installed")
    if raw_client is None:
        raw_client = payload
    if not isinstance(raw_client, dict):
        raise ValueError(f"Google Calendar client secret file must contain web/installed credentials: {file_path}")

    redirect_uris = raw_client.get("redirect_uris")
    redirect_uri = ""
    if isinstance(redirect_uris, (list, tuple, set)):
        for item in redirect_uris:
            redirect_uri = _normalize_text(item)
            if redirect_uri:
                break
    if not redirect_uri:
        redirect_uri = _normalize_text(raw_client.get("redirect_uri"))

    return {
        "client_id": _normalize_text(raw_client.get("client_id")),
        "client_secret": _normalize_text(raw_client.get("client_secret")),
        "redirect_uri": redirect_uri,
    }

def _resolve_tenant_google_calendar_scopes() -> tuple[str, ...]:
    tenant_config = _get_tenant_google_calendar_block()
    raw_scopes = (
        tenant_config.get("scopes")
        or tenant_config.get("scope")
        or tenant_config.get("GOOGLE_CALENDAR_SCOPES")
        or os.getenv("LEAVESPHERE_GOOGLE_CALENDAR_SCOPES")
        or os.getenv("GOOGLE_CALENDAR_SCOPES")
    )
    return _normalize_scope_list(raw_scopes)


def _normalize_scope_list(raw: object | None) -> tuple[str, ...]:
    if raw is None:
        return DEFAULT_GOOGLE_CALENDAR_SCOPES

    values: list[object]
    if isinstance(raw, (list, tuple, set)):
        values = list(raw)
    else:
        text = _normalize_text(raw)
        if not text:
            return DEFAULT_GOOGLE_CALENDAR_SCOPES
        if text.startswith("[") or text.startswith("(") or text.startswith("{"):
            try:
                parsed = json.loads(text)
            except Exception:
                try:
                    parsed = ast.literal_eval(text)
                except (ValueError, SyntaxError):
                    parsed = text
            if isinstance(parsed, (list, tuple, set)):
                values = list(parsed)
            else:
                values = [parsed]
        else:
            parts = [part.strip() for part in text.replace(";", ",").split(",")]
            values = [part for part in parts if part]

    normalized: list[str] = []
    seen: set[str] = set()
    for item in values:
        scope = _normalize_text(item)
        if not scope or scope in seen:
            continue
        seen.add(scope)
        normalized.append(scope)

    return tuple(normalized) if normalized else DEFAULT_GOOGLE_CALENDAR_SCOPES


def _resolve_redirect_uri(*, request=None) -> str:
    configured = _normalize_text(
        os.getenv("LEAVESPHERE_GOOGLE_CALENDAR_REDIRECT_URI")
        or os.getenv("GOOGLE_CALENDAR_REDIRECT_URI")
    )
    if configured:
        return configured.rstrip("/")

    if request is None:
        raise ValueError(
            "Missing Google Calendar redirect URI. Set LEAVESPHERE_GOOGLE_CALENDAR_REDIRECT_URI."
        )

    base_url = _normalize_text(getattr(request, "base_url", ""))
    if not base_url:
        raise ValueError(
            "Missing Google Calendar redirect URI. Set LEAVESPHERE_GOOGLE_CALENDAR_REDIRECT_URI."
        )
    return f"{base_url.rstrip('/')}{GOOGLE_CALENDAR_CALLBACK_PATH}"


def _resolve_google_calendar_redirect_uri(*, request=None, file_config: dict[str, str] | None = None) -> str:
    tenant_config = _get_tenant_google_calendar_block()
    configured = _normalize_text(
        tenant_config.get("redirect_uri")
        or tenant_config.get("REDIRECT_URI")
        or tenant_config.get("callback_uri")
        or tenant_config.get("CALLBACK_URI")
        or os.getenv("LEAVESPHERE_GOOGLE_CALENDAR_REDIRECT_URI")
        or os.getenv("GOOGLE_CALENDAR_REDIRECT_URI")
    )
    if configured:
        return configured.rstrip("/")

    if request is not None:
        return _resolve_redirect_uri(request=request)

    fallback = _normalize_text((file_config or {}).get("redirect_uri"))
    if fallback:
        return fallback.rstrip("/")

    return ""


def get_google_calendar_id() -> str:
    tenant_config = _get_tenant_google_calendar_block()
    return _normalize_text(
        tenant_config.get("ggCalendarId")
        or tenant_config.get("calendarId")
        or get_app_scoped_env("leavesphere", GOOGLE_CALENDAR_ID_ENV_KEY)
        or os.getenv("LEAVESPHERE_GGCALENDARID")
        or os.getenv("GGCALENDARID")
    )


def resolve_google_calendar_oauth_settings(*, request=None) -> GoogleOAuthConfig:
    file_config = _load_google_calendar_client_file_config()
    client_id = _normalize_text(
        os.getenv("LEAVESPHERE_GOOGLE_CALENDAR_CLIENT_ID")
        or os.getenv("GOOGLE_CALENDAR_CLIENT_ID")
        or file_config.get("client_id")
    )
    client_secret = _normalize_text(
        os.getenv("LEAVESPHERE_GOOGLE_CALENDAR_CLIENT_SECRET")
        or os.getenv("GOOGLE_CALENDAR_CLIENT_SECRET")
        or file_config.get("client_secret")
    )
    redirect_uri = _resolve_google_calendar_redirect_uri(request=request, file_config=file_config)
    scopes = _resolve_tenant_google_calendar_scopes()
    state_secret = _normalize_text(
        os.getenv("LEAVESPHERE_GOOGLE_CALENDAR_STATE_SECRET")
        or os.getenv("GOOGLE_CALENDAR_STATE_SECRET")
        or client_secret
    )

    missing: list[str] = []
    if not client_id:
        missing.append("LEAVESPHERE_GOOGLE_CALENDAR_CLIENT_ID")
    if not client_secret:
        missing.append("LEAVESPHERE_GOOGLE_CALENDAR_CLIENT_SECRET")
    if not state_secret:
        missing.append("LEAVESPHERE_GOOGLE_CALENDAR_STATE_SECRET")
    if missing:
        raise ValueError(
            "Missing Google Calendar OAuth config: " + ", ".join(missing)
        )

    return GoogleOAuthConfig(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scopes=scopes,
        state_secret=state_secret,
        state_bucket=GOOGLE_CALENDAR_STATE_BUCKET,
        connection_bucket=GOOGLE_CALENDAR_CONNECTION_BUCKET,
        connection_table=GOOGLE_CALENDAR_CONNECTION_TABLE,
        connection_key=GOOGLE_CALENDAR_CONNECTION_KEY,
        state_purpose="leavesphere.google_calendar.oauth",
        service_name="calendar",
        service_version="v3",
    )


def _resolve_google_calendar_storage_settings() -> GoogleOAuthConfig:
    scopes = _resolve_tenant_google_calendar_scopes()
    return GoogleOAuthConfig(
        scopes=scopes,
        state_bucket=GOOGLE_CALENDAR_STATE_BUCKET,
        connection_bucket=GOOGLE_CALENDAR_CONNECTION_BUCKET,
        connection_table=GOOGLE_CALENDAR_CONNECTION_TABLE,
        connection_key=GOOGLE_CALENDAR_CONNECTION_KEY,
        state_purpose="leavesphere.google_calendar.oauth",
        service_name="calendar",
        service_version="v3",
    )


def _resolve_google_calendar_credentials_settings() -> GoogleOAuthConfig:
    file_config = _load_google_calendar_client_file_config()
    client_id = _normalize_text(
        os.getenv("LEAVESPHERE_GOOGLE_CALENDAR_CLIENT_ID")
        or os.getenv("GOOGLE_CALENDAR_CLIENT_ID")
        or file_config.get("client_id")
    )
    client_secret = _normalize_text(
        os.getenv("LEAVESPHERE_GOOGLE_CALENDAR_CLIENT_SECRET")
        or os.getenv("GOOGLE_CALENDAR_CLIENT_SECRET")
        or file_config.get("client_secret")
    )
    redirect_uri = _resolve_google_calendar_redirect_uri(file_config=file_config)
    scopes = _resolve_tenant_google_calendar_scopes()
    if not client_id or not client_secret:
        raise ValueError(
            "Missing Google Calendar OAuth config: LEAVESPHERE_GOOGLE_CALENDAR_CLIENT_ID, LEAVESPHERE_GOOGLE_CALENDAR_CLIENT_SECRET"
        )

    return GoogleOAuthConfig(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scopes=scopes,
        state_bucket=GOOGLE_CALENDAR_STATE_BUCKET,
        connection_bucket=GOOGLE_CALENDAR_CONNECTION_BUCKET,
        connection_table=GOOGLE_CALENDAR_CONNECTION_TABLE,
        connection_key=GOOGLE_CALENDAR_CONNECTION_KEY,
        state_purpose="leavesphere.google_calendar.oauth",
        service_name="calendar",
        service_version="v3",
    )


def get_google_calendar_connection_status(*, tenant_id: str | None = None) -> dict[str, object]:
    return _get_google_oauth_connection_status(
        tenant_id=tenant_id,
        config=_resolve_google_calendar_storage_settings(),
    )


def build_google_calendar_oauth_authorization_url(*, request, tenant_id: str) -> dict[str, object]:
    return _build_google_oauth_authorization_url(
        tenant_id=tenant_id,
        config=resolve_google_calendar_oauth_settings(request=request),
    )


def handle_google_calendar_oauth_callback(
    *,
    code: str,
    state: str,
    error: str | None = None,
    error_description: str | None = None,
    request=None,
) -> dict[str, object]:
    bootstrap_token = None
    try:
        bootstrap_claims = decode_json_token_payload_unverified(token=state)
    except ValueError:
        bootstrap_claims = {}

    tenant_id = _normalize_text(bootstrap_claims.get("tenantId"))
    if tenant_id:
        try:
            bootstrap_token = set_tenant_context(tenant_id)
        except TenantConfigError:
            bootstrap_token = None

    try:
        return _handle_google_oauth_callback(
            code=code,
            state=state,
            config=resolve_google_calendar_oauth_settings(request=request),
            error=error,
            error_description=error_description,
        )
    finally:
        if bootstrap_token is not None:
            reset_tenant_context(bootstrap_token)


def clear_google_calendar_connection(*, tenant_id: str | None = None) -> int:
    return _clear_google_oauth_connection(
        tenant_id=tenant_id,
        config=_resolve_google_calendar_storage_settings(),
    )


def revoke_google_calendar_connection(*, tenant_id: str | None = None) -> dict[str, object]:
    return _revoke_google_oauth_connection(
        tenant_id=tenant_id,
        config=_resolve_google_calendar_storage_settings(),
    )


def get_google_calendar_credentials(*, tenant_id: str | None = None):
    return _get_google_oauth_credentials(
        tenant_id=tenant_id,
        config=_resolve_google_calendar_credentials_settings(),
    )


def build_google_calendar_service(*, tenant_id: str | None = None):
    return build_google_service(
        tenant_id=tenant_id,
        config=_resolve_google_calendar_credentials_settings(),
    )


def create_google_calendar_event(
    *,
    tenant_id: str | None = None,
    calendar_id: str | None = None,
    event: dict[str, object],
    send_updates: str | None = None,
):
    return _create_google_calendar_event(
        tenant_id=tenant_id,
        config=_resolve_google_calendar_credentials_settings(),
        calendar_id=calendar_id,
        event=event,
        send_updates=send_updates,
    )


def update_google_calendar_event(
    *,
    tenant_id: str | None = None,
    calendar_id: str | None = None,
    event_id: str,
    event: dict[str, object],
    send_updates: str | None = None,
):
    return _update_google_calendar_event(
        tenant_id=tenant_id,
        config=_resolve_google_calendar_credentials_settings(),
        calendar_id=calendar_id,
        event_id=event_id,
        event=event,
        send_updates=send_updates,
    )


def patch_google_calendar_event(
    *,
    tenant_id: str | None = None,
    calendar_id: str | None = None,
    event_id: str,
    event: dict[str, object],
    send_updates: str | None = None,
):
    return _patch_google_calendar_event(
        tenant_id=tenant_id,
        config=_resolve_google_calendar_credentials_settings(),
        calendar_id=calendar_id,
        event_id=event_id,
        event=event,
        send_updates=send_updates,
    )


def delete_google_calendar_event(
    *,
    tenant_id: str | None = None,
    calendar_id: str | None = None,
    event_id: str,
    send_updates: str | None = None,
):
    return _delete_google_calendar_event(
        tenant_id=tenant_id,
        config=_resolve_google_calendar_credentials_settings(),
        calendar_id=calendar_id,
        event_id=event_id,
        send_updates=send_updates,
    )


def get_google_calendar_event(
    *,
    tenant_id: str | None = None,
    calendar_id: str | None = None,
    event_id: str,
):
    return _get_google_calendar_event(
        tenant_id=tenant_id,
        config=_resolve_google_calendar_credentials_settings(),
        calendar_id=calendar_id,
        event_id=event_id,
    )


def list_google_calendar_events(
    *,
    tenant_id: str | None = None,
    calendar_id: str | None = None,
    time_min: str | None = None,
    time_max: str | None = None,
    q: str | None = None,
    max_results: int | None = None,
):
    return _list_google_calendar_events(
        tenant_id=tenant_id,
        config=_resolve_google_calendar_credentials_settings(),
        calendar_id=calendar_id,
        time_min=time_min,
        time_max=time_max,
        q=q,
        max_results=max_results,
    )
