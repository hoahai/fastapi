from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any
from uuid import uuid4
from urllib.parse import urlencode
from urllib.request import Request as UrlRequest, urlopen

from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build

from shared.auth.supabase_client import SupabaseClientError, supabase_client
from shared.auth.signed_token import sign_json_token, verify_json_token
from shared.tenantDataCache import (
    delete_tenant_shared_cache_values_by_prefix,
    get_tenant_shared_cache_value,
    set_tenant_shared_cache_value,
)

GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
GOOGLE_AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_REVOCATION_URI = "https://oauth2.googleapis.com/revoke"


@dataclass(frozen=True)
class GoogleOAuthConfig:
    client_id: str = ""
    client_secret: str = ""
    redirect_uri: str = ""
    scopes: tuple[str, ...] = ()
    state_secret: str = ""
    state_bucket: str = ""
    connection_bucket: str = ""
    connection_table: str = ""
    connection_key: str = "primary"
    state_ttl_seconds: int = 600
    state_purpose: str = "google.oauth"
    service_name: str = "calendar"
    service_version: str = "v3"


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _normalize_scope_list(raw: object | None, *, default_scopes: tuple[str, ...]) -> tuple[str, ...]:
    if raw is None:
        return default_scopes

    values: list[object]
    if isinstance(raw, (list, tuple, set)):
        values = list(raw)
    else:
        text = _normalize_text(raw)
        if not text:
            return default_scopes
        if text.startswith("[") or text.startswith("(") or text.startswith("{"):
            try:
                parsed = json.loads(text)
            except Exception:
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

    return tuple(normalized) if normalized else default_scopes


def _normalize_supabase_scopes(raw: object | None) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        values = list(raw)
    else:
        text = _normalize_text(raw)
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = text
        if isinstance(parsed, (list, tuple, set)):
            values = list(parsed)
        else:
            values = [parsed]

    normalized: list[str] = []
    seen: set[str] = set()
    for item in values:
        scope = _normalize_text(item)
        if not scope or scope in seen:
            continue
        seen.add(scope)
        normalized.append(scope)
    return normalized


def _has_supabase_connection_storage(config: GoogleOAuthConfig) -> bool:
    return bool(_normalize_text(config.connection_table))


def _is_missing_supabase_table_error(exc: Exception, *, table: str) -> bool:
    message = str(exc).lower()
    normalized_table = _normalize_text(table).lower()
    return normalized_table and (
        f"could not find the table 'public.{normalized_table}'" in message
        or f"could not find the table '{normalized_table}'" in message
        or f"relation \"public.{normalized_table}\" does not exist" in message
        or f"relation \"{normalized_table}\" does not exist" in message
        or f"table '{normalized_table}' not found" in message
    )


def _connection_record_to_storage_row(*, tenant_id: str, config: GoogleOAuthConfig, record: dict[str, Any]) -> dict[str, Any]:
    scopes = record.get("scopes")
    if not isinstance(scopes, list):
        scopes = []
    normalized_scopes = _normalize_supabase_scopes(scopes)
    now_text = datetime.now(timezone.utc).isoformat()
    return {
        "tenant_id": tenant_id,
        "connection_key": _normalize_text(config.connection_key) or "primary",
        "client_id": _normalize_text(record.get("clientId") or config.client_id),
        "token_uri": _normalize_text(record.get("tokenUri") or GOOGLE_TOKEN_URI),
        "refresh_token": _normalize_text(record.get("refreshToken")),
        "scopes": normalized_scopes or list(config.scopes),
        "connected_at": record.get("connectedAt") or now_text,
        "last_authorized_at": record.get("lastAuthorizedAt") or now_text,
        "updated_at": now_text,
    }


def _connection_row_to_record(row: dict[str, Any], *, config: GoogleOAuthConfig) -> dict[str, Any]:
    scopes = row.get("scopes")
    normalized_scopes = _normalize_supabase_scopes(scopes)
    return {
        "tenantId": _normalize_text(row.get("tenant_id")),
        "connectionKey": _normalize_text(row.get("connection_key") or config.connection_key),
        "clientId": _normalize_text(row.get("client_id") or config.client_id),
        "tokenUri": _normalize_text(row.get("token_uri") or GOOGLE_TOKEN_URI),
        "refreshToken": _normalize_text(row.get("refresh_token")),
        "scopes": normalized_scopes or list(config.scopes),
        "connectedAt": row.get("connected_at"),
        "lastAuthorizedAt": row.get("last_authorized_at"),
    }


def build_google_oauth_flow(config: GoogleOAuthConfig) -> Flow:
    client_config = {
        "web": {
            "client_id": config.client_id,
            "client_secret": config.client_secret,
            "auth_uri": GOOGLE_AUTH_URI,
            "token_uri": GOOGLE_TOKEN_URI,
            "redirect_uris": [config.redirect_uri],
        }
    }
    return Flow.from_client_config(
        client_config,
        scopes=list(config.scopes),
        redirect_uri=config.redirect_uri,
    )


def _state_cache_key(*, jti: str) -> str:
    return f"oauth_state::{jti}"


def _read_connection_record(*, tenant_id: str, config: GoogleOAuthConfig) -> dict[str, Any] | None:
    if _has_supabase_connection_storage(config):
        table = _normalize_text(config.connection_table)
        try:
            row = supabase_client.select_single_query(
                table=table,
                query={
                    "tenant_id": f"eq.{tenant_id}",
                    "connection_key": f"eq.{_normalize_text(config.connection_key) or 'primary'}",
                },
                select=(
                    "tenant_id,connection_key,client_id,token_uri,refresh_token,scopes,"
                    "connected_at,last_authorized_at,created_at,updated_at"
                ),
            )
        except SupabaseClientError as exc:
            if not _is_missing_supabase_table_error(exc, table=table):
                raise
            row = None
        if not row or not isinstance(row, dict):
            # Fallback for environments where the Supabase migration has not
            # been applied yet. The cache path keeps the app usable and avoids
            # 500s during rollout.
            value, found = get_tenant_shared_cache_value(
                bucket=config.connection_bucket,
                cache_key=config.connection_key,
                ttl_seconds=0,
                tenant_id=tenant_id,
            )
            if not found or not isinstance(value, dict):
                return None
            return value
        return _connection_row_to_record(row, config=config)

    value, found = get_tenant_shared_cache_value(
        bucket=config.connection_bucket,
        cache_key=config.connection_key,
        ttl_seconds=0,
        tenant_id=tenant_id,
    )
    if not found or not isinstance(value, dict):
        return None
    return value


def _write_connection_record(*, tenant_id: str, config: GoogleOAuthConfig, record: dict[str, Any]) -> None:
    if _has_supabase_connection_storage(config):
        table = _normalize_text(config.connection_table)
        if not table:
            raise ValueError("connection_table is required")
        row = _connection_record_to_storage_row(tenant_id=tenant_id, config=config, record=record)
        patch_row = {
            "client_id": row["client_id"],
            "token_uri": row["token_uri"],
            "refresh_token": row["refresh_token"],
            "scopes": row["scopes"],
            "connected_at": row["connected_at"],
            "last_authorized_at": row["last_authorized_at"],
            "updated_at": row["updated_at"],
        }
        try:
            existing = supabase_client.select_single_query(
                table=table,
                query={
                    "tenant_id": f"eq.{tenant_id}",
                    "connection_key": f"eq.{row['connection_key']}",
                },
                select="tenant_id,connection_key",
            )
            if existing:
                supabase_client.patch_rows(
                    table=table,
                    filters={
                        "tenant_id": tenant_id,
                        "connection_key": row["connection_key"],
                    },
                    patch=patch_row,
                )
            else:
                supabase_client.insert_row(table=table, row=row)
            return
        except SupabaseClientError as exc:
            if not _is_missing_supabase_table_error(exc, table=table):
                raise
            # Fallback to cache until the Supabase migration is available in
            # the target environment.
            pass

    set_tenant_shared_cache_value(
        bucket=config.connection_bucket,
        cache_key=config.connection_key,
        value=record,
        tenant_id=tenant_id,
    )


def get_google_oauth_connection_status(*, tenant_id: str | None, config: GoogleOAuthConfig) -> dict[str, Any]:
    normalized_tenant_id = _normalize_text(tenant_id)
    if not normalized_tenant_id:
        return {"connected": False}

    record = _read_connection_record(tenant_id=normalized_tenant_id, config=config)
    if not record:
        return {"connected": False}

    return {
        "connected": True,
        "connectedAt": record.get("connectedAt"),
        "scopes": record.get("scopes") or list(config.scopes),
    }


def build_google_oauth_authorization_url(*, tenant_id: str, config: GoogleOAuthConfig) -> dict[str, Any]:
    normalized_tenant_id = _normalize_text(tenant_id)
    if not normalized_tenant_id:
        raise ValueError("tenantId is required")

    if not _normalize_text(config.client_id):
        raise ValueError("client_id is required")
    if not _normalize_text(config.client_secret):
        raise ValueError("client_secret is required")
    if not _normalize_text(config.state_secret):
        raise ValueError("state_secret is required")
    if not _normalize_text(config.redirect_uri):
        raise ValueError("redirect_uri is required")
    if not _normalize_text(config.state_bucket):
        raise ValueError("state_bucket is required")
    if not _normalize_text(config.connection_bucket) and not _has_supabase_connection_storage(config):
        raise ValueError("connection_bucket is required")

    delete_tenant_shared_cache_values_by_prefix(
        bucket=config.state_bucket,
        cache_key_prefix="oauth_state::",
        tenant_id=normalized_tenant_id,
    )

    jti = uuid4().hex
    now = int(datetime.now(timezone.utc).timestamp())
    state_claims = {
        "tenantId": normalized_tenant_id,
        "purpose": config.state_purpose,
        "jti": jti,
        "iat": now,
        "exp": now + max(int(config.state_ttl_seconds), 1),
    }
    set_tenant_shared_cache_value(
        bucket=config.state_bucket,
        cache_key=_state_cache_key(jti=jti),
        value=state_claims,
        tenant_id=normalized_tenant_id,
    )

    flow = build_google_oauth_flow(config)
    authorization_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
        state=sign_json_token(payload=state_claims, secret=config.state_secret),
    )
    return {
        "authorizationUrl": authorization_url,
        "redirectUri": config.redirect_uri,
        "scopes": list(config.scopes),
        "connection": get_google_oauth_connection_status(
            tenant_id=normalized_tenant_id,
            config=config,
        ),
    }


def _consume_state_claims(*, tenant_id: str, jti: str, config: GoogleOAuthConfig) -> bool:
    removed = delete_tenant_shared_cache_values_by_prefix(
        bucket=config.state_bucket,
        cache_key_prefix=_state_cache_key(jti=jti),
        tenant_id=tenant_id,
    )
    return removed > 0


def handle_google_oauth_callback(
    *,
    code: str,
    state: str,
    config: GoogleOAuthConfig,
    error: str | None = None,
    error_description: str | None = None,
) -> dict[str, Any]:
    normalized_state = _normalize_text(state)
    claims: dict[str, Any] | None = None
    tenant_id = ""

    if normalized_state:
        claims = verify_json_token(token=normalized_state, secret=config.state_secret)
        tenant_id = _normalize_text(claims.get("tenantId"))
        purpose = _normalize_text(claims.get("purpose"))
        jti = _normalize_text(claims.get("jti"))
        exp = int(claims.get("exp") or 0)
        now = int(datetime.now(timezone.utc).timestamp())
        if purpose != config.state_purpose:
            raise ValueError("Invalid OAuth state")
        if not tenant_id or not jti:
            raise ValueError("Invalid OAuth state")
        if exp <= now:
            raise ValueError("OAuth state expired")
        if not _consume_state_claims(tenant_id=tenant_id, jti=jti, config=config):
            raise ValueError("OAuth state is invalid or has already been used")

    if error:
        message = _normalize_text(error_description) or _normalize_text(error) or "Google authorization failed"
        raise ValueError(message)
    if not normalized_state:
        raise ValueError("state is required")
    if claims is None:
        raise ValueError("state is required")

    normalized_code = _normalize_text(code)
    if not normalized_code:
        raise ValueError("code is required")

    flow = build_google_oauth_flow(config)
    flow.fetch_token(code=normalized_code)
    credentials = flow.credentials
    if not isinstance(credentials, Credentials):
        raise ValueError("Google OAuth credentials could not be created")

    existing_record = _read_connection_record(tenant_id=tenant_id, config=config) or {}
    refresh_token = _normalize_text(credentials.refresh_token) or _normalize_text(existing_record.get("refreshToken"))
    if not refresh_token:
        raise ValueError("Google did not return a refresh token. Re-authorize with consent prompt.")

    scopes = tuple(
        str(scope).strip()
        for scope in (credentials.scopes or config.scopes)
        if str(scope).strip()
    ) or config.scopes
    now_text = datetime.now(timezone.utc).isoformat()
    record = {
        "tenantId": tenant_id,
        "clientId": config.client_id,
        "tokenUri": credentials.token_uri or GOOGLE_TOKEN_URI,
        "refreshToken": refresh_token,
        "scopes": list(scopes),
        "connectedAt": existing_record.get("connectedAt") or now_text,
        "lastAuthorizedAt": now_text,
    }
    _write_connection_record(tenant_id=tenant_id, config=config, record=record)
    return {
        "connected": True,
        "tenantId": tenant_id,
        "redirectUri": config.redirect_uri,
        "scopes": list(scopes),
        "connection": get_google_oauth_connection_status(tenant_id=tenant_id, config=config),
    }


def clear_google_oauth_connection(*, tenant_id: str | None, config: GoogleOAuthConfig) -> int:
    normalized_tenant_id = _normalize_text(tenant_id)
    if not normalized_tenant_id:
        return 0
    if _has_supabase_connection_storage(config):
        table = _normalize_text(config.connection_table)
        if not table:
            return 0
        try:
            removed_rows = supabase_client.delete_rows(
                table=table,
                filters={
                    "tenant_id": normalized_tenant_id,
                    "connection_key": _normalize_text(config.connection_key) or "primary",
                },
            )
            return len(removed_rows)
        except SupabaseClientError as exc:
            if not _is_missing_supabase_table_error(exc, table=table):
                raise
    return delete_tenant_shared_cache_values_by_prefix(
        bucket=config.connection_bucket,
        cache_key_prefix=config.connection_key,
        tenant_id=normalized_tenant_id,
    )


def revoke_google_oauth_connection(*, tenant_id: str | None, config: GoogleOAuthConfig) -> dict[str, Any]:
    normalized_tenant_id = _normalize_text(tenant_id)
    result: dict[str, Any] = {
        "connected": False,
        "revokedRemote": False,
        "removed": 0,
        "revocationError": None,
    }
    if not normalized_tenant_id:
        return result

    record = _read_connection_record(tenant_id=normalized_tenant_id, config=config)
    refresh_token = _normalize_text((record or {}).get("refreshToken"))
    if refresh_token:
        try:
            body = urlencode({"token": refresh_token}).encode("utf-8")
            request = UrlRequest(
                GOOGLE_REVOCATION_URI,
                data=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            with urlopen(request, timeout=10):
                pass
            result["revokedRemote"] = True
        except Exception as exc:  # pragma: no cover - network / transport failures
            result["revocationError"] = str(exc)

    result["removed"] = clear_google_oauth_connection(tenant_id=normalized_tenant_id, config=config)
    return result


def get_google_oauth_credentials(*, tenant_id: str | None, config: GoogleOAuthConfig) -> Credentials | None:
    normalized_tenant_id = _normalize_text(tenant_id)
    if not normalized_tenant_id:
        return None

    record = _read_connection_record(tenant_id=normalized_tenant_id, config=config)
    if not record:
        return None

    refresh_token = _normalize_text(record.get("refreshToken"))
    if not refresh_token:
        return None

    scopes = record.get("scopes")
    if not isinstance(scopes, list) or not scopes:
        scopes = list(config.scopes)

    credentials = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri=str(record.get("tokenUri") or GOOGLE_TOKEN_URI),
        client_id=config.client_id,
        client_secret=config.client_secret,
        scopes=[str(scope).strip() for scope in scopes if str(scope).strip()],
    )
    if not credentials.valid:
        credentials.refresh(GoogleAuthRequest())
    return credentials


def build_google_service(*, tenant_id: str | None, config: GoogleOAuthConfig):
    credentials = get_google_oauth_credentials(tenant_id=tenant_id, config=config)
    if credentials is None:
        raise ValueError("Google service is not connected for this tenant")
    return build(
        config.service_name,
        config.service_version,
        credentials=credentials,
        cache_discovery=False,
    )
