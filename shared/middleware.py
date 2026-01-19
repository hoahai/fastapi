from __future__ import annotations

import hmac
import json
import os
import time
import uuid
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import Request
from starlette.responses import JSONResponse, Response

from shared.constants import LOG_LEVEL
from shared.logger import (
    get_logger,
    set_request_id,
    reset_request_id,
    add_debug_handler,
    remove_debug_handler,
    create_request_debug_handler,
)
from shared.tenant import (
    TenantConfigError,
    set_tenant_context,
    reset_tenant_context,
    get_tenant_id,
    get_timezone,
)
_API_KEY_REGISTRY: dict[str, str] | None = None
_API_LOGGER = get_logger("api")


def _is_docs_path(path: str) -> bool:
    normalized = (path or "").rstrip("/")
    return (
        normalized.endswith("/docs")
        or normalized.endswith("/docs/oauth2-redirect")
        or normalized.endswith("/redoc")
        or normalized.endswith("/openapi.json")
    )


async def timing_middleware(request: Request, call_next):
    # Set start time BEFORE route executes
    request.state.start_time = time.perf_counter()

    response = await call_next(request)

    return response


async def tenant_context_middleware(request: Request, call_next):
    path = request.url.path or ""
    if _is_docs_path(path):
        request.state.tenant_id = None
        return await call_next(request)
    requires_tenant = path.startswith("/api") or path.startswith("/spendsphere/api")
    tenant_header = request.headers.get("x-tenant-id")
    token = None
    if not requires_tenant and not tenant_header:
        request.state.tenant_id = None
        return await call_next(request)

    if not tenant_header:
        return JSONResponse(
            status_code=400,
            content={"detail": "Missing X-Tenant-Id header"},
        )

    try:
        token = set_tenant_context(tenant_header)
        request.state.tenant_id = get_tenant_id()
        if path.startswith("/api"):
            validator = getattr(request.app.state, "tenant_validator", None)
            if callable(validator):
                validator()
    except TenantConfigError as exc:
        error_text = str(exc)
        if "Tenant config not found for" in error_text:
            match = re.search(r"Tenant config not found for '([^']+)'", error_text)
            tenant_id = match.group(1) if match else None
            detail = (
                f"Tenant ({tenant_id}) not found!"
                if tenant_id
                else "Tenant not found!"
            )
            return JSONResponse(status_code=400, content={"detail": detail})
        return JSONResponse(status_code=400, content={"detail": error_text})

    try:
        return await call_next(request)
    finally:
        if token:
            reset_tenant_context(token)


def _parse_api_key_registry(raw: str) -> dict[str, str]:
    registry: dict[str, str] = {}
    entries = [entry.strip() for entry in raw.split(",") if entry.strip()]

    for entry in entries:
        if ":" not in entry:
            raise ValueError("Invalid API_KEY_REGISTRY entry format")

        client_id, api_key = entry.split(":", 1)
        client_id = client_id.strip()
        api_key = api_key.strip()

        if not client_id or not api_key:
            raise ValueError("Invalid API_KEY_REGISTRY entry format")

        if client_id in registry:
            raise ValueError(f"Duplicate client_id in API_KEY_REGISTRY: {client_id}")

        registry[client_id] = api_key

    return registry


def _get_api_key_registry() -> dict[str, str]:
    global _API_KEY_REGISTRY

    if _API_KEY_REGISTRY is not None:
        return _API_KEY_REGISTRY

    raw = os.getenv("API_KEY_REGISTRY", "").strip()
    _API_KEY_REGISTRY = _parse_api_key_registry(raw) if raw else {}
    return _API_KEY_REGISTRY


def _extract_api_key(request: Request) -> str | None:
    api_key = request.headers.get("x-api-key")
    if api_key:
        return api_key.strip() or None

    auth_header = request.headers.get("authorization")
    if auth_header and auth_header.lower().startswith("bearer "):
        token = auth_header[7:].strip()
        return token or None

    return None


def _match_api_key(api_key: str, registry: dict[str, str]) -> str | None:
    for client_id, expected in registry.items():
        if hmac.compare_digest(api_key, expected):
            return client_id
    return None


def _log_api_request(
    *,
    method: str,
    path: str,
    status_code: int,
    duration_ms: int,
    client_id: str | None,
    tenant_id: str | None,
    request_host: str | None,
    request_scheme: str | None,
    error: str | None = None,
) -> None:
    payload: dict[str, object] = {
        "event": "api_request",
        "method": method,
        "path": path,
        "status_code": status_code,
        "duration_ms": duration_ms,
        "client_id": client_id,
        "tenant_id": tenant_id,
        "request_host": request_host,
        "request_scheme": request_scheme,
        "timestamp": datetime.now(ZoneInfo(get_timezone())).isoformat(),
    }

    if error:
        payload["error"] = error

    _API_LOGGER.info(
        "API request",
        extra={
            "extra_fields": payload,
        },
    )


def _safe_parse_json(payload: bytes | str) -> object | None:
    try:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _safe_decode_text(payload: bytes) -> str:
    return payload.decode("utf-8", errors="replace")


async def request_response_logger_middleware(request: Request, call_next):
    start_time = time.perf_counter()
    request_id = uuid.uuid4().hex[:8]
    request.state.request_id = request_id
    token = set_request_id(request_id)
    debug_handler = None

    if LOG_LEVEL == "DEBUG":
        debug_handler = create_request_debug_handler(request_id)
        add_debug_handler(debug_handler)

    try:
        body_bytes = await request.body()

        async def receive() -> dict:
            return {"type": "http.request", "body": body_bytes, "more_body": False}

        request._receive = receive  # type: ignore[attr-defined]

        response = await call_next(request)

        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk

        response_headers = dict(response.headers)
        response_headers.pop("content-length", None)

        response = Response(
            content=response_body,
            status_code=response.status_code,
            headers=response_headers,
            media_type=response.media_type,
            background=response.background,
        )

        duration_ms = int((time.perf_counter() - start_time) * 1000)
        content_type = response.headers.get("content-type", "")

        if body_bytes:
            request_json = _safe_parse_json(body_bytes)
            request_body = (
                request_json
                if request_json is not None
                else _safe_decode_text(body_bytes)
            )
        else:
            request_body = None

        if response_body:
            response_json = (
                _safe_parse_json(response_body)
                if "application/json" in content_type
                else None
            )
            response_body_out = (
                response_json
                if response_json is not None
                else _safe_decode_text(response_body)
            )
        else:
            response_body_out = None

        request_host = request.headers.get("x-forwarded-host") or request.headers.get(
            "host"
        )
        request_scheme = request.headers.get("x-forwarded-proto") or request.url.scheme

        _API_LOGGER.info(
            "HTTP request/response",
            extra={
                "extra_fields": {
                    "event": "http_request_response",
                    "timestamp": datetime.now(ZoneInfo(get_timezone())).isoformat(),
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "duration_ms": duration_ms,
                    "client_id": getattr(request.state, "client_id", None),
                    "tenant_id": getattr(request.state, "tenant_id", None),
                    "request_host": request_host,
                    "request_scheme": request_scheme,
                    "request_body": request_body,
                    "response_body": response_body_out,
                }
            },
        )

        return response
    finally:
        if debug_handler:
            remove_debug_handler(debug_handler)
        reset_request_id(token)


async def api_key_auth_middleware(request: Request, call_next):
    path = request.url.path or ""
    if _is_docs_path(path):
        return await call_next(request)
    is_api_route = (
        path == "/api"
        or path.startswith("/api/")
        or path.startswith("/spendsphere/api")
    )

    if request.method == "OPTIONS":
        return await call_next(request)

    try:
        registry = _get_api_key_registry()
    except ValueError:
        if is_api_route:
            return JSONResponse(
                status_code=500,
                content={"detail": "API key registry is misconfigured"},
            )
        registry = {}

    if not registry and is_api_route:
        return JSONResponse(
            status_code=500,
            content={"detail": "API key registry is not configured"},
        )

    api_key = _extract_api_key(request)
    client_id = _match_api_key(api_key, registry) if api_key and registry else None

    if is_api_route:
        if not api_key:
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing API key"},
            )
        if not client_id:
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid API key"},
            )

        request.state.client_id = client_id
        response = await call_next(request)
        response.headers["X-API-Client"] = client_id
        return response

    if api_key:
        client_label = client_id or "Not Found"
    else:
        client_label = "Unauthenticated"

    request.state.client_id = client_label
    response = await call_next(request)
    response.headers["X-API-Client"] = client_label
    return response
