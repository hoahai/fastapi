from __future__ import annotations

import hmac
import json
import os
import time
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
    set_client_id,
    reset_client_id,
    add_debug_handler,
    remove_debug_handler,
    create_request_debug_handler,
)
from shared.tenant import (
    TenantConfigError,
    TenantConfigValidationError,
    build_tenant_config_payload,
    set_tenant_context,
    reset_tenant_context,
    get_tenant_id,
    get_timezone,
)
from shared.response import (
    ensure_request_id,
    wrap_error,
    wrap_success,
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


def _normalize_path(path: str | None) -> str:
    if not path or path == "/":
        return "/"
    return path.rstrip("/")


def _normalize_public_paths(paths: object) -> set[str]:
    if isinstance(paths, str):
        values = [paths]
    else:
        try:
            values = list(paths)
        except TypeError:
            return {"/", "/ping"}

    normalized: set[str] = set()
    for value in values:
        if value is None:
            continue
        normalized.add(_normalize_path(str(value)))
    return normalized or {"/", "/ping"}


def _get_public_paths(request: Request) -> set[str]:
    paths = getattr(request.app.state, "public_paths", None)
    if paths is None:
        return {"/", "/ping"}
    return _normalize_public_paths(paths)


def _is_public_path(path: str, public_paths: set[str]) -> bool:
    normalized = _normalize_path(path)
    return normalized in public_paths


def _should_wrap_response(
    request: Request,
    response: Response,
    content_type: str,
) -> bool:
    if _is_docs_path(request.url.path or ""):
        return False
    if response.status_code in {204, 304}:
        return False
    return "application/json" in (content_type or "").lower()


def _duration_since(request: Request, fallback_start: float) -> float:
    start_time = getattr(request.state, "start_time", None)
    if isinstance(start_time, (int, float)):
        return time.perf_counter() - start_time
    return time.perf_counter() - fallback_start


def _extract_payload(
    response_json: object | None,
    response_body: bytes,
) -> object | None:
    if response_json is not None:
        return response_json
    if response_body:
        return _safe_decode_text(response_body)
    return None


def _unwrap_success_payload(payload: object | None) -> object | None:
    if isinstance(payload, dict) and "meta" in payload and "data" in payload:
        return payload.get("data")
    return payload


def _unwrap_error_payload(payload: object | None) -> object | None:
    if isinstance(payload, dict) and "meta" in payload and "error" in payload:
        return payload.get("error")
    return payload


def _wrap_response_payload(
    request: Request,
    response: Response,
    response_body: bytes,
    *,
    duration_s: float,
) -> tuple[Response, object | None]:
    response_headers = dict(response.headers)
    response_headers.pop("content-length", None)

    content_type = response.headers.get("content-type", "")
    response_json = None
    if response_body and "application/json" in (content_type or "").lower():
        response_json = _safe_parse_json(response_body)

    if _should_wrap_response(request, response, content_type):
        raw_payload = _extract_payload(response_json, response_body)
        if response.status_code < 400:
            payload = wrap_success(
                _unwrap_success_payload(raw_payload),
                request,
                duration_s=duration_s,
            )
        else:
            payload = wrap_error(
                _unwrap_error_payload(raw_payload),
                request,
                duration_s=duration_s,
            )

        response_headers.pop("content-type", None)
        response = JSONResponse(
            content=payload,
            status_code=response.status_code,
            headers=response_headers,
            background=response.background,
        )
        return response, payload

    response = Response(
        content=response_body,
        status_code=response.status_code,
        headers=response_headers,
        media_type=response.media_type,
        background=response.background,
    )

    if response_body:
        if response_json is not None:
            response_body_out = response_json
        else:
            response_body_out = _safe_decode_text(response_body)
    else:
        response_body_out = None

    return response, response_body_out


def _error_response(
    request: Request,
    *,
    status_code: int,
    detail: object | None,
    duration_s: float,
) -> JSONResponse:
    payload = wrap_error(detail, request, duration_s=duration_s)
    return JSONResponse(status_code=status_code, content=payload)


def _should_validate_tenant(path: str, prefixes: tuple[str, ...] | None) -> bool:
    if not prefixes:
        return True
    return any(path.startswith(prefix) for prefix in prefixes)


def _resolve_tenant_validator(path: str, registry: object) -> tuple[str | None, object]:
    if not registry:
        return None, None

    best_validator = None
    best_app_name = None
    best_prefix_len = -1

    for entry in registry:
        try:
            if len(entry) == 3:
                prefixes, app_name, validator = entry
            else:
                prefixes, validator = entry
                app_name = None
        except (TypeError, ValueError):
            continue
        if not callable(validator):
            continue
        if isinstance(prefixes, str):
            prefixes = (prefixes,)
        if _should_validate_tenant(path, prefixes):
            prefix_len = max((len(p) for p in prefixes), default=0) if prefixes else 0
            if prefix_len > best_prefix_len:
                best_prefix_len = prefix_len
                best_app_name = app_name
                best_validator = validator

    return best_app_name, best_validator


async def timing_middleware(request: Request, call_next):
    # Set start time BEFORE route executes
    if getattr(request.state, "start_time", None) is None:
        request.state.start_time = time.perf_counter()

    response = await call_next(request)

    return response


async def tenant_context_middleware(request: Request, call_next):
    start_time = time.perf_counter()
    path = request.url.path or ""
    public_paths = _get_public_paths(request)
    if _is_docs_path(path) or _is_public_path(path, public_paths):
        request.state.tenant_id = None
        return await call_next(request)
    requires_tenant = path.startswith("/api") or path.startswith("/spendsphere/api")
    tenant_header = request.headers.get("x-tenant-id")
    token = None
    if not requires_tenant and not tenant_header:
        request.state.tenant_id = None
        return await call_next(request)

    if not tenant_header:
        return _error_response(
            request,
            status_code=400,
            detail="Missing X-Tenant-Id header",
            duration_s=_duration_since(request, start_time),
        )

    try:
        token = set_tenant_context(tenant_header)
        request.state.tenant_id = get_tenant_id()
        registry = getattr(request.app.state, "tenant_validator_registry", None)
        app_name, validator = _resolve_tenant_validator(path, registry)
        if app_name:
            request.state.tenant_app = app_name
        if validator is None:
            validator = getattr(request.app.state, "tenant_validator", None)
            validator_prefixes = getattr(
                request.app.state, "tenant_validator_prefixes", None
            )
            if callable(validator) and _should_validate_tenant(path, validator_prefixes):
                validator()
        elif callable(validator):
            validator()
    except TenantConfigError as exc:
        if isinstance(exc, TenantConfigValidationError):
            app_name = exc.app_name or getattr(request.state, "tenant_app", None)
            payload = build_tenant_config_payload(
                app_name,
                missing=exc.missing,
                invalid=exc.invalid,
            )
            return _error_response(
                request,
                status_code=400,
                detail=payload,
                duration_s=_duration_since(request, start_time),
            )
        error_text = str(exc)
        if "Tenant config not found for" in error_text:
            match = re.search(r"Tenant config not found for '([^']+)'", error_text)
            tenant_id = match.group(1) if match else None
            detail = (
                f"Tenant ({tenant_id}) not found!"
                if tenant_id
                else "Tenant not found!"
            )
            return _error_response(
                request,
                status_code=400,
                detail={"detail": detail},
                duration_s=_duration_since(request, start_time),
            )
        return _error_response(
            request,
            status_code=400,
            detail={"detail": error_text},
            duration_s=_duration_since(request, start_time),
        )

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


def _safe_parse_json(payload: bytes | str) -> object | None:
    try:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _safe_decode_text(payload: bytes) -> str:
    return payload.decode("utf-8", errors="replace")


def _normalize_query_params(params: object) -> dict[str, object] | None:
    if not params:
        return None
    result: dict[str, object] = {}
    try:
        items = params.multi_items()
    except AttributeError:
        try:
            items = dict(params).items()
        except Exception:
            return None
    for key, value in items:
        if key in result:
            existing = result[key]
            if isinstance(existing, list):
                existing.append(value)
            else:
                result[key] = [existing, value]
        else:
            result[key] = value
    return result


def _is_update_budget_async_accept_response(
    request: Request,
    response: Response,
    response_body: object | None,
) -> bool:
    if request.method != "POST":
        return False
    path = _normalize_path(request.url.path or "")
    if not (path.endswith("/updateBudgetAsync") or path.endswith("/update/async")):
        return False
    if response.status_code >= 400:
        return False
    if not isinstance(response_body, dict):
        return False
    payload = response_body.get("data") if "data" in response_body else response_body
    return isinstance(payload, dict) and payload.get("status") == "accepted"


async def request_response_logger_middleware(request: Request, call_next):
    start_time = time.perf_counter()
    request_id = ensure_request_id(request)
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

        duration_s = _duration_since(request, start_time)
        duration_ms = int(duration_s * 1000)

        if body_bytes:
            request_json = _safe_parse_json(body_bytes)
            request_body = (
                request_json
                if request_json is not None
                else _safe_decode_text(body_bytes)
            )
        else:
            request_body = None
        request_params = _normalize_query_params(request.query_params)

        response, response_body_out = _wrap_response_payload(
            request,
            response,
            response_body,
            duration_s=duration_s,
        )

        request_host = request.headers.get("x-forwarded-host") or request.headers.get(
            "host"
        )
        request_scheme = request.headers.get("x-forwarded-proto") or request.url.scheme

        if not _is_update_budget_async_accept_response(
            request,
            response,
            response_body_out,
        ):
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
                        "request_params": request_params,
                        "response_body": response_body_out,
                    }
                },
            )

        return response
    finally:
        if debug_handler:
            remove_debug_handler(debug_handler)
        reset_request_id(token)


async def response_envelope_middleware(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)

    response_body = b""
    async for chunk in response.body_iterator:
        response_body += chunk

    duration_s = _duration_since(request, start_time)
    response, _ = _wrap_response_payload(
        request,
        response,
        response_body,
        duration_s=duration_s,
    )
    return response


async def api_key_auth_middleware(request: Request, call_next):
    start_time = time.perf_counter()
    path = request.url.path or ""
    public_paths = _get_public_paths(request)
    if _is_docs_path(path) or _is_public_path(path, public_paths):
        return await call_next(request)
    is_api_route = (
        path == "/api"
        or path.startswith("/api/")
        or path.startswith("/spendsphere/api")
    )

    if request.method == "OPTIONS":
        return await call_next(request)

    client_token = None
    try:
        try:
            registry = _get_api_key_registry()
        except ValueError:
            if is_api_route:
                return _error_response(
                    request,
                    status_code=500,
                    detail="API key registry is misconfigured",
                    duration_s=_duration_since(request, start_time),
                )
            registry = {}

        if not registry and is_api_route:
            return _error_response(
                request,
                status_code=500,
                detail="API key registry is not configured",
                duration_s=_duration_since(request, start_time),
            )

        api_key = _extract_api_key(request)
        client_id = _match_api_key(api_key, registry) if api_key and registry else None

        if is_api_route:
            if not api_key:
                request.state.client_id = "Unauthenticated"
                client_token = set_client_id(request.state.client_id)
                return _error_response(
                    request,
                    status_code=401,
                    detail="Missing API key",
                    duration_s=_duration_since(request, start_time),
                )
            if not client_id:
                request.state.client_id = "Not Found"
                client_token = set_client_id(request.state.client_id)
                return _error_response(
                    request,
                    status_code=401,
                    detail="Invalid API key",
                    duration_s=_duration_since(request, start_time),
                )

            request.state.client_id = client_id
            client_token = set_client_id(client_id)
            response = await call_next(request)
            response.headers["X-API-Client"] = client_id
            return response

        if api_key:
            client_label = client_id or "Not Found"
        else:
            client_label = "Unauthenticated"

        request.state.client_id = client_label
        client_token = set_client_id(client_label)
        response = await call_next(request)
        response.headers["X-API-Client"] = client_label
        return response
    finally:
        if client_token is not None:
            reset_client_id(client_token)
