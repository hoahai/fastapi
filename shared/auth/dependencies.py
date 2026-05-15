from __future__ import annotations

from fastapi import HTTPException, Request

from shared.auth.config import (
    get_auth_mode,
    is_legacy_api_key_fallback_enabled,
    should_protect_tradsphere,
)
from shared.auth.jwt_verify import JwtVerificationError, verify_supabase_jwt
from shared.auth.permissions_repo import TenantAccessError, get_tenant_access_cached
from shared.auth.types import AuthorizationResult, AuthPrincipal, TenantAccessProfile


def _extract_bearer_token(request: Request) -> str | None:
    auth_header = str(request.headers.get("authorization") or "").strip()
    if not auth_header.lower().startswith("bearer "):
        return None
    token = auth_header[7:].strip()
    return token or None


def authorize_bearer_for_tenant_app(
    *,
    request: Request,
    app_code: str,
) -> AuthorizationResult:
    token = _extract_bearer_token(request)
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    tenant_slug = str(request.headers.get("x-tenant-id") or "").strip()
    if not tenant_slug:
        raise HTTPException(status_code=400, detail="Missing X-Tenant-Id header")

    try:
        principal = verify_supabase_jwt(token)
    except JwtVerificationError as exc:
        status_code = int(getattr(exc, "status_code", 401) or 401)
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc

    try:
        access = get_tenant_access_cached(
            user_id=principal.user_id,
            tenant_slug=tenant_slug,
            app_code=app_code,
        )
    except TenantAccessError as exc:
        code = str(getattr(exc, "code", "") or "").strip().lower()
        status_code = 403
        if code == "missing_tenant":
            status_code = 400
        elif code == "supabase_unavailable":
            status_code = 503
        raise HTTPException(
            status_code=status_code,
            detail={
                "message": str(exc),
                "code": code or "forbidden",
            },
        ) from exc

    request.state.auth_principal = principal
    request.state.tenant_access = access
    request.state.auth_mode = "supabase_jwt"
    return AuthorizationResult(principal=principal, access=access)


def authenticate_bearer(request: Request) -> AuthPrincipal:
    token = _extract_bearer_token(request)
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    try:
        principal = verify_supabase_jwt(token)
    except JwtVerificationError as exc:
        status_code = int(getattr(exc, "status_code", 401) or 401)
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc

    request.state.auth_principal = principal
    request.state.auth_mode = "supabase_jwt"
    return principal


def get_auth_principal(request: Request) -> AuthPrincipal | None:
    principal = getattr(request.state, "auth_principal", None)
    return principal if isinstance(principal, AuthPrincipal) else None


def get_tenant_access(request: Request) -> TenantAccessProfile | None:
    access = getattr(request.state, "tenant_access", None)
    return access if isinstance(access, TenantAccessProfile) else None


def require_auth_principal(request: Request) -> AuthPrincipal:
    principal = get_auth_principal(request)
    if principal is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    return principal


def require_permission(request: Request, permission: str) -> None:
    access = get_tenant_access(request)
    if access is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    if permission not in access.permissions:
        raise HTTPException(status_code=403, detail="Forbidden")


def _is_read_method(method: str) -> bool:
    return method in {"GET", "HEAD", "OPTIONS"}


def _resolve_tradsphere_permission(method: str, path: str) -> str:
    normalized_path = str(path or "").lower()
    if "/contacts" in normalized_path:
        return "tradsphere.contacts.viewer" if _is_read_method(method) else "tradsphere.contacts.editor"
    if "/stationscontacts" in normalized_path:
        return "tradsphere.contacts.viewer" if _is_read_method(method) else "tradsphere.contacts.editor"
    if "/stations" in normalized_path:
        return "tradsphere.stations.viewer" if _is_read_method(method) else "tradsphere.stations.editor"
    if "/estnums" in normalized_path:
        return "tradsphere.estnums.viewer" if _is_read_method(method) else "tradsphere.estnums.editor"
    if "/schedules" in normalized_path:
        if _is_read_method(method):
            return "tradsphere.schedules.viewer"
        return "tradsphere.editor"
    if "/broadcastcalendar" in normalized_path:
        return "tradsphere.viewer"
    if "/accounts" in normalized_path:
        return "tradsphere.viewer" if _is_read_method(method) else "tradsphere.editor"
    if "/deliverymethods" in normalized_path:
        return "tradsphere.viewer" if _is_read_method(method) else "tradsphere.editor"
    if "/ui/" in normalized_path:
        return "tradsphere.viewer" if _is_read_method(method) else "tradsphere.editor"
    return "tradsphere.viewer" if _is_read_method(method) else "tradsphere.editor"


def enforce_tradsphere_permission(request: Request) -> None:
    if not should_protect_tradsphere():
        return

    auth_mode = str(getattr(request.state, "auth_mode", "")).strip().lower()
    if (
        auth_mode == "legacy_api_key"
        and get_auth_mode() == "compat"
        and is_legacy_api_key_fallback_enabled()
    ):
        return

    access = get_tenant_access(request)
    if access is None:
        raise HTTPException(status_code=401, detail="Authentication required")

    method = str(request.method or "").upper()
    required_permission = _resolve_tradsphere_permission(method, request.url.path)

    if required_permission not in access.permissions:
        raise HTTPException(status_code=403, detail="Forbidden")
