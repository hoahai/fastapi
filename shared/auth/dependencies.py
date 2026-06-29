from __future__ import annotations

from fastapi import HTTPException, Request

from shared.auth.config import (
    get_auth_mode,
    is_legacy_api_key_fallback_enabled,
)
from shared.auth.jwt_verify import JwtVerificationError, verify_supabase_jwt
from shared.auth.page_permissions_catalog import resolve_page_keys_for_api_path
from shared.auth.page_permissions_repo import list_page_keys_for_user_scope
from shared.auth.permission_registry import resolve_required_permissions
from shared.auth.permissions_repo import TenantAccessError, get_tenant_access_cached
from shared.auth.supabase_client import SupabaseClientError
from shared.auth.types import AuthorizationResult, AuthPrincipal, TenantAccessProfile
from shared.auth.user_status import is_auth_user_disabled


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
    _require_active_principal(principal)

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
    _require_active_principal(principal)

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
    if "workspace.super_admin" in access.permissions:
        return
    if permission not in access.permissions:
        raise HTTPException(status_code=403, detail="Forbidden")


def _require_active_principal(principal: AuthPrincipal) -> None:
    raw_user = principal.raw_user if isinstance(principal.raw_user, dict) else {}
    if is_auth_user_disabled(raw_user):
        raise HTTPException(
            status_code=403,
            detail={
                "message": "Your account has been disabled. Contact your workspace administrator.",
                "code": "user_disabled",
            },
        )


def _enforce_route_permission_for_app(request: Request, *, app_code: str) -> None:
    normalized_app_code = str(app_code or "").strip().lower()
    if not normalized_app_code:
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
    if "workspace.super_admin" in access.permissions:
        return

    required_permissions = resolve_required_permissions(
        app_code=normalized_app_code,
        method=str(request.method or "").upper(),
        path=str(request.url.path or ""),
    )
    if not required_permissions:
        return

    if not any(permission in access.permissions for permission in required_permissions):
        raise HTTPException(status_code=403, detail="Forbidden")


def _enforce_page_permission_for_app(request: Request, *, app_code: str) -> None:
    normalized_app_code = str(app_code or "").strip().lower()
    if not normalized_app_code:
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
    principal = get_auth_principal(request)
    if principal is None:
        raise HTTPException(status_code=401, detail="Authentication required")

    try:
        allowed_page_keys = list_page_keys_for_user_scope(
            user_id=principal.user_id,
            tenant_id=access.tenant_id,
            app_id=access.app_id,
        )
    except SupabaseClientError as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Unable to verify page permissions",
                "code": "page_permissions_unavailable",
                "detail": str(exc),
            },
        ) from exc

    # No explicit rows means unrestricted access for this app scope.
    if not allowed_page_keys:
        return

    route_page_keys = resolve_page_keys_for_api_path(
        app_code=normalized_app_code,
        path=str(request.url.path or ""),
    )
    if not route_page_keys or allowed_page_keys.isdisjoint(route_page_keys):
        raise HTTPException(
            status_code=403,
            detail={
                "message": "Forbidden by page permissions",
                "code": "page_permission_denied",
            },
        )


def enforce_tradsphere_permission(request: Request) -> None:
    _enforce_route_permission_for_app(request, app_code="tradsphere")
    _enforce_page_permission_for_app(request, app_code="tradsphere")


def enforce_leavesphere_permission(request: Request) -> None:
    _enforce_route_permission_for_app(request, app_code="leavesphere")
    _enforce_page_permission_for_app(request, app_code="leavesphere")


def enforce_spendsphere_permission(request: Request) -> None:
    _enforce_route_permission_for_app(request, app_code="spendsphere")


def enforce_fundsphere_permission(request: Request) -> None:
    _enforce_route_permission_for_app(request, app_code="fundsphere")
    _enforce_page_permission_for_app(request, app_code="fundsphere")


def enforce_shiftzy_permission(request: Request) -> None:
    _enforce_route_permission_for_app(request, app_code="shiftzy")
    _enforce_page_permission_for_app(request, app_code="shiftzy")


def enforce_opssphere_permission(request: Request) -> None:
    _enforce_route_permission_for_app(request, app_code="opssphere")
