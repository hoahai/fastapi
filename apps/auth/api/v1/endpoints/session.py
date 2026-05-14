from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Request

from shared.auth.admin_repo import is_user_super_admin, list_active_apps, list_active_tenants, list_user_access_assignments
from shared.auth.config import is_debug_endpoint_enabled
from shared.auth.dependencies import authorize_bearer_for_tenant_app
from shared.auth.profile_repo import compose_full_name, select_profile_for_user, split_full_name, upsert_profile_basic_info
from shared.auth.providers import get_auth_provider
from shared.auth.supabase_client import SupabaseClientError
from shared.tenant import TenantConfigError, reset_tenant_context, set_tenant_context
from apps.tradsphere.api.v1.helpers.config import validate_tenant_config as validate_tradsphere_tenant_config

router = APIRouter(prefix="/session")
debug_router = APIRouter(prefix="/debug")


def _provider():
    return get_auth_provider()


def _parse_profile_full_name(payload: dict | None) -> str | None:
    body = payload if isinstance(payload, dict) else {}
    profile_payload = body.get("profile")
    profile = profile_payload if isinstance(profile_payload, dict) else {}
    return compose_full_name(
        full_name=str(profile.get("fullName") or body.get("fullName") or "").strip() or None,
        first_name=str(profile.get("firstName") or body.get("firstName") or "").strip() or None,
        last_name=str(profile.get("lastName") or body.get("lastName") or "").strip() or None,
    )


def _build_user_payload(*, user_id: str, email: str | None) -> dict[str, object]:
    profile = select_profile_for_user(provider=_provider(), user_id=user_id) or {}
    profile_full_name = str(profile.get("full_name") or "").strip() or None
    first_name, last_name = split_full_name(profile_full_name)
    resolved_email = str(profile.get("email") or "").strip() or (str(email or "").strip() or None)

    return {
        "id": user_id,
        "email": resolved_email,
        "fullName": profile_full_name,
        "firstName": first_name,
        "lastName": last_name,
    }


def _build_assignments_scope_payload(*, user_id: str, role: str, permissions: set[str]) -> tuple[list[dict[str, object]], dict[str, bool]]:
    is_super_admin = str(role or "").strip().lower() == "super_admin" or "workspace.super_admin" in permissions
    has_any_admin_scope = is_super_admin or str(role or "").strip().lower() == "admin" or "tradsphere.admin" in permissions
    assignments: list[dict[str, object]] = []

    try:
        assignments = list_user_access_assignments(user_id=user_id)
        is_super_admin = is_user_super_admin(user_id=user_id) or is_super_admin
        has_any_admin_scope = is_super_admin or any(
            str(item.get("role") or "").strip().lower() == "admin"
            for item in assignments
            if isinstance(item, dict)
        )
    except SupabaseClientError:
        # Keep backward-compatible session payload even when optional assignment lookup is unavailable.
        assignments = []

    return assignments, {
        "isSuperAdmin": is_super_admin,
        "hasAnyAdminScope": has_any_admin_scope,
    }


def _build_super_admin_tenant_assignments_for_app(*, app_id: str, app_code: str) -> list[dict[str, object]]:
    normalized_app_id = str(app_id or "").strip()
    normalized_app_code = str(app_code or "").strip().lower()
    if not normalized_app_id or not normalized_app_code:
        return []

    app_name = normalized_app_code.title()
    for app in list_active_apps():
        if not isinstance(app, dict):
            continue
        app_row_id = str(app.get("id") or "").strip()
        app_row_code = str(app.get("code") or "").strip().lower()
        if app_row_id == normalized_app_id or app_row_code == normalized_app_code:
            app_name = str(app.get("name") or "").strip() or app_name
            break

    assignments: list[dict[str, object]] = []
    for tenant in list_active_tenants():
        if not isinstance(tenant, dict):
            continue
        tenant_id = str(tenant.get("id") or "").strip()
        tenant_slug = str(tenant.get("slug") or "").strip()
        if not tenant_id or not tenant_slug:
            continue
        assignments.append(
            {
                "tenant": {
                    "id": tenant_id,
                    "slug": tenant_slug,
                    "name": str(tenant.get("name") or "").strip() or None,
                    "status": "active",
                },
                "app": {
                    "id": normalized_app_id,
                    "code": normalized_app_code,
                    "name": app_name,
                },
                "role": "super_admin",
            }
        )

    assignments.sort(
        key=lambda item: (
            str(((item.get("tenant") or {}).get("slug") if isinstance(item.get("tenant"), dict) else "") or "~").lower(),
            str(((item.get("tenant") or {}).get("id") if isinstance(item.get("tenant"), dict) else "") or ""),
        )
    )
    return assignments


def _is_assignment_backend_enabled(*, app_code: str, tenant_slug: str) -> bool:
    normalized_app_code = str(app_code or "").strip().lower()
    normalized_tenant_slug = str(tenant_slug or "").strip().lower()
    if not normalized_app_code or not normalized_tenant_slug:
        return False

    if normalized_app_code == "tradsphere":
        token = None
        try:
            token = set_tenant_context(normalized_tenant_slug)
            validate_tradsphere_tenant_config(normalized_tenant_slug)
            return True
        except TenantConfigError:
            return False
        finally:
            if token is not None:
                reset_tenant_context(token)

    return True


def _filter_assignments_by_backend_app_config(assignments: list[dict[str, object]]) -> list[dict[str, object]]:
    filtered: list[dict[str, object]] = []
    for item in assignments:
        if not isinstance(item, dict):
            continue
        tenant = item.get("tenant")
        app = item.get("app")
        tenant_slug = str(tenant.get("slug") if isinstance(tenant, dict) else "").strip().lower()
        app_code = str(app.get("code") if isinstance(app, dict) else "").strip().lower()
        if _is_assignment_backend_enabled(app_code=app_code, tenant_slug=tenant_slug):
            filtered.append(item)
    return filtered


@router.get("/me")
def get_session_me(request: Request):
    """
    Return authenticated user info, current tenant/app access, and all tenant-app assignments for Workspace Home/sidebar UX.

    Example request:
        GET /api/auth/v1/session/me

    Example response:
        {
          "user": {
            "id": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
            "email": "alex@example.com",
            "fullName": "Alex Johnson",
            "firstName": "Alex",
            "lastName": "Johnson"
          },
          "tenant": {
            "id": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
            "slug": "taaa"
          },
          "app": {
            "id": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
            "code": "tradsphere"
          },
          "role": "viewer",
          "assignments": [
            {
              "tenant": {
                "id": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
                "slug": "taaa",
                "name": "TAAA"
              },
              "app": {
                "id": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
                "code": "tradsphere",
                "name": "Tradsphere"
              },
              "role": "admin"
            },
            {
              "tenant": {
                "id": "7fc4c71f-63b8-4d8a-a518-88f6fefec142",
                "slug": "nucar",
                "name": "NuCar"
              },
              "app": {
                "id": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
                "code": "tradsphere",
                "name": "Tradsphere"
              },
              "role": "viewer"
            }
          ],
          "scope": {
            "isSuperAdmin": false,
            "hasAnyAdminScope": true
          },
          "permissions": [
            "tradsphere.viewer"
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id tenant slug
        - Validates membership and app access for TradSphere
    """
    result = authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")
    assignments, scope = _build_assignments_scope_payload(
        user_id=result.principal.user_id,
        role=result.access.role,
        permissions=set(result.access.permissions),
    )
    if scope.get("isSuperAdmin") and not assignments:
        try:
            assignments = _build_super_admin_tenant_assignments_for_app(
                app_id=result.access.app_id,
                app_code=result.access.app_code,
            )
        except SupabaseClientError:
            assignments = []
    assignments = _filter_assignments_by_backend_app_config(assignments)

    return {
        "user": _build_user_payload(
            user_id=result.principal.user_id,
            email=result.principal.email,
        ),
        "tenant": {
            "id": result.access.tenant_id,
            "slug": result.access.tenant_slug,
        },
        "app": {
            "id": result.access.app_id,
            "code": result.access.app_code,
        },
        "role": result.access.role,
        "assignments": assignments,
        "scope": scope,
        "permissions": sorted(result.access.permissions),
    }


@router.patch("/me/profile")
def patch_session_me_profile(
    request: Request,
    payload: dict | None = Body(default=None),
):
    """
    Update basic profile info for the currently authenticated tenant user.

    Example request:
        PATCH /api/auth/v1/session/me/profile

    Example request body:
        {
          "profile": {
            "firstName": "Alex",
            "lastName": "Johnson",
            "fullName": "Alex Johnson"
          }
        }

    Example response:
        {
          "user": {
            "id": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
            "email": "alex@example.com",
            "fullName": "Alex Johnson",
            "firstName": "Alex",
            "lastName": "Johnson"
          }
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id tenant slug
        - Validates membership and app access for TradSphere
        - Accepts either `fullName` or `firstName`/`lastName`
    """
    result = authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")
    full_name = _parse_profile_full_name(payload)
    if full_name is None:
        raise HTTPException(status_code=400, detail="Profile name is required")
    if len(full_name) > 255:
        raise HTTPException(status_code=400, detail="Profile name exceeds maximum length")

    try:
        upsert_profile_basic_info(
            provider=_provider(),
            user_id=result.principal.user_id,
            email=result.principal.email,
            full_name=full_name,
        )
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {
        "user": _build_user_payload(
            user_id=result.principal.user_id,
            email=result.principal.email,
        )
    }


@debug_router.get("/whoami")
def get_debug_whoami(request: Request):
    """
    Dev-only auth debug endpoint.

    Example request:
        GET /api/auth/v1/debug/whoami

    Requirements:
        - Enabled only when AUTH_ENABLE_DEBUG_ENDPOINTS=true
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id tenant slug
        - Does not return sensitive tokens/secrets
    """
    if not is_debug_endpoint_enabled():
        raise HTTPException(status_code=404, detail="Not Found")

    result = authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")
    return {
        "authMode": "supabase_jwt",
        "user": _build_user_payload(
            user_id=result.principal.user_id,
            email=result.principal.email,
        ),
        "tenant": {
            "id": result.access.tenant_id,
            "slug": result.access.tenant_slug,
        },
        "role": result.access.role,
        "permissions": sorted(result.access.permissions),
    }
