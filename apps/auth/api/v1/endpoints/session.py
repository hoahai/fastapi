from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Request

from shared.auth.admin_repo import is_user_super_admin, list_active_apps, list_active_tenants, list_user_access_assignments
from shared.auth.config import is_debug_endpoint_enabled
from shared.auth.dependencies import authenticate_bearer, authorize_bearer_for_tenant_app
from shared.auth.page_permissions_repo import list_page_permissions_for_user
from shared.auth.permissions_repo import TenantAccessError, validate_active_tenant_membership
from shared.auth.profile_repo import compose_full_name, select_profile_for_user, split_full_name, upsert_profile_basic_info
from shared.auth.providers import get_auth_provider
from shared.auth.roles import normalize_role_key
from shared.auth.supabase_client import SupabaseClientError
from shared.tenant import TenantConfigError, get_timezone, reset_tenant_context, set_tenant_context
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


def _permission_set_for_assignment(*, app_code: str, role: str) -> set[str]:
    normalized_app_code = str(app_code or "").strip().lower()
    normalized_role = normalize_role_key(role)
    if not normalized_app_code:
        return set()

    permissions = {f"{normalized_app_code}.viewer"}
    if normalized_role in {"editor", "admin", "super_admin"}:
        permissions.add(f"{normalized_app_code}.editor")
    if normalized_role in {"admin", "super_admin"}:
        permissions.add(f"{normalized_app_code}.admin")
    return permissions


def _build_app_access_payload(assignments: list[dict[str, object]]) -> list[dict[str, object]]:
    by_app: dict[str, dict[str, object]] = {}
    role_priority = {"viewer": 0, "editor": 1, "admin": 2, "super_admin": 3}

    for assignment in assignments:
        if not isinstance(assignment, dict):
            continue
        app = assignment.get("app")
        if not isinstance(app, dict):
            continue
        app_id = str(app.get("id") or "").strip()
        app_code = str(app.get("code") or "").strip().lower()
        app_name = str(app.get("name") or "").strip() or None
        role = normalize_role_key(str(assignment.get("role") or "").strip())
        if not app_id or not app_code:
            continue

        key = f"{app_code}::{app_id}"
        candidate_permissions = sorted(_permission_set_for_assignment(app_code=app_code, role=role))
        candidate_rank = role_priority.get(role, -1)
        existing = by_app.get(key)
        existing_role = normalize_role_key(str((existing or {}).get("role") or ""))
        existing_rank = role_priority.get(existing_role, -1)

        if existing and candidate_rank <= existing_rank:
            continue

        by_app[key] = {
            "app": {
                "id": app_id,
                "code": app_code,
                "name": app_name,
            },
            "role": role or "viewer",
            "permissions": candidate_permissions,
        }

    return sorted(
        by_app.values(),
        key=lambda item: (
            str(((item.get("app") or {}).get("code") if isinstance(item.get("app"), dict) else "") or "~").lower(),
            str(((item.get("app") or {}).get("id") if isinstance(item.get("app"), dict) else "") or ""),
        ),
    )


def _resolve_selected_assignment(
    *,
    assignments: list[dict[str, object]],
    tenant_slug: str,
    preferred_app_code: str | None,
) -> dict[str, object] | None:
    normalized_tenant_slug = str(tenant_slug or "").strip().lower()
    normalized_preferred_app_code = str(preferred_app_code or "").strip().lower()

    if not normalized_tenant_slug:
        if normalized_preferred_app_code:
            match = next(
                (
                    item
                    for item in assignments
                    if isinstance(item, dict)
                    and isinstance(item.get("app"), dict)
                    and str((item.get("app") or {}).get("code") or "").strip().lower() == normalized_preferred_app_code
                ),
                None,
            )
            if match:
                return match
        return next((item for item in assignments if isinstance(item, dict)), None)

    def _matches(item: dict[str, object], *, require_app: bool) -> bool:
        tenant = item.get("tenant")
        app = item.get("app")
        if not isinstance(tenant, dict) or not isinstance(app, dict):
            return False
        item_tenant_slug = str(tenant.get("slug") or "").strip().lower()
        item_app_code = str(app.get("code") or "").strip().lower()
        if item_tenant_slug != normalized_tenant_slug:
            return False
        if require_app:
            return bool(item_app_code and item_app_code == normalized_preferred_app_code)
        return bool(item_app_code)

    if normalized_preferred_app_code:
        match = next((item for item in assignments if isinstance(item, dict) and _matches(item, require_app=True)), None)
        if match:
            return match

    match = next((item for item in assignments if isinstance(item, dict) and _matches(item, require_app=False)), None)
    if match:
        return match

    return next((item for item in assignments if isinstance(item, dict)), None)


def _build_validate_payload(
    *,
    user_id: str,
    user_email: str | None,
    tenant_id: str,
    tenant_slug: str,
    assignments: list[dict[str, object]],
    scope: dict[str, bool],
    preferred_app_code: str | None = None,
    include_selected_assignment: bool = True,
    resolved_permissions: set[str] | None = None,
    page_permissions: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    selected_assignment: dict[str, object] | None = None
    if include_selected_assignment:
        selected_assignment = _resolve_selected_assignment(
            assignments=assignments,
            tenant_slug=tenant_slug,
            preferred_app_code=preferred_app_code,
        )

    permission_set: set[str]
    if resolved_permissions is not None:
        permission_set = {str(value or "").strip() for value in resolved_permissions if str(value or "").strip()}
    else:
        permission_set = set()
        selected_assignment_for_permissions = selected_assignment
        if selected_assignment_for_permissions is None and not tenant_slug:
            selected_assignment_for_permissions = _resolve_selected_assignment(
                assignments=assignments,
                tenant_slug=tenant_slug,
                preferred_app_code=preferred_app_code,
            )
        if selected_assignment_for_permissions is not None:
            tenant = selected_assignment_for_permissions.get("tenant")
            app = selected_assignment_for_permissions.get("app")
            if isinstance(app, dict):
                app_code = str(app.get("code") or "").strip().lower()
                role = str(selected_assignment_for_permissions.get("role") or "").strip()
                permission_set.update(_permission_set_for_assignment(app_code=app_code, role=role))
        else:
            for assignment in assignments:
                if not isinstance(assignment, dict):
                    continue
                tenant = assignment.get("tenant")
                app = assignment.get("app")
                if not isinstance(tenant, dict) or not isinstance(app, dict):
                    continue
                assignment_tenant_slug = str(tenant.get("slug") or "").strip().lower()
                if assignment_tenant_slug != tenant_slug:
                    continue
                app_code = str(app.get("code") or "").strip().lower()
                role = str(assignment.get("role") or "").strip()
                permission_set.update(_permission_set_for_assignment(app_code=app_code, role=role))
    if scope.get("isSuperAdmin"):
        permission_set.add("workspace.super_admin")

    selected_tenant = selected_assignment.get("tenant") if isinstance(selected_assignment, dict) else None
    selected_app = selected_assignment.get("app") if isinstance(selected_assignment, dict) else None
    selected_role = normalize_role_key(str((selected_assignment or {}).get("role") or "").strip()) or None

    response_tenant_id = tenant_id
    response_tenant_slug = tenant_slug
    if include_selected_assignment:
        response_tenant_id = str((selected_tenant or {}).get("id") if isinstance(selected_tenant, dict) else "").strip() or tenant_id
        response_tenant_slug = (
            str((selected_tenant or {}).get("slug") if isinstance(selected_tenant, dict) else "").strip().lower()
            or tenant_slug
        )

    app_payload: dict[str, object] | None = None
    if include_selected_assignment and isinstance(selected_app, dict):
        app_id = str(selected_app.get("id") or "").strip()
        app_code = str(selected_app.get("code") or "").strip().lower()
        if app_id and app_code:
            app_payload = {
                "id": app_id,
                "code": app_code,
            }

    return {
        "user": _build_user_payload(
            user_id=user_id,
            email=user_email,
        ),
        "tenant": {
            "id": response_tenant_id,
            "slug": response_tenant_slug,
            "timezone": get_timezone(),
        },
        "app": app_payload,
        "role": selected_role if include_selected_assignment else None,
        "assignments": assignments,
        "appAccess": _build_app_access_payload(assignments),
        "scope": scope,
        "permissions": sorted(permission_set),
        "pagePermissions": page_permissions or [],
    }


def _status_code_for_access_error(exc: TenantAccessError) -> int:
    code = str(getattr(exc, "code", "") or "").strip().lower()
    if code == "missing_tenant":
        return 400
    if code == "supabase_unavailable":
        return 503
    return 403


def _build_workspace_session_me_payload(
    *,
    user_id: str,
    user_email: str | None,
    preferred_app_code: str | None,
) -> dict[str, object]:
    assignments = list_user_access_assignments(user_id=user_id)
    try:
        is_super_admin = is_user_super_admin(user_id=user_id)
    except SupabaseClientError:
        is_super_admin = False

    scope = {
        "isSuperAdmin": is_super_admin,
        "hasAnyAdminScope": is_super_admin
        or any(
            str(item.get("role") or "").strip().lower() == "admin"
            for item in assignments
            if isinstance(item, dict)
        ),
    }

    if is_super_admin and not assignments:
        app = None
        if preferred_app_code:
            app = next(
                (
                    item
                    for item in list_active_apps()
                    if str(item.get("code") or "").strip().lower() == preferred_app_code
                ),
                None,
            )
        if app is None:
            app = next(
                (
                    item
                    for item in list_active_apps()
                    if str(item.get("code") or "").strip().lower() == "tradsphere"
                ),
                None,
            )
        if app:
            assignments = _build_super_admin_tenant_assignments_for_app(
                app_id=str(app.get("id") or "").strip(),
                app_code=str(app.get("code") or "").strip().lower(),
            )

    assignments = _filter_assignments_by_backend_app_config(assignments)
    selected_assignment = _resolve_selected_assignment(
        assignments=assignments,
        tenant_slug="",
        preferred_app_code=preferred_app_code,
    )
    tenant_id = ""
    resolved_tenant_slug = ""
    page_permissions: list[dict[str, object]] = []
    permissions: set[str] = {"workspace.super_admin"} if scope.get("isSuperAdmin") else set()
    if selected_assignment is not None:
        selected_tenant = selected_assignment.get("tenant")
        selected_app = selected_assignment.get("app")
        if isinstance(selected_tenant, dict):
            tenant_id = str(selected_tenant.get("id") or "").strip()
            resolved_tenant_slug = str(selected_tenant.get("slug") or "").strip().lower()
        if tenant_id:
            page_permissions = list_page_permissions_for_user(user_id=user_id, tenant_id=tenant_id)
        if isinstance(selected_app, dict):
            permissions = _permission_set_for_assignment(
                app_code=str(selected_app.get("code") or "").strip().lower(),
                role=str(selected_assignment.get("role") or "").strip(),
            )
            if scope.get("isSuperAdmin"):
                permissions.add("workspace.super_admin")

    return _build_validate_payload(
        user_id=user_id,
        user_email=user_email,
        tenant_id=tenant_id,
        tenant_slug=resolved_tenant_slug,
        assignments=assignments,
        scope=scope,
        preferred_app_code=preferred_app_code,
        include_selected_assignment=True,
        resolved_permissions=permissions,
        page_permissions=page_permissions,
    )


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
            "slug": "taaa",
            "timezone": "America/Chicago"
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
          "pagePermissions": [
            {
              "tenantId": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
              "tenantSlug": "taaa",
              "appId": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
              "appCode": "tradsphere",
              "pageKey": "tradsphere_home"
            }
          ],
          "permissions": [
            "tradsphere.viewer"
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - X-Tenant-Id is optional for workspace-level login/home/profile access
        - Returns the user's workspace assignments when no tenant is selected
        - Uses app-level validation only when X-App-Code is provided
        - With X-Tenant-Id and X-App-Code, non-super users require active tenant membership + app role
        - Returns assignments and permission scopes in one call
    """
    principal = authenticate_bearer(request)
    tenant_slug = str(request.headers.get("x-tenant-id") or "").strip().lower()
    preferred_app_code = str(request.headers.get("x-app-code") or "").strip().lower() or None

    if tenant_slug:
        try:
            tenant_id, resolved_tenant_slug = validate_active_tenant_membership(
                user_id=principal.user_id,
                tenant_slug=tenant_slug,
            )
        except TenantAccessError as exc:
            if str(getattr(exc, "code", "") or "").strip().lower() == "tenant_membership_required":
                return _build_workspace_session_me_payload(
                    user_id=principal.user_id,
                    user_email=principal.email,
                    preferred_app_code=preferred_app_code,
                )
            raise
        permissions: set[str] = set()
        role = ""
        access_app_id = ""
        access_app_code = ""
        if preferred_app_code:
            result = authorize_bearer_for_tenant_app(request=request, app_code=preferred_app_code)
            tenant_id = result.access.tenant_id
            resolved_tenant_slug = result.access.tenant_slug
            permissions = set(result.access.permissions)
            role = result.access.role
            access_app_id = result.access.app_id
            access_app_code = result.access.app_code

        assignments, scope = _build_assignments_scope_payload(
            user_id=principal.user_id,
            role=role,
            permissions=permissions,
        )
        if preferred_app_code and scope.get("isSuperAdmin") and not assignments and access_app_id and access_app_code:
            try:
                assignments = _build_super_admin_tenant_assignments_for_app(
                    app_id=access_app_id,
                    app_code=access_app_code,
                )
            except SupabaseClientError:
                assignments = []
        assignments = _filter_assignments_by_backend_app_config(assignments)

        return _build_validate_payload(
            user_id=principal.user_id,
            user_email=principal.email,
            tenant_id=tenant_id,
            tenant_slug=resolved_tenant_slug,
            assignments=assignments,
            scope=scope,
            preferred_app_code=preferred_app_code,
            include_selected_assignment=True,
            resolved_permissions=None,
            page_permissions=list_page_permissions_for_user(
                user_id=principal.user_id,
                tenant_id=tenant_id,
            ),
        )
    return _build_workspace_session_me_payload(
        user_id=principal.user_id,
        user_email=principal.email,
        preferred_app_code=preferred_app_code,
    )


@router.get("/assignments")
def get_session_assignments(request: Request):
    """
    Return all tenant-app assignments for the authenticated user without requiring a tenant header, for auth recovery/tenant-selection UX.

    Example request:
        GET /api/auth/v1/session/assignments

    Example response:
        {
          "assignments": [
            {
              "tenant": {"id": "tenant-1", "slug": "taaa", "name": "TAAA"},
              "app": {"id": "app-1", "code": "tradsphere", "name": "Tradsphere"},
              "role": "viewer"
            }
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Returns assignments with active tenant membership only
    """
    principal = authenticate_bearer(request)
    assignments = list_user_access_assignments(user_id=principal.user_id)
    try:
        is_super = is_user_super_admin(user_id=principal.user_id)
    except SupabaseClientError:
        is_super = False

    if is_super and not assignments:
        tradsphere_app = next(
            (
                app
                for app in list_active_apps()
                if str(app.get("code") or "").strip().lower() == "tradsphere"
            ),
            None,
        )
        if tradsphere_app:
            assignments = _build_super_admin_tenant_assignments_for_app(
                app_id=str(tradsphere_app.get("id") or "").strip(),
                app_code=str(tradsphere_app.get("code") or "").strip().lower(),
            )

    assignments = _filter_assignments_by_backend_app_config(assignments)
    return {"assignments": assignments}


@router.get("/validate")
def get_session_validate(request: Request):
    """
    Validate session + tenant membership and return user access state in one response for frontend auth guards.

    Example request:
        GET /api/auth/v1/session/validate

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
                "name": "TAAA",
                "status": "active"
              },
              "app": {
                "id": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
                "code": "tradsphere",
                "name": "Tradsphere"
              },
              "role": "viewer"
            }
          ],
          "appAccess": [
            {
              "app": {
                "id": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
                "code": "tradsphere",
                "name": "Tradsphere"
              },
              "role": "viewer",
              "permissions": ["tradsphere.viewer"]
            }
          ],
          "scope": {
            "isSuperAdmin": false,
            "hasAnyAdminScope": false
          },
          "pagePermissions": [
            {
              "tenantId": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
              "tenantSlug": "taaa",
              "appId": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
              "appCode": "tradsphere",
              "pageKey": "tradsphere_home"
            }
          ],
          "permissions": ["tradsphere.viewer"]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - X-Tenant-Id is optional for workspace-level login/home/profile access
        - Uses app-level validation only when X-App-Code is provided
        - Without X-Tenant-Id, returns the user's workspace assignments and selects a primary tenant/app if available
        - Enforces active user status before tenant/app permission checks
        - With X-Tenant-Id and X-App-Code, non-super users require active tenant membership + app role
        - Super Admin can validate tenant/app access without explicit tenant/app assignment
        - Returns assignments and permission scopes in one call
    """
    principal = authenticate_bearer(request)
    tenant_slug = str(request.headers.get("x-tenant-id") or "").strip().lower()
    preferred_app_code = str(request.headers.get("x-app-code") or "").strip().lower() or None

    if tenant_slug:
        try:
            tenant_id, resolved_tenant_slug = validate_active_tenant_membership(
                user_id=principal.user_id,
                tenant_slug=tenant_slug,
            )
        except TenantAccessError as exc:
            if str(getattr(exc, "code", "") or "").strip().lower() == "tenant_membership_required":
                return _build_workspace_session_me_payload(
                    user_id=principal.user_id,
                    user_email=principal.email,
                    preferred_app_code=preferred_app_code,
                )
            raise HTTPException(
                status_code=_status_code_for_access_error(exc),
                detail={
                    "message": str(exc),
                    "code": str(getattr(exc, "code", "") or "").strip().lower() or "forbidden",
                },
            ) from exc

        permissions: set[str] = set()
        role = ""
        if preferred_app_code:
            result = authorize_bearer_for_tenant_app(request=request, app_code=preferred_app_code)
            tenant_id = result.access.tenant_id
            resolved_tenant_slug = result.access.tenant_slug
            role = result.access.role
            permissions = set(result.access.permissions)

        assignments, scope = _build_assignments_scope_payload(
            user_id=principal.user_id,
            role=role,
            permissions=permissions,
        )
        if preferred_app_code and scope.get("isSuperAdmin") and not assignments:
            try:
                assignments = _build_super_admin_tenant_assignments_for_app(
                    app_id=result.access.app_id,
                    app_code=result.access.app_code,
                )
            except SupabaseClientError:
                assignments = []
        assignments = _filter_assignments_by_backend_app_config(assignments)

        return _build_validate_payload(
            user_id=principal.user_id,
            user_email=principal.email,
            tenant_id=tenant_id,
            tenant_slug=resolved_tenant_slug,
            assignments=assignments,
            scope=scope,
            preferred_app_code=preferred_app_code,
            include_selected_assignment=True,
            resolved_permissions=None,
            page_permissions=list_page_permissions_for_user(
                user_id=principal.user_id,
                tenant_id=tenant_id,
            ),
        )

    assignments = list_user_access_assignments(user_id=principal.user_id)
    try:
        is_super_admin = is_user_super_admin(user_id=principal.user_id)
    except SupabaseClientError:
        is_super_admin = False

    scope = {
        "isSuperAdmin": is_super_admin,
        "hasAnyAdminScope": is_super_admin
        or any(
            str(item.get("role") or "").strip().lower() == "admin"
            for item in assignments
            if isinstance(item, dict)
        ),
    }

    if is_super_admin and not assignments:
        app = None
        if preferred_app_code:
            app = next(
                (
                    item
                    for item in list_active_apps()
                    if str(item.get("code") or "").strip().lower() == preferred_app_code
                ),
                None,
            )
        if app is None:
            app = next(
                (
                    item
                    for item in list_active_apps()
                    if str(item.get("code") or "").strip().lower() == "tradsphere"
                ),
                None,
            )
        if app:
            assignments = _build_super_admin_tenant_assignments_for_app(
                app_id=str(app.get("id") or "").strip(),
                app_code=str(app.get("code") or "").strip().lower(),
            )

    assignments = _filter_assignments_by_backend_app_config(assignments)
    selected_assignment = _resolve_selected_assignment(
        assignments=assignments,
        tenant_slug="",
        preferred_app_code=preferred_app_code,
    )
    tenant_id = ""
    resolved_tenant_slug = ""
    page_permissions: list[dict[str, object]] = []
    if selected_assignment is not None:
        selected_tenant = selected_assignment.get("tenant")
        selected_app = selected_assignment.get("app")
        if isinstance(selected_tenant, dict):
            tenant_id = str(selected_tenant.get("id") or "").strip()
            resolved_tenant_slug = str(selected_tenant.get("slug") or "").strip().lower()
        if tenant_id:
            page_permissions = list_page_permissions_for_user(user_id=principal.user_id, tenant_id=tenant_id)
        if isinstance(selected_app, dict):
            role = str(selected_assignment.get("role") or "").strip()
            permissions = _permission_set_for_assignment(
                app_code=str(selected_app.get("code") or "").strip().lower(),
                role=role,
            )
            if scope.get("isSuperAdmin"):
                permissions.add("workspace.super_admin")
        else:
            permissions = {"workspace.super_admin"} if scope.get("isSuperAdmin") else set()
    else:
        permissions = {"workspace.super_admin"} if scope.get("isSuperAdmin") else set()

    return _build_validate_payload(
        user_id=principal.user_id,
        user_email=principal.email,
        tenant_id=tenant_id,
        tenant_slug=resolved_tenant_slug,
        assignments=assignments,
        scope=scope,
        preferred_app_code=preferred_app_code,
        include_selected_assignment=True,
        resolved_permissions=permissions,
        page_permissions=page_permissions,
    )


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
        - X-Tenant-Id is optional
        - Accepts either `fullName` or `firstName`/`lastName`
    """
    principal = authenticate_bearer(request)
    full_name = _parse_profile_full_name(payload)
    if full_name is None:
        raise HTTPException(status_code=400, detail="Profile name is required")
    if len(full_name) > 255:
        raise HTTPException(status_code=400, detail="Profile name exceeds maximum length")

    try:
        upsert_profile_basic_info(
            provider=_provider(),
            user_id=principal.user_id,
            email=principal.email,
            full_name=full_name,
        )
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return {
        "user": _build_user_payload(
            user_id=principal.user_id,
            email=principal.email,
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
