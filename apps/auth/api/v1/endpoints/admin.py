from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Body, HTTPException, Query, Request

from shared.auth.admin_repo import (
    get_user_app_roles,
    get_user_memberships,
    is_user_super_admin,
    list_admin_assignment_scopes_for_user,
    list_active_apps,
    list_active_tenants,
    list_role_keys_from_store,
    list_users_with_access,
    remove_tenant_app_role,
    remove_tenant_app_roles_for_tenant,
    set_tenant_app_role,
    set_tenant_user_status,
    unique_text_values,
)
from shared.auth.config import get_invite_base_url, get_invite_ttl_hours
from shared.auth.dependencies import authorize_bearer_for_tenant_app
from shared.auth.invite_url import build_invite_url
from shared.auth.invitations_repo import (
    create_invitation,
    get_invitation_by_id,
    insert_invitation_assignments,
    list_invitation_assignments,
    list_invitations_scoped,
    patch_invitation_status,
)
from shared.auth.permissions_cache import permission_cache
from shared.auth.profile_repo import compose_full_name, upsert_profile_basic_info
from shared.auth.roles import (
    ADMIN_MANAGE_PERMISSION_KEYS,
    ROLE_ADMIN,
    ROLE_EDITOR,
    ROLE_SUPER_ADMIN,
    ROLE_VIEWER,
    is_super_admin_role,
    normalize_role_key,
    role_group_key,
    role_label,
)
from shared.auth.supabase_client import SupabaseClientError

router = APIRouter(prefix="/admin")

VALID_MEMBERSHIP_STATUSES = {"active", "pending", "disabled"}
VALID_INVITATION_STATUSES = {"pending", "accepted", "revoked", "expired"}
FALLBACK_ROLE_KEYS = [ROLE_VIEWER, ROLE_EDITOR, ROLE_ADMIN]
ASSIGNABLE_ROLE_KEYS = [ROLE_ADMIN, ROLE_EDITOR, ROLE_VIEWER]


def _is_super_admin(*, role: str, permissions: set[str]) -> bool:
    return is_super_admin_role(role) or "workspace.super_admin" in permissions


def _can_manage_admin_users(*, role: str, permissions: set[str]) -> bool:
    if _is_super_admin(role=role, permissions=permissions):
        return True
    return any(permission in permissions for permission in ADMIN_MANAGE_PERMISSION_KEYS)


def _require_admin(request: Request):
    result = authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")
    permissions = set(result.access.permissions)
    if not _can_manage_admin_users(role=result.access.role, permissions=permissions):
        raise HTTPException(status_code=403, detail="Forbidden")
    is_super_admin = _is_super_admin(role=result.access.role, permissions=permissions)
    admin_scopes = None if is_super_admin else list_admin_assignment_scopes_for_user(user_id=result.principal.user_id)
    if not is_super_admin and not admin_scopes:
        admin_scopes = {(result.access.tenant_id, result.access.app_id)}
    return result, is_super_admin, admin_scopes


def _invite_url(token: str) -> str:
    return build_invite_url(token=token, base_url=get_invite_base_url())


def _parse_profile_full_name(payload: dict[str, object]) -> str | None:
    profile_payload = payload.get("profile")
    profile = profile_payload if isinstance(profile_payload, dict) else {}
    return compose_full_name(
        full_name=str(profile.get("fullName") or payload.get("fullName") or "").strip() or None,
        first_name=str(profile.get("firstName") or payload.get("firstName") or "").strip() or None,
        last_name=str(profile.get("lastName") or payload.get("lastName") or "").strip() or None,
    )


def _parse_optional_email(payload: dict[str, object]) -> str | None:
    email = str(payload.get("email") or "").strip().lower()
    return email or None


def _normalized_role_keys() -> list[str]:
    role_keys = unique_text_values(list_role_keys_from_store() + FALLBACK_ROLE_KEYS)
    ordered: list[str] = []
    for key in [ROLE_ADMIN, ROLE_EDITOR, ROLE_VIEWER]:
        if key in role_keys:
            ordered.append(key)
            role_keys.remove(key)
    ordered.extend(sorted(role_keys))
    return ordered


def _role_catalog() -> tuple[list[dict[str, object]], set[str], bool]:
    role_keys = _normalized_role_keys()
    role_set = set(role_keys)
    super_admin_supported = True
    role_keys = [ROLE_SUPER_ADMIN, *[role_key for role_key in role_keys if role_key != ROLE_SUPER_ADMIN]]

    items: list[dict[str, object]] = []
    for role_key in role_keys:
        is_super_admin = role_key == ROLE_SUPER_ADMIN
        assignable = role_key in ASSIGNABLE_ROLE_KEYS and role_key in role_set
        items.append(
            {
                "key": role_key,
                "label": role_label(role_key),
                "group": role_group_key(role_key),
                "assignable": assignable,
                "requiresSchemaChange": False,
            }
        )
    assignable_set = {str(item["key"]) for item in items if bool(item.get("assignable"))}
    return items, assignable_set, super_admin_supported


def _scope_tenants_for_actor(
    *,
    is_super_admin: bool,
    admin_scopes: set[tuple[str, str]] | None,
    current_tenant_id: str,
) -> set[str] | None:
    if is_super_admin:
        return None
    if admin_scopes:
        return {tenant_id for tenant_id, _ in admin_scopes}
    return {current_tenant_id}


def _scope_apps_for_actor(
    *,
    is_super_admin: bool,
    admin_scopes: set[tuple[str, str]] | None,
    current_app_id: str,
) -> set[str] | None:
    if is_super_admin:
        return None
    if admin_scopes:
        return {app_id for _, app_id in admin_scopes}
    return {current_app_id}


def _validate_scope_pairs(
    *,
    assignment_pairs: set[tuple[str, str]],
    allowed_scope_pairs: set[tuple[str, str]] | None,
) -> None:
    if allowed_scope_pairs is None:
        return
    outside = [pair for pair in assignment_pairs if pair not in allowed_scope_pairs]
    if outside:
        raise HTTPException(status_code=403, detail="Forbidden tenant/app scope")


def _resolve_tenant_catalog(
    *,
    is_super_admin: bool,
    current_tenant_id: str,
    admin_scopes: set[tuple[str, str]] | None,
) -> list[dict[str, object]]:
    tenants = list_active_tenants()
    if is_super_admin:
        return tenants
    allowed_tenant_ids = _scope_tenants_for_actor(
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_tenant_id=current_tenant_id,
    )
    return [tenant for tenant in tenants if str(tenant.get("id") or "") in (allowed_tenant_ids or set())]


def _resolve_app_catalog(
    *,
    is_super_admin: bool,
    current_app_id: str,
    admin_scopes: set[tuple[str, str]] | None,
) -> list[dict[str, object]]:
    apps = list_active_apps()
    if is_super_admin:
        return apps
    allowed_app_ids = _scope_apps_for_actor(
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_app_id=current_app_id,
    )
    return [app for app in apps if str(app.get("id") or "") in (allowed_app_ids or set())]


def _parse_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "yes", "on"}


def _query_text(value: object, default: str | None = None) -> str | None:
    if value is None:
        return default
    if isinstance(value, str):
        return value
    return default


def _validate_tenant_scope(*, tenant_ids: set[str], allowed_tenant_ids: set[str] | None) -> None:
    if allowed_tenant_ids is None:
        return
    outside = [tenant_id for tenant_id in tenant_ids if tenant_id not in allowed_tenant_ids]
    if outside:
        raise HTTPException(status_code=403, detail="Forbidden tenant scope")


def _validate_app_scope(*, app_ids: set[str], allowed_app_ids: set[str] | None) -> None:
    if allowed_app_ids is None:
        return
    outside = [app_id for app_id in app_ids if app_id not in allowed_app_ids]
    if outside:
        raise HTTPException(status_code=403, detail="Forbidden app scope")


def _scope_user_rows_by_assignment_pairs(
    *,
    rows: list[dict[str, object]],
    allowed_scope_pairs: set[tuple[str, str]] | None,
) -> list[dict[str, object]]:
    if allowed_scope_pairs is None:
        return rows

    allowed_tenant_ids = {tenant_id for tenant_id, _ in allowed_scope_pairs}
    scoped_rows: list[dict[str, object]] = []
    for row in rows:
        app_assignments = row.get("appAssignments")
        tenant_memberships = row.get("tenantMemberships")
        assignments = app_assignments if isinstance(app_assignments, list) else []
        memberships = tenant_memberships if isinstance(tenant_memberships, list) else []
        next_assignments = [
            item
            for item in assignments
            if isinstance(item, dict)
            and (str(item.get("tenantId") or "").strip(), str(item.get("appId") or "").strip()) in allowed_scope_pairs
        ]
        next_memberships = [
            item
            for item in memberships
            if isinstance(item, dict) and str(item.get("tenantId") or "").strip() in allowed_tenant_ids
        ]
        if not next_assignments and not next_memberships:
            continue
        next_row = dict(row)
        next_row["appAssignments"] = next_assignments
        next_row["tenantMemberships"] = next_memberships
        role_candidates = [normalize_role_key(str(item.get("role") or "").strip()) for item in next_assignments if isinstance(item, dict)]
        role_candidates = [role for role in role_candidates if role]
        if next_row.get("isSuperAdmin"):
            next_row["role"] = ROLE_SUPER_ADMIN
        elif role_candidates:
            rank = {
                ROLE_ADMIN: 1,
                ROLE_EDITOR: 2,
                ROLE_VIEWER: 3,
            }
            next_row["role"] = sorted(role_candidates, key=lambda role: rank.get(role, 99))[0]
        scoped_rows.append(next_row)
    return scoped_rows


def _list_roles_payload() -> dict[str, object]:
    items, _, super_admin_supported = _role_catalog()
    return {
        "roles": [item["key"] for item in items if item.get("assignable")],
        "items": items,
        "superAdminSupported": super_admin_supported,
    }


def _list_users_payload(
    *,
    result,
    is_super_admin: bool,
    admin_scopes: set[tuple[str, str]] | None,
    include_all_tenants: str | None,
) -> dict[str, object]:
    include_all_filter = _query_text(include_all_tenants, default=None)
    include_all = _parse_bool(include_all_filter, default=False)
    include_global = bool(is_super_admin and (include_all_filter is None or include_all))
    tenant_scope = None if include_global or is_super_admin else _scope_tenants_for_actor(
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_tenant_id=result.access.tenant_id,
    )
    app_scope = None if include_global or is_super_admin else _scope_apps_for_actor(
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_app_id=result.access.app_id,
    )

    items = list_users_with_access(
        scope_tenant_ids=tenant_scope,
        scope_app_ids=app_scope,
        primary_tenant_id=result.access.tenant_id,
        primary_app_id=result.access.app_id,
    )
    if not is_super_admin:
        items = _scope_user_rows_by_assignment_pairs(rows=items, allowed_scope_pairs=admin_scopes)
    return {
        "items": items,
        "scope": {
            "isSuperAdmin": is_super_admin,
            "includeAllTenants": include_global,
            "tenantId": result.access.tenant_id,
            "tenantSlug": result.access.tenant_slug,
            "appId": result.access.app_id,
            "appCode": result.access.app_code,
        },
    }


def _list_invitations_payload(
    *,
    result,
    is_super_admin: bool,
    admin_scopes: set[tuple[str, str]] | None,
    status: str | None,
) -> dict[str, object]:
    status_filter = _query_text(status, default=None)
    normalized_status = str(status_filter or "").strip().lower() or None
    if normalized_status and normalized_status not in VALID_INVITATION_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid status filter")

    tenant_scope = _scope_tenants_for_actor(
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_tenant_id=result.access.tenant_id,
    )
    app_scope = _scope_apps_for_actor(
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_app_id=result.access.app_id,
    )
    rows = list_invitations_scoped(tenant_ids=tenant_scope, app_ids=app_scope, status=normalized_status)

    tenant_by_id = {str(item.get("id") or ""): item for item in list_active_tenants()}
    app_by_id = {str(item.get("id") or ""): item for item in list_active_apps()}

    items: list[dict[str, object]] = []
    for row in rows:
        invitation_id = str(row.get("id") or "").strip()
        tenant_id = str(row.get("tenant_id") or "").strip()
        app_id = str(row.get("app_id") or "").strip()
        assignments_raw = list_invitation_assignments(invitation_id=invitation_id)
        assignments = assignments_raw or [
            {
                "tenant_id": tenant_id,
                "app_id": app_id,
                "role": normalize_role_key(str(row.get("role") or "").strip()),
            }
        ]
        assignment_pairs = {
            (str(item.get("tenant_id") or "").strip(), str(item.get("app_id") or "").strip())
            for item in assignments
            if str(item.get("tenant_id") or "").strip() and str(item.get("app_id") or "").strip()
        }
        if not is_super_admin and admin_scopes is not None and any(pair not in admin_scopes for pair in assignment_pairs):
            continue

        formatted_assignments: list[dict[str, object]] = []
        for item in assignments:
            assignment_tenant_id = str(item.get("tenant_id") or "").strip()
            assignment_app_id = str(item.get("app_id") or "").strip()
            assignment_role = normalize_role_key(str(item.get("role") or "").strip())
            if not assignment_tenant_id or not assignment_app_id or not assignment_role:
                continue
            assignment_tenant = tenant_by_id.get(assignment_tenant_id, {})
            assignment_app = app_by_id.get(assignment_app_id, {})
            formatted_assignments.append(
                {
                    "tenantId": assignment_tenant_id,
                    "tenantSlug": assignment_tenant.get("slug"),
                    "tenantName": assignment_tenant.get("name"),
                    "appId": assignment_app_id,
                    "appCode": assignment_app.get("code"),
                    "appName": assignment_app.get("name"),
                    "role": assignment_role,
                }
            )
        formatted_assignments.sort(
            key=lambda item: (
                str(item.get("tenantSlug") or "~").lower(),
                str(item.get("appCode") or "~").lower(),
                str(item.get("role") or "~").lower(),
            )
        )
        primary_assignment = formatted_assignments[0] if formatted_assignments else {}
        token = str(row.get("token") or "").strip()
        items.append(
            {
                "id": invitation_id,
                "email": row.get("email"),
                "tenantId": primary_assignment.get("tenantId") or tenant_id,
                "tenantSlug": primary_assignment.get("tenantSlug"),
                "tenantName": primary_assignment.get("tenantName"),
                "appId": primary_assignment.get("appId") or app_id,
                "appCode": primary_assignment.get("appCode"),
                "appName": primary_assignment.get("appName"),
                "role": primary_assignment.get("role") or normalize_role_key(str(row.get("role") or "").strip()),
                "status": row.get("status"),
                "expiresAt": row.get("expires_at"),
                "createdAt": row.get("created_at"),
                "acceptedAt": row.get("accepted_at"),
                "inviteUrl": _invite_url(token) if token else None,
                "assignments": formatted_assignments,
                "assignmentCount": len(formatted_assignments),
            }
        )
    items.sort(key=lambda item: (str(item.get("createdAt") or ""), str(item.get("email") or "").lower()), reverse=True)
    return {"items": items}


def _list_admin_users_page_load_payload(
    *,
    result,
    is_super_admin: bool,
    admin_scopes: set[tuple[str, str]] | None,
    include_all_tenants: str | None,
    invitation_status: str | None,
) -> dict[str, object]:
    tenants = _resolve_tenant_catalog(
        is_super_admin=is_super_admin,
        current_tenant_id=result.access.tenant_id,
        admin_scopes=admin_scopes,
    )
    apps = _resolve_app_catalog(
        is_super_admin=is_super_admin,
        current_app_id=result.access.app_id,
        admin_scopes=admin_scopes,
    )
    roles_payload = _list_roles_payload()
    users_payload = _list_users_payload(
        result=result,
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        include_all_tenants=include_all_tenants,
    )
    invitations_payload = _list_invitations_payload(
        result=result,
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        status=invitation_status,
    )
    return {
        "tenants": tenants,
        "apps": apps,
        "roles": roles_payload.get("items") or [],
        "users": users_payload.get("items") or [],
        "invitations": invitations_payload.get("items") or [],
        "scope": users_payload.get("scope"),
    }


@router.get("/tenants")
def list_tenants_route(request: Request):
    """
    List active tenants available for admin user assignment workflows.

    Example request:
        GET /api/auth/v1/admin/tenants

    Example response:
        {
          "items": [
            {
              "id": "tenant-1",
              "slug": "taaa",
              "name": "Tenant AAA",
              "active": true
            }
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires admin manage permission (`tradsphere.admin` or `workspace.super_admin`)
    """
    result, is_super_admin, admin_scopes = _require_admin(request)
    return {
        "items": _resolve_tenant_catalog(
            is_super_admin=is_super_admin,
            current_tenant_id=result.access.tenant_id,
            admin_scopes=admin_scopes,
        )
    }


@router.get("/apps")
def list_apps_route(request: Request):
    """
    List active apps available for admin user assignment workflows.

    Example request:
        GET /api/auth/v1/admin/apps

    Example response:
        {
          "items": [
            {
              "id": "app-1",
              "code": "tradsphere",
              "name": "TradSphere",
              "active": true
            }
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires admin manage permission (`tradsphere.admin` or `workspace.super_admin`)
    """
    result, is_super_admin, admin_scopes = _require_admin(request)
    return {
        "items": _resolve_app_catalog(
            is_super_admin=is_super_admin,
            current_app_id=result.access.app_id,
            admin_scopes=admin_scopes,
        )
    }


@router.get("/roles")
def list_roles_route(request: Request):
    """
    Return assignable role catalog and display labels for admin user-management workflows.

    Example request:
        GET /api/auth/v1/admin/roles

    Example response:
        {
          "roles": ["viewer", "editor", "admin"],
          "items": [
            {"key": "super_admin", "label": "Super Admin", "assignable": false, "requiresSchemaChange": false},
            {"key": "admin", "label": "Admin", "assignable": true, "requiresSchemaChange": false}
          ],
          "superAdminSupported": true
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires admin manage permission (`tradsphere.admin` or `workspace.super_admin`)
    """
    _require_admin(request)
    return _list_roles_payload()


@router.get("/users")
def list_users_route(
    request: Request,
    include_all_tenants: str | None = Query(default=None, alias="includeAllTenants"),
):
    """
    List users with tenant memberships and app-role assignments for admin management.

    Example request:
        GET /api/auth/v1/admin/users

    Example response:
        {
          "items": [
            {
              "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
              "email": "admin@company.com",
              "fullName": "Workspace Admin",
              "status": "active",
              "role": "admin",
              "tenantMemberships": [{"tenantId": "tenant-1", "tenantSlug": "taaa", "status": "active"}],
              "appAssignments": [{"tenantId": "tenant-1", "appCode": "tradsphere", "role": "admin"}]
            }
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires admin manage permission (`tradsphere.admin` or `workspace.super_admin`)
        - `includeAllTenants=true` is effective only for `workspace.super_admin`
    """
    result, is_super_admin, admin_scopes = _require_admin(request)
    return _list_users_payload(
        result=result,
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        include_all_tenants=_query_text(include_all_tenants, default=None),
    )


@router.get("/users/load")
def load_admin_users_page_route(
    request: Request,
    include_all_tenants: str | None = Query(default=None, alias="includeAllTenants"),
    invitation_status: str | None = Query(default="pending", alias="invitationStatus"),
):
    """
    Load all Admin Users page data in one request for initial UI render and single-shot refreshes.

    Example request:
        GET /api/auth/v1/admin/users/load

    Example request (super admin all-tenant scope):
        GET /api/auth/v1/admin/users/load?includeAllTenants=true

    Example response:
        {
          "tenants": [{"id": "tenant-1", "slug": "taaa", "name": "Tenant AAA", "active": true}],
          "apps": [{"id": "app-1", "code": "tradsphere", "name": "TradSphere", "active": true}],
          "roles": [{"key": "admin", "label": "Admin", "assignable": true, "requiresSchemaChange": false}],
          "users": [{"userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d", "email": "admin@company.com"}],
          "invitations": [{"id": "invite-1", "email": "new.user@company.com", "status": "pending"}]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires admin manage permission (`tradsphere.admin` or `workspace.super_admin`)
        - `includeAllTenants=true` is effective only for `workspace.super_admin`
        - `invitationStatus` accepts: pending, accepted, revoked, expired
    """
    result, is_super_admin, admin_scopes = _require_admin(request)
    return _list_admin_users_page_load_payload(
        result=result,
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        include_all_tenants=_query_text(include_all_tenants, default=None),
        invitation_status=_query_text(invitation_status, default="pending"),
    )


def _normalize_membership_payload(
    payload: dict[str, object],
    *,
    default_tenant_id: str,
) -> dict[str, str]:
    items_raw = payload.get("tenantMemberships")
    items = items_raw if isinstance(items_raw, list) else None
    memberships: dict[str, str] = {}
    if items is not None:
        for item in items:
            if not isinstance(item, dict):
                continue
            tenant_id = str(item.get("tenantId") or "").strip()
            status = str(item.get("status") or "active").strip().lower()
            if not tenant_id:
                continue
            memberships[tenant_id] = status
        return memberships

    next_status_raw = payload.get("status")
    status = str(next_status_raw or "").strip().lower() if next_status_raw is not None else ""
    if status:
        memberships[default_tenant_id] = status
    return memberships


def _normalize_assignment_payload(
    payload: dict[str, object],
    *,
    default_tenant_id: str,
    default_app_id: str,
    app_id_by_code: dict[str, str],
) -> dict[tuple[str, str], str]:
    items_raw = payload.get("appAssignments")
    items = items_raw if isinstance(items_raw, list) else None
    assignments: dict[tuple[str, str], str] = {}
    if items is not None:
        for item in items:
            if not isinstance(item, dict):
                continue
            tenant_id = str(item.get("tenantId") or "").strip()
            app_id = str(item.get("appId") or "").strip()
            app_code = str(item.get("appCode") or "").strip().lower()
            role = normalize_role_key(str(item.get("role") or "").strip())
            if not tenant_id or not role:
                continue
            resolved_app_id = app_id or app_id_by_code.get(app_code, "")
            if not resolved_app_id:
                continue
            assignments[(tenant_id, resolved_app_id)] = role
        return assignments

    next_role_raw = payload.get("role")
    role = normalize_role_key(str(next_role_raw or "").strip()) if next_role_raw is not None else ""
    if role:
        assignments[(default_tenant_id, default_app_id)] = role
    return assignments


def _lookup_auth_email(*, user_id: str) -> str | None:
    from shared.auth.providers import get_auth_provider

    try:
        auth_user = get_auth_provider().get_auth_user_by_id(user_id=user_id) or {}
    except SupabaseClientError:
        return None
    email = str(auth_user.get("email") or "").strip().lower()
    return email or None


def _upsert_user_profile(
    *,
    user_id: str,
    payload: dict[str, object],
) -> bool:
    from shared.auth.providers import get_auth_provider

    next_full_name = _parse_profile_full_name(payload)
    if next_full_name is None:
        return False
    if len(next_full_name) > 255:
        raise HTTPException(status_code=400, detail="Full name exceeds maximum length")
    resolved_email = _parse_optional_email(payload) or _lookup_auth_email(user_id=user_id)
    upsert_profile_basic_info(
        provider=get_auth_provider(),
        user_id=user_id,
        email=resolved_email,
        full_name=next_full_name,
    )
    return True


def _apply_membership_and_role_changes(
    *,
    user_id: str,
    desired_memberships: dict[str, str],
    desired_assignments: dict[tuple[str, str], str],
    is_super_admin: bool,
    admin_scopes: set[tuple[str, str]] | None,
    current_tenant_id: str,
    current_app_id: str,
    assignable_roles: set[str],
    replace_memberships: bool,
    replace_assignments: bool,
) -> tuple[bool, set[str], set[str]]:
    tenant_ids_in_payload = set(desired_memberships.keys()) | {tenant_id for tenant_id, _ in desired_assignments.keys()}
    app_ids_in_payload = {app_id for _, app_id in desired_assignments.keys()}
    scope_pairs_in_payload = set(desired_assignments.keys())

    tenant_scope = _scope_tenants_for_actor(
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_tenant_id=current_tenant_id,
    )
    app_scope = _scope_apps_for_actor(
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_app_id=current_app_id,
    )
    _validate_tenant_scope(tenant_ids=tenant_ids_in_payload, allowed_tenant_ids=tenant_scope)
    _validate_app_scope(app_ids=app_ids_in_payload, allowed_app_ids=app_scope)
    _validate_scope_pairs(assignment_pairs=scope_pairs_in_payload, allowed_scope_pairs=admin_scopes if not is_super_admin else None)

    for status in desired_memberships.values():
        if status not in VALID_MEMBERSHIP_STATUSES:
            raise HTTPException(status_code=400, detail="Invalid status")

    for role in desired_assignments.values():
        normalized_role = normalize_role_key(role)
        if not normalized_role:
            raise HTTPException(status_code=400, detail=f"Invalid or unsupported role: {role}")
        if normalized_role != role:
            role = normalized_role
        if role not in assignable_roles:
            raise HTTPException(status_code=400, detail=f"Invalid or unsupported role: {role}")
        if role == ROLE_SUPER_ADMIN:
            raise HTTPException(status_code=400, detail="super_admin cannot be assigned from this endpoint")

    for (tenant_id, _), role in desired_assignments.items():
        membership_status = desired_memberships.get(tenant_id)
        if membership_status == "disabled":
            raise HTTPException(
                status_code=400,
                detail=f"Cannot assign role '{role}' on disabled tenant membership '{tenant_id}'",
            )
        if membership_status is None:
            desired_memberships[tenant_id] = "active"

    existing_memberships = get_user_memberships(user_id=user_id, scope_tenant_ids=tenant_scope)
    existing_roles = get_user_app_roles(user_id=user_id, scope_tenant_ids=tenant_scope)
    if not is_super_admin and admin_scopes is not None:
        existing_roles = [
            row
            for row in existing_roles
            if (
                str(row.get("tenant_id") or "").strip(),
                str(row.get("app_id") or "").strip(),
            ) in admin_scopes
        ]

    existing_membership_ids = {str(row.get("tenant_id") or "").strip() for row in existing_memberships}
    existing_role_keys = {
        (str(row.get("tenant_id") or "").strip(), str(row.get("app_id") or "").strip())
        for row in existing_roles
        if str(row.get("tenant_id") or "").strip() and str(row.get("app_id") or "").strip()
    }

    changed = False
    affected_tenant_ids: set[str] = set()
    affected_app_ids: set[str] = set()

    for tenant_id, status in desired_memberships.items():
        row = next((item for item in existing_memberships if str(item.get("tenant_id") or "").strip() == tenant_id), None)
        current_status = str((row or {}).get("status") or "").strip().lower() or None
        if current_status != status:
            set_tenant_user_status(tenant_id=tenant_id, user_id=user_id, status=status)
            changed = True
            affected_tenant_ids.add(tenant_id)

    if replace_memberships:
        all_existing_roles = get_user_app_roles(user_id=user_id, scope_tenant_ids=None)
        for tenant_id in existing_membership_ids:
            if tenant_id in desired_memberships:
                continue
            if not is_super_admin and admin_scopes is not None:
                outside_scope_exists = any(
                    str(row.get("tenant_id") or "").strip() == tenant_id
                    and (
                        str(row.get("tenant_id") or "").strip(),
                        str(row.get("app_id") or "").strip(),
                    )
                    not in admin_scopes
                    for row in all_existing_roles
                )
                if outside_scope_exists:
                    raise HTTPException(
                        status_code=403,
                        detail="Cannot disable tenant membership containing app assignments outside your admin scope",
                    )
            set_tenant_user_status(tenant_id=tenant_id, user_id=user_id, status="disabled")
            remove_tenant_app_roles_for_tenant(tenant_id=tenant_id, user_id=user_id)
            changed = True
            affected_tenant_ids.add(tenant_id)

    for (tenant_id, app_id), role in desired_assignments.items():
        row = next(
            (
                item
                for item in existing_roles
                if str(item.get("tenant_id") or "").strip() == tenant_id and str(item.get("app_id") or "").strip() == app_id
            ),
            None,
        )
        current_role = normalize_role_key(str((row or {}).get("role") or "").strip()) or None
        if current_role != role:
            set_tenant_app_role(tenant_id=tenant_id, user_id=user_id, app_id=app_id, role=role)
            changed = True
            affected_tenant_ids.add(tenant_id)
            affected_app_ids.add(app_id)

    if replace_assignments:
        for tenant_id, app_id in existing_role_keys:
            if (tenant_id, app_id) in desired_assignments:
                continue
            remove_tenant_app_role(tenant_id=tenant_id, user_id=user_id, app_id=app_id)
            changed = True
            affected_tenant_ids.add(tenant_id)
            affected_app_ids.add(app_id)

    if replace_assignments and not replace_memberships:
        managed_tenant_ids = {tenant_id for tenant_id, _ in desired_assignments.keys()} | {
            tenant_id for tenant_id, _ in existing_role_keys
        }
        if managed_tenant_ids:
            latest_roles = get_user_app_roles(user_id=user_id, scope_tenant_ids=None)
            latest_memberships = get_user_memberships(user_id=user_id, scope_tenant_ids=tenant_scope)
            for tenant_id in managed_tenant_ids:
                has_any_role = any(str(row.get("tenant_id") or "").strip() == tenant_id for row in latest_roles)
                if has_any_role:
                    continue
                membership_row = next(
                    (item for item in latest_memberships if str(item.get("tenant_id") or "").strip() == tenant_id),
                    None,
                )
                membership_status = str((membership_row or {}).get("status") or "").strip().lower()
                if membership_status == "disabled":
                    continue
                set_tenant_user_status(tenant_id=tenant_id, user_id=user_id, status="disabled")
                changed = True
                affected_tenant_ids.add(tenant_id)

    return changed, affected_tenant_ids, affected_app_ids


def _refresh_user_row(
    *,
    user_id: str,
    is_super_admin: bool,
    admin_scopes: set[tuple[str, str]] | None,
    current_tenant_id: str,
    current_app_id: str,
) -> dict[str, object]:
    tenant_scope = _scope_tenants_for_actor(
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_tenant_id=current_tenant_id,
    )
    app_scope = _scope_apps_for_actor(
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_app_id=current_app_id,
    )
    refreshed = list_users_with_access(
        scope_tenant_ids=tenant_scope,
        scope_app_ids=app_scope,
        primary_tenant_id=current_tenant_id,
        primary_app_id=current_app_id,
    )
    if not is_super_admin:
        refreshed = _scope_user_rows_by_assignment_pairs(rows=refreshed, allowed_scope_pairs=admin_scopes)
    return next((item for item in refreshed if str(item.get("userId") or "") == user_id), {"userId": user_id})


def _update_user_access_impl(request: Request, user_id: str, payload: dict[str, object]) -> dict[str, object]:
    result, is_super_admin, admin_scopes = _require_admin(request)
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        raise HTTPException(status_code=400, detail="user_id is required")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid payload")

    if is_user_super_admin(user_id=normalized_user_id):
        raise HTTPException(status_code=403, detail="Super Admin users cannot be edited from this admin flow")

    app_catalog = _resolve_app_catalog(
        is_super_admin=True,
        current_app_id=result.access.app_id,
        admin_scopes=admin_scopes,
    )
    app_id_by_code = {str(item.get("code") or "").strip().lower(): str(item.get("id") or "").strip() for item in app_catalog}

    replace_memberships = isinstance(payload.get("tenantMemberships"), list)
    replace_assignments = isinstance(payload.get("appAssignments"), list)
    desired_memberships = _normalize_membership_payload(payload, default_tenant_id=result.access.tenant_id)
    desired_assignments = _normalize_assignment_payload(
        payload,
        default_tenant_id=result.access.tenant_id,
        default_app_id=result.access.app_id,
        app_id_by_code=app_id_by_code,
    )

    full_name_updated = False
    memberships_or_roles_updated = False

    role_items, assignable_roles, _ = _role_catalog()
    _ = role_items
    has_access_update_intent = (
        replace_memberships
        or replace_assignments
        or bool(desired_memberships)
        or bool(desired_assignments)
    )
    if not has_access_update_intent and _parse_profile_full_name(payload) is None:
        raise HTTPException(status_code=400, detail="At least one of tenantMemberships, appAssignments, status, role, or fullName is required")

    try:
        if has_access_update_intent:
            memberships_or_roles_updated, _, _ = _apply_membership_and_role_changes(
                user_id=normalized_user_id,
                desired_memberships=desired_memberships,
                desired_assignments=desired_assignments,
                is_super_admin=is_super_admin,
                admin_scopes=admin_scopes,
                current_tenant_id=result.access.tenant_id,
                current_app_id=result.access.app_id,
                assignable_roles=assignable_roles,
                replace_memberships=replace_memberships,
                replace_assignments=replace_assignments,
            )
        full_name_updated = _upsert_user_profile(user_id=normalized_user_id, payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    permission_cache.invalidate(user_id=normalized_user_id)
    row = _refresh_user_row(
        user_id=normalized_user_id,
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_tenant_id=result.access.tenant_id,
        current_app_id=result.access.app_id,
    )
    row["permissionCacheInvalidated"] = True
    row["updatedMembershipOrRole"] = memberships_or_roles_updated
    row["updatedProfile"] = full_name_updated
    return row


@router.patch("/users/{user_id}")
def update_user_access_route(
    request: Request,
    user_id: str,
    payload: dict = Body(...),
):
    """
    Update app-role assignments and/or profile full name for a user.
    Tenant memberships are reconciled automatically from assignment rows when `appAssignments` is provided.

    Example request:
        PATCH /api/auth/v1/admin/users/762fec4b-1da3-4ba7-80e2-dd21622b6e0d

    Example request body:
        {
          "appAssignments": [
            {"tenantId": "tenant-1", "appCode": "tradsphere", "role": "admin"},
            {"tenantId": "tenant-2", "appCode": "tradsphere", "role": "viewer"}
          ],
          "fullName": "Workspace Admin"
        }

    Example response:
        {
          "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
          "permissionCacheInvalidated": true
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires admin manage permission (`tradsphere.admin` or `workspace.super_admin`)
        - Access should be assigned through `appAssignments` (tenantId + appId/appCode + role)
        - Sending `appAssignments: []` removes managed-scope assignments for this user
        - Omitting `tenantMemberships` is supported; backend auto-manages tenant membership status from assignment rows
        - Membership status must be one of: active, pending, disabled
        - Role keys must exist in assignable role catalog
    """
    return _update_user_access_impl(request=request, user_id=user_id, payload=payload if isinstance(payload, dict) else {})


@router.patch("/users/{user_id}/access")
def update_user_access_v2_route(
    request: Request,
    user_id: str,
    payload: dict = Body(...),
):
    """
    Update tenant/app access for a user (alias endpoint for explicit access-management route naming).

    Example request:
        PATCH /api/auth/v1/admin/users/762fec4b-1da3-4ba7-80e2-dd21622b6e0d/access

    Example response:
        {
          "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
          "permissionCacheInvalidated": true
        }

    Requirements:
        - Same as `PATCH /api/auth/v1/admin/users/{user_id}`
        - `appAssignments` can be used as the single source of access updates; tenant memberships are reconciled automatically
    """
    return _update_user_access_impl(request=request, user_id=user_id, payload=payload if isinstance(payload, dict) else {})


@router.get("/invitations")
def list_invitations_route(
    request: Request,
    status: str | None = Query(default=None),
):
    """
    List invitations for the actor scope (tenant/app for admin; all for super admin).

    Example request:
        GET /api/auth/v1/admin/invitations?status=pending

    Example response:
        {
          "items": [
            {
              "id": "invite-1",
              "email": "new.user@company.com",
              "tenantSlug": "taaa",
              "appCode": "tradsphere",
              "role": "viewer",
              "status": "pending",
              "expiresAt": "2026-05-13T08:00:00+00:00",
              "inviteUrl": "https://workspace.example.com/auth/invite/abc123"
            }
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires admin manage permission (`tradsphere.admin` or `workspace.super_admin`)
        - Optional status filter: pending, accepted, revoked, expired
    """
    result, is_super_admin, admin_scopes = _require_admin(request)
    return _list_invitations_payload(
        result=result,
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        status=_query_text(status, default=None),
    )


def _resolve_invitation_assignments(
    *,
    payload: dict[str, object],
    current_tenant_id: str,
    current_app_id: str,
    allowed_tenant_ids: set[str] | None,
    allowed_app_ids: set[str] | None,
    allowed_scope_pairs: set[tuple[str, str]] | None,
    app_id_by_code: dict[str, str],
    tenant_id_set: set[str],
    app_id_set: set[str],
    assignable_roles: set[str],
) -> list[tuple[str, str, str]]:
    role_default = normalize_role_key(str(payload.get("role") or ROLE_VIEWER).strip())
    assignments_raw = payload.get("assignments")
    assignments = assignments_raw if isinstance(assignments_raw, list) else None

    resolved: list[tuple[str, str, str]] = []
    if assignments is not None:
        for item in assignments:
            if not isinstance(item, dict):
                continue
            tenant_id = str(item.get("tenantId") or "").strip()
            app_id = str(item.get("appId") or "").strip()
            app_code = str(item.get("appCode") or "").strip().lower()
            role = normalize_role_key(str(item.get("role") or role_default).strip())
            if not tenant_id:
                continue
            resolved_app_id = app_id or app_id_by_code.get(app_code, "")
            if not resolved_app_id:
                continue
            resolved.append((tenant_id, resolved_app_id, role))
    else:
        tenant_ids_raw = payload.get("tenantIds")
        tenant_ids = [str(value or "").strip() for value in tenant_ids_raw] if isinstance(tenant_ids_raw, list) else []
        tenant_ids = [tenant_id for tenant_id in tenant_ids if tenant_id]
        if not tenant_ids:
            tenant_ids = [current_tenant_id]

        app_ids_raw = payload.get("appIds")
        app_ids = [str(value or "").strip() for value in app_ids_raw] if isinstance(app_ids_raw, list) else []
        app_ids = [app_id for app_id in app_ids if app_id]
        app_codes_raw = payload.get("appCodes")
        app_codes = [str(value or "").strip().lower() for value in app_codes_raw] if isinstance(app_codes_raw, list) else []
        app_codes = [app_code for app_code in app_codes if app_code]

        app_code_single = str(payload.get("appCode") or "").strip().lower()
        if app_code_single:
            app_codes.append(app_code_single)

        explicit_app_filters = bool(app_ids or app_codes)
        resolved_app_ids = unique_text_values(app_ids + [app_id_by_code.get(code, "") for code in app_codes])
        resolved_app_ids = [app_id for app_id in resolved_app_ids if app_id]
        if explicit_app_filters and not resolved_app_ids:
            raise HTTPException(status_code=400, detail="Unknown app selection")
        if not resolved_app_ids:
            resolved_app_ids = [current_app_id]

        for tenant_id in tenant_ids:
            for app_id in resolved_app_ids:
                resolved.append((tenant_id, app_id, role_default))

    if not resolved:
        raise HTTPException(status_code=400, detail="No valid invitation assignments found")

    tenant_ids = {tenant_id for tenant_id, _, _ in resolved}
    app_ids = {app_id for _, app_id, _ in resolved}
    scope_pairs = {(tenant_id, app_id) for tenant_id, app_id, _ in resolved}
    _validate_tenant_scope(tenant_ids=tenant_ids, allowed_tenant_ids=allowed_tenant_ids)
    _validate_app_scope(app_ids=app_ids, allowed_app_ids=allowed_app_ids)
    _validate_scope_pairs(assignment_pairs=scope_pairs, allowed_scope_pairs=allowed_scope_pairs)

    for tenant_id in tenant_ids:
        if tenant_id not in tenant_id_set:
            raise HTTPException(status_code=400, detail=f"Unknown tenantId: {tenant_id}")
    for app_id in app_ids:
        if app_id not in app_id_set:
            raise HTTPException(status_code=400, detail=f"Unknown appId: {app_id}")
    for _, _, role in resolved:
        if role == ROLE_SUPER_ADMIN:
            raise HTTPException(status_code=400, detail="super_admin cannot be assigned from this endpoint")
        if role not in assignable_roles:
            raise HTTPException(status_code=400, detail=f"Invalid or unsupported role: {role}")

    deduped = sorted(set(resolved), key=lambda entry: (entry[0], entry[1], entry[2]))
    return deduped


@router.post("/invitations")
def create_invitation_admin_route(
    request: Request,
    payload: dict = Body(...),
):
    """
    Create one or more invitations for tenant/app assignments and return invite URLs.

    Example request:
        POST /api/auth/v1/admin/invitations

    Example request body:
        {
          "email": "new.user@company.com",
          "tenantIds": ["tenant-1", "tenant-2"],
          "appCodes": ["tradsphere"],
          "role": "viewer",
          "expirationHours": 72
        }

    Example response:
        {
          "count": 2,
          "items": [
            {
              "id": "invite-1",
              "tenantId": "tenant-1",
              "appCode": "tradsphere",
              "role": "viewer",
              "status": "pending",
              "inviteUrl": "https://workspace.example.com/auth/invite/abc123"
            }
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires admin manage permission (`tradsphere.admin` or `workspace.super_admin`)
        - Invitation token stores multiple assignment rows via `invitation_assignments` when available
    """
    result, is_super_admin, admin_scopes = _require_admin(request)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid payload")

    email = str(payload.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="email is required")

    role_items, assignable_roles, super_admin_supported = _role_catalog()
    _ = role_items
    if not super_admin_supported and normalize_role_key(str(payload.get("role") or "").strip()) == ROLE_SUPER_ADMIN:
        raise HTTPException(
            status_code=400,
            detail="super_admin cannot be assigned from this endpoint",
        )

    tenant_catalog = list_active_tenants()
    app_catalog = list_active_apps()
    tenant_id_set = {str(item.get("id") or "") for item in tenant_catalog}
    app_id_set = {str(item.get("id") or "") for item in app_catalog}
    app_id_by_code = {str(item.get("code") or "").strip().lower(): str(item.get("id") or "") for item in app_catalog}
    app_by_id = {str(item.get("id") or ""): item for item in app_catalog}
    tenant_by_id = {str(item.get("id") or ""): item for item in tenant_catalog}

    allowed_tenant_ids = _scope_tenants_for_actor(
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_tenant_id=result.access.tenant_id,
    )
    allowed_app_ids = _scope_apps_for_actor(
        is_super_admin=is_super_admin,
        admin_scopes=admin_scopes,
        current_app_id=result.access.app_id,
    )

    assignments = _resolve_invitation_assignments(
        payload=payload,
        current_tenant_id=result.access.tenant_id,
        current_app_id=result.access.app_id,
        allowed_tenant_ids=allowed_tenant_ids,
        allowed_app_ids=allowed_app_ids,
        allowed_scope_pairs=admin_scopes if not is_super_admin else None,
        app_id_by_code=app_id_by_code,
        tenant_id_set=tenant_id_set,
        app_id_set=app_id_set,
        assignable_roles=assignable_roles,
    )

    ttl_hours = get_invite_ttl_hours()
    expires_hours = payload.get("expirationHours")
    if expires_hours is not None:
        try:
            parsed = int(str(expires_hours).strip())
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="expirationHours must be an integer") from exc
        ttl_hours = max(parsed, 1)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=ttl_hours)

    try:
        primary_tenant_id, primary_app_id, primary_role = assignments[0]
        created = create_invitation(
            email=email,
            tenant_id=primary_tenant_id,
            app_id=primary_app_id,
            role=primary_role,
            invited_by_user_id=result.principal.user_id,
            expires_at=expires_at,
        )
        created_id = str(created.get("id") or "").strip()
        if created_id:
            insert_invitation_assignments(invitation_id=created_id, assignments=assignments)
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    created_items: list[dict[str, object]] = []
    for tenant_id, app_id, role in assignments:
        app = app_by_id.get(app_id, {})
        tenant = tenant_by_id.get(tenant_id, {})
        created_items.append(
            {
                "tenantId": tenant_id,
                "tenantSlug": tenant.get("slug"),
                "tenantName": tenant.get("name"),
                "appId": app_id,
                "appCode": app.get("code"),
                "appName": app.get("name"),
                "role": role,
            }
        )
    created_items.sort(
        key=lambda item: (
            str(item.get("tenantSlug") or "~").lower(),
            str(item.get("appCode") or "~").lower(),
            str(item.get("role") or "~").lower(),
        )
    )
    first = created_items[0]
    token = str(created.get("token") or "").strip()
    return {
        "id": created.get("id"),
        "email": email,
        "role": first.get("role"),
        "status": created.get("status") or "pending",
        "expiresAt": created.get("expires_at"),
        "inviteUrl": _invite_url(token) if token else None,
        "count": len(created_items),
        "items": created_items,
        "assignments": created_items,
        "schemaNote": "Invitation token can include multiple tenant/app assignments via invitation_assignments.",
    }


@router.post("/invitations/{invitation_id}/revoke")
def revoke_invitation_admin_route(request: Request, invitation_id: str):
    """
    Revoke a pending invitation from actor scope.

    Example request:
        POST /api/auth/v1/admin/invitations/invite-1/revoke

    Example response:
        {
          "status": "revoked",
          "id": "invite-1"
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires admin manage permission (`tradsphere.admin` or `workspace.super_admin`)
    """
    result, is_super_admin, admin_scopes = _require_admin(request)

    row = get_invitation_by_id(invitation_id=invitation_id)
    if not row:
        raise HTTPException(status_code=404, detail="Invitation not found")

    assignments = list_invitation_assignments(invitation_id=str(row.get("id") or "").strip())
    if not assignments:
        assignments = [
            {
                "tenant_id": str(row.get("tenant_id") or "").strip(),
                "app_id": str(row.get("app_id") or "").strip(),
            }
        ]
    tenant_ids = {str(item.get("tenant_id") or "").strip() for item in assignments if str(item.get("tenant_id") or "").strip()}
    app_ids = {str(item.get("app_id") or "").strip() for item in assignments if str(item.get("app_id") or "").strip()}
    scope_pairs = {
        (str(item.get("tenant_id") or "").strip(), str(item.get("app_id") or "").strip())
        for item in assignments
        if str(item.get("tenant_id") or "").strip() and str(item.get("app_id") or "").strip()
    }
    _validate_tenant_scope(
        tenant_ids=tenant_ids,
        allowed_tenant_ids=_scope_tenants_for_actor(
            is_super_admin=is_super_admin,
            admin_scopes=admin_scopes,
            current_tenant_id=result.access.tenant_id,
        ),
    )
    _validate_app_scope(
        app_ids=app_ids,
        allowed_app_ids=_scope_apps_for_actor(
            is_super_admin=is_super_admin,
            admin_scopes=admin_scopes,
            current_app_id=result.access.app_id,
        ),
    )
    _validate_scope_pairs(assignment_pairs=scope_pairs, allowed_scope_pairs=admin_scopes if not is_super_admin else None)

    accepted_by_user_id = str(row.get("accepted_by_user_id") or "").strip()
    try:
        patch_invitation_status(invitation_id=invitation_id, status="revoked")
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    if accepted_by_user_id:
        permission_cache.invalidate(user_id=accepted_by_user_id)

    return {"status": "revoked", "id": invitation_id}
