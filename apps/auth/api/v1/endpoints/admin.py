from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Body, HTTPException, Query, Request

from shared.auth.admin_repo import (
    get_user_app_roles,
    get_user_memberships,
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
    role_group_key,
    role_label,
)
from shared.auth.supabase_client import SupabaseClientError

router = APIRouter(prefix="/admin")

VALID_MEMBERSHIP_STATUSES = {"active", "pending", "disabled"}
VALID_INVITATION_STATUSES = {"pending", "accepted", "revoked", "expired"}
FALLBACK_ROLE_KEYS = [ROLE_VIEWER, ROLE_EDITOR, ROLE_ADMIN]


def _is_super_admin(*, role: str, permissions: set[str]) -> bool:
    normalized_role = str(role or "").strip()
    return normalized_role == ROLE_SUPER_ADMIN or ROLE_SUPER_ADMIN in permissions


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
    return result, is_super_admin


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
    for key in [ROLE_SUPER_ADMIN, ROLE_ADMIN, ROLE_EDITOR, ROLE_VIEWER]:
        if key in role_keys:
            ordered.append(key)
            role_keys.remove(key)
    ordered.extend(sorted(role_keys))
    return ordered


def _role_catalog() -> tuple[list[dict[str, object]], set[str], bool]:
    role_keys = _normalized_role_keys()
    role_set = set(role_keys)
    super_admin_supported = ROLE_SUPER_ADMIN in role_set
    if not super_admin_supported:
        role_keys = [ROLE_SUPER_ADMIN, *role_keys]

    items: list[dict[str, object]] = []
    for role_key in role_keys:
        is_super_admin = role_key == ROLE_SUPER_ADMIN
        assignable = role_key in role_set
        items.append(
            {
                "key": role_key,
                "label": role_label(role_key),
                "group": role_group_key(role_key),
                "assignable": assignable,
                "requiresSchemaChange": is_super_admin and not assignable,
            }
        )
    assignable_set = {str(item["key"]) for item in items if bool(item.get("assignable"))}
    return items, assignable_set, super_admin_supported


def _scope_tenants_for_actor(*, is_super_admin: bool, current_tenant_id: str) -> set[str] | None:
    if is_super_admin:
        return None
    return {current_tenant_id}


def _scope_apps_for_actor(*, is_super_admin: bool, current_app_id: str) -> set[str] | None:
    if is_super_admin:
        return None
    return {current_app_id}


def _resolve_tenant_catalog(*, is_super_admin: bool, current_tenant_id: str) -> list[dict[str, object]]:
    tenants = list_active_tenants()
    if is_super_admin:
        return tenants
    return [tenant for tenant in tenants if str(tenant.get("id") or "") == current_tenant_id]


def _resolve_app_catalog(*, is_super_admin: bool, current_app_id: str) -> list[dict[str, object]]:
    apps = list_active_apps()
    if is_super_admin:
        return apps
    return [app for app in apps if str(app.get("id") or "") == current_app_id]


def _parse_bool(value: object, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    return normalized in {"1", "true", "yes", "on"}


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
    result, is_super_admin = _require_admin(request)
    return {"items": _resolve_tenant_catalog(is_super_admin=is_super_admin, current_tenant_id=result.access.tenant_id)}


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
    result, is_super_admin = _require_admin(request)
    return {"items": _resolve_app_catalog(is_super_admin=is_super_admin, current_app_id=result.access.app_id)}


@router.get("/roles")
def list_roles_route(request: Request):
    """
    Return assignable role catalog and display labels for admin user-management workflows.

    Example request:
        GET /api/auth/v1/admin/roles

    Example response:
        {
          "roles": ["tradsphere.viewer", "tradsphere.editor", "tradsphere.admin"],
          "items": [
            {"key": "workspace.super_admin", "label": "Super Admin", "assignable": false, "requiresSchemaChange": true},
            {"key": "tradsphere.admin", "label": "Admin", "assignable": true, "requiresSchemaChange": false}
          ],
          "superAdminSupported": false
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires admin manage permission (`tradsphere.admin` or `workspace.super_admin`)
    """
    _require_admin(request)
    items, assignable_set, super_admin_supported = _role_catalog()
    return {
        "roles": [item["key"] for item in items if item.get("assignable")],
        "items": items,
        "superAdminSupported": super_admin_supported,
    }


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
              "role": "tradsphere.admin",
              "tenantMemberships": [{"tenantId": "tenant-1", "tenantSlug": "taaa", "status": "active"}],
              "appAssignments": [{"tenantId": "tenant-1", "appCode": "tradsphere", "role": "tradsphere.admin"}]
            }
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires admin manage permission (`tradsphere.admin` or `workspace.super_admin`)
        - `includeAllTenants=true` is effective only for `workspace.super_admin`
    """
    result, is_super_admin = _require_admin(request)
    include_all = _parse_bool(include_all_tenants, default=False)
    include_global = bool(is_super_admin and (include_all_tenants is None or include_all))
    tenant_scope = None if include_global or is_super_admin else {result.access.tenant_id}
    app_scope = None if include_global or is_super_admin else {result.access.app_id}

    items = list_users_with_access(
        scope_tenant_ids=tenant_scope,
        scope_app_ids=app_scope,
        primary_tenant_id=result.access.tenant_id,
        primary_app_id=result.access.app_id,
    )
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
            role = str(item.get("role") or "").strip()
            if not tenant_id or not role:
                continue
            resolved_app_id = app_id or app_id_by_code.get(app_code, "")
            if not resolved_app_id:
                continue
            assignments[(tenant_id, resolved_app_id)] = role
        return assignments

    next_role_raw = payload.get("role")
    role = str(next_role_raw or "").strip() if next_role_raw is not None else ""
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
    current_tenant_id: str,
    current_app_id: str,
    assignable_roles: set[str],
    replace_memberships: bool,
    replace_assignments: bool,
) -> tuple[bool, set[str], set[str]]:
    tenant_ids_in_payload = set(desired_memberships.keys()) | {tenant_id for tenant_id, _ in desired_assignments.keys()}
    app_ids_in_payload = {app_id for _, app_id in desired_assignments.keys()}

    tenant_scope = _scope_tenants_for_actor(is_super_admin=is_super_admin, current_tenant_id=current_tenant_id)
    app_scope = _scope_apps_for_actor(is_super_admin=is_super_admin, current_app_id=current_app_id)
    _validate_tenant_scope(tenant_ids=tenant_ids_in_payload, allowed_tenant_ids=tenant_scope)
    _validate_app_scope(app_ids=app_ids_in_payload, allowed_app_ids=app_scope)

    for status in desired_memberships.values():
        if status not in VALID_MEMBERSHIP_STATUSES:
            raise HTTPException(status_code=400, detail="Invalid status")

    for role in desired_assignments.values():
        if role not in assignable_roles:
            raise HTTPException(status_code=400, detail=f"Invalid or unsupported role: {role}")
        if role == ROLE_SUPER_ADMIN and not is_super_admin:
            raise HTTPException(status_code=403, detail="Only Super Admin can assign Super Admin role")

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
        for tenant_id in existing_membership_ids:
            if tenant_id in desired_memberships:
                continue
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
        current_role = str((row or {}).get("role") or "").strip() or None
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

    return changed, affected_tenant_ids, affected_app_ids


def _refresh_user_row(
    *,
    user_id: str,
    is_super_admin: bool,
    current_tenant_id: str,
    current_app_id: str,
) -> dict[str, object]:
    tenant_scope = _scope_tenants_for_actor(is_super_admin=is_super_admin, current_tenant_id=current_tenant_id)
    app_scope = _scope_apps_for_actor(is_super_admin=is_super_admin, current_app_id=current_app_id)
    refreshed = list_users_with_access(
        scope_tenant_ids=tenant_scope,
        scope_app_ids=app_scope,
        primary_tenant_id=current_tenant_id,
        primary_app_id=current_app_id,
    )
    return next((item for item in refreshed if str(item.get("userId") or "") == user_id), {"userId": user_id})


def _update_user_access_impl(request: Request, user_id: str, payload: dict[str, object]) -> dict[str, object]:
    result, is_super_admin = _require_admin(request)
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        raise HTTPException(status_code=400, detail="user_id is required")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid payload")

    app_catalog = _resolve_app_catalog(is_super_admin=True, current_app_id=result.access.app_id)
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
    if not desired_memberships and not desired_assignments and _parse_profile_full_name(payload) is None:
        raise HTTPException(status_code=400, detail="At least one of tenantMemberships, appAssignments, status, role, or fullName is required")

    try:
        if desired_memberships or desired_assignments:
            memberships_or_roles_updated, _, _ = _apply_membership_and_role_changes(
                user_id=normalized_user_id,
                desired_memberships=desired_memberships,
                desired_assignments=desired_assignments,
                is_super_admin=is_super_admin,
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
    Update tenant memberships, app-role assignments, and/or profile full name for a user.

    Example request:
        PATCH /api/auth/v1/admin/users/762fec4b-1da3-4ba7-80e2-dd21622b6e0d

    Example request body:
        {
          "tenantMemberships": [
            {"tenantId": "tenant-1", "status": "active"},
            {"tenantId": "tenant-2", "status": "active"}
          ],
          "appAssignments": [
            {"tenantId": "tenant-1", "appCode": "tradsphere", "role": "tradsphere.admin"},
            {"tenantId": "tenant-2", "appCode": "tradsphere", "role": "tradsphere.viewer"}
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
              "role": "tradsphere.viewer",
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
    result, is_super_admin = _require_admin(request)
    normalized_status = str(status or "").strip().lower() or None
    if normalized_status and normalized_status not in VALID_INVITATION_STATUSES:
        raise HTTPException(status_code=400, detail="Invalid status filter")

    tenant_scope = _scope_tenants_for_actor(is_super_admin=is_super_admin, current_tenant_id=result.access.tenant_id)
    app_scope = _scope_apps_for_actor(is_super_admin=is_super_admin, current_app_id=result.access.app_id)
    rows = list_invitations_scoped(tenant_ids=tenant_scope, app_ids=app_scope, status=normalized_status)

    tenant_by_id = {str(item.get("id") or ""): item for item in list_active_tenants()}
    app_by_id = {str(item.get("id") or ""): item for item in list_active_apps()}

    items: list[dict[str, object]] = []
    for row in rows:
        tenant_id = str(row.get("tenant_id") or "").strip()
        app_id = str(row.get("app_id") or "").strip()
        tenant = tenant_by_id.get(tenant_id, {})
        app = app_by_id.get(app_id, {})
        token = str(row.get("token") or "").strip()
        items.append(
            {
                "id": row.get("id"),
                "email": row.get("email"),
                "tenantId": tenant_id,
                "tenantSlug": tenant.get("slug"),
                "tenantName": tenant.get("name"),
                "appId": app_id,
                "appCode": app.get("code"),
                "appName": app.get("name"),
                "role": row.get("role"),
                "status": row.get("status"),
                "expiresAt": row.get("expires_at"),
                "createdAt": row.get("created_at"),
                "acceptedAt": row.get("accepted_at"),
                "inviteUrl": _invite_url(token) if token else None,
            }
        )
    items.sort(key=lambda item: (str(item.get("createdAt") or ""), str(item.get("email") or "").lower()), reverse=True)
    return {"items": items}


def _resolve_invitation_assignments(
    *,
    payload: dict[str, object],
    current_tenant_id: str,
    current_app_id: str,
    allowed_tenant_ids: set[str] | None,
    allowed_app_ids: set[str] | None,
    app_id_by_code: dict[str, str],
    tenant_id_set: set[str],
    app_id_set: set[str],
    assignable_roles: set[str],
) -> list[tuple[str, str, str]]:
    role_default = str(payload.get("role") or ROLE_VIEWER).strip()
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
            role = str(item.get("role") or role_default).strip()
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
    _validate_tenant_scope(tenant_ids=tenant_ids, allowed_tenant_ids=allowed_tenant_ids)
    _validate_app_scope(app_ids=app_ids, allowed_app_ids=allowed_app_ids)

    for tenant_id in tenant_ids:
        if tenant_id not in tenant_id_set:
            raise HTTPException(status_code=400, detail=f"Unknown tenantId: {tenant_id}")
    for app_id in app_ids:
        if app_id not in app_id_set:
            raise HTTPException(status_code=400, detail=f"Unknown appId: {app_id}")
    for _, _, role in resolved:
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
          "role": "tradsphere.viewer",
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
              "role": "tradsphere.viewer",
              "status": "pending",
              "inviteUrl": "https://workspace.example.com/auth/invite/abc123"
            }
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires admin manage permission (`tradsphere.admin` or `workspace.super_admin`)
        - Invitation schema currently stores one tenant/app assignment per token row
    """
    result, is_super_admin = _require_admin(request)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid payload")

    email = str(payload.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="email is required")

    role_items, assignable_roles, super_admin_supported = _role_catalog()
    _ = role_items
    if not super_admin_supported and str(payload.get("role") or "").strip() == ROLE_SUPER_ADMIN:
        raise HTTPException(
            status_code=400,
            detail="workspace.super_admin is not assignable with current schema; add global-role schema support first",
        )

    tenant_catalog = list_active_tenants()
    app_catalog = list_active_apps()
    tenant_id_set = {str(item.get("id") or "") for item in tenant_catalog}
    app_id_set = {str(item.get("id") or "") for item in app_catalog}
    app_id_by_code = {str(item.get("code") or "").strip().lower(): str(item.get("id") or "") for item in app_catalog}
    app_by_id = {str(item.get("id") or ""): item for item in app_catalog}
    tenant_by_id = {str(item.get("id") or ""): item for item in tenant_catalog}

    allowed_tenant_ids = _scope_tenants_for_actor(is_super_admin=is_super_admin, current_tenant_id=result.access.tenant_id)
    allowed_app_ids = _scope_apps_for_actor(is_super_admin=is_super_admin, current_app_id=result.access.app_id)

    assignments = _resolve_invitation_assignments(
        payload=payload,
        current_tenant_id=result.access.tenant_id,
        current_app_id=result.access.app_id,
        allowed_tenant_ids=allowed_tenant_ids,
        allowed_app_ids=allowed_app_ids,
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
        created_items: list[dict[str, object]] = []
        for tenant_id, app_id, role in assignments:
            created = create_invitation(
                email=email,
                tenant_id=tenant_id,
                app_id=app_id,
                role=role,
                invited_by_user_id=result.principal.user_id,
                expires_at=expires_at,
            )
            app = app_by_id.get(app_id, {})
            tenant = tenant_by_id.get(tenant_id, {})
            token = str(created.get("token") or "")
            created_items.append(
                {
                    "id": created.get("id"),
                    "email": email,
                    "tenantId": tenant_id,
                    "tenantSlug": tenant.get("slug"),
                    "tenantName": tenant.get("name"),
                    "appId": app_id,
                    "appCode": app.get("code"),
                    "appName": app.get("name"),
                    "role": role,
                    "status": created.get("status") or "pending",
                    "expiresAt": created.get("expires_at"),
                    "inviteUrl": _invite_url(token),
                }
            )
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    created_items.sort(
        key=lambda item: (
            str(item.get("tenantSlug") or "~").lower(),
            str(item.get("appCode") or "~").lower(),
            str(item.get("role") or "~").lower(),
        )
    )
    first = created_items[0]
    return {
        "id": first.get("id"),
        "email": first.get("email"),
        "role": first.get("role"),
        "status": first.get("status"),
        "expiresAt": first.get("expiresAt"),
        "inviteUrl": first.get("inviteUrl"),
        "count": len(created_items),
        "items": created_items,
        "schemaNote": (
            "Current schema stores one tenant/app assignment per invitation token. "
            "Multi-selection creates one invite row per assignment."
        ),
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
    result, is_super_admin = _require_admin(request)

    row = get_invitation_by_id(invitation_id=invitation_id)
    if not row:
        raise HTTPException(status_code=404, detail="Invitation not found")

    tenant_id = str(row.get("tenant_id") or "").strip()
    app_id = str(row.get("app_id") or "").strip()
    _validate_tenant_scope(
        tenant_ids={tenant_id},
        allowed_tenant_ids=_scope_tenants_for_actor(is_super_admin=is_super_admin, current_tenant_id=result.access.tenant_id),
    )
    _validate_app_scope(
        app_ids={app_id},
        allowed_app_ids=_scope_apps_for_actor(is_super_admin=is_super_admin, current_app_id=result.access.app_id),
    )

    accepted_by_user_id = str(row.get("accepted_by_user_id") or "").strip()
    try:
        patch_invitation_status(invitation_id=invitation_id, status="revoked")
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    if accepted_by_user_id:
        permission_cache.invalidate(user_id=accepted_by_user_id)

    return {"status": "revoked", "id": invitation_id}
