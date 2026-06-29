from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Body, HTTPException, Query, Request

from shared.auth.admin_repo import (
    find_user_id_by_email,
    is_auth_user_active,
    is_user_super_admin,
    list_tenant_users_with_app_role,
    remove_tenant_app_role,
)
from shared.auth.config import get_invite_base_url
from shared.auth.dependencies import authenticate_bearer, authorize_bearer_for_tenant_app
from shared.auth.invite_url import build_invite_url
from shared.auth.invitations_repo import (
    activate_tenant_user,
    create_invitation,
    get_active_app,
    get_invitation_by_id,
    get_invitation_by_token,
    list_invitation_assignments,
    mark_invitation_accepted,
    patch_invitation_status,
    upsert_tenant_app_role,
    upsert_profile_for_invited_user,
)
from shared.auth.page_permissions_catalog import list_page_catalog_for_app, normalize_page_key, normalize_page_keys
from shared.auth.page_permissions_repo import list_page_permissions_for_scope, replace_page_permissions_for_user
from shared.auth.permissions_cache import permission_cache
from shared.auth.profile_repo import compose_full_name, select_profile_for_user, split_full_name
from shared.auth.providers import get_auth_provider
from shared.auth.roles import ROLE_ADMIN, ROLE_EDITOR, ROLE_SUPER_ADMIN, ROLE_VIEWER, normalize_role_key
from shared.auth.supabase_client import SupabaseClientError

router = APIRouter(prefix="/invitations")
VALID_SCOPED_ROLE_KEYS = {ROLE_VIEWER, ROLE_EDITOR, ROLE_ADMIN}
ROLE_RANKS = {
    ROLE_SUPER_ADMIN: 0,
    ROLE_ADMIN: 1,
    ROLE_EDITOR: 2,
    ROLE_VIEWER: 3,
}


def _normalize_app_code(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    if not normalized.replace("-", "").replace("_", "").isalnum():
        return ""
    return normalized


def _resolve_target_app_code(
    *,
    request: Request,
    override_app_code: str | None = None,
    fallback_app_code: str = "tradsphere",
) -> str:
    override = _normalize_app_code(override_app_code)
    if override:
        return override
    header_code = _normalize_app_code(request.headers.get("x-app-code"))
    if header_code:
        return header_code
    fallback = _normalize_app_code(fallback_app_code)
    return fallback or "tradsphere"


def _require_admin(
    request: Request,
    *,
    app_code: str | None = None,
) -> tuple[str, str, str, str, str, bool, str]:
    target_app_code = _resolve_target_app_code(request=request, override_app_code=app_code)
    result = authorize_bearer_for_tenant_app(request=request, app_code=target_app_code)
    permissions = set(result.access.permissions)
    is_super_admin = "workspace.super_admin" in permissions or str(result.access.role or "").strip().lower() == "super_admin"
    if f"{target_app_code}.admin" not in permissions and not is_super_admin:
        raise HTTPException(status_code=403, detail="Forbidden")
    return (
        result.principal.user_id,
        result.access.tenant_id,
        result.access.tenant_slug,
        result.access.app_id,
        result.access.app_code,
        is_super_admin,
        normalize_role_key(str(result.access.role or "").strip()) or "",
    )


def _invite_url(token: str) -> str:
    return build_invite_url(token=token, base_url=get_invite_base_url())


def _parse_invitation_expiry(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invitation expiration is invalid") from exc


def _is_missing_profile_column_error(exc: Exception, *, column: str) -> bool:
    message = str(exc).lower()
    return (
        f"column profiles.{column} does not exist" in message
        or f'column "{column}" of relation "profiles" does not exist' in message
    )


def _resolve_tenant_slug(*, tenant_id: str) -> str | None:
    normalized_tenant_id = str(tenant_id or "").strip()
    if not normalized_tenant_id:
        return None
    row = get_auth_provider().select_single(
        table="tenants",
        filters={"id": normalized_tenant_id, "active": "true"},
        select="id,slug",
    )
    if not row:
        return None
    return str(row.get("slug") or "").strip().lower() or None


def _resolve_app_code(*, app_id: str) -> str | None:
    normalized_app_id = str(app_id or "").strip()
    if not normalized_app_id:
        return None
    row = get_auth_provider().select_single(
        table="apps",
        filters={"id": normalized_app_id, "active": "true"},
        select="id,code",
    )
    if not row:
        return None
    return str(row.get("code") or "").strip().lower() or None


def _serialize_invitation_response(*, row: dict[str, object], assignments: list[dict[str, object]]) -> dict[str, object]:
    tenant_id = str(row.get("tenant_id") or "").strip()
    app_id = str(row.get("app_id") or "").strip()
    role = normalize_role_key(str(row.get("role") or "").strip())
    tenant_slug = _resolve_tenant_slug(tenant_id=tenant_id)
    app_code = _resolve_app_code(app_id=app_id)

    resolved_assignments = [
        {
            "tenantId": str(item.get("tenant_id") or "").strip(),
            "tenantSlug": _resolve_tenant_slug(tenant_id=str(item.get("tenant_id") or "").strip()),
            "appId": str(item.get("app_id") or "").strip(),
            "appCode": _resolve_app_code(app_id=str(item.get("app_id") or "").strip()),
            "role": normalize_role_key(str(item.get("role") or "").strip()),
        }
        for item in assignments
        if str(item.get("tenant_id") or "").strip()
        and str(item.get("app_id") or "").strip()
        and normalize_role_key(str(item.get("role") or "").strip())
    ]
    if not resolved_assignments:
        resolved_assignments = [
            {
                "tenantId": tenant_id,
                "tenantSlug": tenant_slug,
                "appId": app_id,
                "appCode": app_code,
                "role": role,
            }
        ]

    return {
        "id": row.get("id"),
        "email": row.get("email"),
        "tenantId": row.get("tenant_id"),
        "tenantSlug": tenant_slug,
        "appId": row.get("app_id"),
        "appCode": app_code,
        "role": role,
        "status": row.get("status"),
        "expiresAt": row.get("expires_at"),
        "acceptedAt": row.get("accepted_at"),
        "createdAt": row.get("created_at"),
        "assignments": resolved_assignments,
    }


def _serialize_scoped_user_row(
    *,
    row: dict[str, object],
    tenant_id: str,
    app_id: str,
) -> dict[str, object] | None:
    user_id = str(row.get("userId") or "").strip()
    if not user_id:
        return None
    email = str(row.get("email") or "").strip().lower() or None
    full_name = str(row.get("fullName") or "").strip() or None
    first_name, last_name = split_full_name(full_name)
    status = str(row.get("status") or "").strip().lower() or "active"

    app_assignments = row.get("appAssignments")
    assignments = app_assignments if isinstance(app_assignments, list) else []
    current_assignment = next(
        (
            item
            for item in assignments
            if isinstance(item, dict)
            and str(item.get("tenantId") or "").strip() == tenant_id
            and str(item.get("appId") or "").strip() == app_id
        ),
        None,
    )
    if not isinstance(current_assignment, dict):
        return None

    role = normalize_role_key(str(current_assignment.get("role") or "").strip()) or None
    is_super_admin = bool(row.get("isSuperAdmin"))
    return {
        "userId": user_id,
        "email": email,
        "fullName": full_name,
        "firstName": first_name,
        "lastName": last_name,
        "status": status,
        "role": role,
        "isSuperAdmin": is_super_admin,
        "assignedInScope": True,
    }


def _scoped_users_by_user_id(
    *,
    tenant_id: str,
    app_id: str,
) -> dict[str, dict[str, object]]:
    scoped_rows = list_tenant_users_with_app_role(tenant_id=tenant_id, app_id=app_id)
    users_by_id: dict[str, dict[str, object]] = {}
    for row in scoped_rows:
        if not isinstance(row, dict):
            continue
        serialized = _serialize_scoped_user_row(row=row, tenant_id=tenant_id, app_id=app_id)
        if not serialized:
            continue
        user_id = str(serialized.get("userId") or "").strip()
        if user_id:
            users_by_id[user_id] = serialized
    return users_by_id


def _role_rank(role_key: str | None) -> int:
    normalized = normalize_role_key(role_key)
    return ROLE_RANKS.get(normalized, 99)


def _assert_actor_can_manage_target(
    *,
    actor_user_id: str,
    actor_role: str | None,
    actor_is_super_admin: bool,
    target_user: dict[str, object],
    action_label: str,
) -> None:
    target_user_id = str(target_user.get("userId") or "").strip()
    target_role = normalize_role_key(str(target_user.get("role") or "").strip()) or None
    target_is_super_admin = bool(target_user.get("isSuperAdmin"))

    if target_user_id and target_user_id == actor_user_id:
        raise HTTPException(status_code=403, detail=f"You cannot {action_label} your own access.")

    if target_is_super_admin and not actor_is_super_admin:
        raise HTTPException(status_code=403, detail=f"You cannot {action_label} a Super Admin.")

    if not actor_is_super_admin and _role_rank(target_role) < _role_rank(actor_role):
        raise HTTPException(status_code=403, detail=f"You cannot {action_label} a higher-role user.")


@router.post("")
def create_invitation_route(
    request: Request,
    payload: dict = Body(...),
):
    """
    Create a pending app-scoped invitation and return a reusable invite URL.

    Example request:
        POST /api/auth/v1/invitations

    Example response:
        {
          "id": "0ec0a2f8-0a59-42e4-b296-8f8b4a8e6d6d",
          "email": "new.user@company.com",
          "role": "viewer",
          "status": "pending",
          "expiresAt": "2026-05-13T08:00:00+00:00",
          "inviteUrl": "https://workspace.example.com/auth/invite/abc123"
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires current app admin permission for app-scope invites
        - Non-super admins can invite only existing active users
        - Brand-new user invites are restricted to workspace super admins
        - Tenant/app admins can assign only: viewer, editor, admin
        - super_admin cannot be assigned from this endpoint
    """
    payload_app_code = _normalize_app_code(payload.get("appCode"))
    invited_by_user_id, tenant_id, _, _, app_code, is_super_admin, _ = _require_admin(
        request,
        app_code=payload_app_code or None,
    )

    email = str(payload.get("email") or "").strip().lower()
    role = normalize_role_key(str(payload.get("role") or "").strip())

    if not email:
        raise HTTPException(status_code=400, detail="email is required")
    if role not in VALID_SCOPED_ROLE_KEYS:
        raise HTTPException(status_code=400, detail="Invalid role")
    if not is_super_admin:
        existing_user_id = find_user_id_by_email(email=email)
        if not existing_user_id:
            raise HTTPException(
                status_code=403,
                detail="Only Super Admin can invite brand-new users. Tenant/App admin can invite active existing users only.",
            )
        if not is_auth_user_active(user_id=existing_user_id):
            raise HTTPException(
                status_code=403,
                detail="Tenant/App admin can invite active existing users only.",
            )

    app_row = get_active_app(app_code=app_code)
    if not app_row:
        raise HTTPException(status_code=400, detail="Requested app is not enabled")

    try:
        created = create_invitation(
            email=email,
            tenant_id=tenant_id,
            app_id=str(app_row.get("id") or ""),
            role=role,
            invited_by_user_id=invited_by_user_id,
        )
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    token = str(created.get("token") or "")
    return {
        "id": created.get("id"),
        "email": email,
        "role": role,
        "status": created.get("status") or "pending",
        "expiresAt": created.get("expires_at"),
        "inviteUrl": _invite_url(token),
    }


@router.post("/users/access")
def grant_scoped_user_access_route(
    request: Request,
    payload: dict = Body(...),
):
    """
    Grant tenant+app access immediately for an existing active user.

    Example request:
        POST /api/auth/v1/invitations/users/access

    Example request body:
        {
          "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
          "role": "editor"
        }

    Example response:
        {
          "status": "assigned",
          "user": {
            "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
            "email": "alex@example.com",
            "fullName": "Alex Johnson",
            "firstName": "Alex",
            "lastName": "Johnson",
            "status": "active",
            "role": "editor",
            "isSuperAdmin": false,
            "assignedInScope": true
          }
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires current app admin permission or workspace.super_admin
        - Target user must already exist and be active in auth/profile
        - Role must be one of: viewer, editor, admin
        - super_admin cannot be assigned from this endpoint
        - Actor cannot grant/modify their own scoped access
    """
    payload_app_code = _normalize_app_code(payload.get("appCode"))
    actor_user_id, tenant_id, _, app_id, _, actor_is_super_admin, actor_role = _require_admin(
        request,
        app_code=payload_app_code or None,
    )

    normalized_user_id = str(payload.get("userId") or "").strip()
    normalized_email = str(payload.get("email") or "").strip().lower()
    role = normalize_role_key(str(payload.get("role") or "").strip())

    if role not in VALID_SCOPED_ROLE_KEYS:
        raise HTTPException(status_code=400, detail="Invalid role")
    if not actor_is_super_admin and _role_rank(role) < _role_rank(actor_role):
        raise HTTPException(status_code=403, detail="You cannot assign a higher role than your own.")
    if not normalized_user_id:
        if not normalized_email or "@" not in normalized_email:
            raise HTTPException(status_code=400, detail="userId or valid email is required")
        normalized_user_id = str(find_user_id_by_email(email=normalized_email) or "").strip()
    if not normalized_user_id:
        raise HTTPException(status_code=404, detail="Existing user not found")
    if normalized_user_id == actor_user_id:
        raise HTTPException(status_code=403, detail="You cannot update your own access.")
    if not is_auth_user_active(user_id=normalized_user_id):
        raise HTTPException(status_code=403, detail="Target user is not active.")

    target_super_admin = is_user_super_admin(user_id=normalized_user_id)
    if target_super_admin and not actor_is_super_admin:
        raise HTTPException(status_code=403, detail="You cannot update a Super Admin.")

    scoped_users = _scoped_users_by_user_id(tenant_id=tenant_id, app_id=app_id)
    existing_user = scoped_users.get(normalized_user_id)
    if existing_user:
        _assert_actor_can_manage_target(
            actor_user_id=actor_user_id,
            actor_role=actor_role,
            actor_is_super_admin=actor_is_super_admin,
            target_user=existing_user,
            action_label="update",
        )

    try:
        activate_tenant_user(tenant_id=tenant_id, user_id=normalized_user_id)
        upsert_tenant_app_role(
            tenant_id=tenant_id,
            user_id=normalized_user_id,
            app_id=app_id,
            role=role,
        )
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    permission_cache.invalidate(user_id=normalized_user_id)
    refreshed_user = _scoped_users_by_user_id(tenant_id=tenant_id, app_id=app_id).get(normalized_user_id)
    if not refreshed_user:
        profile = select_profile_for_user(provider=get_auth_provider(), user_id=normalized_user_id) or {}
        full_name = str(profile.get("full_name") or "").strip() or None
        first_name, last_name = split_full_name(full_name)
        refreshed_user = {
            "userId": normalized_user_id,
            "email": str(profile.get("email") or normalized_email).strip().lower() or None,
            "fullName": full_name,
            "firstName": first_name,
            "lastName": last_name,
            "status": "active",
            "role": role,
            "isSuperAdmin": target_super_admin,
            "assignedInScope": True,
        }

    return {
        "status": "assigned",
        "user": refreshed_user,
    }


@router.get("/users")
def list_scoped_invitation_users_route(
    request: Request,
    query: str | None = Query(default=None),
):
    """
    List users currently assigned to the active tenant + app scope for app-scoped access management UI.

    Example request:
        GET /api/auth/v1/invitations/users

    Example request (with filter):
        GET /api/auth/v1/invitations/users?query=alex@example.com

    Example response:
        {
          "items": [
            {
              "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
              "email": "alex@example.com",
              "fullName": "Alex Johnson",
              "firstName": "Alex",
              "lastName": "Johnson",
              "status": "active",
              "role": "editor",
              "isSuperAdmin": false,
              "assignedInScope": true
            }
          ],
          "scope": {
            "tenantId": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
            "appCode": "shiftzy"
          }
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires current app admin permission or workspace.super_admin
    """
    _, tenant_id, _, app_id, app_code, _, _ = _require_admin(request)
    app_rows = list_tenant_users_with_app_role(tenant_id=tenant_id, app_id=app_id)
    search_text = str(query or "").strip().lower()

    items: list[dict[str, object]] = []
    for row in app_rows:
        if not isinstance(row, dict):
            continue
        normalized = _serialize_scoped_user_row(row=row, tenant_id=tenant_id, app_id=app_id)
        if not normalized:
            continue
        if search_text:
            haystack = " ".join(
                [
                    str(normalized.get("email") or ""),
                    str(normalized.get("fullName") or ""),
                    str(normalized.get("userId") or ""),
                    str(normalized.get("role") or ""),
                ]
            ).lower()
            if search_text not in haystack:
                continue
        items.append(normalized)

    items.sort(key=lambda item: (str(item.get("email") or "~").lower(), str(item.get("userId") or "")))
    return {
        "items": items,
        "scope": {
            "tenantId": tenant_id,
            "appCode": app_code,
        },
    }


@router.get("/users/lookup")
def lookup_active_existing_user_route(
    request: Request,
    query: str | None = Query(default=None),
    email: str | None = Query(default=None),
):
    """
    Lookup active existing users by email or name for app-scoped access-add flows.

    Example request:
        GET /api/auth/v1/invitations/users/lookup?query=alex@example.com

    Example request (name search):
        GET /api/auth/v1/invitations/users/lookup?query=alex

    Example response:
        {
          "items": [
            {
              "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
              "email": "alex@example.com",
              "fullName": "Alex Johnson",
              "firstName": "Alex",
              "lastName": "Johnson",
              "status": "active",
              "role": null,
              "isSuperAdmin": false,
              "assignedInScope": false
            }
          ],
          "scope": {
            "tenantId": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
            "appCode": "shiftzy"
          }
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires current app admin permission or workspace.super_admin
        - Returns only active existing users
        - `query` supports email or name search (case-insensitive contains)
    """
    _, tenant_id, _, app_id, app_code, actor_is_super_admin, _ = _require_admin(request)
    normalized_query = str(query or email or "").strip().lower()
    if len(normalized_query) < 2:
        raise HTTPException(status_code=400, detail="query must be at least 2 characters")

    scoped_users_by_id = _scoped_users_by_user_id(tenant_id=tenant_id, app_id=app_id)
    provider = get_auth_provider()

    matched_user_ids: list[str] = []
    if "@" in normalized_query:
        by_email_user_id = str(find_user_id_by_email(email=normalized_query) or "").strip()
        if by_email_user_id:
            matched_user_ids.append(by_email_user_id)
    try:
        profile_rows = provider.select_many(
            table="profiles",
            filters={},
            select="user_id,email,full_name",
        )
        profile_user_id_key = "user_id"
    except SupabaseClientError as exc:
        if not _is_missing_profile_column_error(exc, column="user_id"):
            raise
        profile_rows = provider.select_many(
            table="profiles",
            filters={},
            select="id,email,full_name",
        )
        profile_user_id_key = "id"
    for row in profile_rows:
        user_id = str(row.get(profile_user_id_key) or "").strip()
        if not user_id:
            continue
        profile_email = str(row.get("email") or "").strip().lower()
        profile_name = str(row.get("full_name") or "").strip().lower()
        haystack = " ".join([profile_email, profile_name, user_id]).strip()
        if normalized_query and normalized_query not in haystack:
            continue
        matched_user_ids.append(user_id)

    deduped_user_ids: list[str] = []
    seen_user_ids: set[str] = set()
    for user_id in matched_user_ids:
        if not user_id or user_id in seen_user_ids:
            continue
        seen_user_ids.add(user_id)
        deduped_user_ids.append(user_id)
        if len(deduped_user_ids) >= 60:
            break

    items: list[dict[str, object]] = []
    for user_id in deduped_user_ids:
        if not is_auth_user_active(user_id=user_id):
            continue
        existing_scoped_user = scoped_users_by_id.get(user_id)
        if existing_scoped_user:
            continue
        target_super_admin = is_user_super_admin(user_id=user_id)
        if target_super_admin and not actor_is_super_admin:
            continue

        profile = select_profile_for_user(provider=provider, user_id=user_id) or {}
        full_name = str(profile.get("full_name") or "").strip() or None
        first_name, last_name = split_full_name(full_name)
        profile_email = str(profile.get("email") or "").strip().lower() or None
        items.append(
            {
                "userId": user_id,
                "email": profile_email,
                "fullName": full_name,
                "firstName": first_name,
                "lastName": last_name,
                "status": "active",
                "role": None,
                "isSuperAdmin": target_super_admin,
                "assignedInScope": False,
            }
        )
        if len(items) >= 24:
            break

    items.sort(
        key=lambda item: (
            0 if normalized_query and normalized_query in str(item.get("fullName") or "").strip().lower() else 1,
            0 if normalized_query and str(item.get("fullName") or "").strip().lower().startswith(normalized_query) else 1,
            str(item.get("fullName") or item.get("email") or item.get("userId") or "").strip().lower(),
        )
    )

    return {
        "items": items,
        "scope": {
            "tenantId": tenant_id,
            "appCode": app_code,
        },
    }


@router.get("/users/page-permissions")
def list_scoped_user_page_permissions_route(request: Request):
    """
    List app-page permissions for users in the active tenant+app scope.

    Example request:
        GET /api/auth/v1/invitations/users/page-permissions

    Example response:
        {
          "scope": {
            "tenantId": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
            "appId": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
            "appCode": "tradsphere"
          },
          "availablePages": [
            {"key": "tradsphere_home", "label": "Accounts", "route": "/tradsphere/home"}
          ],
          "items": [
            {
              "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
              "email": "alex@example.com",
              "fullName": "Alex Johnson",
              "status": "active",
              "role": "admin",
              "isSuperAdmin": false,
              "pageKeys": ["tradsphere_home"],
              "hasRestrictions": true
            }
          ]
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires current app admin permission or workspace.super_admin
        - If a user has zero page keys, they keep full page access for that app
    """
    _, tenant_id, _, app_id, app_code, _, _ = _require_admin(request)
    available_pages = list_page_catalog_for_app(app_code=app_code)
    valid_page_keys = {
        str(item.get("key") or "").strip().lower()
        for item in available_pages
        if str(item.get("key") or "").strip()
    }
    users_by_user_id = _scoped_users_by_user_id(tenant_id=tenant_id, app_id=app_id)
    scoped_user_ids = set(users_by_user_id.keys())
    permission_rows = list_page_permissions_for_scope(
        tenant_id=tenant_id,
        app_id=app_id,
        user_ids=scoped_user_ids if scoped_user_ids else None,
    )

    page_keys_by_user_id: dict[str, set[str]] = {}
    for row in permission_rows:
        user_id = str(row.get("user_id") or "").strip()
        page_key = normalize_page_key(row.get("page_key") or "")
        if not user_id or not page_key:
            continue
        if valid_page_keys and page_key not in valid_page_keys:
            continue
        page_keys_by_user_id.setdefault(user_id, set()).add(page_key)

    items: list[dict[str, object]] = []
    for user_id, user_payload in users_by_user_id.items():
        user_page_keys = sorted(page_keys_by_user_id.get(user_id, set()))
        items.append(
            {
                "userId": user_id,
                "email": user_payload.get("email"),
                "fullName": user_payload.get("fullName"),
                "status": user_payload.get("status"),
                "role": user_payload.get("role"),
                "isSuperAdmin": bool(user_payload.get("isSuperAdmin")),
                "pageKeys": user_page_keys,
                "hasRestrictions": len(user_page_keys) > 0,
            }
        )

    items.sort(key=lambda item: (str(item.get("email") or "~").lower(), str(item.get("userId") or "")))
    return {
        "scope": {
            "tenantId": tenant_id,
            "appId": app_id,
            "appCode": app_code,
        },
        "availablePages": available_pages,
        "items": items,
    }


@router.put("/users/{user_id}/page-permissions")
def replace_scoped_user_page_permissions_route(
    request: Request,
    user_id: str,
    payload: dict = Body(...),
):
    """
    Replace page-level access restrictions for one scoped tenant+app user.

    Example request:
        PUT /api/auth/v1/invitations/users/762fec4b-1da3-4ba7-80e2-dd21622b6e0d/page-permissions

    Example request body:
        {
          "pageKeys": ["tradsphere_home", "tradsphere_contacts"]
        }

    Example response:
        {
          "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d",
          "pageKeys": ["tradsphere_contacts", "tradsphere_home"],
          "hasRestrictions": true
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires current app admin permission or workspace.super_admin
        - Target user must already be assigned in the current tenant+app scope
        - Empty pageKeys clears restrictions and restores full page access
    """
    actor_user_id, tenant_id, _, app_id, app_code, actor_is_super_admin, actor_role = _require_admin(request)
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        raise HTTPException(status_code=400, detail="user_id is required")

    users_by_user_id = _scoped_users_by_user_id(tenant_id=tenant_id, app_id=app_id)
    if normalized_user_id not in users_by_user_id:
        raise HTTPException(status_code=404, detail="Target user is not assigned in the current app scope")
    target_user = users_by_user_id[normalized_user_id]
    _assert_actor_can_manage_target(
        actor_user_id=actor_user_id,
        actor_role=actor_role,
        actor_is_super_admin=actor_is_super_admin,
        target_user=target_user,
        action_label="set page permissions for",
    )

    raw_page_keys = payload.get("pageKeys")
    if not isinstance(raw_page_keys, list):
        raise HTTPException(status_code=400, detail="pageKeys must be an array")
    page_key_values = raw_page_keys
    normalized_page_keys = normalize_page_keys(app_code=app_code, page_keys=page_key_values)
    provided_non_empty = {
        str(value or "").strip().lower()
        for value in raw_page_keys
        if str(value or "").strip()
    }
    if provided_non_empty and not normalized_page_keys:
        raise HTTPException(status_code=400, detail="No valid page keys were provided for this app")

    try:
        saved_page_keys = replace_page_permissions_for_user(
            tenant_id=tenant_id,
            app_id=app_id,
            app_code=app_code,
            user_id=normalized_user_id,
            page_keys=normalized_page_keys,
        )
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    permission_cache.invalidate(user_id=normalized_user_id)
    return {
        "userId": normalized_user_id,
        "pageKeys": saved_page_keys,
        "hasRestrictions": len(saved_page_keys) > 0,
    }


@router.delete("/users/{user_id}/access")
def remove_scoped_user_access_route(
    request: Request,
    user_id: str,
):
    """
    Remove app access for one user in the active tenant+app scope.

    Example request:
        DELETE /api/auth/v1/invitations/users/762fec4b-1da3-4ba7-80e2-dd21622b6e0d/access

    Example response:
        {
          "status": "removed",
          "userId": "762fec4b-1da3-4ba7-80e2-dd21622b6e0d"
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires current app admin permission or workspace.super_admin
        - Actor cannot remove their own app access
        - Non-super-admin actor cannot remove super-admin access
        - Non-super-admin actor cannot remove higher-role user access
    """
    actor_user_id, tenant_id, _, app_id, app_code, actor_is_super_admin, actor_role = _require_admin(request)
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        raise HTTPException(status_code=400, detail="user_id is required")

    users_by_user_id = _scoped_users_by_user_id(tenant_id=tenant_id, app_id=app_id)
    target_user = users_by_user_id.get(normalized_user_id)
    if not target_user:
        raise HTTPException(status_code=404, detail="Target user is not assigned in the current app scope")

    _assert_actor_can_manage_target(
        actor_user_id=actor_user_id,
        actor_role=actor_role,
        actor_is_super_admin=actor_is_super_admin,
        target_user=target_user,
        action_label="remove access for",
    )

    try:
        remove_tenant_app_role(
            tenant_id=tenant_id,
            user_id=normalized_user_id,
            app_id=app_id,
        )
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    permission_cache.invalidate(user_id=normalized_user_id)
    return {
        "status": "removed",
        "userId": normalized_user_id,
    }


@router.get("/{token}")
def get_invitation_route(token: str):
    """
    Resolve invitation metadata for invite landing and acceptance UI.

    Example request:
        GET /api/auth/v1/invitations/abc123

    Example response:
        {
          "id": "0ec0a2f8-0a59-42e4-b296-8f8b4a8e6d6d",
          "email": "new.user@company.com",
          "tenantId": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
          "tenantSlug": "lacs",
          "appId": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
          "appCode": "shiftzy",
          "role": "viewer",
          "status": "pending",
          "expiresAt": "2026-05-13T08:00:00+00:00",
          "acceptedAt": null,
          "createdAt": "2026-05-10T08:00:00+00:00"
        }

    Requirements:
        - Public endpoint
        - Token must exist
        - Returns no secrets/tokens beyond invitation token route lookup
    """
    row = get_invitation_by_token(token)
    if not row:
        raise HTTPException(status_code=404, detail="Invitation not found")

    invitation_id = str(row.get("id") or "").strip()
    assignments = list_invitation_assignments(invitation_id=invitation_id)

    return _serialize_invitation_response(row=row, assignments=assignments)


@router.post("/{token}/accept")
def accept_invitation_route(
    request: Request,
    token: str,
    payload: dict | None = Body(default=None),
):
    """
    Accept a pending invitation for the currently authenticated user.

    Example request:
        POST /api/auth/v1/invitations/abc123/accept

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
          "status": "accepted",
          "tenantId": "a4f4fd7d-2c0d-4bb2-bf73-26e5f7f918bf",
          "tenantSlug": "lacs",
          "appId": "f57fc74c-b429-4ce2-8bd0-c6f154a2cb18",
          "appCode": "shiftzy",
          "role": "viewer"
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Invitation must be pending and not expired
        - Signed-in user email must match invite email when invite email is present
    """
    principal = authenticate_bearer(request)
    user_id = principal.user_id

    row = get_invitation_by_token(token)
    if not row:
        raise HTTPException(status_code=404, detail="Invitation not found")

    status = str(row.get("status") or "").strip().lower()
    if status != "pending":
        raise HTTPException(status_code=400, detail="Invitation is not pending")

    expires_at_raw = str(row.get("expires_at") or "").strip()
    expires_at = _parse_invitation_expiry(expires_at_raw)
    if datetime.now(timezone.utc) > expires_at:
        patch_invitation_status(invitation_id=str(row.get("id") or ""), status="expired")
        raise HTTPException(status_code=400, detail="Invitation has expired")

    invitation_id = str(row.get("id") or "").strip()
    tenant_id = str(row.get("tenant_id") or "").strip()
    app_id = str(row.get("app_id") or "").strip()
    role = normalize_role_key(str(row.get("role") or "").strip())
    assignments = list_invitation_assignments(invitation_id=invitation_id)
    resolved_assignments = [
        (
            str(item.get("tenant_id") or "").strip(),
            str(item.get("app_id") or "").strip(),
            normalize_role_key(str(item.get("role") or "").strip()),
        )
        for item in assignments
        if str(item.get("tenant_id") or "").strip()
        and str(item.get("app_id") or "").strip()
        and normalize_role_key(str(item.get("role") or "").strip())
    ]
    if not resolved_assignments:
        resolved_assignments = [(tenant_id, app_id, role)]
    invite_email = str(row.get("email") or "").strip().lower()
    principal_email = str(principal.email or "").strip().lower()
    if invite_email and principal_email and invite_email != principal_email:
        raise HTTPException(status_code=403, detail="Invite email does not match authenticated user")

    body = payload if isinstance(payload, dict) else {}
    profile_payload = body.get("profile")
    profile = profile_payload if isinstance(profile_payload, dict) else {}
    full_name = compose_full_name(
        full_name=str(profile.get("fullName") or body.get("fullName") or "").strip() or None,
        first_name=str(profile.get("firstName") or body.get("firstName") or "").strip() or None,
        last_name=str(profile.get("lastName") or body.get("lastName") or "").strip() or None,
    )
    if full_name is not None and len(full_name) > 255:
        raise HTTPException(status_code=400, detail="Full name exceeds maximum length")

    try:
        upsert_profile_for_invited_user(
            user_id=user_id,
            email=principal.email or invite_email or None,
            full_name=full_name,
        )
        for assignment_tenant_id, assignment_app_id, assignment_role in resolved_assignments:
            activate_tenant_user(tenant_id=assignment_tenant_id, user_id=user_id)
            upsert_tenant_app_role(
                tenant_id=assignment_tenant_id,
                user_id=user_id,
                app_id=assignment_app_id,
                role=assignment_role,
            )
        mark_invitation_accepted(invitation_id=invitation_id, accepted_by_user_id=user_id)
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    permission_cache.invalidate(user_id=user_id)

    return {
        "status": "accepted",
        "tenantId": tenant_id,
        "tenantSlug": _resolve_tenant_slug(tenant_id=tenant_id),
        "appId": app_id,
        "appCode": _resolve_app_code(app_id=app_id),
        "role": role,
        "assignments": [
            {
                "tenantId": assignment_tenant_id,
                "tenantSlug": _resolve_tenant_slug(tenant_id=assignment_tenant_id),
                "appId": assignment_app_id,
                "appCode": _resolve_app_code(app_id=assignment_app_id),
                "role": assignment_role,
            }
            for assignment_tenant_id, assignment_app_id, assignment_role in resolved_assignments
        ],
    }


@router.post("/{invitation_id}/revoke")
def revoke_invitation_route(request: Request, invitation_id: str):
    """
    Revoke an invitation belonging to the active tenant.

    Example request:
        POST /api/auth/v1/invitations/0ec0a2f8-0a59-42e4-b296-8f8b4a8e6d6d/revoke

    Example response:
        {
          "status": "revoked",
          "id": "0ec0a2f8-0a59-42e4-b296-8f8b4a8e6d6d"
        }

    Requirements:
        - Requires Authorization: Bearer <Supabase JWT>
        - Requires X-Tenant-Id
        - Requires current app admin permission
    """
    _, tenant_id, _, _, _, _, _ = _require_admin(request)

    row = get_invitation_by_id(invitation_id=invitation_id, tenant_id=tenant_id)
    if not row:
        raise HTTPException(status_code=404, detail="Invitation not found")

    accepted_by_user_id = str(row.get("accepted_by_user_id") or "").strip()
    try:
        patch_invitation_status(invitation_id=invitation_id, status="revoked")
    except SupabaseClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    if accepted_by_user_id:
        permission_cache.invalidate(user_id=accepted_by_user_id)

    return {"status": "revoked", "id": invitation_id}
