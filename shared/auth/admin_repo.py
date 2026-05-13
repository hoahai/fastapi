from __future__ import annotations

from collections.abc import Iterable

from shared.auth.providers import get_auth_provider
from shared.auth.profile_repo import select_profile_for_user
from shared.auth.roles import ROLE_ADMIN, ROLE_ORDER, ROLE_SUPER_ADMIN, normalize_role_key
from shared.auth.supabase_client import SupabaseClientError


def _provider():
    return get_auth_provider()


def _as_non_empty_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _role_rank(role_key: str | None) -> int:
    normalized = normalize_role_key(role_key)
    try:
        return ROLE_ORDER.index(normalized)
    except ValueError:
        return len(ROLE_ORDER) + 1


def _is_missing_table_error(exc: Exception, table_name: str) -> bool:
    detail = str(exc).lower()
    return table_name.lower() in detail and ("does not exist" in detail or "not found" in detail)


def _collect_table_rows(
    *,
    table: str,
    filters: dict[str, str],
    select: str,
) -> list[dict[str, object]]:
    return _provider().select_many(
        table=table,
        filters=filters,
        select=select,
    )


def _select_invitation_email_for_user(*, provider, user_id: str) -> str | None:
    rows = provider.select_many(
        table="invitations",
        filters={"accepted_by_user_id": user_id},
        select="email,accepted_at,created_at",
    )
    candidates: list[tuple[str, str]] = []
    for row in rows:
        email = _as_non_empty_text(row.get("email"))
        if not email:
            continue
        timestamp = _as_non_empty_text(row.get("accepted_at")) or _as_non_empty_text(row.get("created_at")) or ""
        candidates.append((timestamp, email))
    if not candidates:
        return None
    candidates.sort(key=lambda entry: entry[0], reverse=True)
    return candidates[0][1]


def list_active_tenants() -> list[dict[str, object]]:
    rows = _collect_table_rows(
        table="tenants",
        filters={"active": "true"},
        select="id,slug,name,active,created_at",
    )
    items = [
        {
            "id": row.get("id"),
            "slug": row.get("slug"),
            "name": row.get("name"),
            "active": bool(row.get("active", True)),
            "createdAt": row.get("created_at"),
        }
        for row in rows
    ]
    items.sort(key=lambda item: (str(item.get("slug") or "~").lower(), str(item.get("id") or "")))
    return items


def list_active_apps() -> list[dict[str, object]]:
    rows = _collect_table_rows(
        table="apps",
        filters={"active": "true"},
        select="id,code,name,active,created_at",
    )
    items = [
        {
            "id": row.get("id"),
            "code": str(row.get("code") or "").strip().lower(),
            "name": row.get("name"),
            "active": bool(row.get("active", True)),
            "createdAt": row.get("created_at"),
        }
        for row in rows
    ]
    items.sort(
        key=lambda item: (
            0 if str(item.get("code") or "").strip().lower() == "tradsphere" else 1,
            str(item.get("code") or "~").lower(),
        )
    )
    return items


def list_role_keys_from_store() -> list[str]:
    roles: set[str] = set()
    for row in _collect_table_rows(
        table="role_permissions",
        filters={},
        select="role",
    ):
        role = normalize_role_key(str(row.get("role") or "").strip())
        if role:
            roles.add(role)
    for row in _collect_table_rows(
        table="tenant_app_roles",
        filters={},
        select="role",
    ):
        role = normalize_role_key(str(row.get("role") or "").strip())
        if role:
            roles.add(role)
    try:
        for row in _collect_table_rows(
            table="user_global_roles",
            filters={"active": "true"},
            select="role",
        ):
            role = normalize_role_key(str(row.get("role") or "").strip())
            if role:
                roles.add(role)
    except SupabaseClientError as exc:
        if not _is_missing_table_error(exc, "user_global_roles"):
            raise
    return sorted(roles)


def list_active_global_roles_by_user(*, user_ids: set[str] | None = None) -> dict[str, list[str]]:
    try:
        rows = _collect_table_rows(
            table="user_global_roles",
            filters={"active": "true"},
            select="id,user_id,role,active,created_at,updated_at",
        )
    except SupabaseClientError as exc:
        if _is_missing_table_error(exc, "user_global_roles"):
            return {}
        raise

    results: dict[str, list[str]] = {}
    for row in rows:
        user_id = str(row.get("user_id") or "").strip()
        role = normalize_role_key(str(row.get("role") or "").strip())
        if not user_id or not role:
            continue
        if user_ids and user_id not in user_ids:
            continue
        results.setdefault(user_id, [])
        if role not in results[user_id]:
            results[user_id].append(role)
    for roles in results.values():
        roles.sort(key=_role_rank)
    return results


def _tenant_rows_by_scope(*, scope_tenant_ids: set[str] | None) -> list[dict[str, object]]:
    if scope_tenant_ids and len(scope_tenant_ids) == 1:
        tenant_id = next(iter(scope_tenant_ids))
        return _collect_table_rows(
            table="tenant_users",
            filters={"tenant_id": tenant_id},
            select="id,tenant_id,user_id,status,created_at,updated_at",
        )
    rows = _collect_table_rows(
        table="tenant_users",
        filters={},
        select="id,tenant_id,user_id,status,created_at,updated_at",
    )
    if not scope_tenant_ids:
        return rows
    return [row for row in rows if str(row.get("tenant_id") or "").strip() in scope_tenant_ids]


def _role_rows_by_scope(
    *,
    scope_tenant_ids: set[str] | None,
    scope_app_ids: set[str] | None,
) -> list[dict[str, object]]:
    if scope_tenant_ids and len(scope_tenant_ids) == 1 and scope_app_ids and len(scope_app_ids) == 1:
        return _collect_table_rows(
            table="tenant_app_roles",
            filters={
                "tenant_id": next(iter(scope_tenant_ids)),
                "app_id": next(iter(scope_app_ids)),
            },
            select="id,tenant_id,user_id,app_id,role,created_at,updated_at",
        )
    rows = _collect_table_rows(
        table="tenant_app_roles",
        filters={},
        select="id,tenant_id,user_id,app_id,role,created_at,updated_at",
    )
    filtered = rows
    if scope_tenant_ids:
        filtered = [row for row in filtered if str(row.get("tenant_id") or "").strip() in scope_tenant_ids]
    if scope_app_ids:
        filtered = [row for row in filtered if str(row.get("app_id") or "").strip() in scope_app_ids]
    return filtered


def list_users_with_access(
    *,
    scope_tenant_ids: set[str] | None = None,
    scope_app_ids: set[str] | None = None,
    primary_tenant_id: str | None = None,
    primary_app_id: str | None = None,
) -> list[dict[str, object]]:
    provider = _provider()
    membership_rows = _tenant_rows_by_scope(scope_tenant_ids=scope_tenant_ids)
    role_rows = _role_rows_by_scope(scope_tenant_ids=scope_tenant_ids, scope_app_ids=scope_app_ids)

    tenant_map: dict[str, dict[str, object]] = {}
    for row in _collect_table_rows(table="tenants", filters={}, select="id,slug,name,active"):
        tenant_id = str(row.get("id") or "").strip()
        if tenant_id:
            tenant_map[tenant_id] = row

    app_map: dict[str, dict[str, object]] = {}
    for row in _collect_table_rows(table="apps", filters={}, select="id,code,name,active"):
        app_id = str(row.get("id") or "").strip()
        if app_id:
            app_map[app_id] = row

    memberships_by_user: dict[str, list[dict[str, object]]] = {}
    roles_by_user: dict[str, list[dict[str, object]]] = {}
    primary_membership_by_user: dict[str, dict[str, object]] = {}
    primary_role_by_user: dict[str, dict[str, object]] = {}

    for row in membership_rows:
        user_id = str(row.get("user_id") or "").strip()
        tenant_id = str(row.get("tenant_id") or "").strip()
        if not user_id or not tenant_id:
            continue
        tenant_row = tenant_map.get(tenant_id) or {}
        item = {
            "tenantId": tenant_id,
            "tenantSlug": tenant_row.get("slug"),
            "tenantName": tenant_row.get("name"),
            "status": row.get("status") or "pending",
            "createdAt": row.get("created_at"),
            "updatedAt": row.get("updated_at"),
        }
        memberships_by_user.setdefault(user_id, []).append(item)
        if primary_tenant_id and tenant_id == primary_tenant_id:
            primary_membership_by_user[user_id] = item

    for row in role_rows:
        user_id = str(row.get("user_id") or "").strip()
        tenant_id = str(row.get("tenant_id") or "").strip()
        app_id = str(row.get("app_id") or "").strip()
        if not user_id or not tenant_id or not app_id:
            continue
        tenant_row = tenant_map.get(tenant_id) or {}
        app_row = app_map.get(app_id) or {}
        item = {
            "tenantId": tenant_id,
            "tenantSlug": tenant_row.get("slug"),
            "tenantName": tenant_row.get("name"),
            "appId": app_id,
            "appCode": str(app_row.get("code") or "").strip().lower() or None,
            "appName": app_row.get("name"),
            "role": row.get("role"),
            "createdAt": row.get("created_at"),
            "updatedAt": row.get("updated_at"),
        }
        roles_by_user.setdefault(user_id, []).append(item)
        if primary_tenant_id and primary_app_id and tenant_id == primary_tenant_id and app_id == primary_app_id:
            primary_role_by_user[user_id] = item

    user_ids = sorted(set(memberships_by_user.keys()) | set(roles_by_user.keys()))
    global_roles_by_user = list_active_global_roles_by_user(user_ids=set(user_ids))
    user_ids = sorted(set(user_ids) | set(global_roles_by_user.keys()))

    results: list[dict[str, object]] = []
    for user_id in user_ids:
        profile = select_profile_for_user(provider=provider, user_id=user_id)
        invitation_email = _select_invitation_email_for_user(provider=provider, user_id=user_id)
        resolved_email = _as_non_empty_text(profile.get("email") if profile else None) or invitation_email
        memberships = sorted(
            memberships_by_user.get(user_id, []),
            key=lambda item: (str(item.get("tenantSlug") or "~").lower(), str(item.get("tenantId") or "")),
        )
        assignments = sorted(
            roles_by_user.get(user_id, []),
            key=lambda item: (
                str(item.get("tenantSlug") or "~").lower(),
                str(item.get("appCode") or "~").lower(),
                str(item.get("role") or "~").lower(),
            ),
        )
        normalized_assignments = [
            {**item, "role": normalize_role_key(str(item.get("role") or "").strip()) or item.get("role")}
            for item in assignments
        ]

        primary_membership = primary_membership_by_user.get(user_id)
        primary_assignment = primary_role_by_user.get(user_id)
        if primary_membership is None and memberships:
            primary_membership = memberships[0]
        if primary_assignment is None and assignments:
            primary_assignment = assignments[0]

        top_role = None
        if normalized_assignments:
            top_role = sorted(
                [
                    normalize_role_key(str(item.get("role") or "").strip())
                    for item in normalized_assignments
                    if normalize_role_key(str(item.get("role") or "").strip())
                ],
                key=_role_rank,
            )[0]
        global_roles = global_roles_by_user.get(user_id, [])
        if ROLE_SUPER_ADMIN in global_roles:
            top_role = ROLE_SUPER_ADMIN

        created_at = None
        updated_at = None
        for row in [primary_membership, primary_assignment]:
            if row and not created_at:
                created_at = row.get("createdAt")
            if row:
                updated_at = row.get("updatedAt") or updated_at
        if not created_at and profile:
            created_at = profile.get("created_at")
        if not updated_at and profile:
            updated_at = profile.get("updated_at")

        results.append(
            {
                "userId": user_id,
                "email": resolved_email,
                "fullName": profile.get("full_name") if profile else None,
                "status": (primary_membership or {}).get("status") or "pending",
                "role": top_role or normalize_role_key(str((primary_assignment or {}).get("role") or "").strip()) or None,
                "createdAt": created_at,
                "updatedAt": updated_at,
                "roleUpdatedAt": (primary_assignment or {}).get("updatedAt") or (primary_assignment or {}).get("createdAt"),
                "tenantMemberships": memberships,
                "appAssignments": normalized_assignments,
                "globalRoles": global_roles,
                "isSuperAdmin": ROLE_SUPER_ADMIN in global_roles,
            }
        )

    results.sort(key=lambda row: (str(row.get("email") or "~").lower(), str(row.get("userId") or "")))
    return results


def list_tenant_users_with_app_role(*, tenant_id: str, app_id: str) -> list[dict[str, object]]:
    return list_users_with_access(
        scope_tenant_ids={tenant_id},
        scope_app_ids={app_id},
        primary_tenant_id=tenant_id,
        primary_app_id=app_id,
    )


def get_user_memberships(
    *,
    user_id: str,
    scope_tenant_ids: set[str] | None = None,
) -> list[dict[str, object]]:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return []
    rows = _collect_table_rows(
        table="tenant_users",
        filters={"user_id": normalized_user_id},
        select="id,tenant_id,user_id,status,created_at,updated_at",
    )
    if scope_tenant_ids:
        rows = [row for row in rows if str(row.get("tenant_id") or "").strip() in scope_tenant_ids]
    return rows


def get_user_app_roles(
    *,
    user_id: str,
    scope_tenant_ids: set[str] | None = None,
) -> list[dict[str, object]]:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return []
    rows = _collect_table_rows(
        table="tenant_app_roles",
        filters={"user_id": normalized_user_id},
        select="id,tenant_id,user_id,app_id,role,created_at,updated_at",
    )
    if scope_tenant_ids:
        rows = [row for row in rows if str(row.get("tenant_id") or "").strip() in scope_tenant_ids]
    return rows


def set_tenant_user_status(*, tenant_id: str, user_id: str, status: str) -> None:
    provider = _provider()
    existing = provider.select_single(
        table="tenant_users",
        filters={"tenant_id": tenant_id, "user_id": user_id},
        select="id,status",
    )

    if existing:
        provider.patch_rows(
            table="tenant_users",
            filters={"id": str(existing.get("id") or "")},
            patch={"status": status, "updated_at": provider.now_iso()},
        )
        return

    provider.insert_row(
        table="tenant_users",
        row={
            "tenant_id": tenant_id,
            "user_id": user_id,
            "status": status,
        },
    )


def set_tenant_app_role(*, tenant_id: str, user_id: str, app_id: str, role: str) -> None:
    provider = _provider()
    normalized_role = normalize_role_key(role)
    existing = provider.select_single(
        table="tenant_app_roles",
        filters={"tenant_id": tenant_id, "user_id": user_id, "app_id": app_id},
        select="id,role",
    )

    if existing:
        provider.patch_rows(
            table="tenant_app_roles",
            filters={"id": str(existing.get("id") or "")},
            patch={"role": normalized_role, "updated_at": provider.now_iso()},
        )
        return

    provider.insert_row(
        table="tenant_app_roles",
        row={
            "tenant_id": tenant_id,
            "user_id": user_id,
            "app_id": app_id,
            "role": normalized_role,
        },
    )


def remove_tenant_app_role(*, tenant_id: str, user_id: str, app_id: str) -> None:
    _provider().delete_rows(
        table="tenant_app_roles",
        filters={"tenant_id": tenant_id, "user_id": user_id, "app_id": app_id},
    )


def remove_tenant_app_roles_for_tenant(*, tenant_id: str, user_id: str) -> None:
    _provider().delete_rows(
        table="tenant_app_roles",
        filters={"tenant_id": tenant_id, "user_id": user_id},
    )


def unique_text_values(values: Iterable[object]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        results.append(normalized)
    return results


def is_user_super_admin(*, user_id: str) -> bool:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return False
    roles_by_user = list_active_global_roles_by_user(user_ids={normalized_user_id})
    return ROLE_SUPER_ADMIN in roles_by_user.get(normalized_user_id, [])


def list_admin_assignment_scopes_for_user(*, user_id: str) -> set[tuple[str, str]]:
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return set()
    rows = _collect_table_rows(
        table="tenant_app_roles",
        filters={"user_id": normalized_user_id},
        select="tenant_id,app_id,role",
    )
    scopes: set[tuple[str, str]] = set()
    for row in rows:
        tenant_id = str(row.get("tenant_id") or "").strip()
        app_id = str(row.get("app_id") or "").strip()
        role = normalize_role_key(str(row.get("role") or "").strip())
        if not tenant_id or not app_id or role != ROLE_ADMIN:
            continue
        scopes.add((tenant_id, app_id))
    return scopes
