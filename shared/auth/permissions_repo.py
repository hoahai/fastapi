from __future__ import annotations

from dataclasses import dataclass

from shared.auth.permission_map import expand_permissions_for_role
from shared.auth.permissions_cache import permission_cache
from shared.auth.providers import get_auth_provider
from shared.auth.roles import ROLE_SUPER_ADMIN, normalize_role_key
from shared.auth.supabase_client import SupabaseClientError
from shared.auth.types import TenantAccessProfile


class TenantAccessError(RuntimeError):
    def __init__(self, message: str, *, code: str = "forbidden") -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class _ResolvedTenant:
    tenant_id: str
    slug: str


@dataclass(frozen=True)
class _ResolvedApp:
    app_id: str
    code: str


def _resolve_active_tenant(tenant_slug: str) -> _ResolvedTenant:
    normalized = str(tenant_slug or "").strip()
    if not normalized:
        raise TenantAccessError("Missing X-Tenant-Id header", code="missing_tenant")

    provider = get_auth_provider()
    row = provider.select_single(
        table="tenants",
        filters={"slug": normalized, "active": "true"},
        select="id,slug,active",
    )
    if not row:
        raise TenantAccessError("Tenant not found or inactive", code="tenant_not_found")

    tenant_id = str(row.get("id") or "").strip()
    slug = str(row.get("slug") or "").strip()
    if not tenant_id or slug != normalized:
        raise TenantAccessError("Tenant not found or inactive", code="tenant_not_found")

    return _ResolvedTenant(tenant_id=tenant_id, slug=slug)


def _resolve_active_app(app_code: str) -> _ResolvedApp:
    normalized = str(app_code or "").strip().lower()
    if not normalized:
        raise TenantAccessError("Missing app code", code="app_not_enabled")

    provider = get_auth_provider()
    row = provider.select_single(
        table="apps",
        filters={"code": normalized, "active": "true"},
        select="id,code,active",
    )
    if not row:
        raise TenantAccessError("Requested app is not enabled", code="app_not_enabled")

    app_id = str(row.get("id") or "").strip()
    code = str(row.get("code") or "").strip().lower()
    if not app_id or code != normalized:
        raise TenantAccessError("Requested app is not enabled", code="app_not_enabled")

    return _ResolvedApp(app_id=app_id, code=code)


def _resolve_active_tenant_user(*, tenant_id: str, user_id: str) -> None:
    provider = get_auth_provider()
    rows = provider.select_many(
        table="tenant_users",
        filters={
            "tenant_id": tenant_id,
            "user_id": user_id,
        },
        select="id,tenant_id,user_id,status",
    )
    if not rows:
        raise TenantAccessError("User is not a member of tenant", code="tenant_membership_required")

    statuses = {str((row or {}).get("status") or "").strip().lower() for row in rows}
    statuses.discard("")

    if statuses & {"disabled", "inactive", "terminated"}:
        raise TenantAccessError(
            "Your account has been disabled. Contact your workspace administrator.",
            code="tenant_membership_disabled",
        )

    if "active" in statuses:
        return

    if "pending" in statuses:
        raise TenantAccessError(
            "Your account is pending activation. Contact your workspace administrator.",
            code="tenant_membership_pending",
        )

    raise TenantAccessError("User is not an active member of tenant", code="tenant_membership_inactive")


def _resolve_tenant_role(*, tenant_id: str, user_id: str, app_id: str) -> tuple[str, str]:
    provider = get_auth_provider()
    row = provider.select_single(
        table="tenant_app_roles",
        filters={
            "tenant_id": tenant_id,
            "user_id": user_id,
            "app_id": app_id,
        },
        select="id,role",
    )
    raw_role = str((row or {}).get("role") or "").strip()
    role = normalize_role_key(raw_role)
    if not role:
        raise TenantAccessError("User does not have access to this app", code="app_role_required")
    return role, raw_role


def _resolve_role_permissions(*, role: str, raw_role: str | None = None) -> list[str]:
    provider = get_auth_provider()
    role_candidates = [str(role or "").strip()]
    raw = str(raw_role or "").strip()
    if raw and raw not in role_candidates:
        role_candidates.append(raw)

    permissions: list[str] = []
    for role_key in role_candidates:
        rows = provider.select_many(
            table="role_permissions",
            filters={"role": role_key},
            select="permission",
        )
        for row in rows:
            permission = str(row.get("permission") or "").strip()
            if permission:
                permissions.append(permission)
    return permissions


def _has_global_super_admin(*, user_id: str) -> bool:
    provider = get_auth_provider()
    try:
        row = provider.select_single(
            table="user_global_roles",
            filters={
                "user_id": user_id,
                "role": ROLE_SUPER_ADMIN,
                "active": "true",
            },
            select="id,role,active",
        )
    except SupabaseClientError as exc:
        detail = str(exc).lower()
        if "user_global_roles" in detail and ("does not exist" in detail or "not found" in detail):
            return False
        raise
    return bool(row)


def resolve_tenant_access(*, user_id: str, tenant_slug: str, app_code: str) -> TenantAccessProfile:
    try:
        tenant = _resolve_active_tenant(tenant_slug)
        app = _resolve_active_app(app_code)
        is_super_admin = _has_global_super_admin(user_id=user_id)
        if is_super_admin:
            # Super-admin scope still respects tenant membership disable state when a membership row exists.
            # This keeps status enforcement as the top-priority gate.
            provider = get_auth_provider()
            membership_rows = provider.select_many(
                table="tenant_users",
                filters={
                    "tenant_id": tenant.tenant_id,
                    "user_id": user_id,
                },
                select="id,tenant_id,user_id,status",
            )
            if membership_rows:
                _resolve_active_tenant_user(tenant_id=tenant.tenant_id, user_id=user_id)
            role = ROLE_SUPER_ADMIN
            raw_role: str | None = ROLE_SUPER_ADMIN
        else:
            _resolve_active_tenant_user(tenant_id=tenant.tenant_id, user_id=user_id)
            role, raw_role = _resolve_tenant_role(tenant_id=tenant.tenant_id, user_id=user_id, app_id=app.app_id)
        role_permissions = _resolve_role_permissions(role=role, raw_role=raw_role)
    except SupabaseClientError as exc:
        raise TenantAccessError("Unable to verify tenant permissions", code="supabase_unavailable") from exc

    permissions = frozenset(
        expand_permissions_for_role(role, role_permissions, app_code=app.code)
    )
    return TenantAccessProfile(
        tenant_id=tenant.tenant_id,
        tenant_slug=tenant.slug,
        app_id=app.app_id,
        app_code=app.code,
        role=role,
        permissions=permissions,
    )


def validate_active_tenant_membership(*, user_id: str, tenant_slug: str) -> tuple[str, str]:
    """
    Validate that tenant exists/active and the user has an active membership for that tenant.

    Returns:
        tuple[str, str]: (tenant_id, tenant_slug)
    """
    try:
        tenant = _resolve_active_tenant(tenant_slug)
        _resolve_active_tenant_user(tenant_id=tenant.tenant_id, user_id=user_id)
    except SupabaseClientError as exc:
        raise TenantAccessError("Unable to verify tenant permissions", code="supabase_unavailable") from exc
    return tenant.tenant_id, tenant.slug


def get_tenant_access_cached(*, user_id: str, tenant_slug: str, app_code: str) -> TenantAccessProfile:
    cached = permission_cache.get(user_id=user_id, tenant_slug=tenant_slug, app_code=app_code)
    if cached is not None:
        normalized_role = normalize_role_key(cached.role)
        is_super_admin = normalized_role == ROLE_SUPER_ADMIN or "workspace.super_admin" in cached.permissions
        if not is_super_admin:
            try:
                _resolve_active_tenant_user(tenant_id=cached.tenant_id, user_id=user_id)
            except SupabaseClientError as exc:
                raise TenantAccessError("Unable to verify tenant permissions", code="supabase_unavailable") from exc
        return cached

    fresh = resolve_tenant_access(user_id=user_id, tenant_slug=tenant_slug, app_code=app_code)
    permission_cache.set(user_id=user_id, tenant_slug=tenant_slug, app_code=app_code, value=fresh)
    return fresh
