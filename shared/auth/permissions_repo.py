from __future__ import annotations

from dataclasses import dataclass

from shared.auth.permission_map import expand_permissions_for_role
from shared.auth.permissions_cache import permission_cache
from shared.auth.supabase_client import SupabaseClientError, supabase_client
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

    row = supabase_client.select_single(
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

    row = supabase_client.select_single(
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
    row = supabase_client.select_single(
        table="tenant_users",
        filters={
            "tenant_id": tenant_id,
            "user_id": user_id,
            "status": "active",
        },
        select="id,tenant_id,user_id,status",
    )
    if not row:
        raise TenantAccessError("User is not an active member of tenant", code="tenant_membership_required")


def _resolve_tenant_role(*, tenant_id: str, user_id: str, app_id: str) -> str:
    row = supabase_client.select_single(
        table="tenant_app_roles",
        filters={
            "tenant_id": tenant_id,
            "user_id": user_id,
            "app_id": app_id,
        },
        select="id,role",
    )
    role = str((row or {}).get("role") or "").strip()
    if not role:
        raise TenantAccessError("User does not have access to this app", code="app_role_required")
    return role


def _resolve_role_permissions(role: str) -> list[str]:
    rows = supabase_client.select_many(
        table="role_permissions",
        filters={"role": role},
        select="permission",
    )
    permissions: list[str] = []
    for row in rows:
        permission = str(row.get("permission") or "").strip()
        if permission:
            permissions.append(permission)
    return permissions


def resolve_tenant_access(*, user_id: str, tenant_slug: str, app_code: str) -> TenantAccessProfile:
    try:
        tenant = _resolve_active_tenant(tenant_slug)
        app = _resolve_active_app(app_code)
        _resolve_active_tenant_user(tenant_id=tenant.tenant_id, user_id=user_id)
        role = _resolve_tenant_role(tenant_id=tenant.tenant_id, user_id=user_id, app_id=app.app_id)
        role_permissions = _resolve_role_permissions(role)
    except SupabaseClientError as exc:
        raise TenantAccessError("Unable to verify tenant permissions", code="supabase_unavailable") from exc

    permissions = frozenset(expand_permissions_for_role(role, role_permissions))
    return TenantAccessProfile(
        tenant_id=tenant.tenant_id,
        tenant_slug=tenant.slug,
        app_id=app.app_id,
        app_code=app.code,
        role=role,
        permissions=permissions,
    )


def get_tenant_access_cached(*, user_id: str, tenant_slug: str, app_code: str) -> TenantAccessProfile:
    cached = permission_cache.get(user_id=user_id, tenant_slug=tenant_slug, app_code=app_code)
    if cached is not None:
        return cached

    fresh = resolve_tenant_access(user_id=user_id, tenant_slug=tenant_slug, app_code=app_code)
    permission_cache.set(user_id=user_id, tenant_slug=tenant_slug, app_code=app_code, value=fresh)
    return fresh
