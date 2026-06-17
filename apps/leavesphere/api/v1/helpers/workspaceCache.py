from __future__ import annotations

import hashlib
import json
from typing import Any

from shared.tenantDataCache import (
    delete_tenant_shared_cache_values_by_prefix,
    get_shared_cache_ttl_seconds,
    get_tenant_shared_cache_value,
    set_tenant_shared_cache_value,
)

_CACHE_BUCKET = "leavesphere_workspace"
_CACHE_TTL_SECONDS = get_shared_cache_ttl_seconds(
    key="leavesphere_workspace_ttl_time",
    default_seconds=300,
    app_name="leavesphere",
)


def _normalize_cache_component(value: object | None) -> str:
    text = str(value or "").strip().lower()
    return text or "unknown"


def _build_cache_suffix(*, year: int | None, params: dict[str, Any] | None) -> str:
    payload = {
        "year": year,
        "params": params or {},
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    digest = hashlib.md5(raw.encode("utf-8")).hexdigest()
    return digest


def build_leave_sphere_workspace_cache_key(
    *,
    page_code: str,
    user_key: str,
    year: int | None,
    params: dict[str, Any] | None = None,
) -> str:
    page = _normalize_cache_component(page_code)
    user = _normalize_cache_component(user_key)
    suffix = _build_cache_suffix(year=year, params=params)
    return f"{page}::{user}::{year or 'any'}::{suffix}"


def build_leave_sphere_workspace_latest_cache_key(
    *,
    page_code: str,
    user_key: str,
) -> str:
    page = _normalize_cache_component(page_code)
    user = _normalize_cache_component(user_key)
    return f"{page}::{user}::latest"


def read_leave_sphere_workspace_cache(
    *,
    page_code: str,
    user_key: str,
    year: int | None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    cache_key = build_leave_sphere_workspace_cache_key(
        page_code=page_code,
        user_key=user_key,
        year=year,
        params=params,
    )
    value, found = get_tenant_shared_cache_value(
        bucket=_CACHE_BUCKET,
        cache_key=cache_key,
        ttl_seconds=_CACHE_TTL_SECONDS,
    )
    if not found or not isinstance(value, dict):
        return None
    workspace = value.get("workspace")
    if not isinstance(workspace, dict):
        return None
    return value


def read_leave_sphere_workspace_latest_cache(
    *,
    page_code: str,
    user_key: str,
) -> dict[str, Any] | None:
    latest_key = build_leave_sphere_workspace_latest_cache_key(
        page_code=page_code,
        user_key=user_key,
    )
    pointer, found = get_tenant_shared_cache_value(
        bucket=_CACHE_BUCKET,
        cache_key=latest_key,
        ttl_seconds=_CACHE_TTL_SECONDS,
    )
    if not found or not isinstance(pointer, dict):
        return None
    cache_key = str(pointer.get("cacheKey") or "").strip()
    if not cache_key:
        return None
    value, found = get_tenant_shared_cache_value(
        bucket=_CACHE_BUCKET,
        cache_key=cache_key,
        ttl_seconds=_CACHE_TTL_SECONDS,
    )
    if not found or not isinstance(value, dict):
        return None
    workspace = value.get("workspace")
    if not isinstance(workspace, dict):
        return None
    return value


def write_leave_sphere_workspace_cache(
    *,
    page_code: str,
    user_key: str,
    year: int | None,
    workspace: dict[str, Any],
    params: dict[str, Any] | None = None,
) -> str:
    cache_key = build_leave_sphere_workspace_cache_key(
        page_code=page_code,
        user_key=user_key,
        year=year,
        params=params,
    )
    value = {
        "workspace": workspace,
        "year": year,
        "params": params or {},
    }
    set_tenant_shared_cache_value(
        bucket=_CACHE_BUCKET,
        cache_key=cache_key,
        value=value,
    )
    set_tenant_shared_cache_value(
        bucket=_CACHE_BUCKET,
        cache_key=build_leave_sphere_workspace_latest_cache_key(
            page_code=page_code,
            user_key=user_key,
        ),
        value={"cacheKey": cache_key},
    )
    return cache_key


def write_leave_sphere_workspace_cache_value(
    *,
    cache_key: str,
    page_code: str,
    user_key: str,
    year: int | None,
    workspace: dict[str, Any],
    params: dict[str, Any] | None = None,
) -> None:
    set_tenant_shared_cache_value(
        bucket=_CACHE_BUCKET,
        cache_key=cache_key,
        value={
            "workspace": workspace,
            "year": year,
            "params": params or {},
        },
    )
    set_tenant_shared_cache_value(
        bucket=_CACHE_BUCKET,
        cache_key=build_leave_sphere_workspace_latest_cache_key(
            page_code=page_code,
            user_key=user_key,
        ),
        value={"cacheKey": cache_key},
    )


def clear_leave_sphere_workspace_cache(
    *,
    page_code: str,
    user_key: str,
    year: int | None = None,
) -> int:
    page = _normalize_cache_component(page_code)
    user = _normalize_cache_component(user_key)
    prefix = f"{page}::{user}::{year or ''}"
    removed = delete_tenant_shared_cache_values_by_prefix(
        bucket=_CACHE_BUCKET,
        cache_key_prefix=prefix,
    )
    removed += delete_tenant_shared_cache_values_by_prefix(
        bucket=_CACHE_BUCKET,
        cache_key_prefix=build_leave_sphere_workspace_latest_cache_key(
            page_code=page_code,
            user_key=user_key,
        ),
    )
    return removed


def clear_leave_sphere_workspace_cache_by_page(*, page_code: str) -> int:
    page = _normalize_cache_component(page_code)
    return delete_tenant_shared_cache_values_by_prefix(
        bucket=_CACHE_BUCKET,
        cache_key_prefix=f"{page}::",
    )


def invalidate_leave_sphere_workspace_caches(*, include_catalogs: bool = False) -> int:
    removed = 0
    try:
        removed += clear_leave_sphere_workspace_cache_by_page(page_code="admin-pto")
    except Exception:
        pass
    try:
        removed += clear_leave_sphere_workspace_cache_by_page(page_code="my-pto")
    except Exception:
        pass
    if include_catalogs:
        from apps.leavesphere.api.v1.helpers.ptoWorkspaceShared import clear_leave_sphere_pto_workspace_catalog_cache

        try:
            clear_leave_sphere_pto_workspace_catalog_cache()
        except Exception:
            pass
    try:
        from apps.leavesphere.api.v1.helpers.readCache import clear_leave_sphere_read_cache

        removed += clear_leave_sphere_read_cache()
    except Exception:
        pass
    return removed
