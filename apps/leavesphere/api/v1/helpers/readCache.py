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

_CACHE_BUCKET = "leavesphere_read"
_CACHE_TTL_SECONDS = get_shared_cache_ttl_seconds(
    key="leavesphere_read_ttl_time",
    default_seconds=300,
    app_name="leavesphere",
)


def _normalize_cache_component(value: object | None) -> str:
    text = str(value or "").strip().lower()
    return text or "unknown"


def _build_cache_suffix(*, namespace: str, params: dict[str, Any] | None) -> str:
    payload = {
        "namespace": namespace,
        "params": params or {},
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def build_leave_sphere_read_cache_key(*, namespace: str, params: dict[str, Any] | None = None) -> str:
    return f"{_normalize_cache_component(namespace)}::{_build_cache_suffix(namespace=namespace, params=params)}"


def read_leave_sphere_read_cache(
    *,
    namespace: str,
    params: dict[str, Any] | None = None,
) -> object | None:
    cache_key = build_leave_sphere_read_cache_key(namespace=namespace, params=params)
    value, found = get_tenant_shared_cache_value(
        bucket=_CACHE_BUCKET,
        cache_key=cache_key,
        ttl_seconds=_CACHE_TTL_SECONDS,
    )
    return value if found else None


def write_leave_sphere_read_cache(
    *,
    namespace: str,
    value: object,
    params: dict[str, Any] | None = None,
) -> str:
    cache_key = build_leave_sphere_read_cache_key(namespace=namespace, params=params)
    set_tenant_shared_cache_value(
        bucket=_CACHE_BUCKET,
        cache_key=cache_key,
        value=value,
    )
    return cache_key


def clear_leave_sphere_read_cache_by_namespace(*, namespace: str) -> int:
    page = _normalize_cache_component(namespace)
    return delete_tenant_shared_cache_values_by_prefix(
        bucket=_CACHE_BUCKET,
        cache_key_prefix=f"{page}::",
    )


def clear_leave_sphere_read_cache() -> int:
    return delete_tenant_shared_cache_values_by_prefix(
        bucket=_CACHE_BUCKET,
        cache_key_prefix="",
    )
