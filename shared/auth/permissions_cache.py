from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from shared.auth.config import get_permission_cache_ttl_seconds
from shared.auth.types import TenantAccessProfile


@dataclass
class _CacheEntry:
    expires_at: float
    value: TenantAccessProfile


class PermissionCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, _CacheEntry] = {}

    @staticmethod
    def _key(user_id: str, tenant_slug: str, app_code: str) -> str:
        return f"{user_id.strip()}::{tenant_slug.strip().lower()}::{app_code.strip().lower()}"

    def get(self, *, user_id: str, tenant_slug: str, app_code: str) -> TenantAccessProfile | None:
        cache_key = self._key(user_id, tenant_slug, app_code)
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(cache_key)
            if entry is None:
                return None
            if now >= entry.expires_at:
                self._entries.pop(cache_key, None)
                return None
            return entry.value

    def set(self, *, user_id: str, tenant_slug: str, app_code: str, value: TenantAccessProfile) -> None:
        ttl = get_permission_cache_ttl_seconds()
        cache_key = self._key(user_id, tenant_slug, app_code)
        with self._lock:
            self._entries[cache_key] = _CacheEntry(
                expires_at=time.monotonic() + float(ttl),
                value=value,
            )

    def invalidate(self, *, user_id: str | None = None, tenant_slug: str | None = None, app_code: str | None = None) -> int:
        with self._lock:
            if user_id is None and tenant_slug is None and app_code is None:
                count = len(self._entries)
                self._entries.clear()
                return count

            target_user = str(user_id or "").strip()
            target_tenant = str(tenant_slug or "").strip().lower()
            target_app = str(app_code or "").strip().lower()
            to_remove: list[str] = []
            for key in self._entries.keys():
                user, tenant, app = key.split("::", 2)
                if target_user and user != target_user:
                    continue
                if target_tenant and tenant != target_tenant:
                    continue
                if target_app and app != target_app:
                    continue
                to_remove.append(key)
            for key in to_remove:
                self._entries.pop(key, None)
            return len(to_remove)


permission_cache = PermissionCache()
