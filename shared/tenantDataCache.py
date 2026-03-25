from __future__ import annotations

import ast
import json
import os
from decimal import Decimal
from datetime import datetime
from pathlib import Path
from threading import Lock
from zoneinfo import ZoneInfo

from shared.fileCache import FileCache, normalize_tenant_key
from shared.tenant import get_app_scoped_env, get_env, get_tenant_id, get_timezone

_SHARED_CACHE_PATH = Path(
    os.getenv(
        "SPENDSPHERE_ACCOUNT_CODE_CACHE_PATH",
        Path(__file__).resolve().parents[1] / "caches.json",
    )
)
_CACHE_STORES: dict[str, FileCache] = {}
_CACHE_STORES_LOCK = Lock()


def _get_cache_store() -> FileCache:
    key = str(_SHARED_CACHE_PATH)
    with _CACHE_STORES_LOCK:
        store = _CACHE_STORES.get(key)
        if store is None:
            store = FileCache(_SHARED_CACHE_PATH)
            _CACHE_STORES[key] = store
    return store


def _parse_cache_config(raw: object) -> dict[str, object]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, str):
        return {}

    cleaned = raw.strip()
    if not cleaned:
        return {}

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(cleaned)
        except (ValueError, SyntaxError):
            return {}

    if not isinstance(parsed, dict):
        return {}
    return parsed


def _parse_ttl_seconds(raw: object) -> int | None:
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        value = raw
    else:
        cleaned = str(raw).strip()
        if not cleaned:
            return None
        try:
            value = int(cleaned)
        except ValueError:
            return None

    if value < 0:
        return None
    return value


def _get_shared_cache_config(*, app_name: str | None = None) -> dict[str, object]:
    raw = (
        get_app_scoped_env(app_name, "CACHE")
        or get_env("CACHE")
        or get_env("cache")
    )
    parsed = _parse_cache_config(raw)
    normalized: dict[str, object] = {}
    for key, value in parsed.items():
        if not isinstance(key, str):
            continue
        normalized[key.strip().lower()] = value
    return normalized


def get_shared_cache_ttl_seconds(
    *,
    key: str | None = None,
    default_seconds: int,
    app_name: str | None = None,
) -> int:
    config = _get_shared_cache_config(app_name=app_name)
    candidates: list[str] = []
    if key:
        candidates.append(key.strip().lower())
    candidates.append("ttl_time")

    for candidate in candidates:
        if not candidate:
            continue
        value = _parse_ttl_seconds(config.get(candidate))
        if value is not None:
            return value

    return max(int(default_seconds), 0)


def _parse_cache_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=ZoneInfo(get_timezone()))
    return parsed


def _tenant_key(tenant_id: str | None = None) -> str:
    return normalize_tenant_key(tenant_id or get_tenant_id())


def _to_json_compatible(value: object) -> object:
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _to_json_compatible(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_json_compatible(item) for item in value]
    if isinstance(value, tuple):
        return [_to_json_compatible(item) for item in value]
    if isinstance(value, set):
        return [_to_json_compatible(item) for item in value]
    return value


def _set_bucket_cache(
    root: dict[str, object],
    *,
    bucket_name: str,
    bucket_cache: dict[str, object],
) -> None:
    root[bucket_name] = bucket_cache


def get_tenant_shared_cache_value(
    *,
    bucket: str,
    cache_key: str,
    ttl_seconds: int,
    tenant_id: str | None = None,
) -> tuple[object | None, bool]:
    bucket_name = str(bucket or "").strip()
    item_key = str(cache_key or "").strip()
    if not bucket_name or not item_key:
        return None, False

    tenant_cache_key = _tenant_key(tenant_id)
    ttl = max(int(ttl_seconds), 0)
    now = datetime.now(ZoneInfo(get_timezone()))
    cache_store = _get_cache_store()

    try:
        with cache_store.lock():
            root = cache_store.load_root()
            if not isinstance(root, dict):
                root = {}

            bucket_cache = root.get(bucket_name)
            if not isinstance(bucket_cache, dict):
                return None, False

            tenant_entries = bucket_cache.get(tenant_cache_key)
            if not isinstance(tenant_entries, dict):
                return None, False

            entry = tenant_entries.get(item_key)
            if not isinstance(entry, dict) or "value" not in entry:
                return None, False

            updated_at = _parse_cache_datetime(entry.get("updated_at"))
            if ttl > 0 and (
                updated_at is None or (now - updated_at).total_seconds() > ttl
            ):
                tenant_entries.pop(item_key, None)
                if tenant_entries:
                    bucket_cache[tenant_cache_key] = tenant_entries
                else:
                    bucket_cache.pop(tenant_cache_key, None)
                _set_bucket_cache(
                    root,
                    bucket_name=bucket_name,
                    bucket_cache=bucket_cache,
                )
                cache_store.write_root(root)
                return None, False

            return entry.get("value"), True
    except (OSError, ValueError, TypeError):
        return None, False


def set_tenant_shared_cache_value(
    *,
    bucket: str,
    cache_key: str,
    value: object,
    tenant_id: str | None = None,
) -> bool:
    bucket_name = str(bucket or "").strip()
    item_key = str(cache_key or "").strip()
    if not bucket_name or not item_key:
        return False

    tenant_cache_key = _tenant_key(tenant_id)
    cache_store = _get_cache_store()
    now_iso = datetime.now(ZoneInfo(get_timezone())).isoformat()

    try:
        with cache_store.lock():
            root = cache_store.load_root()
            if not isinstance(root, dict):
                root = {}

            bucket_cache = root.get(bucket_name)
            if not isinstance(bucket_cache, dict):
                bucket_cache = {}

            tenant_entries = bucket_cache.get(tenant_cache_key)
            if not isinstance(tenant_entries, dict):
                tenant_entries = {}

            tenant_entries[item_key] = {
                "updated_at": now_iso,
                "value": _to_json_compatible(value),
            }
            bucket_cache[tenant_cache_key] = tenant_entries
            _set_bucket_cache(
                root,
                bucket_name=bucket_name,
                bucket_cache=bucket_cache,
            )
            cache_store.write_root(root)
        return True
    except (OSError, ValueError, TypeError):
        return False


def delete_tenant_shared_cache_values_by_prefix(
    *,
    bucket: str,
    cache_key_prefix: str,
    tenant_id: str | None = None,
) -> int:
    bucket_name = str(bucket or "").strip()
    key_prefix = str(cache_key_prefix or "").strip()
    if not bucket_name or not key_prefix:
        return 0

    tenant_cache_key = _tenant_key(tenant_id)
    cache_store = _get_cache_store()

    try:
        with cache_store.lock():
            root = cache_store.load_root()
            if not isinstance(root, dict):
                return 0

            bucket_cache = root.get(bucket_name)
            if not isinstance(bucket_cache, dict):
                return 0

            tenant_entries = bucket_cache.get(tenant_cache_key)
            if not isinstance(tenant_entries, dict):
                return 0

            remove_keys = [
                cache_key
                for cache_key in tenant_entries.keys()
                if str(cache_key).startswith(key_prefix)
            ]
            if not remove_keys:
                return 0

            for cache_key in remove_keys:
                tenant_entries.pop(cache_key, None)

            if tenant_entries:
                bucket_cache[tenant_cache_key] = tenant_entries
            else:
                bucket_cache.pop(tenant_cache_key, None)

            _set_bucket_cache(
                root,
                bucket_name=bucket_name,
                bucket_cache=bucket_cache,
            )
            cache_store.write_root(root)
            return len(remove_keys)
    except (OSError, ValueError, TypeError):
        return 0
