from __future__ import annotations

from fastapi import HTTPException

from shared.tenantDataCache import (
    delete_tenant_shared_cache_values_by_prefix,
    get_tenant_shared_cache_value,
    set_tenant_shared_cache_value,
)

from apps.tradsphere.api.v1.helpers.config import get_validation_cache_ttl_seconds
from apps.tradsphere.api.v1.helpers.dbQueries import (
    get_all_contact_ids,
    get_all_delivery_method_ids,
    get_all_master_account_codes,
    get_all_station_codes,
    get_all_stations_contacts_ids,
    get_all_tradsphere_account_codes,
)


_VALIDATION_CACHE_BUCKET = "db_reads"
_VALIDATION_CACHE_PREFIX = "tradsphere_validation::"
_ACCOUNT_CODES_CACHE_KEY = f"{_VALIDATION_CACHE_PREFIX}account_codes"
_STATION_CODES_CACHE_KEY = f"{_VALIDATION_CACHE_PREFIX}station_codes"
_DELIVERY_METHOD_IDS_CACHE_KEY = f"{_VALIDATION_CACHE_PREFIX}delivery_method_ids"
_CONTACT_IDS_CACHE_KEY = f"{_VALIDATION_CACHE_PREFIX}contact_ids"
_STATIONS_CONTACT_IDS_CACHE_KEY = f"{_VALIDATION_CACHE_PREFIX}stations_contacts_ids"


def _cache_ttl_seconds() -> int:
    return max(int(get_validation_cache_ttl_seconds()), 0)


def _get_cached_values(
    *,
    cache_key: str,
    fetcher,
) -> list[object]:
    cached_value, cache_hit = get_tenant_shared_cache_value(
        bucket=_VALIDATION_CACHE_BUCKET,
        cache_key=cache_key,
        ttl_seconds=_cache_ttl_seconds(),
    )
    if cache_hit and isinstance(cached_value, list):
        return cached_value

    fresh_value = fetcher()
    set_tenant_shared_cache_value(
        bucket=_VALIDATION_CACHE_BUCKET,
        cache_key=cache_key,
        value=fresh_value,
    )
    return list(fresh_value or [])


def invalidate_validation_cache() -> int:
    return delete_tenant_shared_cache_values_by_prefix(
        bucket=_VALIDATION_CACHE_BUCKET,
        cache_key_prefix=_VALIDATION_CACHE_PREFIX,
    )


def require_account_code(value: object, *, field: str = "accountCode") -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise ValueError(f"{field} is required")
    return normalized


def normalize_account_codes(values: list[object] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        code = str(value or "").strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized.append(code)
    return normalized


def _get_valid_account_codes() -> set[str]:
    tradsphere_codes = _get_cached_values(
        cache_key=_ACCOUNT_CODES_CACHE_KEY,
        fetcher=lambda: sorted(
            set(get_all_tradsphere_account_codes()).union(get_all_master_account_codes())
        ),
    )
    normalized = {str(item or "").strip().upper() for item in tradsphere_codes}
    normalized.discard("")
    return normalized


def ensure_account_codes_exist(account_codes: list[object]) -> list[str]:
    normalized = normalize_account_codes(account_codes)
    if not normalized:
        return normalized
    valid_codes = _get_valid_account_codes()
    missing = sorted([code for code in normalized if code not in valid_codes])
    if missing:
        raise ValueError(f"Unknown accountCode values: {', '.join(missing)}")
    return normalized


def _normalize_int_values(values: list[object] | None, *, field: str) -> list[int]:
    normalized: list[int] = []
    seen: set[int] = set()
    for value in values or []:
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be integer values") from exc
        if parsed in seen:
            continue
        seen.add(parsed)
        normalized.append(parsed)
    return normalized


def ensure_station_codes_exist(station_codes: list[object]) -> list[str]:
    normalized = normalize_account_codes(station_codes)
    if not normalized:
        return normalized
    valid_codes = {
        str(item or "").strip().upper()
        for item in _get_cached_values(
            cache_key=_STATION_CODES_CACHE_KEY,
            fetcher=get_all_station_codes,
        )
    }
    valid_codes.discard("")
    missing = sorted([code for code in normalized if code not in valid_codes])
    if missing:
        raise ValueError(f"Unknown stationCode values: {', '.join(missing)}")
    return normalized


def ensure_delivery_method_ids_exist(ids: list[object]) -> list[int]:
    normalized = _normalize_int_values(ids, field="deliveryMethodId")
    if not normalized:
        return normalized
    valid_ids = {
        int(item)
        for item in _get_cached_values(
            cache_key=_DELIVERY_METHOD_IDS_CACHE_KEY,
            fetcher=get_all_delivery_method_ids,
        )
    }
    missing = sorted([item for item in normalized if item not in valid_ids])
    if missing:
        raise ValueError(f"Unknown deliveryMethodId values: {', '.join(map(str, missing))}")
    return normalized


def ensure_contact_ids_exist(ids: list[object]) -> list[int]:
    normalized = _normalize_int_values(ids, field="contactId")
    if not normalized:
        return normalized
    valid_ids = {
        int(item)
        for item in _get_cached_values(
            cache_key=_CONTACT_IDS_CACHE_KEY,
            fetcher=get_all_contact_ids,
        )
    }
    missing = sorted([item for item in normalized if item not in valid_ids])
    if missing:
        raise ValueError(f"Unknown contactId values: {', '.join(map(str, missing))}")
    return normalized


def ensure_stations_contact_ids_exist(ids: list[object]) -> list[int]:
    normalized = _normalize_int_values(ids, field="id")
    if not normalized:
        return normalized
    valid_ids = {
        int(item)
        for item in _get_cached_values(
            cache_key=_STATIONS_CONTACT_IDS_CACHE_KEY,
            fetcher=get_all_stations_contacts_ids,
        )
    }
    missing = sorted([item for item in normalized if item not in valid_ids])
    if missing:
        raise ValueError(
            f"Unknown stationsContacts id values: {', '.join(map(str, missing))}"
        )
    return normalized


def as_bad_request(exc: Exception) -> HTTPException:
    return HTTPException(status_code=400, detail=str(exc))
