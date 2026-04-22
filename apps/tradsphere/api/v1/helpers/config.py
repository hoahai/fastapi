from __future__ import annotations

import ast
import json
import re
import threading

from shared.tenant import (
    TenantConfigValidationError,
    get_app_scoped_env,
    get_env,
    get_tenant_id,
)
from shared.tenantDataCache import get_shared_cache_ttl_seconds


APP_NAME = "TradSphere"
APP_ENV_PREFIX = "TRADSPHERE"
_DB_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$")
_DEFAULT_DB_VALIDATION_TTL_SECONDS = 300

_DEFAULT_DB_TABLES = {
    "ACCOUNTS": "TradSphere_Accounts",
    "MASTERACCOUNTS": "Accounts",
    "ESTNUMS": "TradSphere_EstNums",
    "DELIVERYMETHODS": "TradSphere_DeliveryMethods",
    "STATIONS": "TradSphere_Stations",
    "SCHEDULES": "TradSphere_Schedules",
    "SCHEDULESWEEKS": "TradSphere_ScheduleWeeks",
    "CONTACTS": "TradSphere_Contacts",
    "STATIONSCONTACTS": "TradSphere_StationsContacts",
}

_DB_KEY_ALIASES = {
    "accounts": "ACCOUNTS",
    "masteraccounts": "MASTERACCOUNTS",
    "master_accounts": "MASTERACCOUNTS",
    "estnums": "ESTNUMS",
    "est_nums": "ESTNUMS",
    "deliverymethods": "DELIVERYMETHODS",
    "delivery_methods": "DELIVERYMETHODS",
    "stations": "STATIONS",
    "schedules": "SCHEDULES",
    "schedulesweeks": "SCHEDULESWEEKS",
    "schedules_weeks": "SCHEDULESWEEKS",
    "contacts": "CONTACTS",
    "stationscontacts": "STATIONSCONTACTS",
    "stations_contacts": "STATIONSCONTACTS",
}

_DEFAULT_MEDIA_TYPES = ["TV", "RA", "CA", "OD", "NP", "CINE", "OTT"]
_DEFAULT_CONTACT_TYPES = ["REP", "TRAFFIC", "BILLING"]
_ENUM_KEYS = {
    "mediatype": "MEDIA_TYPE",
    "media_type": "MEDIA_TYPE",
    "contacttype": "CONTACT_TYPE",
    "contact_type": "CONTACT_TYPE",
}

_VALIDATED_TENANTS: set[str] = set()
_VALIDATION_LOCK = threading.Lock()


def _parse_raw_value(raw: str, key: str, expected_type):
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(raw)
        except (ValueError, SyntaxError) as exc:
            raise TenantConfigValidationError(app_name=APP_NAME, invalid=[key]) from exc

    if not isinstance(parsed, expected_type):
        raise TenantConfigValidationError(app_name=APP_NAME, invalid=[key])
    return parsed


def _get_scoped_env(key: str) -> str | None:
    return (
        get_app_scoped_env(APP_NAME, key)
        or get_env(f"{APP_ENV_PREFIX}_{str(key).strip().upper()}")
    )


def _has_tradsphere_config() -> bool:
    for key in ("DB_TABLES", "ENUMS", "CACHE"):
        raw = _get_scoped_env(key)
        if raw is not None and str(raw).strip() != "":
            return True
    return False


def get_db_tables() -> dict[str, str]:
    raw = _get_scoped_env("DB_TABLES")
    if raw is None or str(raw).strip() == "":
        return dict(_DEFAULT_DB_TABLES)

    parsed = _parse_raw_value(str(raw), "TRADSPHERE_DB_TABLES", dict)
    resolved = dict(_DEFAULT_DB_TABLES)

    for key, value in parsed.items():
        normalized_key = _DB_KEY_ALIASES.get(str(key).strip().lower())
        if not normalized_key:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[f"tradsphere.DB_TABLES.{key}"],
            )
        table_name = str(value or "").strip()
        if not table_name or not _DB_TABLE_RE.fullmatch(table_name):
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[f"tradsphere.DB_TABLES.{key}"],
            )
        resolved[normalized_key] = table_name

    return resolved


def _normalize_enum_list(
    values: object,
    *,
    field: str,
) -> list[str]:
    if not isinstance(values, list):
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            invalid=[f"tradsphere.ENUMS.{field}"],
        )

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = str(raw or "").strip().upper()
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        normalized.append(text)

    if not normalized:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            invalid=[f"tradsphere.ENUMS.{field}"],
        )
    return normalized


def get_enums() -> dict[str, list[str]]:
    raw = _get_scoped_env("ENUMS")
    if raw is None or str(raw).strip() == "":
        return {
            "MEDIA_TYPE": list(_DEFAULT_MEDIA_TYPES),
            "CONTACT_TYPE": list(_DEFAULT_CONTACT_TYPES),
        }

    parsed = _parse_raw_value(str(raw), "TRADSPHERE_ENUMS", dict)
    resolved = {
        "MEDIA_TYPE": list(_DEFAULT_MEDIA_TYPES),
        "CONTACT_TYPE": list(_DEFAULT_CONTACT_TYPES),
    }

    for key, value in parsed.items():
        normalized_key = _ENUM_KEYS.get(str(key).strip().lower())
        if not normalized_key:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[f"tradsphere.ENUMS.{key}"],
            )
        if normalized_key == "MEDIA_TYPE":
            resolved["MEDIA_TYPE"] = _normalize_enum_list(value, field="mediaType")
        elif normalized_key == "CONTACT_TYPE":
            resolved["CONTACT_TYPE"] = _normalize_enum_list(value, field="contactType")

    return resolved


def get_media_types() -> list[str]:
    return list(get_enums()["MEDIA_TYPE"])


def get_contact_types() -> list[str]:
    return list(get_enums()["CONTACT_TYPE"])


def get_default_contact_type() -> str:
    values = get_contact_types()
    if "REP" in values:
        return "REP"
    return values[0]


def get_validation_cache_ttl_seconds() -> int:
    return get_shared_cache_ttl_seconds(
        key="db_validation_ttl_time",
        default_seconds=_DEFAULT_DB_VALIDATION_TTL_SECONDS,
        app_name=APP_NAME,
    )


def validate_tenant_config(tenant_id: str | None = None) -> None:
    tenant_id = tenant_id or get_tenant_id()
    if not tenant_id:
        raise TenantConfigValidationError(app_name=APP_NAME, missing=["tenant_id"])

    with _VALIDATION_LOCK:
        if tenant_id in _VALIDATED_TENANTS:
            return

        missing: list[str] = []
        invalid: list[str] = []

        if not _has_tradsphere_config():
            missing.append("tradsphere")
        else:
            try:
                get_db_tables()
            except TenantConfigValidationError as exc:
                missing.extend(exc.missing)
                invalid.extend(exc.invalid)

            try:
                get_enums()
            except TenantConfigValidationError as exc:
                missing.extend(exc.missing)
                invalid.extend(exc.invalid)

            try:
                _ = get_validation_cache_ttl_seconds()
            except Exception:
                invalid.append("tradsphere.CACHE.db_validation_ttl_time")

        if missing or invalid:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                missing=sorted(set(missing)),
                invalid=sorted(set(invalid)),
            )

        _VALIDATED_TENANTS.add(tenant_id)
