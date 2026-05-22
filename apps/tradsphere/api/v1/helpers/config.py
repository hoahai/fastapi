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
_DEFAULT_DB_READ_TTL_SECONDS = 300
_DB_READ_TTL_DEFAULT_OVERRIDES = {
    "db_inv_checklists_ttl_time": 60,
    "db_inv_checklist_row_ttl_time": 60,
    "db_inv_checklist_stations_ttl_time": 60,
    "db_inv_checklist_station_rows_ttl_time": 60,
    "db_inv_checklist_station_search_ttl_time": 60,
    # Station-matched invoice checklist notes are read-heavy in UI.
    "db_invoice_checklist_notes_ttl_time": 43200,
}

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
    "INVCHECKLISTS": "TradSphere_InvChecklist",
    "INVCHECKLISTSTATIONS": "TradSphere_InvChecklistStation",
    "INVCHECKLISTNOTES": "TradSphere_InvChecklistNote",
    "TRAFFIC": "TradSphere_Traffic",
    "TRAFFICFLIGHTS": "TradSphere_TrafficFlight",
    "TRAFFICSTATIONS": "TradSphere_TrafficStation",
    "TRAFFICEMAILS": "TradSphere_TrafficEmail",
    "APPATTACHMENTS": "AppAttachment",
    "INVNOTEATTACHMENTS": "TradSphere_InvNoteAttachment",
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
    "invchecklists": "INVCHECKLISTS",
    "inv_checklists": "INVCHECKLISTS",
    "invcheckliststations": "INVCHECKLISTSTATIONS",
    "inv_checklist_stations": "INVCHECKLISTSTATIONS",
    "invchecklistnotes": "INVCHECKLISTNOTES",
    "inv_checklist_notes": "INVCHECKLISTNOTES",
    "traffic": "TRAFFIC",
    "trafficflights": "TRAFFICFLIGHTS",
    "traffic_flights": "TRAFFICFLIGHTS",
    "trafficstations": "TRAFFICSTATIONS",
    "traffic_stations": "TRAFFICSTATIONS",
    "trafficemails": "TRAFFICEMAILS",
    "traffic_emails": "TRAFFICEMAILS",
    "appattachment": "APPATTACHMENTS",
    "appattachments": "APPATTACHMENTS",
    "app_attachment": "APPATTACHMENTS",
    "app_attachments": "APPATTACHMENTS",
    "invnoteattachments": "INVNOTEATTACHMENTS",
    "inv_note_attachments": "INVNOTEATTACHMENTS",
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


def get_db_read_cache_ttl_seconds(*, key: str | None = None) -> int:
    normalized_key = str(key or "").strip().lower()
    default_seconds = int(
        _DB_READ_TTL_DEFAULT_OVERRIDES.get(
            normalized_key,
            _DEFAULT_DB_READ_TTL_SECONDS,
        )
    )
    return get_shared_cache_ttl_seconds(
        key=key or "db_read_ttl_time",
        default_seconds=default_seconds,
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

            try:
                _ = get_db_read_cache_ttl_seconds()
            except Exception:
                invalid.append("tradsphere.CACHE.db_read_ttl_time")

        if missing or invalid:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                missing=sorted(set(missing)),
                invalid=sorted(set(invalid)),
            )

        _VALIDATED_TENANTS.add(tenant_id)
