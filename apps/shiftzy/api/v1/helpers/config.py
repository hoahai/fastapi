from __future__ import annotations

import ast
from datetime import date
import json
import re
import threading

from shared.tenant import (
    TenantConfigValidationError,
    get_env,
    get_tenant_id,
)

APP_NAME = "Shiftzy"

_DB_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$")
_REQUIRED_DB_TABLE_KEYS = {
    "POSITIONS",
    "EMPLOYEES",
    "SHIFTS",
    "SCHEDULES",
}

_VALIDATED_TENANTS: set[str] = set()
_VALIDATION_LOCK = threading.Lock()


# ============================================================
# ENUMS
# ============================================================


def _parse_list(key: str) -> list[str]:
    raw = get_env(key)
    if raw is None or str(raw).strip() == "":
        raise TenantConfigValidationError(app_name=APP_NAME, missing=[key])

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(raw)
        except (ValueError, SyntaxError) as exc:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[key],
            ) from exc

    if not isinstance(parsed, list):
        raise TenantConfigValidationError(app_name=APP_NAME, invalid=[key])

    return [str(item) for item in parsed]


def _parse_raw_value(raw: str, key: str, expected_type):
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(raw)
        except (ValueError, SyntaxError) as exc:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[key],
            ) from exc

    if not isinstance(parsed, expected_type):
        raise TenantConfigValidationError(app_name=APP_NAME, invalid=[key])
    return parsed


def _get_db_tables_raw() -> str | None:
    return get_env("DB_TABLES") or get_env("db_tables")


def _get_pdf_raw() -> str | None:
    return get_env("PDF") or get_env("pdf")


def get_position_areas() -> list[str]:
    return _parse_list("POSITION_AREAS_ENUM")


def get_schedule_sections() -> list[str]:
    return _parse_list("SCHEDULE_SECTIONS_ENUM")


def get_db_tables() -> dict[str, str]:
    raw = _get_db_tables_raw()
    if raw is None or str(raw).strip() == "":
        raise TenantConfigValidationError(app_name=APP_NAME, missing=["DB_TABLES"])

    parsed = _parse_raw_value(raw, "DB_TABLES", dict)
    tables: dict[str, str] = {}
    for key, value in parsed.items():
        name = str(value).strip()
        if not name:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[f"DB_TABLES.{key}"],
            )
        if not _DB_TABLE_RE.fullmatch(name):
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[f"DB_TABLES.{key}"],
            )
        tables[str(key).upper()] = name

    missing_keys = _REQUIRED_DB_TABLE_KEYS.difference(tables.keys())
    if missing_keys:
        missing = [f"DB_TABLES.{key}" for key in sorted(missing_keys)]
        raise TenantConfigValidationError(app_name=APP_NAME, missing=missing)

    return tables


# ============================================================
# VALIDATION
# ============================================================


def validate_tenant_config(tenant_id: str | None = None) -> None:
    tenant_id = tenant_id or get_tenant_id()
    if not tenant_id:
        raise TenantConfigValidationError(app_name=APP_NAME, missing=["tenant_id"])

    with _VALIDATION_LOCK:
        if tenant_id in _VALIDATED_TENANTS:
            return

        missing: list[str] = []
        invalid: list[str] = []

        def _check_required(key: str) -> str | None:
            raw = get_env(key)
            if raw is None or str(raw).strip() == "":
                missing.append(key)
                return None
            return str(raw).strip()

        def _check_int(key: str) -> int | None:
            raw = _check_required(key)
            if raw is None:
                return None
            try:
                return int(raw)
            except ValueError:
                invalid.append(key)
                return None

        def _check_start_date() -> None:
            raw = _check_required("START_DATE")
            if raw is None:
                return
            try:
                parsed = date.fromisoformat(raw)
            except ValueError:
                invalid.append("START_DATE")
                return
            if parsed.weekday() != 0:
                invalid.append("START_DATE")

        def _check_list(key: str) -> None:
            raw = _check_required(key)
            if raw is None:
                return
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                try:
                    parsed = ast.literal_eval(raw)
                except (ValueError, SyntaxError):
                    invalid.append(key)
                    return
            if not isinstance(parsed, list):
                invalid.append(key)

        def _check_db_tables() -> None:
            raw = _get_db_tables_raw()
            if raw is None or str(raw).strip() == "":
                missing.append("DB_TABLES")
                return
            try:
                parsed = _parse_raw_value(raw, "DB_TABLES", dict)
            except TenantConfigValidationError:
                invalid.append("DB_TABLES")
                return

            normalized: dict[str, str] = {}
            for key, value in parsed.items():
                name = str(value).strip()
                if not name or not _DB_TABLE_RE.fullmatch(name):
                    invalid.append(f"DB_TABLES.{key}")
                normalized[str(key).upper()] = name

            missing_keys = _REQUIRED_DB_TABLE_KEYS.difference(normalized.keys())
            for key in sorted(missing_keys):
                missing.append(f"DB_TABLES.{key}")

        def _check_pdf() -> None:
            raw = _get_pdf_raw()
            if raw is None or str(raw).strip() == "":
                return
            try:
                parsed = _parse_raw_value(raw, "PDF", dict)
            except TenantConfigValidationError:
                invalid.append("PDF")
                return
            if not isinstance(parsed, dict):
                invalid.append("PDF")

        _check_int("START_WEEK_NO")
        _check_start_date()

        before = _check_int("WEEK_BEFORE")
        after = _check_int("WEEK_AFTER")
        if before is not None and before < 0:
            invalid.append("WEEK_BEFORE")
        if after is not None and after < 0:
            invalid.append("WEEK_AFTER")

        _check_list("POSITION_AREAS_ENUM")
        _check_list("SCHEDULE_SECTIONS_ENUM")
        _check_db_tables()
        _check_pdf()

        if missing or invalid:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                missing=missing,
                invalid=invalid,
            )

        _VALIDATED_TENANTS.add(tenant_id)
