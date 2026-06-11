from __future__ import annotations

import ast
import json
import re
import threading

from shared.tenant import TenantConfigValidationError, get_app_scoped_env, get_env, get_tenant_id

APP_NAME = "LeaveSphere"
APP_ENV_PREFIX = "LEAVESPHERE"

_DB_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$")

_DEFAULT_DB_TABLES = {
    "EMPLOYEES": "Employees",
    "EMPLOYEEMANAGERS": "EmployeeManagers",
    "PTOTYPES": "PTOTypes",
    "PTOACTIONS": "PTOActions",
    "PTOTRANSACTIONS": "PTOTransactions",
    "HOLIDAYS": "Holidays",
}

_DB_KEY_ALIASES = {
    "employees": "EMPLOYEES",
    "employeemanagers": "EMPLOYEEMANAGERS",
    "employee_managers": "EMPLOYEEMANAGERS",
    "ptotypes": "PTOTYPES",
    "pto_types": "PTOTYPES",
    "ptoactions": "PTOACTIONS",
    "pto_actions": "PTOACTIONS",
    "ptotransactions": "PTOTRANSACTIONS",
    "pto_transactions": "PTOTRANSACTIONS",
    "holidays": "HOLIDAYS",
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


def _has_leavesphere_config() -> bool:
    app_section = str(get_env("leavesphere") or "").strip()
    if app_section:
        return True
    for key in ("DB_TABLES", "CACHE", "ENABLED"):
        raw = _get_scoped_env(key)
        if raw is not None and str(raw).strip() != "":
            return True
    return False


def get_db_tables() -> dict[str, str]:
    raw = _get_scoped_env("DB_TABLES")
    if raw is None or str(raw).strip() == "":
        return dict(_DEFAULT_DB_TABLES)

    parsed = _parse_raw_value(str(raw), "LEAVESPHERE_DB_TABLES", dict)
    resolved = dict(_DEFAULT_DB_TABLES)
    for key, value in parsed.items():
        normalized_key = _DB_KEY_ALIASES.get(str(key).strip().lower())
        if not normalized_key:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[f"leavesphere.DB_TABLES.{key}"],
            )
        table_name = str(value or "").strip()
        if not table_name or not _DB_TABLE_RE.fullmatch(table_name):
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[f"leavesphere.DB_TABLES.{key}"],
            )
        resolved[normalized_key] = table_name
    return resolved


def validate_tenant_config(tenant_id: str | None = None) -> None:
    tenant_id = tenant_id or get_tenant_id()
    if not tenant_id:
        raise TenantConfigValidationError(app_name=APP_NAME, missing=["tenant_id"])

    with _VALIDATION_LOCK:
        if tenant_id in _VALIDATED_TENANTS:
            return

        missing: list[str] = []
        invalid: list[str] = []

        if not _has_leavesphere_config():
            missing.append("leavesphere")
        else:
            try:
                get_db_tables()
            except TenantConfigValidationError as exc:
                missing.extend(exc.missing)
                invalid.extend(exc.invalid)

        if missing or invalid:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                missing=sorted(set(missing)),
                invalid=sorted(set(invalid)),
            )

        _VALIDATED_TENANTS.add(tenant_id)
