from __future__ import annotations

import ast
import json
import re

from shared.tenant import (
    TenantConfigValidationError,
    get_app_scoped_env,
    get_env,
)


APP_NAME = "FundSphere"

_DB_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$")
_REQUIRED_TABLE_KEYS = {
    "ACCOUNTS",
    "DEPARTMENTS",
    "SERVICES",
    "ACCOUNTREPS",
    "BUDGETS",
    "BUDGETCHANGEHISTORIES",
}
_OPTIONAL_TABLE_KEYS = {"EMPLOYEES"}

_TABLE_KEY_ALIASES = {
    "ACCOUNTS": ("accounts", "account"),
    "DEPARTMENTS": ("departments", "department"),
    "SERVICES": ("services", "service"),
    "ACCOUNTREPS": ("accountreps", "account_reps"),
    "BUDGETS": ("budgets", "budget"),
    "BUDGETCHANGEHISTORIES": (
        "budgetchangehistories",
        "budget_change_histories",
        "changehistories",
        "change_histories",
    ),
    "EMPLOYEES": ("employees", "employee"),
}


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


def _get_db_tables_raw() -> str | None:
    return (
        get_app_scoped_env(APP_NAME, "DB_TABLES")
        or get_env("DB_TABLES")
        or get_env("db_tables")
    )


def _resolve_table_name(
    normalized: dict[str, object],
    *,
    canonical_key: str,
    missing: list[str],
    invalid: list[str],
    required: bool,
) -> str:
    aliases = (canonical_key.lower(),) + _TABLE_KEY_ALIASES.get(canonical_key, ())
    raw_value = ""
    for alias in aliases:
        candidate = normalized.get(alias)
        if candidate is None:
            continue
        raw_value = str(candidate or "").strip()
        if raw_value:
            break

    if not raw_value:
        if required:
            missing.append(f"DB_TABLES.{canonical_key}")
        return ""

    if not _DB_TABLE_RE.fullmatch(raw_value):
        invalid.append(f"DB_TABLES.{canonical_key}")
        return ""

    return raw_value


def get_direct_db_tables(*, require_employees: bool = False) -> dict[str, str]:
    """
    Resolve FundSphere DB table names for the direct CRUD surface.

    The direct APIs look for explicit lower-case keys first, but this helper also
    accepts legacy upper-case variants so existing tenants can opt in safely.
    """
    raw = _get_db_tables_raw()
    if raw is None or str(raw).strip() == "":
        raise TenantConfigValidationError(app_name=APP_NAME, missing=["DB_TABLES"])

    parsed = _parse_raw_value(str(raw), "DB_TABLES", dict)
    normalized = {str(key).strip().lower(): value for key, value in parsed.items()}

    missing: list[str] = []
    invalid: list[str] = []
    resolved: dict[str, str] = {}

    for key in sorted(_REQUIRED_TABLE_KEYS):
        resolved_value = _resolve_table_name(
            normalized,
            canonical_key=key,
            missing=missing,
            invalid=invalid,
            required=True,
        )
        if resolved_value:
            resolved[key] = resolved_value

    for key in sorted(_OPTIONAL_TABLE_KEYS):
        resolved_value = _resolve_table_name(
            normalized,
            canonical_key=key,
            missing=missing,
            invalid=invalid,
            required=key == "EMPLOYEES" and require_employees,
        )
        if resolved_value:
            resolved[key] = resolved_value

    if missing or invalid:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            missing=missing,
            invalid=invalid,
        )

    return resolved
