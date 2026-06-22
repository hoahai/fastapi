from __future__ import annotations

import ast
from datetime import date, datetime
import json
import re
import threading
from zoneinfo import ZoneInfo

from shared.tenant import (
    TenantConfigValidationError,
    get_app_scoped_env,
    get_env,
    get_tenant_id,
    get_timezone,
)

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

_EMPLOYEE_EMAIL_MAP_CONFIG_KEY = "EMPLOYEE_EMAIL_MAP"
_REMINDER_CC_CONFIG_KEY = "REMINDERCC"
_LAST_SUBMISSION_DATE_CONFIG_KEY = "LASTSUBMISSIONDATE"
_LAST_SUBMISSION_DATE_RE = re.compile(
    r"^(?P<month>0?[1-9]|1[0-2])-(?P<day>0?[1-9]|[12]\d|3[01])$"
)

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


def _normalize_email(value: object | None) -> str:
    return str(value or "").strip().lower()


def _normalize_last_submission_date(value: object | None) -> str | None:
    text = str(value or "").strip().replace("/", "-")
    if not text:
        return None

    match = _LAST_SUBMISSION_DATE_RE.fullmatch(text)
    if not match:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            invalid=[f"leavesphere.{_LAST_SUBMISSION_DATE_CONFIG_KEY}"],
        )

    month = int(match.group("month"))
    day = int(match.group("day"))
    try:
        date(2000, month, day)
    except ValueError as exc:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            invalid=[f"leavesphere.{_LAST_SUBMISSION_DATE_CONFIG_KEY}"],
        ) from exc

    return f"{month:02d}-{day:02d}"


def _has_leavesphere_config() -> bool:
    app_section = str(get_env("leavesphere") or "").strip()
    if app_section:
        return True
    for key in ("DB_TABLES", "CACHE", "ENABLED", _EMPLOYEE_EMAIL_MAP_CONFIG_KEY, _LAST_SUBMISSION_DATE_CONFIG_KEY):
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


def get_employee_email_map() -> dict[str, str]:
    raw = _get_scoped_env(_EMPLOYEE_EMAIL_MAP_CONFIG_KEY)
    if raw is None or str(raw).strip() == "":
        return {}

    parsed = _parse_raw_value(str(raw), "LEAVESPHERE_EMPLOYEE_EMAIL_MAP", dict)
    resolved: dict[str, str] = {}
    for raw_login_email, raw_employee_email in parsed.items():
        login_email = _normalize_email(raw_login_email)
        employee_email = _normalize_email(raw_employee_email)
        if not login_email or "@" not in login_email:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[f"leavesphere.{_EMPLOYEE_EMAIL_MAP_CONFIG_KEY}.{raw_login_email}"],
            )
        if not employee_email or "@" not in employee_email:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[f"leavesphere.{_EMPLOYEE_EMAIL_MAP_CONFIG_KEY}.{raw_login_email}"],
            )
        resolved[login_email] = employee_email
    return resolved


def _parse_email_list_value(value: object | None, *, config_key: str) -> list[str]:
    if value is None:
        return []

    items: list[object]
    if isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        text = str(value or "").strip()
        if not text:
            return []

        parsed: object = text
        if text.startswith("[") or text.startswith("(") or text.startswith("{"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                try:
                    parsed = ast.literal_eval(text)
                except (ValueError, SyntaxError):
                    parsed = text
        if isinstance(parsed, (list, tuple, set)):
            items = list(parsed)
        else:
            parts = [part.strip() for part in re.split(r"[;,]", str(parsed)) if part.strip()]
            items = parts if len(parts) > 1 else [parsed]

    normalized: list[str] = []
    seen: set[str] = set()
    for item in items:
        email = _normalize_email(item)
        if not email:
            continue
        if "@" not in email:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[f"leavesphere.{config_key}"],
            )
        if email in seen:
            continue
        seen.add(email)
        normalized.append(email)
    return normalized


def get_reminder_cc_emails() -> list[str]:
    raw = _get_scoped_env(_REMINDER_CC_CONFIG_KEY)
    return _parse_email_list_value(raw, config_key=_REMINDER_CC_CONFIG_KEY)


def get_last_submission_date() -> str | None:
    raw = _get_scoped_env(_LAST_SUBMISSION_DATE_CONFIG_KEY)
    if raw is None or str(raw).strip() == "":
        return None
    return _normalize_last_submission_date(raw)


def is_submission_cutoff_passed(*, year: int, today: date | None = None) -> bool:
    deadline = get_last_submission_date()
    if not deadline:
        return False

    current_date = today or datetime.now(ZoneInfo(get_timezone())).date()
    if current_date.year != year:
        return False

    cutoff_month, cutoff_day = (int(part) for part in deadline.split("-"))
    return (current_date.month, current_date.day) > (cutoff_month, cutoff_day)


def format_last_submission_date(deadline: str | None) -> str | None:
    normalized = str(deadline or "").strip()
    if not normalized:
        return None
    return normalized.replace("-", "/")


def resolve_employee_email(value: object | None) -> str:
    email = _normalize_email(value)
    if not email:
        return ""
    return get_employee_email_map().get(email, email)


def resolve_employee_email_candidates(*values: object | None) -> list[str]:
    candidates: list[str] = []
    email_map = get_employee_email_map()
    for value in values:
        email = _normalize_email(value)
        if not email:
            continue
        mapped_email = email_map.get(email)
        for candidate in (mapped_email, email):
            if candidate and candidate not in candidates:
                candidates.append(candidate)
    return candidates


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
                get_employee_email_map()
                get_reminder_cc_emails()
                get_last_submission_date()
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
