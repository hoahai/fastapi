import ast
import json
import re
import threading

from shared.tenant import (
    TenantConfigValidationError,
    get_env,
    get_tenant_id,
)

APP_NAME = "SpendSphere"

_DB_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$")
_REQUIRED_DB_TABLE_KEYS = {
    "ACCOUNTS",
    "BUDGETS",
    "SERVICES",
    "ALLOCATIONS",
    "ROLLBREAKDOWNS",
}


def _require_env_value(key: str) -> str:
    raw = get_env(key)
    if raw is None or str(raw).strip() == "":
        raise TenantConfigValidationError(app_name=APP_NAME, missing=[key])
    return str(raw).strip()


def _parse_env_value(key: str, expected_type):
    raw = _require_env_value(key)

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
    return get_env("DB_TABLES") or get_env("db_tables")


def _get_spreadsheet_raw() -> str | None:
    return get_env("SPREADSHEET") or get_env("spreadsheet")


def get_service_budgets() -> list[str]:
    value = _parse_env_value("SERVICE_BUDGETS", list)
    return [str(v) for v in value]


def get_service_mapping() -> dict:
    return _parse_env_value("SERVICE_MAPPING", dict)


def get_adtypes() -> dict:
    return _parse_env_value("ADTYPES", dict)


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


def get_spendsphere_sheets() -> dict[str, dict[str, str]]:
    raw = _get_spreadsheet_raw()
    if raw is None or str(raw).strip() == "":
        raise TenantConfigValidationError(app_name=APP_NAME, missing=["SPREADSHEET"])

    parsed = _parse_raw_value(raw, "SPREADSHEET", dict)
    normalized = {str(k).strip().lower(): v for k, v in parsed.items()}

    spreadsheet_id = normalized.get("id")
    rollovers_sheet_name = normalized.get("rolloversheetname")
    active_sheet_name = (
        normalized.get("activeperiodsheetname")
        or normalized.get("activepriodsheetname")
    )

    missing: list[str] = []
    if not spreadsheet_id:
        missing.append("SPREADSHEET.id")
    if not rollovers_sheet_name:
        missing.append("SPREADSHEET.rollOverSheetName")
    if not active_sheet_name:
        missing.append("SPREADSHEET.activePriodSheetName")
    if missing:
        raise TenantConfigValidationError(app_name=APP_NAME, missing=missing)

    return {
        "rollovers": {
            "spreadsheet_id": spreadsheet_id,
            "range_name": rollovers_sheet_name,
        },
        "active_period": {
            "spreadsheet_id": spreadsheet_id,
            "range_name": active_sheet_name,
        },
    }


_VALIDATED_TENANTS: set[str] = set()
_VALIDATION_LOCK = threading.Lock()


def validate_tenant_config(tenant_id: str | None = None) -> None:
    """
    Ensure all required tenant config keys exist and are valid for v1.
    Cached per tenant to avoid re-validating on every request.
    """
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

        def _check_json(key: str, expected_type) -> None:
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
            if not isinstance(parsed, expected_type):
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

        def _check_spreadsheet() -> None:
            raw = _get_spreadsheet_raw()
            if raw is None or str(raw).strip() == "":
                missing.append("SPREADSHEET")
                return
            try:
                parsed = _parse_raw_value(raw, "SPREADSHEET", dict)
            except TenantConfigValidationError:
                invalid.append("SPREADSHEET")
                return

            normalized = {str(k).strip().lower(): v for k, v in parsed.items()}
            if not normalized.get("id"):
                missing.append("SPREADSHEET.id")
            if not normalized.get("rolloversheetname"):
                missing.append("SPREADSHEET.rollOverSheetName")
            if not (
                normalized.get("activeperiodsheetname")
                or normalized.get("activepriodsheetname")
            ):
                missing.append("SPREADSHEET.activePriodSheetName")

        _check_json("SERVICE_BUDGETS", list)
        _check_json("SERVICE_MAPPING", dict)
        _check_json("ADTYPES", dict)
        _check_db_tables()
        _check_spreadsheet()

        if missing or invalid:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                missing=missing,
                invalid=invalid,
            )

        _VALIDATED_TENANTS.add(tenant_id)
