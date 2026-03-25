import ast
import json
import re
import threading
from decimal import Decimal, InvalidOperation

from shared.tenant import (
    TenantConfigValidationError,
    get_app_scoped_env,
    get_env,
    get_tenant_id,
)

APP_NAME = "SpendSphere"

_DB_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$")
_REQUIRED_DB_TABLE_KEYS = {
    "BUDGETS",
    "SERVICES",
    "ALLOCATIONS",
    "ROLLBREAKDOWNS",
    "ACCELERATIONS",
}
_GOOGLE_ADS_NAMING_SECTIONS = ("account", "campaign")
_GOOGLE_ADS_ACCOUNT_OVERRIDE_SCOPES = ("byId", "byName")
_DEFAULT_GOOGLE_ADS_INACTIVE_PREFIXES = ("zzz.",)

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
    return (
        get_app_scoped_env(APP_NAME, "DB_TABLES")
        or get_env("DB_TABLES")
        or get_env("db_tables")
    )


def _get_spreadsheets_raw() -> str | None:
    return (
        get_app_scoped_env(APP_NAME, "SPREADSHEETS")
        or get_env("SPREADSHEETS")
        or get_env("spreadsheets")
    )


def _to_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    return None


def get_service_budgets() -> list[str]:
    value = _parse_env_value("SERVICE_BUDGETS", list)
    return [str(v) for v in value]


def get_service_mapping() -> dict:
    return _parse_env_value("SERVICE_MAPPING", dict)


def get_adtypes() -> dict:
    parsed = _parse_env_value("ADTYPES", dict)

    has_unknown = any(str(key).strip().upper() == "UNK" for key in parsed.keys())
    if has_unknown:
        return parsed

    with_unknown = dict(parsed)
    with_unknown["UNK"] = {
        "order": 9999,
        "adTypeQuery": "UNKNOWN",
        "fullName": "Unknown",
        "shortName": "Unknown",
    }
    return with_unknown


def _validate_google_ads_naming(parsed: dict, key: str = "GOOGLE_ADS_NAMING") -> dict:
    if not isinstance(parsed, dict):
        raise TenantConfigValidationError(app_name=APP_NAME, invalid=[key])

    missing: list[str] = []
    invalid: list[str] = []

    for section in _GOOGLE_ADS_NAMING_SECTIONS:
        section_key = f"{key}.{section}"
        section_config = parsed.get(section)
        if section_config is None:
            missing.append(section_key)
            continue
        if not isinstance(section_config, dict):
            invalid.append(section_key)
            continue

        has_format = (
            isinstance(section_config.get("format"), str)
            and bool(section_config.get("format", "").strip())
        )
        has_regex = (
            isinstance(section_config.get("regex"), str)
            and bool(section_config.get("regex", "").strip())
        )
        if not has_format and not has_regex:
            missing.append(f"{section_key}.format|regex")

        if "format" in section_config and not has_format:
            invalid.append(f"{section_key}.format")
        if "regex" in section_config and not has_regex:
            invalid.append(f"{section_key}.regex")

    token_patterns = parsed.get("tokenPatterns")
    if token_patterns is not None and not isinstance(token_patterns, dict):
        invalid.append(f"{key}.tokenPatterns")
    elif isinstance(token_patterns, dict):
        for token_name, token_pattern in token_patterns.items():
            token_key = f"{key}.tokenPatterns.{token_name}"
            if not isinstance(token_pattern, str) or not token_pattern.strip():
                invalid.append(token_key)

    inactive_prefixes = parsed.get("inactivePrefixes")
    if inactive_prefixes is not None and not isinstance(inactive_prefixes, list):
        invalid.append(f"{key}.inactivePrefixes")
    elif isinstance(inactive_prefixes, list):
        for idx, prefix in enumerate(inactive_prefixes):
            prefix_key = f"{key}.inactivePrefixes[{idx}]"
            if not isinstance(prefix, str) or not prefix.strip():
                invalid.append(prefix_key)

    account_overrides = parsed.get("accountOverrides")
    if account_overrides is not None and not isinstance(account_overrides, dict):
        invalid.append(f"{key}.accountOverrides")
    elif isinstance(account_overrides, dict):
        for scope in _GOOGLE_ADS_ACCOUNT_OVERRIDE_SCOPES:
            scoped_overrides = account_overrides.get(scope)
            if scoped_overrides is None:
                continue
            if not isinstance(scoped_overrides, dict):
                invalid.append(f"{key}.accountOverrides.{scope}")
                continue

            for matcher, override in scoped_overrides.items():
                matcher_value = str(matcher).strip()
                matcher_key = f"{key}.accountOverrides.{scope}.{matcher}"
                if not matcher_value:
                    invalid.append(matcher_key)
                    continue
                if not isinstance(override, dict):
                    invalid.append(matcher_key)
                    continue

                account_code = override.get("accountCode")
                if not isinstance(account_code, str) or not account_code.strip():
                    missing.append(f"{matcher_key}.accountCode")

                account_name = override.get("accountName")
                if account_name is not None and (
                    not isinstance(account_name, str) or not account_name.strip()
                ):
                    invalid.append(f"{matcher_key}.accountName")

    if missing or invalid:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            missing=missing,
            invalid=invalid,
        )

    return parsed


def get_google_ads_naming() -> dict:
    parsed = _parse_env_value("GOOGLE_ADS_NAMING", dict)
    return _validate_google_ads_naming(parsed)


def get_google_ads_inactive_prefixes(
    naming: dict | None = None,
) -> tuple[str, ...]:
    naming = naming if isinstance(naming, dict) else get_google_ads_naming()
    raw_prefixes = naming.get("inactivePrefixes")
    if not isinstance(raw_prefixes, list):
        return _DEFAULT_GOOGLE_ADS_INACTIVE_PREFIXES

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_prefix in raw_prefixes:
        if not isinstance(raw_prefix, str):
            continue
        prefix = raw_prefix.strip().lower()
        if not prefix or prefix in seen:
            continue
        seen.add(prefix)
        normalized.append(prefix)

    return tuple(normalized) if normalized else _DEFAULT_GOOGLE_ADS_INACTIVE_PREFIXES


def is_google_ads_inactive_name(
    name: str | None,
    *,
    inactive_prefixes: tuple[str, ...] | None = None,
) -> bool:
    if not name:
        return False
    normalized_name = str(name).strip().lower()
    prefixes = inactive_prefixes or get_google_ads_inactive_prefixes()
    return any(normalized_name.startswith(prefix) for prefix in prefixes)


def get_acceleration_scope_types() -> list[str]:
    raw = get_env("ACCELERATION_SCOPE_TYPES")
    if raw is None or str(raw).strip() == "":
        return ["ACCOUNT", "AD_TYPE", "BUDGET"]
    parsed = _parse_raw_value(raw, "ACCELERATION_SCOPE_TYPES", list)
    return [str(v).strip().upper() for v in parsed if str(v).strip()]


def get_budget_warning_threshold() -> Decimal | None:
    raw = get_env("BUDGET_WARNING_THRESHOLD")
    if raw is None or str(raw).strip() == "":
        return None

    try:
        threshold = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError) as exc:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            invalid=["BUDGET_WARNING_THRESHOLD"],
        ) from exc

    if threshold < 0:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            invalid=["BUDGET_WARNING_THRESHOLD"],
        )
    return threshold


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


def _is_valid_spreadsheet_id(value: object) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_-]+", str(value or "").strip()))


def _normalize_spreadsheet_entry(
    value: object,
    *,
    key_name: str,
    invalid: list[str],
) -> dict[str, object]:
    if value is None:
        return {}
    if isinstance(value, str):
        return {"id": value}
    if isinstance(value, dict):
        return {str(k).strip().lower(): v for k, v in value.items()}
    invalid.append(key_name)
    return {}


def _should_require_custom_spreadsheets() -> bool:
    raw = get_app_scoped_env(APP_NAME, "FEATURE_FLAGS") or get_env("FEATURE_FLAGS")
    if raw is None or str(raw).strip() == "":
        return False
    try:
        parsed = _parse_raw_value(str(raw), "FEATURE_FLAGS", dict)
    except TenantConfigValidationError:
        return False

    normalized = {str(k).strip().lower(): v for k, v in parsed.items()}
    return _to_bool(normalized.get("budget_managements")) is True


def _parse_spendsphere_sheets(
    parsed: dict,
    *,
    key_prefix: str,
    require_custom_sheets: bool,
) -> tuple[dict[str, dict[str, str]], list[str], list[str]]:
    missing: list[str] = []
    invalid: list[str] = []

    normalized = {str(k).strip().lower(): v for k, v in parsed.items()}

    spend_sphere_entry = _normalize_spreadsheet_entry(
        normalized.get("spendsphere"),
        key_name=f"{key_prefix}.spendSphere",
        invalid=invalid,
    )
    digital_ad_center_entry = _normalize_spreadsheet_entry(
        normalized.get("digitaladvertisingcenter"),
        key_name=f"{key_prefix}.digitalAdvertisingCenter",
        invalid=invalid,
    ) if "digitaladvertisingcenter" in normalized else {}
    budget_tool_entry = _normalize_spreadsheet_entry(
        normalized.get("budgettool"),
        key_name=f"{key_prefix}.budgetTool",
        invalid=invalid,
    ) if "budgettool" in normalized else {}

    if not spend_sphere_entry:
        missing.append(f"{key_prefix}.spendSphere")

    spend_sphere_id = str(spend_sphere_entry.get("id", "")).strip()
    spend_sphere_rollover_name = str(spend_sphere_entry.get("rolloversheetname", "")).strip()
    spend_sphere_active_name = str(
        spend_sphere_entry.get("activeperiodsheetname")
        or spend_sphere_entry.get("activepriodsheetname")
        or ""
    ).strip()

    if not spend_sphere_id:
        missing.append(f"{key_prefix}.spendSphere.id")
    elif not _is_valid_spreadsheet_id(spend_sphere_id):
        invalid.append(f"{key_prefix}.spendSphere.id")

    if not spend_sphere_rollover_name:
        missing.append(f"{key_prefix}.spendSphere.rollOverSheetName")
    if not spend_sphere_active_name:
        missing.append(f"{key_prefix}.spendSphere.activePriodSheetName")

    if require_custom_sheets and not digital_ad_center_entry:
        missing.append(f"{key_prefix}.digitalAdvertisingCenter")
    if require_custom_sheets and not budget_tool_entry:
        missing.append(f"{key_prefix}.budgetTool")

    digital_ad_center_id = str(digital_ad_center_entry.get("id", "")).strip()
    if digital_ad_center_entry and not digital_ad_center_id:
        missing.append(f"{key_prefix}.digitalAdvertisingCenter.id")
    elif digital_ad_center_id and not _is_valid_spreadsheet_id(digital_ad_center_id):
        invalid.append(f"{key_prefix}.digitalAdvertisingCenter.id")

    budget_tool_id = str(budget_tool_entry.get("id", "")).strip()
    budget_tool_master_budget_sheet_name = str(
        budget_tool_entry.get("masterbudgetsheetname", "")
    ).strip()
    if budget_tool_entry and not budget_tool_id:
        missing.append(f"{key_prefix}.budgetTool.id")
    elif budget_tool_id and not _is_valid_spreadsheet_id(budget_tool_id):
        invalid.append(f"{key_prefix}.budgetTool.id")
    if budget_tool_entry and not budget_tool_master_budget_sheet_name:
        missing.append(f"{key_prefix}.budgetTool.masterBudgetSheetName")

    sheets: dict[str, dict[str, str]] = {}
    if spend_sphere_id and spend_sphere_rollover_name and spend_sphere_active_name:
        sheets["rollovers"] = {
            "spreadsheet_id": spend_sphere_id,
            "range_name": spend_sphere_rollover_name,
        }
        sheets["active_period"] = {
            "spreadsheet_id": spend_sphere_id,
            "range_name": spend_sphere_active_name,
        }

    if digital_ad_center_id:
        sheets["recommended_budget"] = {
            "spreadsheet_id": digital_ad_center_id,
        }

    if budget_tool_id and budget_tool_master_budget_sheet_name:
        sheets["budget_tool"] = {
            "spreadsheet_id": budget_tool_id,
            "range_name": budget_tool_master_budget_sheet_name,
        }

    return sheets, missing, invalid


def get_spendsphere_sheets() -> dict[str, dict[str, str]]:
    raw = _get_spreadsheets_raw()
    if raw is None or str(raw).strip() == "":
        raise TenantConfigValidationError(app_name=APP_NAME, missing=["SPREADSHEETS"])

    parsed = _parse_raw_value(raw, "SPREADSHEETS", dict)
    sheets, missing, invalid = _parse_spendsphere_sheets(
        parsed,
        key_prefix="SPREADSHEETS",
        require_custom_sheets=_should_require_custom_spreadsheets(),
    )
    if missing or invalid:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            missing=missing,
            invalid=invalid,
        )

    return sheets


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

        def _check_spreadsheets(require_custom_sheets: bool) -> None:
            raw = _get_spreadsheets_raw()
            if raw is None or str(raw).strip() == "":
                missing.append("SPREADSHEETS")
                return
            try:
                parsed = _parse_raw_value(raw, "SPREADSHEETS", dict)
            except TenantConfigValidationError:
                invalid.append("SPREADSHEETS")
                return

            _, sheets_missing, sheets_invalid = _parse_spendsphere_sheets(
                parsed,
                key_prefix="SPREADSHEETS",
                require_custom_sheets=require_custom_sheets,
            )
            missing.extend(sheets_missing)
            invalid.extend(sheets_invalid)

        def _check_google_ads_naming() -> None:
            raw = _check_required("GOOGLE_ADS_NAMING")
            if raw is None:
                return
            try:
                parsed = _parse_raw_value(raw, "GOOGLE_ADS_NAMING", dict)
                _validate_google_ads_naming(parsed)
            except TenantConfigValidationError as exc:
                missing.extend(exc.missing)
                invalid.extend(exc.invalid)

        def _check_budget_warning_threshold() -> None:
            raw = get_env("BUDGET_WARNING_THRESHOLD")
            if raw is None or str(raw).strip() == "":
                return
            try:
                threshold = Decimal(str(raw).strip())
            except (InvalidOperation, ValueError):
                invalid.append("BUDGET_WARNING_THRESHOLD")
                return
            if threshold < 0:
                invalid.append("BUDGET_WARNING_THRESHOLD")

        require_custom_sheets = _should_require_custom_spreadsheets()

        _check_json("SERVICE_BUDGETS", list)
        _check_json("SERVICE_MAPPING", dict)
        _check_json("ADTYPES", dict)
        _check_db_tables()
        _check_spreadsheets(require_custom_sheets=require_custom_sheets)
        _check_google_ads_naming()
        _check_budget_warning_threshold()

        if missing or invalid:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                missing=missing,
                invalid=invalid,
            )

        _VALIDATED_TENANTS.add(tenant_id)
