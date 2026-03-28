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


# ============================================================
# CONSTANTS
# ============================================================

APP_NAME = "FundSphere"

_DB_TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$")
_REQUIRED_DB_TABLE_KEYS = {
    "ACCOUNTS",
    "DEPARTMENTS",
    "BUDGETS",
    "CHANGEHISTORIES",
}

_SPREADSHEET_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_SETTINGS_RANGE_RE = re.compile(r"^[A-Za-z]+[0-9]+:[A-Za-z]+(?:[0-9]+)?$")
_CELL_RANGE_RE = re.compile(r"^[A-Za-z]+[0-9]+$")
_COLUMN_LABEL_RE = re.compile(r"^[A-Za-z]+$")
_REQUIRED_BUDGET_DATA_UPDATE_COLUMNS = (
    "originalSig",
    "budgetId",
    "year",
    "month",
    "subService",
    "grossAmount",
    "commission",
    "netAdjustment",
    "note",
    "changeNote",
    "accountCode",
    "serviceId",
)
_GOOGLE_ACCOUNTS_CONFIG_KEY_PREFIX = "fundsphere.google_accounts"

_VALIDATED_TENANTS: set[str] = set()
_VALIDATION_LOCK = threading.Lock()


# ============================================================
# PARSERS
# ============================================================


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


def _get_spreadsheets_raw() -> str | None:
    return (
        get_app_scoped_env(APP_NAME, "SPREADSHEETS")
        or get_env("SPREADSHEETS")
        or get_env("spreadsheets")
    )


def _get_db_tables_raw() -> str | None:
    return (
        get_app_scoped_env(APP_NAME, "DB_TABLES")
        or get_env("DB_TABLES")
        or get_env("db_tables")
    )


def _get_google_accounts_raw() -> str | None:
    return get_app_scoped_env(APP_NAME, "GOOGLE_ACCOUNTS")


def _normalize_entry(
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


def _validate_google_accounts_config(
    parsed: dict,
    *,
    key_prefix: str,
) -> tuple[list[str], list[str]]:
    missing: list[str] = []
    invalid: list[str] = []

    normalized = {str(k).strip().lower(): v for k, v in parsed.items()}
    json_key_file_path = normalized.get("json_key_file_path")
    google_app_creds = normalized.get("google_application_credentials")
    has_json_key_file_path = (
        isinstance(json_key_file_path, str) and bool(json_key_file_path.strip())
    )
    has_google_app_creds = (
        isinstance(google_app_creds, str) and bool(google_app_creds.strip())
    )

    if not has_json_key_file_path and not has_google_app_creds:
        missing.append(
            f"{key_prefix}.json_key_file_path|GOOGLE_APPLICATION_CREDENTIALS"
        )
    if json_key_file_path is not None and not has_json_key_file_path:
        invalid.append(f"{key_prefix}.json_key_file_path")
    if google_app_creds is not None and not has_google_app_creds:
        invalid.append(f"{key_prefix}.GOOGLE_APPLICATION_CREDENTIALS")

    return missing, invalid


def _is_valid_spreadsheet_id(value: str) -> bool:
    return bool(_SPREADSHEET_ID_RE.fullmatch(value))


def _parse_budget_data_update_columns(
    value: object,
    *,
    key_prefix: str,
    missing: list[str],
    invalid: list[str],
) -> dict[str, str]:
    if not isinstance(value, dict):
        missing.append(f"{key_prefix}.budgetDataUpdateColumns")
        return {}

    normalized = {str(k).strip().lower(): v for k, v in value.items()}
    parsed: dict[str, str] = {}

    for key in _REQUIRED_BUDGET_DATA_UPDATE_COLUMNS:
        raw = normalized.get(key.lower())
        column_label = str(raw or "").strip().upper()
        if not column_label:
            missing.append(f"{key_prefix}.budgetDataUpdateColumns.{key}")
            continue
        if not _COLUMN_LABEL_RE.fullmatch(column_label):
            invalid.append(f"{key_prefix}.budgetDataUpdateColumns.{key}")
            continue
        parsed[key] = column_label

    optional_is_row_changed = str(normalized.get("isrowchanged") or "").strip().upper()
    if optional_is_row_changed:
        if not _COLUMN_LABEL_RE.fullmatch(optional_is_row_changed):
            invalid.append(f"{key_prefix}.budgetDataUpdateColumns.isRowChanged")
        else:
            parsed["isRowChanged"] = optional_is_row_changed

    optional_is_delete = str(normalized.get("isdelete") or "").strip().upper()
    if optional_is_delete:
        if not _COLUMN_LABEL_RE.fullmatch(optional_is_delete):
            invalid.append(f"{key_prefix}.budgetDataUpdateColumns.isDelete")
        else:
            parsed["isDelete"] = optional_is_delete

    return parsed


def _parse_fundsphere_sheet_settings(
    parsed: dict,
    *,
    key_prefix: str,
    require_accounts_range: bool = True,
    require_services_range: bool = False,
    require_budget_data: bool = False,
    require_budget_data_update: bool = False,
    require_net_spend: bool = False,
    require_master_budget_sheet: bool = False,
) -> tuple[dict[str, object], list[str], list[str]]:
    missing: list[str] = []
    invalid: list[str] = []
    require_master_budget_control = (
        require_accounts_range
        or require_services_range
        or require_budget_data
        or require_budget_data_update
        or require_master_budget_sheet
    )

    normalized = {str(k).strip().lower(): v for k, v in parsed.items()}
    master_budget_control_entry = _normalize_entry(
        normalized.get("masterbudgetcontrol"),
        key_name=f"{key_prefix}.masterBudgetControl",
        invalid=invalid,
    )

    if not master_budget_control_entry:
        master_budget_control_entry = _normalize_entry(
            normalized.get("fundsphere"),
            key_name=f"{key_prefix}.fundSphere",
            invalid=invalid,
        )

    if not master_budget_control_entry:
        if require_master_budget_control:
            missing.append(f"{key_prefix}.masterBudgetControl")
            return {}, missing, invalid
        master_budget_control_entry = {}

    spreadsheet_id = str(master_budget_control_entry.get("id", "")).strip()
    sheet_name = str(
        master_budget_control_entry.get("settingssheetname")
        or master_budget_control_entry.get("settingstabname")
        or master_budget_control_entry.get("tabname")
        or master_budget_control_entry.get("sheetname")
        or ""
    ).strip()
    accounts_range = str(
        master_budget_control_entry.get("settingaccountsrange")
        or master_budget_control_entry.get("settingsrange")
        or ""
    ).strip()
    services_range = str(
        master_budget_control_entry.get("settingservicesrange")
        or master_budget_control_entry.get("servicesrange")
        or ""
    ).strip()
    budget_data_sheet_name = str(
        master_budget_control_entry.get("budgetdatasheetname") or ""
    ).strip()
    budget_data_account_selection_range = str(
        master_budget_control_entry.get("budgetdataaccountselectionrange") or ""
    ).strip()
    budget_data_period_selection_range = str(
        master_budget_control_entry.get("budgetdataperiodselectionrange") or ""
    ).strip()
    budget_data_output_range = str(
        master_budget_control_entry.get("budgetdataoutputrange") or ""
    ).strip()
    budget_data_update_read_range = str(
        master_budget_control_entry.get("budgetdataupdatereadrange") or ""
    ).strip()
    budget_data_update_columns = master_budget_control_entry.get(
        "budgetdataupdatecolumns"
    )
    net_spend_entry = _normalize_entry(
        normalized.get("netspend"),
        key_name=f"{key_prefix}.netSpend",
        invalid=invalid,
    )
    if not net_spend_entry:
        net_spend_entry = _normalize_entry(
            master_budget_control_entry.get("netspend"),
            key_name=f"{key_prefix}.masterBudgetControl.netSpend",
            invalid=invalid,
        )
    net_spend_spreadsheet_id = str(net_spend_entry.get("id") or "").strip()
    net_spend_sheet_name = str(net_spend_entry.get("budgetdatasheetname") or "").strip()
    net_spend_data_range = str(net_spend_entry.get("budgetdataoutputrange") or "").strip()
    net_spend_sync_options_entry = _normalize_entry(
        normalized.get("netspendsyncoptions"),
        key_name=f"{key_prefix}.netSpendSyncOptions",
        invalid=invalid,
    )
    net_spend_period = str(net_spend_sync_options_entry.get("period") or "").strip()
    master_budget_entry = _normalize_entry(
        normalized.get("masterbudget"),
        key_name=f"{key_prefix}.masterBudget",
        invalid=invalid,
    )
    master_budget_sheet_name = str(master_budget_entry.get("budgetdatasheetname") or "").strip()
    master_budget_data_range = str(master_budget_entry.get("budgetdataoutputrange") or "").strip()
    master_budget_sync_options_entry = _normalize_entry(
        master_budget_control_entry.get("masterbudgetsyncoptions"),
        key_name=f"{key_prefix}.masterBudgetControl.masterBudgetSyncOptions",
        invalid=invalid,
    )
    master_budget_sync_account_codes = str(
        master_budget_sync_options_entry.get("accountcodes") or ""
    ).strip()
    raw_master_budget_sync_account_code_limit = master_budget_sync_options_entry.get(
        "accountcodelmit"
    )
    master_budget_sync_account_code_limit = 10
    if raw_master_budget_sync_account_code_limit not in (None, ""):
        try:
            master_budget_sync_account_code_limit = int(
                str(raw_master_budget_sync_account_code_limit).strip()
            )
        except (TypeError, ValueError):
            invalid.append(
                f"{key_prefix}.masterBudgetControl.masterBudgetSyncOptions.accountCodeLmit"
            )
            master_budget_sync_account_code_limit = 10
        else:
            if master_budget_sync_account_code_limit <= 0:
                invalid.append(
                    f"{key_prefix}.masterBudgetControl.masterBudgetSyncOptions.accountCodeLmit"
                )
                master_budget_sync_account_code_limit = 10
    master_budget_sync_year = str(master_budget_sync_options_entry.get("year") or "").strip()
    master_budget_sheet_ids_range = str(
        master_budget_control_entry.get("settongsmasterbudgetsheetidsrange") or ""
    ).strip()

    parsed_budget_data_update_columns: dict[str, str] = {}
    if require_master_budget_control:
        if not spreadsheet_id:
            missing.append(f"{key_prefix}.masterBudgetControl.id")
        elif not _is_valid_spreadsheet_id(spreadsheet_id):
            invalid.append(f"{key_prefix}.masterBudgetControl.id")

        if not sheet_name and (
            require_accounts_range or require_services_range or require_master_budget_sheet
        ):
            missing.append(f"{key_prefix}.masterBudgetControl.settingsSheetName")

        if require_accounts_range:
            if not accounts_range:
                missing.append(f"{key_prefix}.masterBudgetControl.settingAccountsRange")
            elif not _SETTINGS_RANGE_RE.fullmatch(accounts_range):
                invalid.append(f"{key_prefix}.masterBudgetControl.settingAccountsRange")

        if require_services_range:
            if not services_range:
                missing.append(f"{key_prefix}.masterBudgetControl.settingServicesRange")
            elif not _SETTINGS_RANGE_RE.fullmatch(services_range):
                invalid.append(f"{key_prefix}.masterBudgetControl.settingServicesRange")

        if require_budget_data:
            if not budget_data_sheet_name:
                missing.append(f"{key_prefix}.masterBudgetControl.budgetDataSheetName")
            if not budget_data_account_selection_range:
                missing.append(
                    f"{key_prefix}.masterBudgetControl.budgetDataAccountSelectionRange"
                )
            elif not _CELL_RANGE_RE.fullmatch(budget_data_account_selection_range):
                invalid.append(
                    f"{key_prefix}.masterBudgetControl.budgetDataAccountSelectionRange"
                )
            if not budget_data_period_selection_range:
                missing.append(
                    f"{key_prefix}.masterBudgetControl.budgetDataPeriodSelectionRange"
                )
            elif not _CELL_RANGE_RE.fullmatch(budget_data_period_selection_range):
                invalid.append(
                    f"{key_prefix}.masterBudgetControl.budgetDataPeriodSelectionRange"
                )
            if not budget_data_output_range:
                missing.append(f"{key_prefix}.masterBudgetControl.budgetDataOutputRange")
            elif not _SETTINGS_RANGE_RE.fullmatch(budget_data_output_range):
                invalid.append(f"{key_prefix}.masterBudgetControl.budgetDataOutputRange")

        if require_budget_data_update:
            if not budget_data_sheet_name:
                missing.append(f"{key_prefix}.masterBudgetControl.budgetDataSheetName")
            if not budget_data_update_read_range:
                missing.append(
                    f"{key_prefix}.masterBudgetControl.budgetDataUpdateReadRange"
                )
            elif not _SETTINGS_RANGE_RE.fullmatch(budget_data_update_read_range):
                invalid.append(
                    f"{key_prefix}.masterBudgetControl.budgetDataUpdateReadRange"
                )

            parsed_budget_data_update_columns = _parse_budget_data_update_columns(
                budget_data_update_columns,
                key_prefix=f"{key_prefix}.masterBudgetControl",
                missing=missing,
                invalid=invalid,
            )

        if require_master_budget_sheet:
            if not master_budget_entry:
                missing.append(f"{key_prefix}.masterBudget")
            if not master_budget_sheet_name:
                missing.append(f"{key_prefix}.masterBudget.budgetDataSheetName")
            if not master_budget_data_range:
                missing.append(f"{key_prefix}.masterBudget.budgetDataOutputRange")
            elif not _SETTINGS_RANGE_RE.fullmatch(master_budget_data_range):
                invalid.append(f"{key_prefix}.masterBudget.budgetDataOutputRange")
            if not master_budget_sync_account_codes:
                missing.append(
                    f"{key_prefix}.masterBudgetControl.masterBudgetSyncOptions.accountCodes"
                )
            elif not _CELL_RANGE_RE.fullmatch(master_budget_sync_account_codes):
                invalid.append(
                    f"{key_prefix}.masterBudgetControl.masterBudgetSyncOptions.accountCodes"
                )
            if not master_budget_sync_year:
                missing.append(f"{key_prefix}.masterBudgetControl.masterBudgetSyncOptions.year")
            elif not _CELL_RANGE_RE.fullmatch(master_budget_sync_year):
                invalid.append(
                    f"{key_prefix}.masterBudgetControl.masterBudgetSyncOptions.year"
                )
            if not master_budget_sheet_ids_range:
                missing.append(
                    f"{key_prefix}.masterBudgetControl.settongsMasterBudgetSheetIdsRange"
                )
            elif not _SETTINGS_RANGE_RE.fullmatch(master_budget_sheet_ids_range):
                invalid.append(
                    f"{key_prefix}.masterBudgetControl.settongsMasterBudgetSheetIdsRange"
                )

    if require_net_spend and not net_spend_entry:
        missing.append(f"{key_prefix}.netSpend")
    if require_net_spend or net_spend_entry:
        if not net_spend_spreadsheet_id:
            missing.append(f"{key_prefix}.netSpend.id")
        elif not _is_valid_spreadsheet_id(net_spend_spreadsheet_id):
            invalid.append(f"{key_prefix}.netSpend.id")

        if not net_spend_sheet_name:
            missing.append(f"{key_prefix}.netSpend.budgetDataSheetName")

        if not net_spend_data_range:
            missing.append(f"{key_prefix}.netSpend.budgetDataOutputRange")
        elif not _SETTINGS_RANGE_RE.fullmatch(net_spend_data_range):
            invalid.append(f"{key_prefix}.netSpend.budgetDataOutputRange")

    if missing or invalid:
        return {}, missing, invalid

    settings: dict[str, object] = {}
    if require_master_budget_control:
        settings["spreadsheet_id"] = spreadsheet_id
        settings["sheet_name"] = sheet_name
        if accounts_range:
            settings["accounts_range"] = accounts_range
        if services_range:
            settings["services_range"] = services_range
        if budget_data_sheet_name:
            settings["budget_data_sheet_name"] = budget_data_sheet_name
        if budget_data_account_selection_range:
            settings["budget_data_account_selection_range"] = (
                budget_data_account_selection_range
            )
        if budget_data_period_selection_range:
            settings["budget_data_period_selection_range"] = (
                budget_data_period_selection_range
            )
        if budget_data_output_range:
            settings["budget_data_output_range"] = budget_data_output_range
        if budget_data_update_read_range:
            settings["budget_data_update_read_range"] = budget_data_update_read_range
        if parsed_budget_data_update_columns:
            settings["budget_data_update_columns"] = parsed_budget_data_update_columns
        if require_master_budget_sheet:
            settings["master_budget_sheet_name"] = master_budget_sheet_name
            settings["master_budget_data_range"] = master_budget_data_range
            settings["master_budget_sync_account_codes_cell"] = (
                master_budget_sync_account_codes
            )
            settings["master_budget_sync_account_code_limit"] = (
                master_budget_sync_account_code_limit
            )
            settings["master_budget_sync_year_cell"] = master_budget_sync_year
            settings["master_budget_sheet_ids_range"] = master_budget_sheet_ids_range
    if net_spend_spreadsheet_id:
        settings["net_spend_spreadsheet_id"] = net_spend_spreadsheet_id
    if net_spend_sheet_name:
        settings["net_spend_sheet_name"] = net_spend_sheet_name
    if net_spend_data_range:
        settings["net_spend_data_range"] = net_spend_data_range
    if net_spend_sync_options_entry:
        settings["net_spend_period"] = net_spend_period
    return settings, missing, invalid


# ============================================================
# PUBLIC HELPERS
# ============================================================


def _get_fundsphere_sheet_settings(
    *,
    require_accounts_range: bool = True,
    require_services_range: bool = False,
    require_budget_data: bool = False,
    require_budget_data_update: bool = False,
    require_net_spend: bool = False,
    require_master_budget_sheet: bool = False,
) -> dict[str, object]:
    raw = _get_spreadsheets_raw()
    if raw is None or str(raw).strip() == "":
        raise TenantConfigValidationError(app_name=APP_NAME, missing=["SPREADSHEETS"])

    parsed = _parse_raw_value(raw, "SPREADSHEETS", dict)
    settings, missing, invalid = _parse_fundsphere_sheet_settings(
        parsed,
        key_prefix="SPREADSHEETS",
        require_accounts_range=require_accounts_range,
        require_services_range=require_services_range,
        require_budget_data=require_budget_data,
        require_budget_data_update=require_budget_data_update,
        require_net_spend=require_net_spend,
        require_master_budget_sheet=require_master_budget_sheet,
    )
    if missing or invalid:
        raise TenantConfigValidationError(
            app_name=APP_NAME,
            missing=missing,
            invalid=invalid,
        )

    return settings


def get_fundsphere_sheet_settings() -> dict[str, object]:
    return _get_fundsphere_sheet_settings()


def get_fundsphere_services_sheet_settings() -> dict[str, object]:
    return _get_fundsphere_sheet_settings(
        require_accounts_range=False,
        require_services_range=True,
    )


def get_fundsphere_budget_data_sheet_settings() -> dict[str, object]:
    return _get_fundsphere_sheet_settings(
        require_accounts_range=False,
        require_budget_data=True,
    )


def get_fundsphere_budget_data_update_settings() -> dict[str, object]:
    return _get_fundsphere_sheet_settings(
        require_accounts_range=False,
        require_budget_data_update=True,
    )


def get_fundsphere_net_spend_settings() -> dict[str, object]:
    return _get_fundsphere_sheet_settings(
        require_accounts_range=False,
        require_net_spend=True,
    )


def get_fundsphere_master_budget_sheet_settings() -> dict[str, object]:
    return _get_fundsphere_sheet_settings(
        require_accounts_range=False,
        require_master_budget_sheet=True,
    )


def get_db_tables(*, require_services: bool = False) -> dict[str, str]:
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

    required_keys = set(_REQUIRED_DB_TABLE_KEYS)
    if require_services:
        required_keys.add("SERVICES")

    missing_keys = required_keys.difference(tables.keys())
    if missing_keys:
        missing = [f"DB_TABLES.{key}" for key in sorted(missing_keys)]
        raise TenantConfigValidationError(app_name=APP_NAME, missing=missing)

    return tables


# ============================================================
# TENANT VALIDATION
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

        raw_spreadsheets = _get_spreadsheets_raw()
        if raw_spreadsheets is None or str(raw_spreadsheets).strip() == "":
            missing.append("SPREADSHEETS")
        else:
            try:
                parsed = _parse_raw_value(raw_spreadsheets, "SPREADSHEETS", dict)
                _, sheets_missing, sheets_invalid = _parse_fundsphere_sheet_settings(
                    parsed,
                    key_prefix="SPREADSHEETS",
                )
                missing.extend(sheets_missing)
                invalid.extend(sheets_invalid)
            except TenantConfigValidationError:
                invalid.append("SPREADSHEETS")

        raw_tables = _get_db_tables_raw()
        if raw_tables is None or str(raw_tables).strip() == "":
            missing.append("DB_TABLES")
        else:
            try:
                parsed_tables = _parse_raw_value(raw_tables, "DB_TABLES", dict)
                normalized: dict[str, str] = {}
                for key, value in parsed_tables.items():
                    name = str(value).strip()
                    if not name or not _DB_TABLE_RE.fullmatch(name):
                        invalid.append(f"DB_TABLES.{key}")
                    normalized[str(key).upper()] = name

                missing_keys = _REQUIRED_DB_TABLE_KEYS.difference(normalized.keys())
                for key in sorted(missing_keys):
                    missing.append(f"DB_TABLES.{key}")
            except TenantConfigValidationError:
                invalid.append("DB_TABLES")

        raw_google_accounts = _get_google_accounts_raw()
        if raw_google_accounts is None or str(raw_google_accounts).strip() == "":
            missing.append(_GOOGLE_ACCOUNTS_CONFIG_KEY_PREFIX)
        else:
            try:
                parsed_google_accounts = _parse_raw_value(
                    raw_google_accounts,
                    "FUNDSPHERE_GOOGLE_ACCOUNTS",
                    dict,
                )
                cfg_missing, cfg_invalid = _validate_google_accounts_config(
                    parsed_google_accounts,
                    key_prefix=_GOOGLE_ACCOUNTS_CONFIG_KEY_PREFIX,
                )
                missing.extend(cfg_missing)
                invalid.extend(cfg_invalid)
            except TenantConfigValidationError:
                invalid.append(_GOOGLE_ACCOUNTS_CONFIG_KEY_PREFIX)

        if missing or invalid:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                missing=missing,
                invalid=invalid,
            )

        _VALIDATED_TENANTS.add(tenant_id)
