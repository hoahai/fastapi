from __future__ import annotations

import re
from datetime import date

from googleapiclient.errors import HttpError

from apps.spendsphere.api.v1.helpers.accountCodes import standardize_account_code
from apps.spendsphere.api.v1.helpers.config import (
    get_service_budgets,
    get_service_mapping,
    get_spendsphere_sheets,
)
from apps.spendsphere.api.v1.helpers.ggSheet import get_rollovers
from apps.spendsphere.api.v1.helpers.pipeline import build_transform_rows_for_period
from apps.spendsphere.api.v1.helpers.spendsphereHelpers import (
    get_services,
    get_google_sheet_cache_entry,
    set_google_sheet_cache,
)
from shared.ggSheet import _clear_sheet_values, _read_sheet_raw, _write_sheet_values

_NUMERIC_KEYS = (
    "calculatedBudget",
    "budget",
    "netAmount",
    "amount",
    "rolloverAmount",
)
_MONTHLY_BUDGET_SHEET_CACHE_KEY_PREFIX = "nucar_recommended_budget"
_CODE_COLUMN_KEYS = ("code", "accountcode")
_MASTER_BUDGET_START_ROW = 9


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    raw = str(value).strip()
    if not raw:
        return None

    negative = raw.startswith("(") and raw.endswith(")")
    cleaned = raw[1:-1] if negative else raw
    cleaned = cleaned.replace("$", "").replace(",", "").strip()
    if cleaned in {"", "-", "--"}:
        return None

    try:
        amount = float(cleaned)
    except (TypeError, ValueError):
        return None
    return -amount if negative else amount


def _normalize_label(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _build_month_sheet_name(month: int, year: int) -> str:
    return date(year, month, 1).strftime("%B %Y")


def _get_budget_sheet_spreadsheet_id() -> str:
    sheets = get_spendsphere_sheets()
    recommended = sheets.get("recommended_budget", {})
    spreadsheet_id = str(recommended.get("spreadsheet_id", "")).strip()
    if spreadsheet_id:
        return spreadsheet_id
    raise ValueError("SPREADSHEETS.digitalAdvertisingCenter.id is missing")


def _get_monthly_budget_sheet_rows(
    month: int,
    year: int,
) -> list[dict]:
    spreadsheet_id = _get_budget_sheet_spreadsheet_id()
    sheet_name = _build_month_sheet_name(month, year)
    range_name = f"'{sheet_name}'!A:ZZ"
    cache_key = f"{_MONTHLY_BUDGET_SHEET_CACHE_KEY_PREFIX}::{year:04d}-{month:02d}"
    config_hash = f"{spreadsheet_id}::{range_name}"

    cached, is_stale = get_google_sheet_cache_entry(cache_key, config_hash=config_hash)
    if cached is not None and not is_stale:
        return cached

    try:
        rows = _read_sheet_raw(
            spreadsheet_id=spreadsheet_id,
            range_name=range_name,
        )
    except HttpError as exc:
        if "unable to parse range" not in str(exc).lower():
            raise
        rows = []

    set_google_sheet_cache(cache_key, rows, config_hash=config_hash)
    return rows


def _normalize_row_keys(row: dict) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for key, value in row.items():
        normalized_key = _normalize_label(key)
        if normalized_key and normalized_key not in normalized:
            normalized[normalized_key] = value
    return normalized


def _extract_account_code(row: dict[str, object]) -> str:
    for key in _CODE_COLUMN_KEYS:
        candidate = str(row.get(key, "")).strip().upper()
        if candidate:
            return candidate
    return ""


def _resolve_budget_sheet_column(
    service_id: str,
    service_name: str,
    service_mapping: dict,
) -> str | None:
    mapping = _get_mapping_entry(service_mapping, service_id)
    if not mapping and service_name:
        mapping = _get_mapping_entry_by_service_name(service_mapping, service_name)

    candidates = []
    if isinstance(mapping, dict):
        candidates.append(mapping.get("sheetBudgetType"))
        candidates.append(mapping.get("serviceName"))
    candidates.append(service_name)

    for candidate in candidates:
        normalized = _normalize_label(candidate)
        if normalized:
            return normalized
    return None


def calculate_nucar_spreadsheet_budgets(
    account_codes: list[str],
    month: int,
    year: int,
) -> list[dict[str, object]]:
    """
    Parse NuCar spreadsheet rows and aggregate calculated budgets by account code.
    """
    rows = get_rollovers(
        account_codes=account_codes,
        month=month,
        year=year,
        include_unrollable=True,
    )

    aggregated: dict[str, dict[str, object]] = {}
    for row in rows:
        account_code = standardize_account_code(row.get("accountCode")) or ""
        if not account_code:
            continue

        amount = None
        for key in _NUMERIC_KEYS:
            amount = _to_float(row.get(key))
            if amount is not None:
                break
        if amount is None:
            continue

        entry = aggregated.setdefault(
            account_code,
            {
                "accountCode": account_code,
                "calculatedBudget": 0.0,
                "source": "spreadsheet",
                "sourceRows": 0,
            },
        )
        entry["calculatedBudget"] = round(
            float(entry["calculatedBudget"]) + amount,
            2,
        )
        entry["sourceRows"] = int(entry["sourceRows"]) + 1

    return list(aggregated.values())


def _resolve_service_name(
    service_id: str,
    service_mapping: dict,
    services_by_id: dict[str, str],
) -> str:
    db_service_name = str(
        services_by_id.get(service_id)
        or services_by_id.get(service_id.upper())
        or services_by_id.get(service_id.lower())
        or ""
    ).strip()
    if db_service_name:
        return db_service_name
    mapping = _get_mapping_entry(service_mapping, service_id)
    mapped_name = str(mapping.get("serviceName", "")).strip() if isinstance(mapping, dict) else ""
    if mapped_name:
        return mapped_name
    return service_id


def _get_mapping_entry(service_mapping: dict, service_id: str) -> dict:
    if not isinstance(service_mapping, dict):
        return {}
    for key in (service_id, service_id.upper(), service_id.lower()):
        entry = service_mapping.get(key)
        if isinstance(entry, dict):
            return entry
    return {}


def _get_mapping_entry_by_service_name(service_mapping: dict, service_name: str) -> dict:
    if not isinstance(service_mapping, dict):
        return {}
    normalized_target = _normalize_label(service_name)
    if not normalized_target:
        return {}
    for entry in service_mapping.values():
        if not isinstance(entry, dict):
            continue
        candidate = _normalize_label(entry.get("serviceName"))
        if candidate and candidate == normalized_target:
            return entry
    return {}


def get_nucar_recommended_budget(
    account_code: str,
    service_id: str,
    month: int,
    year: int,
) -> dict[str, object]:
    """
    Parse NuCar monthly budget sheet and return recommended amount for one
    account + service.
    """
    normalized_account_code = standardize_account_code(account_code) or ""
    normalized_service_id = str(service_id).strip()
    if not normalized_account_code or not normalized_service_id:
        return {
            "accountCode": normalized_account_code,
            "serviceId": normalized_service_id,
            "serviceName": normalized_service_id,
            "amount": None,
        }

    service_mapping = get_service_mapping()
    services = get_services(department_code="DIGM")
    services_by_id = {
        str(service.get("id", "")).strip(): str(service.get("name", "")).strip()
        for service in services
        if str(service.get("id", "")).strip()
    }
    service_name = _resolve_service_name(
        normalized_service_id,
        service_mapping,
        services_by_id,
    )
    target_column = _resolve_budget_sheet_column(
        normalized_service_id,
        service_name,
        service_mapping,
    )

    rows = _get_monthly_budget_sheet_rows(month, year)
    amount = 0.0
    found_amount = False

    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized_row = _normalize_row_keys(row)
        row_account_code = _extract_account_code(normalized_row)
        if not row_account_code or row_account_code != normalized_account_code:
            continue
        if not target_column:
            continue
        parsed = _to_float(normalized_row.get(target_column))
        if parsed is None:
            continue
        found_amount = True
        amount = round(amount + parsed, 2)

    return {
        "accountCode": normalized_account_code,
        "serviceId": normalized_service_id,
        "serviceName": service_name,
        "amount": round(amount, 2) if found_amount else None,
    }


def _resolve_requested_service_ids(service_id: str | None) -> list[str]:
    if service_id is not None:
        cleaned = str(service_id).strip()
        return [cleaned] if cleaned else []

    normalized: list[str] = []
    seen: set[str] = set()
    for value in get_service_budgets():
        cleaned = str(value).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


def get_nucar_recommended_budgets(
    account_code: str,
    service_id: str | None,
    month: int,
    year: int,
) -> list[dict[str, object]]:
    service_ids = _resolve_requested_service_ids(service_id)
    if not service_ids:
        return []

    return [
        get_nucar_recommended_budget(
            account_code=account_code,
            service_id=value,
            month=month,
            year=year,
        )
        for value in service_ids
    ]


def get_nucar_recommended_budgets_bulk(
    account_codes: list[str],
    service_id: str | None,
    month: int,
    year: int,
) -> list[dict[str, object]]:
    normalized_account_codes: list[str] = []
    seen_accounts: set[str] = set()
    for value in account_codes:
        code = standardize_account_code(value)
        if not code or code in seen_accounts:
            continue
        seen_accounts.add(code)
        normalized_account_codes.append(code)
    if not normalized_account_codes:
        return []

    service_ids = _resolve_requested_service_ids(service_id)
    if not service_ids:
        return []

    service_mapping = get_service_mapping()
    services = get_services(department_code="DIGM")
    services_by_id = {
        str(service.get("id", "")).strip(): str(service.get("name", "")).strip()
        for service in services
        if str(service.get("id", "")).strip()
    }
    service_context: list[tuple[str, str, str | None]] = []
    for value in service_ids:
        service_name = _resolve_service_name(
            value,
            service_mapping,
            services_by_id,
        )
        target_column = _resolve_budget_sheet_column(
            value,
            service_name,
            service_mapping,
        )
        service_context.append((value, service_name, target_column))

    target_columns = {
        column
        for _, _, column in service_context
        if isinstance(column, str) and column
    }

    rows = _get_monthly_budget_sheet_rows(month, year)
    requested_accounts = set(normalized_account_codes)
    amount_by_account_column: dict[str, dict[str, float]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized_row = _normalize_row_keys(row)
        row_account_code = _extract_account_code(normalized_row)
        if not row_account_code or row_account_code not in requested_accounts:
            continue
        if not target_columns:
            continue

        account_values = amount_by_account_column.setdefault(row_account_code, {})
        for column in target_columns:
            parsed = _to_float(normalized_row.get(column))
            if parsed is None:
                continue
            account_values[column] = round(account_values.get(column, 0.0) + parsed, 2)

    payload: list[dict[str, object]] = []
    for account_code_value in normalized_account_codes:
        account_values = amount_by_account_column.get(account_code_value, {})
        for service_id_value, service_name, target_column in service_context:
            has_value = bool(target_column and target_column in account_values)
            amount = round(account_values.get(target_column, 0.0), 2) if has_value else None
            payload.append(
                {
                    "accountCode": account_code_value,
                    "serviceId": service_id_value,
                    "serviceName": service_name,
                    "amount": amount,
                }
            )
    return payload


def _resolve_master_budget_sheet_spreadsheet_id() -> str:
    sheets = get_spendsphere_sheets()
    budget_tool = sheets.get("budget_tool", {})
    spreadsheet_id = str(budget_tool.get("spreadsheet_id", "")).strip()
    if not spreadsheet_id:
        raise ValueError("SPREADSHEETS.budgetTool.id is missing")
    return spreadsheet_id


def _resolve_master_budget_sheet_name() -> str:
    sheets = get_spendsphere_sheets()
    budget_tool = sheets.get("budget_tool", {})
    sheet_name = str(budget_tool.get("range_name", "")).strip()
    if not sheet_name:
        raise ValueError("SPREADSHEETS.budgetTool.masterBudgetSheetName is missing")
    return sheet_name


def _build_adjusted_budget_amount(row: dict[str, object]) -> float | None:
    allocated_budget = _to_float(row.get("allocatedBudgetBeforeAcceleration"))
    if allocated_budget is None:
        return None

    multiplier = _to_float(row.get("accelerationMultiplier"))
    if multiplier is None:
        multiplier = 100.0

    return round(allocated_budget * multiplier / 100.0, 2)


def _build_schedule_status(row: dict[str, object]) -> str:
    raw = str(row.get("scheduleStatus", "")).strip()
    return raw if raw else "-"


def _build_sheet_values(rows: list[dict[str, object]]) -> list[list[object]]:
    values: list[list[object]] = []
    for row in rows:
        amount = row.get("amount")
        values.append(
            [
                str(row.get("budgetId", "")).strip(),
                round(float(amount), 2) if amount is not None else "",
                str(row.get("scheduleStatus", "-") or "-"),
            ]
        )
    return values


def get_nucar_master_budget_rows(
    month: int,
    year: int,
    *,
    refresh_google_ads_caches: bool = False,
) -> list[dict[str, object]]:
    transform_payload = build_transform_rows_for_period(
        account_codes=None,
        month=month,
        year=year,
        refresh_google_ads_caches=refresh_google_ads_caches,
        cache_first=True,
        include_costs=False,
    )
    rows = transform_payload.get("rows", [])
    allocations = transform_payload.get("allocations", [])
    if not isinstance(rows, list) or not isinstance(allocations, list):
        return []

    by_budget_id: dict[str, dict[str, object]] = {}
    allocated_budget_ids = {
        str(item.get("ggBudgetId", "")).strip()
        for item in allocations
        if isinstance(item, dict) and str(item.get("ggBudgetId", "")).strip()
    }
    for row in rows:
        budget_id = str(row.get("budgetId", "")).strip()
        if not budget_id:
            continue
        if budget_id not in allocated_budget_ids:
            continue

        amount = _build_adjusted_budget_amount(row)
        status = _build_schedule_status(row)
        current = by_budget_id.get(budget_id)
        if current is None:
            by_budget_id[budget_id] = {
                "budgetId": budget_id,
                "amount": amount,
                "scheduleStatus": status,
            }
            continue

        existing_amount = current.get("amount")
        if amount is not None:
            if existing_amount is None:
                current["amount"] = amount
            else:
                current["amount"] = round(float(existing_amount) + amount, 2)

        existing_status = str(current.get("scheduleStatus", "")).strip()
        if (not existing_status or existing_status == "-") and status and status != "-":
            current["scheduleStatus"] = status

    return sorted(by_budget_id.values(), key=lambda item: str(item.get("budgetId", "")))


def sync_nucar_master_budget_sheet(
    month: int,
    year: int,
    *,
    refresh_google_ads_caches: bool = False,
) -> dict[str, object]:
    rows = get_nucar_master_budget_rows(
        month,
        year,
        refresh_google_ads_caches=refresh_google_ads_caches,
    )

    spreadsheet_id = _resolve_master_budget_sheet_spreadsheet_id()
    sheet_name = _resolve_master_budget_sheet_name()
    clear_range = f"'{sheet_name}'!A{_MASTER_BUDGET_START_ROW}:C"
    _clear_sheet_values(
        spreadsheet_id=spreadsheet_id,
        range_name=clear_range,
    )

    if rows:
        values = _build_sheet_values(rows)
        end_row = _MASTER_BUDGET_START_ROW + len(values) - 1
        write_range = f"'{sheet_name}'!A{_MASTER_BUDGET_START_ROW}:C{end_row}"
        _write_sheet_values(
            spreadsheet_id=spreadsheet_id,
            range_name=write_range,
            values=values,
            value_input_option="USER_ENTERED",
        )

    return {
        "spreadsheetId": spreadsheet_id,
        "sheetName": sheet_name,
        "startRow": _MASTER_BUDGET_START_ROW,
        "rowCount": len(rows),
        "rows": rows,
    }
