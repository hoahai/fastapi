from __future__ import annotations

from decimal import Decimal

from fastapi import APIRouter, HTTPException, Query

from apps.fundsphere.api.v1.helpers.config import (
    get_fundsphere_services_sheet_settings,
    get_fundsphere_sheet_settings,
)
from apps.fundsphere.api.v1.helpers.dbQueries import (
    get_master_budget_control_accounts,
    get_master_budget_control_services,
)
from shared.ggSheet import (
    _clear_sheet_values,
    _parse_row_bounded_a1_range,
    _write_sheet_values,
)


# ============================================================
# ROUTER
# ============================================================

router = APIRouter(prefix="/settings")


# ============================================================
# HELPERS
# ============================================================


def _normalize_account_code(value: object) -> str:
    code = str(value or "").strip()
    if not code:
        raise HTTPException(status_code=500, detail="DB account code is empty")
    return code.upper()


def _normalize_account_name(value: object, *, account_code: str) -> str:
    name = str(value or "").strip()
    return name or account_code


def _normalize_active(value: object) -> int:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 0 if value == 0 else 1
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if cleaned in {"1", "true", "t", "yes", "y", "on"}:
            return 1
        if cleaned in {"0", "false", "f", "no", "n", "off"}:
            return 0

    return 1 if bool(value) else 0


def _parse_settings_range(value: str) -> tuple[str, int, str]:
    try:
        return _parse_row_bounded_a1_range(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=500,
            detail="Invalid tenant settings range format",
        ) from exc


def _build_sheet_values(rows: list[dict]) -> list[list[object]]:
    values: list[list[object]] = [
        ["accountCode", "accountName", "active", "dropdownValue"]
    ]

    for row in rows:
        account_code = _normalize_account_code(row.get("accountCode"))
        account_name = _normalize_account_name(
            row.get("accountName"),
            account_code=account_code,
        )
        active = _normalize_active(row.get("active"))
        dropdown_value = f"{account_code} - {account_name}"
        values.append([account_code, account_name, active, dropdown_value])

    return values


def _normalize_required_text(value: object, *, label: str) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        raise HTTPException(status_code=500, detail=f"DB {label} is empty")
    return cleaned


def _normalize_optional_number(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def _build_services_sheet_values(rows: list[dict]) -> list[list[object]]:
    values: list[list[object]] = [
        [
            "depListingOrder",
            "departmentCode",
            "departmentName",
            "serviceId",
            "serviceName",
            "commission",
        ]
    ]

    for row in rows:
        dep_listing_order = _normalize_optional_number(row.get("depListingOrder"))
        department_code = _normalize_required_text(
            row.get("departmentCode"),
            label="department code",
        )
        department_name = _normalize_required_text(
            row.get("departmentName"),
            label="department name",
        )
        service_id = _normalize_required_text(
            row.get("serviceId"),
            label="service id",
        )
        service_name = _normalize_required_text(
            row.get("serviceName"),
            label="service name",
        )
        commission = _normalize_optional_number(row.get("commission"))
        values.append(
            [
                dep_listing_order,
                department_code,
                department_name,
                service_id,
                service_name,
                commission,
            ]
        )

    return values


def _write_rows_to_settings_range(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    target_range: str,
    values: list[list[object]],
) -> dict[str, object]:
    start_col, start_row, end_col = _parse_settings_range(target_range)
    clear_range = f"'{sheet_name}'!{target_range}"
    _clear_sheet_values(
        spreadsheet_id=spreadsheet_id,
        range_name=clear_range,
        app_name="FundSphere",
    )

    end_row = start_row + len(values) - 1
    write_range = f"'{sheet_name}'!{start_col}{start_row}:{end_col}{end_row}"
    _write_sheet_values(
        spreadsheet_id=spreadsheet_id,
        range_name=write_range,
        values=values,
        app_name="FundSphere",
        value_input_option="USER_ENTERED",
    )
    return {
        "clearRange": clear_range,
        "writeRange": write_range,
        "writtenRowCount": max(len(values) - 1, 0),
    }


# ============================================================
# ROUTES
# ============================================================


@router.post("/accounts", summary="Sync FundSphere setting accounts from DB to sheet")
def sync_settings_accounts_route(
    fresh_data: bool = Query(
        False,
        description="When true, bypass shared cache and pull fresh DB rows before writing sheet.",
    ),
    active_only: bool = Query(
        False,
        description="When true, include only rows with active=1 in the sheet output.",
    ),
):
    """
    Pull account rows from the tenant DB and overwrite the configured
    masterBudgetControl settings range in Google Sheets.

    Example request:
        POST /api/fundsphere/v1/masterBudgetControl/settings/accounts
        Header: X-Tenant-Id: acme

    Example request (fresh DB read):
        POST /api/fundsphere/v1/masterBudgetControl/settings/accounts?fresh_data=true
        Header: X-Tenant-Id: acme

    Example request (active accounts only):
        POST /api/fundsphere/v1/masterBudgetControl/settings/accounts?active_only=true
        Header: X-Tenant-Id: acme

    Example response:
        {
          "meta": {
            "requestId": "...",
            "durationMs": 31,
            "timestamp": "2026-03-24T11:20:30.000Z"
          },
          "data": {
            "spreadsheetId": "1IQQ0vPOwB_8ngST0EbnfCUDcMo5T9Kp7ApGHdrSO_6M",
            "sheetName": "0. Settings",
            "clearRange": "'0. Settings'!E4:H",
            "writeRange": "'0. Settings'!E4:H15",
            "dbRowCount": 11,
            "writtenRowCount": 11
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Tenant config must include SPREADSHEETS.masterBudgetControl.id
        - Tenant config must include SPREADSHEETS.masterBudgetControl.settingsSheetName
        - Tenant config must include SPREADSHEETS.masterBudgetControl.settingAccountsRange
        - Tenant config must include DB_TABLES.accounts, DB_TABLES.departments,
          DB_TABLES.budgets, and DB_TABLES.changeHistories
        - DB account rows are cached in shared tenant-scoped cache
        - Set fresh_data=true to bypass cache and refresh from DB
        - Set active_only=true to write only rows with active=1
        - active is written as numeric 0/1
        - dropdownValue is generated as "<ACCOUNTCODE> - <accountName>"
    """
    sheet_settings = get_fundsphere_sheet_settings()
    spreadsheet_id = sheet_settings["spreadsheet_id"]
    sheet_name = sheet_settings["sheet_name"]
    accounts_range = sheet_settings["accounts_range"]

    db_rows = get_master_budget_control_accounts(refresh_cache=fresh_data)
    if active_only:
        db_rows = [
            row for row in db_rows if _normalize_active(row.get("active")) == 1
        ]
    values = _build_sheet_values(db_rows)
    write_meta = _write_rows_to_settings_range(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        target_range=accounts_range,
        values=values,
    )

    return {
        "spreadsheetId": spreadsheet_id,
        "sheetName": sheet_name,
        "clearRange": write_meta["clearRange"],
        "writeRange": write_meta["writeRange"],
        "dbRowCount": len(db_rows),
        "writtenRowCount": write_meta["writtenRowCount"],
    }


@router.post("/services", summary="Sync FundSphere setting services from DB to sheet")
def sync_settings_services_route(
    fresh_data: bool = Query(
        False,
        description="When true, bypass shared cache and pull fresh DB rows before writing sheet.",
    ),
):
    """
    Pull services joined with departments from the tenant DB and overwrite the
    configured masterBudgetControl services settings range in Google Sheets.

    Example request:
        POST /api/fundsphere/v1/masterBudgetControl/settings/services
        Header: X-Tenant-Id: acme

    Example request (fresh DB read):
        POST /api/fundsphere/v1/masterBudgetControl/settings/services?fresh_data=true
        Header: X-Tenant-Id: acme

    Example response:
        {
          "meta": {
            "requestId": "...",
            "durationMs": 29,
            "timestamp": "2026-03-24T11:30:30.000Z"
          },
          "data": {
            "spreadsheetId": "1IQQ0vPOwB_8ngST0EbnfCUDcMo5T9Kp7ApGHdrSO_6M",
            "sheetName": "0. Settings",
            "clearRange": "'0. Settings'!J4:O",
            "writeRange": "'0. Settings'!J4:O30",
            "dbRowCount": 26,
            "writtenRowCount": 26
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Tenant config must include SPREADSHEETS.masterBudgetControl.id
        - Tenant config must include SPREADSHEETS.masterBudgetControl.settingsSheetName
        - Tenant config must include SPREADSHEETS.masterBudgetControl.settingServicesRange
        - Tenant config must include DB_TABLES.services and DB_TABLES.departments
        - Data is joined by services.departmentCode = departments.code
        - DB service rows are cached in shared tenant-scoped cache
        - Set fresh_data=true to bypass cache and refresh from DB
    """
    sheet_settings = get_fundsphere_services_sheet_settings()
    spreadsheet_id = sheet_settings["spreadsheet_id"]
    sheet_name = sheet_settings["sheet_name"]
    services_range = sheet_settings["services_range"]

    db_rows = get_master_budget_control_services(refresh_cache=fresh_data)
    values = _build_services_sheet_values(db_rows)
    write_meta = _write_rows_to_settings_range(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        target_range=services_range,
        values=values,
    )

    return {
        "spreadsheetId": spreadsheet_id,
        "sheetName": sheet_name,
        "clearRange": write_meta["clearRange"],
        "writeRange": write_meta["writeRange"],
        "dbRowCount": len(db_rows),
        "writtenRowCount": write_meta["writtenRowCount"],
    }
