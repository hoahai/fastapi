from __future__ import annotations

from decimal import Decimal
import re

from fastapi import APIRouter, Body, HTTPException
from pydantic import BaseModel

from apps.fundsphere.api.v1.helpers.config import (
    get_fundsphere_master_budget_sheet_settings,
)
from apps.fundsphere.api.v1.helpers.dbQueries import (
    get_master_budget_control_accounts,
    get_master_budget_control_master_budget_sheet_data_by_accounts,
)
from shared.ggSheet import (
    _clear_sheet_values,
    _column_label_to_index,
    _parse_row_bounded_a1_range,
    _read_sheet_values,
    _write_sheet_values,
)


# ============================================================
# ROUTER
# ============================================================

router = APIRouter(prefix="/masterBudget")


# ============================================================
# REQUEST MODELS
# ============================================================


class MasterBudgetLoadRequest(BaseModel):
    accountCodes: list[str] | None = None
    year: int | None = None


# ============================================================
# CONSTANTS
# ============================================================

_MASTER_BUDGET_HEADER = [
    "accountName",
    "year",
    "month",
    "serviceName",
    "subService",
    "grossAmount",
    "commission",
    "netAdjustment",
    "note",
]
_REQUIRED_MAPPING_HEADERS = ("year", "accountcode", "spreadsheetid")
_SHEET_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_ACCOUNT_CODE_RE = re.compile(r"\(([^)]+)\)")
_ACCOUNT_CODE_DASH_RE = re.compile(r"^\s*([A-Za-z0-9_-]+)\s*-\s*.+$")
_ACCOUNT_CODE_PLAIN_RE = re.compile(r"^[A-Za-z0-9_-]+$")


# ============================================================
# HELPERS
# ============================================================


def _column_index_to_label(index: int) -> str:
    if index < 0:
        raise ValueError("Column index must be >= 0")

    label = ""
    current = index + 1
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        label = chr(ord("A") + remainder) + label
    return label


def _parse_output_range(value: str) -> tuple[str, int, str]:
    try:
        return _parse_row_bounded_a1_range(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=500,
            detail="Invalid master budget output range format",
        ) from exc


def _read_single_cell(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    cell_range: str,
) -> object:
    range_name = f"'{sheet_name}'!{cell_range}"
    values = _read_sheet_values(
        spreadsheet_id=spreadsheet_id,
        range_name=range_name,
        app_name="FundSphere",
        value_render_option="UNFORMATTED_VALUE",
    )
    if not values or not isinstance(values[0], list) or not values[0]:
        return ""
    return values[0][0]


def _extract_account_codes(value: object) -> list[str]:
    account_codes: list[str] = []
    seen: set[str] = set()

    raw_value = str(value or "").strip()
    if not raw_value:
        return []

    for token in [part.strip() for part in raw_value.split(",") if part.strip()]:
        extracted_code = ""

        dash_match = _ACCOUNT_CODE_DASH_RE.fullmatch(token)
        if dash_match:
            extracted_code = str(dash_match.group(1) or "").strip()
        else:
            paren_matches = _ACCOUNT_CODE_RE.findall(token)
            if paren_matches:
                extracted_code = str(paren_matches[-1] or "").strip()
            elif _ACCOUNT_CODE_PLAIN_RE.fullmatch(token):
                extracted_code = token

        account_code = extracted_code.upper()
        if not account_code or account_code in seen:
            continue
        seen.add(account_code)
        account_codes.append(account_code)

    return account_codes


def _parse_selected_account_codes(value: object) -> list[str]:
    account_codes = _extract_account_codes(value)
    if not account_codes:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "No account code selected",
                "expected": "One or more account codes in plain, dashed, or parentheses format",
            },
        )
    return account_codes


def _parse_selected_account_codes_param(values: list[str]) -> list[str]:
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for value in values:
        for code in _extract_account_codes(value):
            if code in seen:
                continue
            seen.add(code)
            normalized_codes.append(code)
    if not normalized_codes:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "No valid accountCodes provided",
                "expected": "Use body field accountCodes, for example [\"ACH\", \"AUC\"]",
            },
        )
    return normalized_codes


def _try_parse_year(value: object) -> int | None:
    if isinstance(value, bool):
        return None

    if isinstance(value, int):
        parsed_year = value
    elif isinstance(value, float):
        if not value.is_integer():
            return None
        parsed_year = int(value)
    elif isinstance(value, Decimal):
        if value != value.to_integral_value():
            return None
        parsed_year = int(value)
    else:
        raw = str(value or "").strip()
        if not re.fullmatch(r"\d{4}", raw):
            return None
        parsed_year = int(raw)

    if parsed_year < 1000 or parsed_year > 9999:
        return None
    return parsed_year


def _parse_year(value: object) -> int:
    parsed_year = _try_parse_year(value)
    if parsed_year is None:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Invalid master budget sync year",
                "value": value,
                "expected": "4-digit year, for example 2026",
            },
        )
    return parsed_year


def _parse_mapping_header_indexes(header_row: list[object]) -> dict[str, int]:
    indexes: dict[str, int] = {}
    for index, value in enumerate(header_row):
        header = str(value or "").strip().lower()
        if header:
            indexes[header] = index

    missing_headers = [key for key in _REQUIRED_MAPPING_HEADERS if key not in indexes]
    if missing_headers:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Invalid master budget sheet-id mapping header",
                "requiredHeaders": list(_REQUIRED_MAPPING_HEADERS),
                "missingHeaders": missing_headers,
            },
        )
    return indexes


def _parse_master_budget_sheet_id_mappings(
    rows: list[list[object]],
) -> dict[tuple[str, int], str]:
    if not rows:
        return {}

    header_row: list[object] | None = None
    data_rows: list[list[object]] = []
    for row_values in rows:
        if not isinstance(row_values, list):
            continue
        if not any(str(value or "").strip() for value in row_values):
            continue
        if header_row is None:
            header_row = row_values
            continue
        data_rows.append(row_values)

    if header_row is None:
        return {}

    header_indexes = _parse_mapping_header_indexes(header_row)
    year_idx = header_indexes["year"]
    account_code_idx = header_indexes["accountcode"]
    spreadsheet_id_idx = header_indexes["spreadsheetid"]

    parsed: dict[tuple[str, int], str] = {}
    for row_offset, row_values in enumerate(data_rows, start=2):
        year_raw = row_values[year_idx] if year_idx < len(row_values) else ""
        account_raw = row_values[account_code_idx] if account_code_idx < len(row_values) else ""
        sheet_id_raw = row_values[spreadsheet_id_idx] if spreadsheet_id_idx < len(row_values) else ""

        if (
            str(year_raw or "").strip() == ""
            and str(account_raw or "").strip() == ""
            and str(sheet_id_raw or "").strip() == ""
        ):
            continue

        year = _try_parse_year(year_raw)
        account_codes = _extract_account_codes(account_raw)
        spreadsheet_id = str(sheet_id_raw or "").strip()

        if year is None or len(account_codes) != 1 or not spreadsheet_id:
            continue
        if not _SHEET_ID_RE.fullmatch(spreadsheet_id):
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "Invalid spreadsheetId in master budget sheet-id mapping",
                    "row": row_offset,
                    "spreadsheetId": spreadsheet_id,
                },
            )

        account_code = account_codes[0]
        key = (account_code, year)
        if key in parsed:
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "Duplicated accountCode/year in master budget sheet-id mapping",
                    "row": row_offset,
                    "accountCode": account_code,
                    "year": year,
                },
            )
        parsed[key] = spreadsheet_id

    return parsed


def _normalize_sheet_cell(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def _build_sheet_values(rows: list[dict[str, object]]) -> list[list[object]]:
    values: list[list[object]] = [_MASTER_BUDGET_HEADER]
    for row in rows:
        values.append([_normalize_sheet_cell(row.get(key)) for key in _MASTER_BUDGET_HEADER])
    return values


def _extract_db_account_codes(rows: list[dict[str, object]]) -> set[str]:
    valid_codes: set[str] = set()
    for row in rows:
        account_code = str(row.get("accountCode") or "").strip().upper()
        if account_code:
            valid_codes.add(account_code)
    return valid_codes


def _validate_selected_account_codes(
    *,
    selected_account_codes: list[str],
    account_code_limit: int,
) -> None:
    if len(selected_account_codes) > account_code_limit:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "accountCodes exceeds configured limit",
                "accountCodeLmit": account_code_limit,
                "selectedCount": len(selected_account_codes),
                "selectedAccountCodes": selected_account_codes,
            },
        )

    cached_rows = get_master_budget_control_accounts(refresh_cache=False)
    cached_valid_codes = _extract_db_account_codes(cached_rows)
    invalid_codes = [
        code for code in selected_account_codes if code not in cached_valid_codes
    ]
    if not invalid_codes:
        return

    fresh_rows = get_master_budget_control_accounts(refresh_cache=True)
    fresh_valid_codes = _extract_db_account_codes(fresh_rows)
    invalid_codes = [code for code in selected_account_codes if code not in fresh_valid_codes]
    if invalid_codes:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Invalid accountCodes",
                "invalidAccountCodes": invalid_codes,
                "validAccountCodes": sorted(fresh_valid_codes),
            },
        )


# ============================================================
# ROUTES
# ============================================================


@router.post("/load", summary="Load FundSphere master budget data from DB to sheets")
def load_master_budget_route(
    body: MasterBudgetLoadRequest | None = Body(default=None),
):
    """
    Load master budget rows for selected accounts and one year from DB,
    then overwrite each mapped account-year destination spreadsheet.

    Example request:
        POST /api/fundsphere/v1/masterBudgetControl/masterBudget/load
        Header: X-Tenant-Id: taaa

    Example request (use request body, skip spreadsheet selection cells):
        POST /api/fundsphere/v1/masterBudgetControl/masterBudget/load
        Body:
          {
            "accountCodes": ["ACH", "AUC"],
            "year": 2026
          }
        Header: X-Tenant-Id: taaa

    Example response:
        {
          "meta": {
            "requestId": "...",
            "durationMs": 102,
            "timestamp": "2026-03-28T09:20:30.000Z"
          },
          "data": {
            "controlSpreadsheetId": "1IQQ0vPOwB_8ngST0EbnfCUDcMo5T9Kp7ApGHdrSO_6M",
            "controlSheetName": "0. Settings",
            "sheetIdMappingRange": "'0. Settings'!V3:AG",
            "accountCodesSelectionRange": "'0. Settings'!M2",
            "yearSelectionRange": "'0. Settings'!M3",
            "selectedAccountCodes": ["ACH", "AUC"],
            "selectedYear": 2026,
            "targetSheetName": "0.1 Budget Data",
            "targetOutputRange": "A5:I",
            "accountResults": [
              {
                "accountCode": "ACH",
                "spreadsheetId": "1gYmlchcc8nrUKv19N6NRQQsbXnUzjQdQM0UnvW_gD08",
                "clearRange": "'0.1 Budget Data'!A5:I",
                "writeRange": "'0.1 Budget Data'!A5:I93",
                "dbRowCount": 89,
                "writtenRowCount": 89
              },
              {
                "accountCode": "AUC",
                "spreadsheetId": "1fVrY0F4BqrgpZFpdO-Ar5iHtQ_8EJURuobpeKClcrb4",
                "clearRange": "'0.1 Budget Data'!A5:I",
                "writeRange": "'0.1 Budget Data'!A5:I76",
                "dbRowCount": 72,
                "writtenRowCount": 72
              }
            ],
            "processedAccountCount": 2,
            "totalDbRowCount": 161,
            "totalWrittenRowCount": 161
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Tenant config must include SPREADSHEETS.masterBudgetControl.id
        - Tenant config must include SPREADSHEETS.masterBudgetControl.settingsSheetName
        - Tenant config must include SPREADSHEETS.masterBudgetControl.masterBudgetSyncOptions.accountCodes
        - Tenant config may include SPREADSHEETS.masterBudgetControl.masterBudgetSyncOptions.accountCodeLmit
          (default 10)
        - Tenant config must include SPREADSHEETS.masterBudgetControl.masterBudgetSyncOptions.year
        - Tenant config must include SPREADSHEETS.masterBudgetControl.settongsMasterBudgetSheetIdsRange
        - Tenant config must include SPREADSHEETS.masterBudget.budgetDataSheetName
        - Tenant config must include SPREADSHEETS.masterBudget.budgetDataOutputRange
        - Reads selected account codes and year from masterBudgetControl option cells
        - Optional request body fields `accountCodes` + `year` can be used to bypass
          spreadsheet selection cells
        - `accountCodes` and `year` must be provided together when using request body
        - Supports multiple account codes in one request (for example: ACH, AUC, DCM)
        - Validates selected accountCodes against DB accounts table:
          cache-first, then refresh-cache retry when needed
        - accountCodes count must be <= accountCodeLmit (default 10)
        - Reads sheet-id mapping rows from:
          masterBudgetControl.settingsSheetName + settongsMasterBudgetSheetIdsRange
        - The mapping range must include a header row with columns:
          year, accountCode, spreadsheetId
        - Mapping lookup key is accountCode + year
        - All selected accounts must have a sheet-id mapping for the selected year
        - Target write location in each destination spreadsheet is fixed by:
          masterBudget.budgetDataSheetName + masterBudget.budgetDataOutputRange
        - Output columns are:
          accountName, year, month, serviceName, subService, grossAmount,
          commission, netAdjustment, note
        - DB master budget rows are read directly from DB (no route-level cache)
    """
    settings = get_fundsphere_master_budget_sheet_settings()
    control_spreadsheet_id = str(settings["spreadsheet_id"])
    control_sheet_name = str(settings["sheet_name"])
    account_codes_selection_cell = str(settings["master_budget_sync_account_codes_cell"])
    account_code_limit = int(settings["master_budget_sync_account_code_limit"])
    year_selection_cell = str(settings["master_budget_sync_year_cell"])
    sheet_id_mapping_range = str(settings["master_budget_sheet_ids_range"])
    target_sheet_name = str(settings["master_budget_sheet_name"])
    target_output_range = str(settings["master_budget_data_range"])

    body_account_codes = body.accountCodes if body is not None else None
    body_year = body.year if body is not None else None
    using_body_params = body_account_codes is not None or body_year is not None
    if using_body_params:
        if body_account_codes is None or body_year is None:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "accountCodes and year must be provided together",
                    "requiredFields": ["accountCodes", "year"],
                },
            )
        selected_account_codes = _parse_selected_account_codes_param(body_account_codes)
        selected_year = _parse_year(body_year)
    else:
        selected_accounts_raw = _read_single_cell(
            spreadsheet_id=control_spreadsheet_id,
            sheet_name=control_sheet_name,
            cell_range=account_codes_selection_cell,
        )
        selected_year_raw = _read_single_cell(
            spreadsheet_id=control_spreadsheet_id,
            sheet_name=control_sheet_name,
            cell_range=year_selection_cell,
        )
        selected_account_codes = _parse_selected_account_codes(selected_accounts_raw)
        selected_year = _parse_year(selected_year_raw)

    _validate_selected_account_codes(
        selected_account_codes=selected_account_codes,
        account_code_limit=account_code_limit,
    )

    mapping_range_name = f"'{control_sheet_name}'!{sheet_id_mapping_range}"
    mapping_rows = _read_sheet_values(
        spreadsheet_id=control_spreadsheet_id,
        range_name=mapping_range_name,
        app_name="FundSphere",
        value_render_option="UNFORMATTED_VALUE",
    )
    sheet_id_mappings = _parse_master_budget_sheet_id_mappings(mapping_rows)
    if not sheet_id_mappings:
        raise HTTPException(
            status_code=500,
            detail="Master budget sheet-id mapping is empty",
        )

    missing_accounts: list[str] = []
    resolved_targets: list[tuple[str, str]] = []
    for account_code in selected_account_codes:
        spreadsheet_id = sheet_id_mappings.get((account_code, selected_year))
        if not spreadsheet_id:
            missing_accounts.append(account_code)
            continue
        resolved_targets.append((account_code, spreadsheet_id))

    if missing_accounts:
        available_for_year = sorted(
            account_code
            for account_code, year in sheet_id_mappings.keys()
            if year == selected_year
        )
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Missing sheet-id mapping for one or more selected account codes",
                "year": selected_year,
                "missingAccountCodes": missing_accounts,
                "availableAccountCodes": available_for_year,
            },
        )

    start_col, start_row, configured_end_col = _parse_output_range(target_output_range)
    start_col_index = _column_label_to_index(start_col)
    configured_end_col_index = _column_label_to_index(configured_end_col)
    configured_column_count = configured_end_col_index - start_col_index + 1
    if len(_MASTER_BUDGET_HEADER) > configured_column_count:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Master budget output range is too narrow for output columns",
                "dataRange": target_output_range,
                "columnCount": len(_MASTER_BUDGET_HEADER),
                "capacity": configured_column_count,
            },
        )

    clear_range = f"'{target_sheet_name}'!{target_output_range}"
    account_results: list[dict[str, object]] = []
    total_db_rows = 0
    total_written_rows = 0

    db_rows_by_account = get_master_budget_control_master_budget_sheet_data_by_accounts(
        account_codes=selected_account_codes,
        year=selected_year,
        refresh_cache=False,
    )

    for account_code, target_spreadsheet_id in resolved_targets:
        db_rows = db_rows_by_account.get(account_code, [])
        values = _build_sheet_values(db_rows)

        _clear_sheet_values(
            spreadsheet_id=target_spreadsheet_id,
            range_name=clear_range,
            app_name="FundSphere",
        )

        end_row = start_row + len(values) - 1
        write_end_col = _column_index_to_label(start_col_index + len(_MASTER_BUDGET_HEADER) - 1)
        write_range = f"'{target_sheet_name}'!{start_col}{start_row}:{write_end_col}{end_row}"
        _write_sheet_values(
            spreadsheet_id=target_spreadsheet_id,
            range_name=write_range,
            values=values,
            app_name="FundSphere",
            value_input_option="USER_ENTERED",
        )

        row_count = len(db_rows)
        total_db_rows += row_count
        total_written_rows += row_count
        account_results.append(
            {
                "accountCode": account_code,
                "spreadsheetId": target_spreadsheet_id,
                "clearRange": clear_range,
                "writeRange": write_range,
                "dbRowCount": row_count,
                "writtenRowCount": row_count,
            }
        )

    return {
        "controlSpreadsheetId": control_spreadsheet_id,
        "controlSheetName": control_sheet_name,
        "sheetIdMappingRange": mapping_range_name,
        "selectionSource": "body" if using_body_params else "spreadsheet",
        "accountCodesSelectionRange": (
            f"'{control_sheet_name}'!{account_codes_selection_cell}"
            if not using_body_params
            else None
        ),
        "yearSelectionRange": (
            f"'{control_sheet_name}'!{year_selection_cell}"
            if not using_body_params
            else None
        ),
        "selectedAccountCodes": selected_account_codes,
        "selectedYear": selected_year,
        "targetSheetName": target_sheet_name,
        "targetOutputRange": target_output_range,
        "accountResults": account_results,
        "processedAccountCount": len(account_results),
        "totalDbRowCount": total_db_rows,
        "totalWrittenRowCount": total_written_rows,
    }
