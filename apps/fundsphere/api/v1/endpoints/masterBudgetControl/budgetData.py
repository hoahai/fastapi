from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request

from apps.fundsphere.api.v1.helpers.config import (
    get_fundsphere_budget_data_sheet_settings,
    get_fundsphere_budget_data_update_settings,
)
from apps.fundsphere.api.v1.helpers.dbQueries import (
    get_master_budget_control_budget_data,
    update_master_budget_control_budget_data,
    validate_master_budget_control_budget_duplicates,
    validate_master_budget_control_budget_refs,
)
from shared.ggSheet import (
    _clear_sheet_notes,
    _clear_sheet_values,
    _column_label_to_index,
    _parse_row_bounded_a1_range,
    _read_sheet_values,
    _set_checkbox_validation,
    _write_sheet_values,
)
from shared.tenant import get_timezone


# ============================================================
# ROUTER
# ============================================================

router = APIRouter(prefix="/budgetData")


# ============================================================
# CONSTANTS
# ============================================================

_ACCOUNT_CODE_RE = re.compile(r"\(([^)]+)\)")
_ACCOUNT_CODE_DASH_RE = re.compile(r"^\s*([A-Za-z0-9_-]+)\s*-\s*.+$")
_ACCOUNT_CODE_PLAIN_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_PERIOD_RE = re.compile(r"^(\d{1,2})\/(\d{4})$")
_BUDGET_DATA_HEADER = [
    "originalSig",
    "budgetId",
    "accountName",
    "year",
    "month",
    "serviceName",
    "subService",
    "grossAmount",
    "commission",
    "netAdjustment",
    "note",
    "changeHistories",
]
_EDITABLE_FIELDS = ("subService", "grossAmount", "commission", "netAdjustment", "note")
_REQUIRED_UPDATE_COLUMN_KEYS = (
    "originalSig",
    "budgetId",
    "year",
    "month",
    "subService",
    "grossAmount",
    "commission",
    "netAdjustment",
    "note",
    "accountCode",
    "serviceId",
)


# ============================================================
# HELPERS
# ============================================================


def _parse_output_range(value: str) -> tuple[str, int, str]:
    try:
        return _parse_row_bounded_a1_range(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=500,
            detail="Invalid tenant budget data output range format",
        ) from exc


def _column_index_to_label(index: int) -> str:
    if index < 0:
        raise ValueError("Column index must be >= 0")

    label = ""
    current = index + 1
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        label = chr(ord("A") + remainder) + label
    return label


def _extract_cell_value(row_values: list[object], index: int) -> object:
    if index < 0 or index >= len(row_values):
        return ""
    return row_values[index]


def _is_row_changed_flag_true(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0

    cleaned = str(value or "").strip()
    if not cleaned:
        return False

    lowered = cleaned.lower()
    if lowered in {"0", "false", "f", "no", "n", "off"}:
        return False
    if lowered in {"1", "true", "t", "yes", "y", "on"}:
        return True

    # Any non-empty/non-false token is treated as changed.
    return True


def _read_single_cell(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    cell_range: str,
) -> str:
    range_name = f"'{sheet_name}'!{cell_range}"
    values = _read_sheet_values(spreadsheet_id=spreadsheet_id, range_name=range_name)
    if not values or not isinstance(values[0], list) or not values[0]:
        return ""
    return str(values[0][0] or "").strip()


def _extract_account_codes(value: str) -> list[str]:
    account_codes: list[str] = []
    seen: set[str] = set()

    raw_value = str(value or "").strip()
    if not raw_value:
        raise HTTPException(status_code=400, detail="No account code selected")

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

        code = extracted_code.upper()
        if not code or code in seen:
            continue
        seen.add(code)
        account_codes.append(code)

    if not account_codes:
        raise HTTPException(status_code=400, detail="No account code selected")

    return account_codes


def _parse_periods(value: str) -> list[tuple[int, int]]:
    raw = str(value or "").strip()
    if not raw:
        current_year = datetime.now(ZoneInfo(get_timezone())).year
        return [(month, current_year) for month in range(1, 13)]

    periods: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    invalid_periods: list[str] = []

    for token in [part.strip() for part in raw.split(",") if part.strip()]:
        match = _PERIOD_RE.fullmatch(token)
        if not match:
            invalid_periods.append(token)
            continue

        month = int(match.group(1))
        year = int(match.group(2))
        if month < 1 or month > 12:
            invalid_periods.append(token)
            continue

        key = (month, year)
        if key in seen:
            continue
        seen.add(key)
        periods.append(key)

    if invalid_periods:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Invalid period selection",
                "invalidPeriods": invalid_periods,
                "expectedFormat": "M/YYYY or MM/YYYY, separated by commas",
            },
        )

    if not periods:
        raise HTTPException(status_code=400, detail="No valid period selected")

    return periods


def _to_decimal(value: object, *, scale: int) -> Decimal:
    if value is None or value == "":
        dec_value = Decimal("0")
    elif isinstance(value, Decimal):
        dec_value = value
    else:
        try:
            dec_value = Decimal(str(value).strip())
        except (InvalidOperation, ValueError):
            dec_value = Decimal("0")

    precision = Decimal("1").scaleb(-scale)
    return dec_value.quantize(precision, rounding=ROUND_HALF_UP)


def _decimal_text(value: object, *, scale: int) -> str:
    dec_value = _to_decimal(value, scale=scale)
    return f"{dec_value:.{scale}f}"


def _to_sheet_number(value: object, *, scale: int) -> int | float:
    dec_value = _to_decimal(value, scale=scale)
    if dec_value == dec_value.to_integral_value():
        return int(dec_value)
    return float(dec_value)


def _build_original_sig(row: dict) -> str:
    sub_service = str(row.get("subService") or "").strip()
    gross_amount = _decimal_text(row.get("grossAmount"), scale=2)
    commission = _decimal_text(row.get("commission"), scale=4)
    net_adjustment = _decimal_text(row.get("netAdjustment"), scale=2)
    note = str(row.get("note") or "").strip()

    return (
        f"subService={sub_service}"
        f"|grossAmount={gross_amount}"
        f"|commission={commission}"
        f"|netAdjustment={net_adjustment}"
        f"|note={note}"
    )


def _build_budget_data_values(rows: list[dict]) -> list[list[object]]:
    values: list[list[object]] = [_BUDGET_DATA_HEADER]

    for row in rows:
        month_value = row.get("month")
        month_cell = (
            ""
            if month_value is None or month_value == ""
            else _to_sheet_number(month_value, scale=0)
        )
        values.append(
            [
                _build_original_sig(row),
                str(row.get("budgetId") or "").strip(),
                str(row.get("accountName") or "").strip(),
                str(row.get("year") or "").strip(),
                month_cell,
                str(row.get("serviceName") or "").strip(),
                str(row.get("subService") or "").strip(),
                _to_sheet_number(row.get("grossAmount"), scale=2),
                _to_sheet_number(row.get("commission"), scale=4),
                _to_sheet_number(row.get("netAdjustment"), scale=2),
                str(row.get("note") or "").strip(),
                str(row.get("changeHistories") or "").strip(),
            ]
        )

    return values


def _load_budget_data_to_sheet(
    *,
    sheet_settings: dict[str, object],
    refresh_budget_data: bool = False,
) -> dict[str, object]:
    spreadsheet_id = str(sheet_settings["spreadsheet_id"])
    sheet_name = str(sheet_settings["budget_data_sheet_name"])
    account_selection_cell = str(sheet_settings["budget_data_account_selection_range"])
    period_selection_cell = str(sheet_settings["budget_data_period_selection_range"])
    output_range = str(sheet_settings["budget_data_output_range"])

    selected_accounts_raw = _read_single_cell(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        cell_range=account_selection_cell,
    )
    selected_periods_raw = _read_single_cell(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        cell_range=period_selection_cell,
    )

    account_codes = _extract_account_codes(selected_accounts_raw)
    periods = _parse_periods(selected_periods_raw)

    db_rows = get_master_budget_control_budget_data(
        account_codes=account_codes,
        periods=periods,
        refresh_cache=refresh_budget_data,
    )
    values = _build_budget_data_values(db_rows)

    start_col, start_row, configured_end_col = _parse_output_range(output_range)
    start_col_index = _column_label_to_index(start_col)
    configured_end_col_index = _column_label_to_index(configured_end_col)
    required_end_col_index = start_col_index + (len(_BUDGET_DATA_HEADER) - 1)
    effective_end_col_index = max(configured_end_col_index, required_end_col_index)
    end_col = _column_index_to_label(effective_end_col_index)

    clear_range = f"'{sheet_name}'!{start_col}{start_row}:{end_col}"
    _clear_sheet_values(
        spreadsheet_id=spreadsheet_id,
        range_name=clear_range,
    )
    _clear_sheet_notes(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        start_col=start_col,
        start_row=start_row,
        end_col=end_col,
    )

    # Reset delete flags right after clear (if isDelete column is configured),
    # and only for the rows being loaded.
    loaded_row_count = len(db_rows)
    if loaded_row_count > 0:
        try:
            update_settings = get_fundsphere_budget_data_update_settings()
            update_spreadsheet_id = str(update_settings.get("spreadsheet_id") or "").strip()
            update_sheet_name = str(update_settings.get("budget_data_sheet_name") or "").strip()
            update_columns = update_settings.get("budget_data_update_columns")
            delete_col = (
                str((update_columns or {}).get("isDelete") or "").strip()
                if isinstance(update_columns, dict)
                else ""
            )
            if (
                delete_col
                and update_spreadsheet_id == spreadsheet_id
                and update_sheet_name == sheet_name
            ):
                data_start_row = start_row + 1
                delete_end_row = data_start_row + loaded_row_count - 1
                _set_checkbox_validation(
                    spreadsheet_id=spreadsheet_id,
                    sheet_name=sheet_name,
                    start_col=delete_col,
                    start_row=data_start_row,
                    end_col=delete_col,
                    end_row=delete_end_row,
                )
                delete_write_range = (
                    f"'{sheet_name}'!{delete_col}{data_start_row}:{delete_col}{delete_end_row}"
                )
                _write_sheet_values(
                    spreadsheet_id=spreadsheet_id,
                    range_name=delete_write_range,
                    values=[[False] for _ in range(loaded_row_count)],
                    value_input_option="RAW",
                )
        except Exception:
            # Do not fail the load response if optional delete-flag reset cannot run.
            pass

    end_row = start_row + len(values) - 1
    write_range = f"'{sheet_name}'!{start_col}{start_row}:{end_col}{end_row}"
    _write_sheet_values(
        spreadsheet_id=spreadsheet_id,
        range_name=write_range,
        values=values,
        value_input_option="USER_ENTERED",
    )

    return {
        "spreadsheetId": spreadsheet_id,
        "sheetName": sheet_name,
        "accountSelectionRange": f"'{sheet_name}'!{account_selection_cell}",
        "periodSelectionRange": f"'{sheet_name}'!{period_selection_cell}",
        "clearRange": clear_range,
        "writeRange": write_range,
        "selectedAccountCodes": account_codes,
        "selectedPeriods": [f"{month}/{year}" for month, year in periods],
        "dbRowCount": len(db_rows),
        "writtenRowCount": max(len(values) - 1, 0),
    }


def _parse_original_sig(value: object) -> dict[str, str]:
    parsed: dict[str, str] = {}
    raw = str(value or "").strip()
    if not raw:
        return parsed

    for token in raw.split("|"):
        if "=" not in token:
            continue
        key, token_value = token.split("=", 1)
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        parsed[normalized_key] = str(token_value or "").strip()
    return parsed


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _is_blank(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _parse_decimal_text(
    value: object,
    *,
    scale: int,
    field_name: str,
    row_number: int,
    errors: list[dict[str, object]],
    required: bool = True,
    default_if_blank: str = "0",
) -> str | None:
    cleaned = "" if value is None else str(value).strip()
    if not cleaned:
        if required:
            errors.append(
                {
                    "row": row_number,
                    "field": field_name,
                    "message": f"{field_name} is required",
                }
            )
            return None
        default_cleaned = str(default_if_blank or "0").strip()
        try:
            default_dec_value = Decimal(default_cleaned)
        except (InvalidOperation, ValueError):
            default_dec_value = Decimal("0")
        precision = Decimal("1").scaleb(-scale)
        default_quantized = default_dec_value.quantize(precision, rounding=ROUND_HALF_UP)
        return f"{default_quantized:.{scale}f}"

    normalized = cleaned.replace(",", "")
    try:
        dec_value = Decimal(normalized)
    except (InvalidOperation, ValueError):
        errors.append(
            {
                "row": row_number,
                "field": field_name,
                "message": f"{field_name} must be a valid number",
            }
        )
        return None

    precision = Decimal("1").scaleb(-scale)
    quantized = dec_value.quantize(precision, rounding=ROUND_HALF_UP)
    return f"{quantized:.{scale}f}"


def _normalize_original_values(original_sig: dict[str, str]) -> dict[str, str]:
    return {
        "subService": str(original_sig.get("subService") or "").strip(),
        "grossAmount": _decimal_text(original_sig.get("grossAmount"), scale=2),
        "commission": _decimal_text(original_sig.get("commission"), scale=4),
        "netAdjustment": _decimal_text(original_sig.get("netAdjustment"), scale=2),
        "note": str(original_sig.get("note") or "").strip(),
    }


def _parse_budget_data_sheet_rows(
    rows: list[list[object]],
    *,
    start_row: int,
    column_indexes: dict[str, int],
    is_row_changed_index: int | None = None,
    is_delete_index: int | None = None,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    changed_items: list[dict[str, object]] = []
    create_items: list[dict[str, object]] = []
    delete_items: list[dict[str, object]] = []
    errors: list[dict[str, object]] = []

    for offset, row_values in enumerate(rows):
        row_number = start_row + offset

        delete_value = (
            _extract_cell_value(row_values, is_delete_index)
            if is_delete_index is not None
            else ""
        )
        is_delete = _is_row_changed_flag_true(delete_value)

        if is_row_changed_index is not None and not is_delete:
            row_changed_value = _extract_cell_value(row_values, is_row_changed_index)
            if not _is_row_changed_flag_true(row_changed_value):
                continue

        original_sig_cell = _extract_cell_value(
            row_values,
            column_indexes["originalSig"],
        )
        budget_id_cell = _extract_cell_value(
            row_values,
            column_indexes["budgetId"],
        )
        year_cell = _extract_cell_value(row_values, column_indexes["year"])
        month_cell = _extract_cell_value(row_values, column_indexes["month"])
        sub_service_cell = _extract_cell_value(
            row_values,
            column_indexes["subService"],
        )
        gross_amount_cell = _extract_cell_value(
            row_values,
            column_indexes["grossAmount"],
        )
        commission_cell = _extract_cell_value(
            row_values,
            column_indexes["commission"],
        )
        net_adjustment_cell = _extract_cell_value(
            row_values,
            column_indexes["netAdjustment"],
        )
        note_cell = _extract_cell_value(row_values, column_indexes["note"])
        account_code_cell = _extract_cell_value(
            row_values,
            column_indexes["accountCode"],
        )
        service_id_cell = _extract_cell_value(
            row_values,
            column_indexes["serviceId"],
        )

        core_values = [
            original_sig_cell,
            budget_id_cell,
            year_cell,
            month_cell,
            sub_service_cell,
            gross_amount_cell,
            commission_cell,
            net_adjustment_cell,
            note_cell,
            account_code_cell,
            service_id_cell,
        ]
        if all(_is_blank(value) for value in core_values) and not is_delete:
            continue

        original_sig_raw = str(original_sig_cell or "").strip()
        budget_id = str(budget_id_cell or "").strip()
        month_value = _coerce_int(month_cell)
        year_value = _coerce_int(year_cell)
        sub_service = str(sub_service_cell or "").strip()
        note = str(note_cell or "").strip()
        account_code = str(account_code_cell or "").strip().upper()
        service_id = str(service_id_cell or "").strip()

        if is_delete:
            if not budget_id:
                errors.append(
                    {
                        "row": row_number,
                        "field": "budgetId",
                        "message": "budgetId is required for delete rows",
                    }
                )
                continue
            delete_items.append({"budgetId": budget_id})
            continue

        gross_amount = _parse_decimal_text(
            gross_amount_cell,
            scale=2,
            field_name="grossAmount",
            row_number=row_number,
            errors=errors,
        )
        commission = _parse_decimal_text(
            commission_cell,
            scale=4,
            field_name="commission",
            row_number=row_number,
            errors=errors,
        )
        net_adjustment = _parse_decimal_text(
            net_adjustment_cell,
            scale=2,
            field_name="netAdjustment",
            row_number=row_number,
            errors=errors,
            required=False,
            default_if_blank="0",
        )
        if gross_amount is None or commission is None or net_adjustment is None:
            continue

        if budget_id:
            if not original_sig_raw:
                errors.append(
                    {
                        "row": row_number,
                        "field": "originalSig",
                        "message": "originalSig is required for existing budget update rows",
                    }
                )
                continue

            original_sig = _parse_original_sig(original_sig_raw)
            if not original_sig:
                errors.append(
                    {
                        "row": row_number,
                        "field": "originalSig",
                        "message": "originalSig format is invalid",
                    }
                )
                continue

            current_values = {
                "subService": sub_service,
                "grossAmount": gross_amount,
                "commission": commission,
                "netAdjustment": net_adjustment,
                "note": note,
            }
            original_values = _normalize_original_values(original_sig)

            diffs: list[dict[str, str]] = []
            for field in _EDITABLE_FIELDS:
                old_value = original_values[field]
                new_value = current_values[field]
                if old_value == new_value:
                    continue
                diffs.append(
                    {
                        "field": field,
                        "oldValue": old_value,
                        "newValue": new_value,
                    }
                )

            if not diffs:
                continue

            changed_items.append(
                {
                    "row": row_number,
                    "budgetId": budget_id,
                    "accountCode": account_code,
                    "serviceId": service_id,
                    "month": month_value,
                    "year": year_value,
                    "subService": sub_service,
                    "grossAmount": gross_amount,
                    "commission": commission,
                    "netAdjustment": net_adjustment,
                    "note": note,
                    "diffs": diffs,
                }
            )
            continue

        if original_sig_raw:
            errors.append(
                {
                    "row": row_number,
                    "field": "originalSig",
                    "message": "originalSig must be empty for new budget rows",
                }
            )
            continue

        row_error_count_before = len(errors)
        if not account_code:
            errors.append(
                {
                    "row": row_number,
                    "field": "accountCode",
                    "message": "accountCode is required for new budget rows",
                }
            )
        if not service_id:
            errors.append(
                {
                    "row": row_number,
                    "field": "serviceId",
                    "message": "serviceId is required for new budget rows",
                }
            )
        if month_value is None or month_value < 1 or month_value > 12:
            errors.append(
                {
                    "row": row_number,
                    "field": "month",
                    "message": "month is required and must be between 1 and 12",
                }
            )
        if year_value is None or year_value <= 0:
            errors.append(
                {
                    "row": row_number,
                    "field": "year",
                    "message": "year is required and must be a positive integer",
                }
            )
        if len(errors) > row_error_count_before:
            continue

        create_items.append(
            {
                "row": row_number,
                "accountCode": account_code,
                "serviceId": service_id,
                "month": month_value,
                "year": year_value,
                "subService": sub_service,
                "grossAmount": gross_amount,
                "commission": commission,
                "netAdjustment": net_adjustment,
                "note": note,
                "diffs": [
                    {"field": "subService", "oldValue": "", "newValue": sub_service},
                    {"field": "grossAmount", "oldValue": "", "newValue": gross_amount},
                    {"field": "commission", "oldValue": "", "newValue": commission},
                    {"field": "netAdjustment", "oldValue": "", "newValue": net_adjustment},
                    {"field": "note", "oldValue": "", "newValue": note},
                ],
            }
        )

    return changed_items, create_items, delete_items, errors


def _build_row_error_messages(errors: list[dict[str, object]]) -> list[str]:
    grouped: dict[int, list[str]] = {}
    row_order: list[int] = []

    for error in errors:
        row_raw = error.get("row")
        try:
            row_number = int(row_raw)
        except (TypeError, ValueError):
            continue

        message = str(error.get("message") or "").strip()
        if not message:
            field_name = str(error.get("field") or "").strip()
            message = f"{field_name} is invalid" if field_name else "Invalid value"

        if row_number not in grouped:
            grouped[row_number] = []
            row_order.append(row_number)
        if message not in grouped[row_number]:
            grouped[row_number].append(message)

    return [f"Row {row}: {', '.join(grouped[row])}" for row in row_order]


# ============================================================
# ROUTES
# ============================================================


@router.post("/load", summary="Load FundSphere budget data from DB to sheet")
def load_budget_data_route():
    """
    Read selected account codes and periods from the configured budget-data sheet,
    fetch matching budgets from DB, and overwrite the budget-data output range.

    Example request:
        POST /api/fundsphere/v1/budgetData/load
        Header: X-Tenant-Id: acme

    Example response:
        {
          "meta": {
            "requestId": "...",
            "durationMs": 42,
            "timestamp": "2026-03-25T10:20:30.000Z"
          },
          "data": {
            "spreadsheetId": "1IQQ0vPOwB_8ngST0EbnfCUDcMo5T9Kp7ApGHdrSO_6M",
            "sheetName": "0.2 Budget Data",
            "accountSelectionRange": "'0.2 Budget Data'!A2",
            "periodSelectionRange": "'0.2 Budget Data'!A3",
            "clearRange": "'0.2 Budget Data'!A5:L",
            "writeRange": "'0.2 Budget Data'!A5:L31",
            "selectedAccountCodes": ["ACH", "AFS"],
            "selectedPeriods": ["3/2026", "4/2026"],
            "dbRowCount": 26,
            "writtenRowCount": 26
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Tenant config must include SPREADSHEETS.masterBudgetControl.id
        - Tenant config must include SPREADSHEETS.masterBudgetControl.budgetDataSheetName
        - Tenant config must include SPREADSHEETS.masterBudgetControl.budgetDataAccountSelectionRange
        - Tenant config must include SPREADSHEETS.masterBudgetControl.budgetDataPeriodSelectionRange
        - Tenant config must include SPREADSHEETS.masterBudgetControl.budgetDataOutputRange
        - Tenant config must include DB_TABLES.accounts, DB_TABLES.departments,
          DB_TABLES.services, DB_TABLES.budgets, and DB_TABLES.changeHistories
        - Account selection cell supports:
          "ACH - Atzenhoffer Chevrolet, AFS - Academy Ford Sales"
          or legacy parentheses format:
          "Atzenhoffer Chevrolet (ACH), Academy Ford Sales (AFS)"
        - Uses cache-first DB read for selected account/period budget data
          (TTL from `fundsphere.CACHE.db_budget_data_ttl_time`, fallback 300s)
        - Period selection cell accepts comma-separated M/YYYY or MM/YYYY values,
          for example "3/2026, 4/2026"
        - When period selection cell is empty, all months (1..12) of current tenant year are used
    """
    return _load_budget_data_to_sheet(
        sheet_settings=get_fundsphere_budget_data_sheet_settings(),
        refresh_budget_data=False,
    )


@router.post("/update", summary="Update FundSphere budget data from sheet to DB")
def update_budget_data_route(request: Request):
    """
    Read edited budget rows from the configured budget-data update range,
    compare against `originalSig`, update changed editable fields in DB,
    then refresh the budget-data sheet using current sheet selections.

    Example request:
        POST /api/fundsphere/v1/budgetData/update
        Header: X-Tenant-Id: acme

    Example response:
        {
          "meta": {
            "requestId": "...",
            "durationMs": 37,
            "timestamp": "2026-03-25T11:10:00.000Z"
          },
          "data": {
            "spreadsheetId": "1IQQ0vPOwB_8ngST0EbnfCUDcMo5T9Kp7ApGHdrSO_6M",
            "sheetName": "0.2 Budget Data",
            "readRange": "'0.2 Budget Data'!A6:X",
            "scannedRowCount": 22,
            "changedRowCount": 5,
            "createdCandidateRowCount": 2,
            "deletedCandidateRowCount": 1,
            "updatedRowCount": 5,
            "createdRowCount": 2,
            "deletedRowCount": 1,
            "historyRowCount": 10,
            "createdBudgetIds": ["f5a0..."],
            "deletedBudgetIds": ["a1b2..."],
            "sheetReloaded": true
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Supports optional X-User-Name header for audit `changedBy`
        - Tenant config must include SPREADSHEETS.masterBudgetControl.id
        - Tenant config must include SPREADSHEETS.masterBudgetControl.budgetDataSheetName
        - Tenant config must include SPREADSHEETS.masterBudgetControl.budgetDataUpdateReadRange
        - Tenant config must include SPREADSHEETS.masterBudgetControl.budgetDataUpdateColumns
        - Tenant config must include DB_TABLES.accounts, DB_TABLES.services,
          DB_TABLES.budgets, and DB_TABLES.changeHistories
        - Reads configured budgetDataUpdateReadRange from budgetDataSheetName
        - Uses budgetDataUpdateColumns mapping to resolve all read columns
        - `originalSig` must follow key-value format:
          "subService=...|grossAmount=...|commission=...|netAdjustment=...|note=..."
        - New budget create rows are supported when:
          `budgetId` is empty, `originalSig` is empty, and
          `accountCode` (col N), `serviceId` (col T), `year`, `month` are provided
        - Existing budget delete rows are supported when:
          `budgetId` is present and `budgetDataUpdateColumns.isDelete` column is true
        - `grossAmount` and `commission` are required on all write rows
        - `netAdjustment` is editable; blank values default to `0`
        - If budgetDataUpdateColumns.isRowChanged is configured, only flagged rows are processed
        - Validates duplicate keys for `accountCode + month + year + serviceId + subService`
          across request rows and existing budgets before write
        - Validation errors are aggregated (row values, references, duplicates)
          and returned together in one response
        - If any validation fails, no update/insert is executed
        - After successful DB update/insert, the endpoint reloads
          budget data to sheet using updated cache buckets
    """
    sheet_settings = get_fundsphere_budget_data_update_settings()
    changed_by = str(request.headers.get("x-user-name") or "").strip() or "budgetData.update"
    spreadsheet_id = sheet_settings["spreadsheet_id"]
    sheet_name = sheet_settings["budget_data_sheet_name"]
    update_read_range = str(sheet_settings["budget_data_update_read_range"])
    update_columns = sheet_settings.get("budget_data_update_columns")
    if not isinstance(update_columns, dict):
        raise HTTPException(
            status_code=500,
            detail="Invalid tenant budget data update columns config",
        )

    missing_column_keys = [
        key
        for key in _REQUIRED_UPDATE_COLUMN_KEYS
        if not str(update_columns.get(key) or "").strip()
    ]
    if missing_column_keys:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Missing budget data update column mappings",
                "missingKeys": missing_column_keys,
            },
        )

    column_indexes: dict[str, int] = {}
    try:
        for key in _REQUIRED_UPDATE_COLUMN_KEYS:
            column_indexes[key] = _column_label_to_index(str(update_columns.get(key)))
    except ValueError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    is_row_changed_index: int | None = None
    is_row_changed_col = str(update_columns.get("isRowChanged") or "").strip()
    if is_row_changed_col:
        try:
            is_row_changed_index = _column_label_to_index(is_row_changed_col)
        except ValueError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
    is_delete_index: int | None = None
    is_delete_col = str(update_columns.get("isDelete") or "").strip()
    if is_delete_col:
        try:
            is_delete_index = _column_label_to_index(is_delete_col)
        except ValueError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    _start_col, start_row, _end_col = _parse_output_range(update_read_range)
    read_range = f"'{sheet_name}'!{update_read_range}"
    sheet_rows = _read_sheet_values(
        spreadsheet_id=spreadsheet_id,
        range_name=read_range,
        value_render_option="UNFORMATTED_VALUE",
    )
    changed_rows, create_rows, delete_rows, row_errors = _parse_budget_data_sheet_rows(
        sheet_rows,
        start_row=start_row,
        column_indexes=column_indexes,
        is_row_changed_index=is_row_changed_index,
        is_delete_index=is_delete_index,
    )

    scanned_row_count = len(sheet_rows)
    validation_messages: list[str] = []
    validation_errors: list[dict[str, object]] = []
    has_non_duplicate_errors = False

    if row_errors:
        row_error_messages = _build_row_error_messages(row_errors)
        validation_messages.extend(row_error_messages)
        validation_errors.extend(row_errors)
        has_non_duplicate_errors = True

    ref_validation = validate_master_budget_control_budget_refs(
        budget_ids=[
            str(item.get("budgetId") or "").strip()
            for item in (changed_rows + delete_rows)
        ],
        account_codes=[str(item.get("accountCode") or "").strip() for item in create_rows],
        service_ids=[str(item.get("serviceId") or "").strip() for item in create_rows],
    )
    has_ref_errors = bool(
        ref_validation.get("missingBudgetIds")
        or ref_validation.get("invalidAccountCodes")
        or ref_validation.get("invalidServiceIds")
    )
    if has_ref_errors:
        ref_error_messages: list[str] = []
        ref_errors: list[dict[str, object]] = []
        missing_budget_ids = ref_validation.get("missingBudgetIds") or []
        invalid_account_codes = ref_validation.get("invalidAccountCodes") or []
        invalid_service_ids = ref_validation.get("invalidServiceIds") or []

        if missing_budget_ids:
            message = "Missing budgetIds: " + ", ".join(str(item) for item in missing_budget_ids)
            ref_error_messages.append(message)
            ref_errors.append(
                {
                    "field": "budgetId",
                    "message": message,
                }
            )
        if invalid_account_codes:
            message = "Invalid accountCodes: " + ", ".join(
                str(item) for item in invalid_account_codes
            )
            ref_error_messages.append(message)
            ref_errors.append(
                {
                    "field": "accountCode",
                    "message": message,
                }
            )
        if invalid_service_ids:
            message = "Invalid serviceIds: " + ", ".join(
                str(item) for item in invalid_service_ids
            )
            ref_error_messages.append(message)
            ref_errors.append(
                {
                    "field": "serviceId",
                    "message": message,
                }
            )

        validation_messages.extend(ref_error_messages)
        validation_errors.extend(ref_errors)
        has_non_duplicate_errors = True

    duplicate_validation = validate_master_budget_control_budget_duplicates(
        changes=changed_rows,
        creates=create_rows,
        deletes=delete_rows,
    )
    duplicate_keys = duplicate_validation.get("duplicateKeys") or []
    if duplicate_keys:
        budget_id_to_rows: dict[str, list[int]] = {}
        for offset, row_values in enumerate(sheet_rows):
            row_number = start_row + offset
            budget_id_value = str(
                _extract_cell_value(row_values, column_indexes["budgetId"]) or ""
            ).strip()
            if not budget_id_value:
                continue
            budget_id_to_rows.setdefault(budget_id_value, []).append(row_number)

        duplicate_errors: list[dict[str, object]] = []
        duplicated_rows_set: set[int] = set()
        for duplicate in duplicate_keys:
            account_code = str(duplicate.get("accountCode") or "").strip()
            month_value = duplicate.get("month")
            year_value = duplicate.get("year")
            service_id = str(duplicate.get("serviceId") or "").strip()
            sub_service = str(duplicate.get("subService") or "").strip()
            source = str(duplicate.get("source") or "").strip()
            existing_budget_id = str(duplicate.get("existingBudgetId") or "").strip()
            duplicate_rows_raw = duplicate.get("rows")
            duplicate_rows = (
                [int(row) for row in duplicate_rows_raw if isinstance(row, int)]
                if isinstance(duplicate_rows_raw, list)
                else []
            )
            if source == "database" and existing_budget_id:
                for row_number in budget_id_to_rows.get(existing_budget_id, []):
                    if row_number not in duplicate_rows:
                        duplicate_rows.append(row_number)
                duplicate_rows.sort()
            for row in duplicate_rows:
                duplicated_rows_set.add(row)
            duplicate_errors.append(
                {
                    "field": "budgetUniqueKey",
                    "message": "Duplicated budget rows found",
                    "key": {
                        "accountCode": account_code,
                        "month": month_value,
                        "year": year_value,
                        "serviceId": service_id,
                        "subService": sub_service,
                    },
                    "rows": duplicate_rows,
                    "existingBudgetId": existing_budget_id,
                    "source": source,
                }
            )

        duplicated_rows = sorted(duplicated_rows_set)
        row_numbers_text = ""
        if len(duplicated_rows) == 1:
            row_numbers_text = str(duplicated_rows[0])
        elif len(duplicated_rows) == 2:
            row_numbers_text = f"{duplicated_rows[0]} and {duplicated_rows[1]}"
        elif len(duplicated_rows) >= 3:
            leading = ", ".join(str(row) for row in duplicated_rows[:-1])
            row_numbers_text = f"{leading}, and {duplicated_rows[-1]}"

        message = (
            f"Duplicated budget rows found: {row_numbers_text}"
            if row_numbers_text
            else "Duplicated budget rows found"
        )
        validation_messages.append(message)
        validation_errors.extend(duplicate_errors)

    if validation_errors:
        unique_messages: list[str] = []
        seen_messages: set[str] = set()
        for message in validation_messages:
            cleaned_message = str(message or "").strip()
            if not cleaned_message or cleaned_message in seen_messages:
                continue
            seen_messages.add(cleaned_message)
            unique_messages.append(cleaned_message)

        response_message = "Validation failed. No rows were updated."
        if unique_messages:
            if has_non_duplicate_errors:
                response_message = "; ".join([response_message] + unique_messages)
            elif len(unique_messages) == 1:
                response_message = unique_messages[0]
            else:
                response_message = "; ".join([response_message] + unique_messages)

        raise HTTPException(
            status_code=400,
            detail={
                "message": response_message,
                "messages": unique_messages,
                "errors": validation_errors,
            },
        )

    if not changed_rows and not create_rows and not delete_rows:
        return {
            "spreadsheetId": spreadsheet_id,
            "sheetName": sheet_name,
            "readRange": read_range,
            "scannedRowCount": scanned_row_count,
            "changedRowCount": 0,
            "createdCandidateRowCount": 0,
            "deletedCandidateRowCount": 0,
            "updatedRowCount": 0,
            "createdRowCount": 0,
            "deletedRowCount": 0,
            "historyRowCount": 0,
            "createdBudgetIds": [],
            "deletedBudgetIds": [],
            "invalidRowCount": 0,
        }

    try:
        update_result = update_master_budget_control_budget_data(
            changes=changed_rows,
            creates=create_rows,
            deletes=delete_rows,
            changed_by=changed_by,
        )
    except Exception as exc:
        exc_text = str(exc)
        if "Duplicate entry" in exc_text and "for key" in exc_text:
            row_message = (
                "Duplicate budget key detected for unique key "
                "(accountCode + month + year + serviceId + subService)"
            )
            raise HTTPException(
                status_code=400,
                detail={
                    "message": (
                        "Validation failed. No rows were updated.; "
                        f"{row_message}."
                    ),
                    "messages": [row_message],
                    "errors": [
                        {
                            "field": "budgetUniqueKey",
                            "message": row_message,
                        }
                    ],
                },
            ) from exc
        if delete_rows and "fk_changehistories_budget" in exc_text:
            row_message = (
                "Delete is blocked by foreign key fk_changehistories_budget "
                "(BudgetChangeHistories.budgetId -> Budgets.id)"
            )
            raise HTTPException(
                status_code=400,
                detail={
                    "message": (
                        "Validation failed. No rows were updated.; "
                        f"{row_message}. Remove this FK to allow hard delete while retaining history."
                    ),
                    "messages": [row_message],
                    "errors": [
                        {
                            "field": "budgetId",
                            "message": row_message,
                        }
                    ],
                },
            ) from exc
        raise

    refreshed_sheet_data: dict[str, object] | None = None
    refresh_error: str | None = None
    try:
        refreshed_sheet_data = _load_budget_data_to_sheet(
            sheet_settings=get_fundsphere_budget_data_sheet_settings(),
            refresh_budget_data=False,
        )
    except HTTPException as exc:
        refresh_error = str(exc.detail)
    except Exception as exc:  # pragma: no cover - defensive guard for post-write refresh
        refresh_error = str(exc)

    return {
        "spreadsheetId": spreadsheet_id,
        "sheetName": sheet_name,
        "readRange": read_range,
        "scannedRowCount": scanned_row_count,
        "changedRowCount": len(changed_rows),
        "createdCandidateRowCount": len(create_rows),
        "deletedCandidateRowCount": len(delete_rows),
        "updatedRowCount": int(update_result.get("updatedRowCount") or 0),
        "createdRowCount": int(update_result.get("createdRowCount") or 0),
        "deletedRowCount": int(update_result.get("deletedRowCount") or 0),
        "historyRowCount": int(update_result.get("historyRowCount") or 0),
        "createdBudgetIds": update_result.get("createdBudgetIds") or [],
        "deletedBudgetIds": update_result.get("deletedBudgetIds") or [],
        "invalidRowCount": 0,
        "historyInsertError": update_result.get("historyInsertError"),
        "sheetReloaded": refresh_error is None,
        "sheetReloadError": refresh_error,
        "sheetReload": refreshed_sheet_data,
    }
