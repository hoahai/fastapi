from __future__ import annotations

from decimal import Decimal
import re

from fastapi import APIRouter, Body, HTTPException
from pydantic import BaseModel

from apps.fundsphere.api.v1.helpers.config import get_fundsphere_net_spend_settings
from apps.fundsphere.api.v1.helpers.dbQueries import get_master_budget_control_net_spend_data
from shared.ggSheet import (
    _clear_sheet_values,
    _column_label_to_index,
    _parse_row_bounded_a1_range,
    _write_sheet_values,
)


# ============================================================
# ROUTER
# ============================================================

router = APIRouter(prefix="/netSpend")


# ============================================================
# REQUEST MODELS
# ============================================================


class NetSpendLoadRequest(BaseModel):
    month: int | None = None
    year: int | None = None


# ============================================================
# CONSTANTS
# ============================================================

_NET_SPEND_HEADER = [
    "accountCode",
    "accountName",
    "serviceId",
    "serviceName",
    "subService",
    "departmentName",
    "budgetId",
    "grossAmount",
    "commission",
    "netAdjustment",
    "month",
    "year",
]
_PERIOD_MONTH_YEAR_RE = re.compile(r"^(1[0-2]|0?[1-9])\/(\d{4})$")


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
            detail="Invalid tenant net spend dataRange format",
        ) from exc


def _normalize_sheet_cell(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    return value


def _build_sheet_values(rows: list[dict[str, object]]) -> list[list[object]]:
    values: list[list[object]] = [_NET_SPEND_HEADER]
    for row in rows:
        values.append([_normalize_sheet_cell(row.get(key)) for key in _NET_SPEND_HEADER])
    return values


def _resolve_period_from_config(period_value: object) -> tuple[int, int] | None:
    raw = str(period_value or "").strip()
    if not raw:
        return None

    if "," in raw:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Only one period is accepted for netSpendSyncOptions.period",
                "value": raw,
                "expected": "M/YYYY",
            },
        )

    month_year_match = _PERIOD_MONTH_YEAR_RE.fullmatch(raw)
    if month_year_match:
        return int(month_year_match.group(1)), int(month_year_match.group(2))

    raise HTTPException(
        status_code=400,
        detail={
            "message": "Invalid netSpendSyncOptions.period format",
            "value": raw,
            "expected": ["1/2026", "01/2026"],
        },
    )


def _resolve_period_from_params(
    *,
    month: int | None,
    year: int | None,
) -> tuple[int, int] | None:
    using_params = month is not None or year is not None
    if not using_params:
        return None

    if month is None or year is None:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "month and year must be provided together",
                "requiredFields": ["month", "year"],
            },
        )

    if month < 1 or month > 12:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Invalid month",
                "value": month,
                "expected": "1..12",
            },
        )
    if year < 1000 or year > 9999:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Invalid year",
                "value": year,
                "expected": "4-digit year, for example 2026",
            },
        )

    return month, year


# ============================================================
# ROUTES
# ============================================================


@router.post("/load", summary="Load FundSphere net spend data from DB to sheet")
def load_net_spend_route(
    body: NetSpendLoadRequest | None = Body(default=None),
):
    """
    Load net spend data for a specific period from DB and overwrite the configured
    net spend output range in Google Sheets.

    Example request:
        POST /api/fundsphere/v1/masterBudgetControl/netSpend/load
        Header: X-Tenant-Id: taaa

    Example request (use request body, skip netSpendSyncOptions.period):
        POST /api/fundsphere/v1/masterBudgetControl/netSpend/load
        Body:
          {
            "month": 3,
            "year": 2026
          }
        Header: X-Tenant-Id: taaa

    Example response:
        {
          "meta": {
            "requestId": "...",
            "durationMs": 58,
            "timestamp": "2026-03-27T09:20:30.000Z"
          },
          "data": {
            "spreadsheetId": "1npAa9vNyn8355-VcfZ7sp5l5dvMEv-YRrXc3rpez_C8",
            "sheetName": "DIGITAL SERVICES & NET BUDGETS",
            "clearRange": "'DIGITAL SERVICES & NET BUDGETS'!A3:L",
            "writeRange": "'DIGITAL SERVICES & NET BUDGETS'!A3:L124",
            "month": 3,
            "year": 2026,
            "dbRowCount": 121,
            "writtenRowCount": 121
          }
        }

    Example response (period is empty):
        {
          "meta": {
            "requestId": "...",
            "durationMs": 5,
            "timestamp": "2026-03-28T09:20:30.000Z"
          },
          "data": {
            "spreadsheetId": "1npAa9vNyn8355-VcfZ7sp5l5dvMEv-YRrXc3rpez_C8",
            "sheetName": "0.0 Settings",
            "skipped": true,
            "reason": "netSpendSyncOptions.period is empty",
            "dbRowCount": 0,
            "writtenRowCount": 0
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Tenant config must include SPREADSHEETS.netSpend.id
        - Tenant config must include SPREADSHEETS.netSpend.budgetDataSheetName
        - Tenant config must include SPREADSHEETS.netSpend.budgetDataOutputRange
        - Reads target period from SPREADSHEETS.netSpendSyncOptions.period
        - Optional request body fields `month` + `year` can be used to bypass
          SPREADSHEETS.netSpendSyncOptions.period
        - `month` and `year` must be provided together when using request body
        - If netSpendSyncOptions.period is empty, no sheet/DB action is executed
        - Supported period formats: 1/2026 or 01/2026
        - Only one period is accepted (comma-separated values are not allowed)
        - Output columns are:
          accountCode, accountName, serviceId, serviceName, subService,
          departmentName, budgetId, grossAmount, commission, netAdjustment,
          month, year
        - Net spend query logic is hard-coded in backend DB helper
    """
    settings = get_fundsphere_net_spend_settings()
    spreadsheet_id = str(settings["net_spend_spreadsheet_id"])
    sheet_name = str(settings["net_spend_sheet_name"])
    data_range = str(settings["net_spend_data_range"])
    body_month = body.month if body is not None else None
    body_year = body.year if body is not None else None
    resolved_period = _resolve_period_from_params(month=body_month, year=body_year)
    period_source = "body" if resolved_period is not None else "config"
    if resolved_period is None:
        resolved_period = _resolve_period_from_config(settings.get("net_spend_period"))
    if resolved_period is None:
        return {
            "spreadsheetId": spreadsheet_id,
            "sheetName": sheet_name,
            "periodSource": "config",
            "skipped": True,
            "reason": "netSpendSyncOptions.period is empty",
            "dbRowCount": 0,
            "writtenRowCount": 0,
        }
    month, year = resolved_period

    db_rows = get_master_budget_control_net_spend_data(month=month, year=year)
    values = _build_sheet_values(db_rows)

    start_col, start_row, configured_end_col = _parse_output_range(data_range)
    start_col_index = _column_label_to_index(start_col)
    configured_end_col_index = _column_label_to_index(configured_end_col)
    configured_column_count = configured_end_col_index - start_col_index + 1
    if len(_NET_SPEND_HEADER) > configured_column_count:
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Net spend dataRange is too narrow for output columns",
                "dataRange": data_range,
                "columnCount": len(_NET_SPEND_HEADER),
                "capacity": configured_column_count,
            },
        )

    clear_range = f"'{sheet_name}'!{data_range}"
    _clear_sheet_values(
        spreadsheet_id=spreadsheet_id,
        range_name=clear_range,
        app_name="FundSphere",
    )

    end_row = start_row + len(values) - 1
    write_end_col = _column_index_to_label(start_col_index + len(_NET_SPEND_HEADER) - 1)
    write_range = f"'{sheet_name}'!{start_col}{start_row}:{write_end_col}{end_row}"
    _write_sheet_values(
        spreadsheet_id=spreadsheet_id,
        range_name=write_range,
        values=values,
        app_name="FundSphere",
        value_input_option="USER_ENTERED",
    )

    return {
        "spreadsheetId": spreadsheet_id,
        "sheetName": sheet_name,
        "periodSource": period_source,
        "clearRange": clear_range,
        "writeRange": write_range,
        "month": month,
        "year": year,
        "dbRowCount": len(db_rows),
        "writtenRowCount": len(db_rows),
    }
