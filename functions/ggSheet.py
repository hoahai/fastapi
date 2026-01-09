# functions/ggSheet.py

from typing import Callable
from google.oauth2 import service_account
from googleapiclient.discovery import build

from functions.utils import get_current_period, resolve_secret_path

# =====================================================
# CONFIG
# =====================================================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# All sheet metadata lives here (single source of truth)
SHEETS: dict[str, dict[str, str]] = {
    "rollovers": {
        "spreadsheet_id": "1wbKImoY7_fv_dcn_bA9pL7g9htrWMKXB0EqVu09-KEQ",
        "range_name": "0.0 LowcoderRollover",
    },
    # Future sheets go here
    # "allocations": {...},
    # "budgets": {...},
}

# =====================================================
# INTERNAL CLIENT (DO NOT USE THREADS)
# =====================================================

def _get_sheets_service():
    """
    Create Google Sheets service.

    IMPORTANT:
    - Must be called in a process that does NOT create threads
    """
    cred_path = resolve_secret_path(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "service-account.json",
    )

    credentials = service_account.Credentials.from_service_account_file(
        cred_path,
        scopes=SCOPES,
    )

    return build(
        "sheets",
        "v4",
        credentials=credentials,
        cache_discovery=False,  # critical on macOS
    )


def _read_sheet_raw(
    spreadsheet_id: str,
    range_name: str,
) -> list[dict]:
    """
    Low-level sheet reader.
    Returns raw rows as list[dict].
    """
    service = _get_sheets_service()

    result = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
        )
        .execute()
    )

    rows = result.get("values", [])
    if not rows:
        return []

    headers = rows[0]
    data_rows = rows[1:]

    return [dict(zip(headers, row)) for row in data_rows]

# =====================================================
# PUBLIC SHEET FUNCTIONS
# =====================================================

def get_rollovers(
    account_codes: list[str] | None = None,
) -> list[dict]:
    """
    Get rollover data for the current month/year.

    NOTE:
    - Must NOT be called in a process that uses threads
    """
    sheet = SHEETS["rollovers"]

    data = _read_sheet_raw(
        spreadsheet_id=sheet["spreadsheet_id"],
        range_name=sheet["range_name"],
    )

    if not data:
        return []

    if isinstance(account_codes, str):
        account_codes = [account_codes]

    period = get_current_period()
    month = period["month"]
    year = period["year"]

    normalized_accounts = (
        {c.strip().upper() for c in account_codes}
        if account_codes
        else None
    )

    return [
        row
        for row in data
        if int(row.get("month", 0)) == month
        and int(row.get("year", 0)) == year
        and (
            normalized_accounts is None
            or row.get("accountCode", "").strip().upper() in normalized_accounts
        )
    ]

# =====================================================
# FUTURE FUNCTIONS (EXAMPLES)
# =====================================================

def get_sheet_raw(name: str) -> list[dict]:
    """
    Generic access if you just want raw data by name.
    """
    if name not in SHEETS:
        raise ValueError(f"Unknown sheet: {name}")

    sheet = SHEETS[name]
    return _read_sheet_raw(
        spreadsheet_id=sheet["spreadsheet_id"],
        range_name=sheet["range_name"],
    )
