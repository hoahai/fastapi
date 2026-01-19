from shared.ggSheet import _read_sheet_raw
from shared.utils import get_current_period
from apps.spendsphere.api.v1.helpers.config import get_spendsphere_sheets


def _get_sheet(name: str) -> dict[str, str]:
    sheets = get_spendsphere_sheets()
    if name not in sheets:
        raise ValueError(f"Unknown sheet: {name}")
    return sheets[name]


def get_sheet_raw(name: str) -> list[dict]:
    sheet = _get_sheet(name)
    return _read_sheet_raw(
        spreadsheet_id=sheet["spreadsheet_id"],
        range_name=sheet["range_name"],
    )


def get_rollovers(
    account_codes: list[str] | None = None,
) -> list[dict]:
    """
    Get rollover data for the current month/year.

    NOTE:
    - Must NOT be called in a process that uses threads
    """
    sheet = _get_sheet("rollovers")

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


def get_active_period(
    account_codes: list[str] | None = None,
) -> list[dict]:
    """
    Get active period data.

    NOTE:
    - Must NOT be called in a process that uses threads
    """
    sheet = _get_sheet("active_period")

    data = _read_sheet_raw(
        spreadsheet_id=sheet["spreadsheet_id"],
        range_name=sheet["range_name"],
    )

    if not data:
        return []

    if isinstance(account_codes, str):
        account_codes = [account_codes]

    normalized_accounts = (
        {c.strip().upper() for c in account_codes}
        if account_codes
        else None
    )

    last_rows: dict[str, dict] = {}
    last_index: dict[str, int] = {}

    for idx, row in enumerate(data):
        account_code = row.get("accountCode", "").strip().upper()
        if not account_code:
            continue
        if normalized_accounts is not None and account_code not in normalized_accounts:
            continue
        last_rows[account_code] = row
        last_index[account_code] = idx

    return [
        last_rows[code]
        for code in sorted(last_index, key=last_index.get)
    ]
