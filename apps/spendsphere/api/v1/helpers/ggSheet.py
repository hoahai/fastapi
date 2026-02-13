import calendar
from datetime import date, datetime

import pytz

from shared.ggSheet import _read_sheet_raw
from shared.utils import get_current_period
from shared.tenant import get_timezone
from apps.spendsphere.api.v1.helpers.config import get_spendsphere_sheets
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
    get_google_sheet_cache_entry,
    set_google_sheet_cache,
)


def _get_sheet(name: str) -> dict[str, str]:
    sheets = get_spendsphere_sheets()
    if name not in sheets:
        raise ValueError(f"Unknown sheet: {name}")
    return sheets[name]


def _build_sheet_cache_hash(sheet: dict[str, str]) -> str:
    return f"{sheet['spreadsheet_id']}::{sheet['range_name']}"


def _get_sheet_data(
    name: str,
    *,
    refresh_cache: bool = False,
) -> list[dict]:
    sheet = _get_sheet(name)
    config_hash = _build_sheet_cache_hash(sheet)

    if not refresh_cache:
        cached, is_stale = get_google_sheet_cache_entry(
            name,
            config_hash=config_hash,
        )
        if cached is not None and not is_stale:
            return cached

    data = _read_sheet_raw(
        spreadsheet_id=sheet["spreadsheet_id"],
        range_name=sheet["range_name"],
    )
    set_google_sheet_cache(name, data, config_hash=config_hash)
    return data


def refresh_google_sheet_cache(name: str) -> list[dict]:
    return _get_sheet_data(name, refresh_cache=True)


def _coerce_date(value: object) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str) and value.strip():
        cleaned = value.strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(cleaned, fmt).date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(cleaned).date()
        except ValueError:
            return None
    return None


def _is_rollable(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if cleaned in {"0", "false", "no", "n", "off"}:
            return False
        if cleaned in {"1", "true", "yes", "y", "on"}:
            return True
        try:
            return int(cleaned) != 0
        except ValueError:
            return True
    return True


def _normalize_rollable_value(value: object) -> int:
    return 1 if _is_rollable(value) else 0


def get_rollovers(
    account_codes: list[str] | None = None,
    month: int | None = None,
    year: int | None = None,
    include_unrollable: bool = False,
) -> list[dict]:
    """
    Get rollover data for the current month/year.

    NOTE:
    - Must NOT be called in a process that uses threads
    """
    data = _get_sheet_data("rollovers")

    if not data:
        return []

    if isinstance(account_codes, str):
        account_codes = [account_codes]

    if month is None or year is None:
        period = get_current_period()
        month = period["month"]
        year = period["year"]

    normalized_accounts = (
        {c.strip().upper() for c in account_codes}
        if account_codes
        else None
    )

    results: list[dict] = []
    for row in data:
        if int(row.get("month", 0)) != month:
            continue
        if int(row.get("year", 0)) != year:
            continue
        rollable_value = _normalize_rollable_value(row.get("rollable"))
        if not include_unrollable and rollable_value == 0:
            continue
        if normalized_accounts is not None and (
            row.get("accountCode", "").strip().upper() not in normalized_accounts
        ):
            continue
        results.append({**row, "rollable": rollable_value})
    return results


def get_active_period(
    account_codes: list[str] | None = None,
    month: int | None = None,
    year: int | None = None,
    *,
    as_of: date | None = None,
) -> list[dict]:
    """
    Get active period data.

    NOTE:
    - Must NOT be called in a process that uses threads
    """
    data = _get_sheet_data("active_period")

    if not data:
        return []

    if isinstance(account_codes, str):
        account_codes = [account_codes]

    normalized_accounts = (
        {c.strip().upper() for c in account_codes}
        if account_codes
        else None
    )

    if as_of is None and (month is None or year is None):
        tz = pytz.timezone(get_timezone())
        as_of = datetime.now(tz).date()

    if as_of is not None:
        if isinstance(as_of, datetime):
            as_of = as_of.date()
        if not isinstance(as_of, date):
            return []

        rows_by_account: dict[str, list[tuple[date, int, dict]]] = {}
        active_rows_by_account: dict[str, list[tuple[int, dict]]] = {}
        next_start_by_account: dict[str, date] = {}

        for idx, row in enumerate(data):
            account_code = row.get("accountCode", "").strip().upper()
            if not account_code:
                continue
            if normalized_accounts is not None and account_code not in normalized_accounts:
                continue

            start_date = _coerce_date(row.get("startDate"))
            end_date = _coerce_date(row.get("endDate"))
            start_key = start_date or date.min
            rows_by_account.setdefault(account_code, []).append((start_key, idx, row))

            start_ok = True if start_date is None else start_date <= as_of
            end_ok = True if end_date is None else end_date >= as_of
            if start_ok and end_ok:
                active_rows_by_account.setdefault(account_code, []).append((idx, row))

            if start_date and start_date > as_of:
                existing = next_start_by_account.get(account_code)
                if existing is None or start_date < existing:
                    next_start_by_account[account_code] = start_date

        results: list[tuple[int, dict]] = []
        for rows in rows_by_account.values():
            rows.sort(key=lambda item: (item[0], item[1]))

            account_code = rows[0][2].get("accountCode", "").strip().upper()
            active_rows = active_rows_by_account.get(account_code, [])
            if len(active_rows) > 1:
                raise ValueError(
                    "Overlapping active_period rows for "
                    f"{account_code} on {as_of.isoformat()}"
                )

            if active_rows:
                selected_idx, selected_row = active_rows[0]
            else:
                selected_idx = rows[0][1]
                selected_row = rows[0][2]
                for start_key, idx, row in rows:
                    if start_key <= as_of:
                        selected_idx = idx
                        selected_row = row
                    else:
                        break

            start_date = _coerce_date(selected_row.get("startDate"))
            end_date = _coerce_date(selected_row.get("endDate"))
            start_ok = True if start_date is None else start_date <= as_of
            end_ok = True if end_date is None else end_date >= as_of
            is_active = start_ok and end_ok
            next_start = next_start_by_account.get(account_code)

            results.append(
                (
                    selected_idx,
                    {
                        **selected_row,
                        "isActive": is_active,
                        **(
                            {"nextStartDate": next_start.isoformat()}
                            if next_start
                            else {}
                        ),
                    },
                )
            )

        return [row for _, row in sorted(results, key=lambda item: item[0])]

    if not isinstance(month, int) or not isinstance(year, int):
        return []

    month_start = date(year, month, 1)
    month_end = date(year, month, calendar.monthrange(year, month)[1])

    last_rows: dict[str, dict] = {}
    last_index: dict[str, int] = {}

    for idx, row in enumerate(data):
        account_code = row.get("accountCode", "").strip().upper()
        if not account_code:
            continue
        if normalized_accounts is not None and account_code not in normalized_accounts:
            continue
        start_date = _coerce_date(row.get("startDate"))
        end_date = _coerce_date(row.get("endDate"))
        start_ok = True if start_date is None else start_date <= month_end
        end_ok = True if end_date is None else end_date >= month_start
        if not (start_ok and end_ok):
            continue
        last_rows[account_code] = row
        last_index[account_code] = idx

    return [last_rows[code] for code in sorted(last_index, key=last_index.get)]
