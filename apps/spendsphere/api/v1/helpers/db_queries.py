from datetime import datetime, date
import pytz

from shared.db import execute_write, fetch_all
from shared.utils import get_current_period
from shared.tenant import get_timezone
from apps.spendsphere.api.v1.helpers.config import get_db_tables, get_service_budgets


# ============================================================
# ACCOUNTS
# ============================================================

def _resolve_period(
    month: int | None,
    year: int | None,
) -> tuple[int, int]:
    if month is not None and year is not None:
        return month, year
    period = get_current_period()
    return period["month"], period["year"]


def get_accounts(
    account_codes: str | list[str] | None = None,
    *,
    include_all: bool = False,
) -> list[dict]:
    """
    Fetch accounts from DB.

    - account_codes is None or empty -> return all active accounts (or all when include_all=True)
    - account_codes is str -> return that account if exists
    - account_codes is list[str] -> return matching accounts
    """

    # -----------------------------------------
    # Normalize input
    # -----------------------------------------
    if account_codes is None:
        codes = None
    elif isinstance(account_codes, str):
        code = account_codes.strip()
        codes = [code.upper()] if code else None
    elif isinstance(account_codes, list):
        codes = [
            c.strip().upper() for c in account_codes if isinstance(c, str) and c.strip()
        ]
        codes = codes or None
    else:
        raise TypeError("account_codes must be None, str, or list[str]")

    # -----------------------------------------
    # Build query
    # -----------------------------------------
    tables = get_db_tables()
    accounts_table = tables["ACCOUNTS"]
    if not codes:
        query = f"""
            SELECT code, name
            FROM {accounts_table}
        """
        if not include_all:
            query += " WHERE active = 1"
        params = ()
    else:
        placeholders = ",".join(["%s"] * len(codes))
        query = f"""
            SELECT code, name
            FROM {accounts_table}
            WHERE UPPER(code) IN ({placeholders})
        """
        if not include_all:
            query += " AND active = 1"
        params = tuple(codes)

    return fetch_all(query, params)


# ============================================================
# BUDGETS
# ============================================================

def get_masterbudgets(
    account_codes: list[str] | None = None,
    month: int | None = None,
    year: int | None = None,
) -> list[dict]:
    """
    Get master budgets for the current month/year.

    - Filters by configured service budgets (always)
    - Optionally filters by one or more account codes
    """
    if isinstance(account_codes, str):
        account_codes = [account_codes]

    month, year = _resolve_period(month, year)

    service_budgets = get_service_budgets()
    if not service_budgets:
        return []

    service_placeholders = ", ".join(["%s"] * len(service_budgets))

    tables = get_db_tables()
    budgets_table = tables["BUDGETS"]
    services_table = tables["SERVICES"]

    query = (
        "SELECT "
        "b.accountCode, "
        "b.serviceId, "
        "s.name AS serviceName, "
        "b.subService, "
        "b.month, "
        "b.year, "
        "b.netAmount "
        f"FROM {budgets_table} AS b "
        f"JOIN {services_table} AS s ON s.id = b.serviceId "
        "WHERE b.month = %s "
        "AND b.year = %s "
        f"AND b.serviceId IN ({service_placeholders})"
    )

    params: list = [month, year, *service_budgets]

    if account_codes:
        placeholders = ", ".join(["%s"] * len(account_codes))
        query += f" AND b.accountCode IN ({placeholders})"
        params.extend(account_codes)

    return fetch_all(query, tuple(params))


# ============================================================
# ALLOCATIONS
# ============================================================

def get_allocations(
    account_codes: list[str] | None = None,
    month: int | None = None,
    year: int | None = None,
) -> list[dict]:
    """
    Get SpendShare allocations for the current month/year.
    """
    if isinstance(account_codes, str):
        account_codes = [account_codes]

    month, year = _resolve_period(month, year)

    tables = get_db_tables()
    allocations_table = tables["ALLOCATIONS"]

    query = (
        "SELECT "
        "id, "
        "accountCode, "
        "ggBudgetId, "
        "allocation "
        f"FROM {allocations_table} "
        "WHERE month = %s "
        "AND year = %s"
    )

    params: list = [month, year]

    if account_codes:
        placeholders = ", ".join(["%s"] * len(account_codes))
        query += f" AND accountCode IN ({placeholders})"
        params.extend(account_codes)

    return fetch_all(query, tuple(params))


def duplicate_allocations(
    *,
    from_month: int,
    from_year: int,
    to_month: int,
    to_year: int,
    overwrite: bool = False,
) -> int:
    """
    Duplicate allocations from one month/year to another for active accounts only.
    Skips rows that already exist in the target month/year.
    """
    tables = get_db_tables()
    allocations_table = tables["ALLOCATIONS"]
    accounts_table = tables["ACCOUNTS"]

    query = (
        f"INSERT INTO {allocations_table} "
        "(id, accountCode, ggBudgetId, allocation, month, year) "
        "SELECT UUID(), a.accountCode, a.ggBudgetId, a.allocation, %s, %s "
        f"FROM {allocations_table} AS a "
        f"JOIN {accounts_table} AS acc ON acc.code = a.accountCode "
        f"LEFT JOIN {allocations_table} AS t "
        "ON t.accountCode = a.accountCode "
        "AND t.ggBudgetId = a.ggBudgetId "
        "AND t.month = %s "
        "AND t.year = %s "
        "WHERE a.month = %s "
        "AND a.year = %s "
        "AND acc.active = 1 "
    )

    params: list = [to_month, to_year, to_month, to_year, from_month, from_year]

    if not overwrite:
        query += "AND t.id IS NULL"
    else:
        query += "ON DUPLICATE KEY UPDATE allocation = VALUES(allocation)"

    return execute_write(query, tuple(params))


# ============================================================
# ROLLBREAKDOWNS
# ============================================================

def get_rollbreakdowns(
    account_codes: list[str] | None = None,
    month: int | None = None,
    year: int | None = None,
) -> list[dict]:
    """
    Get SpendShare roll breakdowns for one or more accounts
    for the current month/year.
    """
    if isinstance(account_codes, str):
        account_codes = [account_codes]

    month, year = _resolve_period(month, year)

    tables = get_db_tables()
    rollbreakdowns_table = tables["ROLLBREAKDOWNS"]

    query = (
        "SELECT "
        "id, "
        "accountCode, "
        "adTypeCode, "
        "amount "
        f"FROM {rollbreakdowns_table} "
        "WHERE month = %s "
        "AND year = %s"
    )

    params: list = [month, year]

    if account_codes:
        placeholders = ", ".join(["%s"] * len(account_codes))
        query += f" AND accountCode IN ({placeholders})"
        params.extend(account_codes)

    return fetch_all(query, tuple(params))


# ============================================================
# ACCELERATIONS
# ============================================================


def get_accelerations(
    account_codes: list[str] | None = None,
    *,
    today: date | None = None,
    include_all: bool = False,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    """
    Get accelerations for today (active only by default).
    """
    if isinstance(account_codes, str):
        account_codes = [account_codes]

    if today is None and not include_all and not (start_date and end_date):
        tz = pytz.timezone(get_timezone())
        today = datetime.now(tz).date()

    tables = get_db_tables()
    accelerations_table = tables["ACCELERATIONS"]

    query = (
        "SELECT "
        "id, "
        "accountCode, "
        "scopeType, "
        "scopeValue, "
        "startDate, "
        "endDate, "
        "multiplier, "
        "dateCreated, "
        "dateUpdated "
        f"FROM {accelerations_table} "
    )

    params: list = []

    if not include_all:
        query += "WHERE active = 1"
        if start_date and end_date:
            query += " AND startDate <= %s AND endDate >= %s"
            params.extend([end_date, start_date])
        else:
            query += " AND startDate <= %s AND endDate >= %s"
            params.extend([today, today])

    if account_codes:
        placeholders = ", ".join(["%s"] * len(account_codes))
        query += f"{' AND' if params else ' WHERE'} accountCode IN ({placeholders})"
        params.extend(account_codes)

    return fetch_all(query, tuple(params))


def insert_accelerations(rows: list[dict]) -> int:
    if not rows:
        return 0

    tables = get_db_tables()
    accelerations_table = tables["ACCELERATIONS"]

    row_placeholder = "(%s, %s, %s, %s, %s, %s, 1)"
    placeholders = ", ".join([row_placeholder] * len(rows))
    query = (
        f"INSERT INTO {accelerations_table} "
        "(accountCode, scopeType, scopeValue, startDate, endDate, multiplier, active) "
        f"VALUES {placeholders}"
    )

    params: list = []
    for r in rows:
        params.extend(
            [
                r.get("accountCode"),
                r.get("scopeType"),
                r.get("scopeValue"),
                r.get("startDate"),
                r.get("endDate"),
                r.get("multiplier"),
            ]
        )

    return execute_write(query, tuple(params))


def update_accelerations(rows: list[dict]) -> int:
    if not rows:
        return 0

    tables = get_db_tables()
    accelerations_table = tables["ACCELERATIONS"]

    key_expr = "(accountCode, scopeType, scopeValue, startDate, endDate)"
    case_parts: list[str] = []
    params: list = []

    for r in rows:
        case_parts.append(f"WHEN {key_expr} = (%s, %s, %s, %s, %s) THEN %s")
        params.extend(
            [
                r.get("accountCode"),
                r.get("scopeType"),
                r.get("scopeValue"),
                r.get("startDate"),
                r.get("endDate"),
                r.get("multiplier"),
            ]
        )

    in_placeholders = ", ".join(["(%s, %s, %s, %s, %s)"] * len(rows))
    for r in rows:
        params.extend(
            [
                r.get("accountCode"),
                r.get("scopeType"),
                r.get("scopeValue"),
                r.get("startDate"),
                r.get("endDate"),
            ]
        )

    query = (
        f"UPDATE {accelerations_table} "
        f"SET multiplier = CASE {' '.join(case_parts)} ELSE multiplier END "
        f"WHERE {key_expr} IN ({in_placeholders})"
    )

    return execute_write(query, tuple(params))


def soft_delete_accelerations(rows: list[dict]) -> int:
    if not rows:
        return 0

    tables = get_db_tables()
    accelerations_table = tables["ACCELERATIONS"]

    key_expr = "(accountCode, scopeType, scopeValue, startDate, endDate)"
    placeholders = ", ".join(["(%s, %s, %s, %s, %s)"] * len(rows))
    query = (
        f"UPDATE {accelerations_table} "
        "SET active = 0 "
        f"WHERE active = 1 AND {key_expr} IN ({placeholders})"
    )

    params: list = []
    for r in rows:
        params.extend(
            [
                r.get("accountCode"),
                r.get("scopeType"),
                r.get("scopeValue"),
                r.get("startDate"),
                r.get("endDate"),
            ]
        )

    return execute_write(query, tuple(params))
