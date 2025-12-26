# functions/repository.py
from typing import Optional, Tuple, List, Optional, Sequence
from collections.abc import Sequence as SeqABC
from functions.db import get_connection
from functions.constants import SERVICE_BUDGETS
from functions.utils import get_current_period
from functions.ggSheet import read_spreadsheet

def fetch_all(query: str, params: Optional[Tuple] = None) -> list[dict]:
    """
    Generic SELECT query executor
    """
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(query, params)
        return cursor.fetchall()
    finally:
        cursor.close()
        conn.close()


# -------------------------------
# Domain-specific queries
# -------------------------------
def get_accounts(limit: int = 10) -> list[dict]:
    query = """
        SELECT code, name
        FROM Accounts
        LIMIT %s
    """
    return fetch_all(query, (limit,))

def get_masterbudgets(
    account_codes: Optional[Sequence[str]] = None
) -> List[dict]:
    """
    Get master budgets for the current month/year.
    
    - Filters by SERVICE_BUDGETS (always)
    - Optionally filters by one or more account codes
    """
    # Normalize string input defensively
    if isinstance(account_codes, str):
        account_codes = [account_codes]

    period = get_current_period()
    month = period["month"]
    year = period["year"]

    service_placeholders = ", ".join(["%s"] * len(SERVICE_BUDGETS))

    query = (
        "SELECT "
        "b.accountCode, "
        "b.serviceId, "
        "s.name AS serviceName, "
        "b.subService, "
        "b.month, "
        "b.year, "
        "b.netAmount "
        "FROM Budgets AS b "
        "JOIN Services AS s ON s.id = b.serviceId "
        "WHERE b.month = %s "
        "AND b.year = %s "
        f"AND b.serviceId IN ({service_placeholders})"
    )

    params = [month, year, *SERVICE_BUDGETS]

    if account_codes:
        account_placeholders = ", ".join(["%s"] * len(account_codes))
        query += f" AND b.accountCode IN ({account_placeholders})"
        params.extend(account_codes)

    return fetch_all(query, tuple(params))

def get_allocations(
    account_codes: Optional[Sequence[str]] = None
) -> List[dict]:
    """
    Get SpendShare allocations for the current month/year.

    - Optionally filtered by one or more account codes
    """
    # Normalize string input defensively
    if isinstance(account_codes, str):
        account_codes = [account_codes]
    
    period = get_current_period()
    month = period["month"]
    year = period["year"]

    query = (
        "SELECT "
        "id, "
        "ggBudgetId, "
        "allocation "
        "FROM SpendShere_Allocations "
        "WHERE month = %s "
        "AND year = %s"
    )

    params = [month, year]

    if account_codes:
        placeholders = ", ".join(["%s"] * len(account_codes))
        query += f" AND accountCode IN ({placeholders})"
        params.extend(account_codes)

    return fetch_all(query, tuple(params))

def get_rollbreakdowns(
    account_codes: Optional[Sequence[str]] = None
) -> List[dict]:
    """
    Get SpendShare roll breakdowns for one or more accounts
    for the current month/year.
    
    If account_codes is None or empty, results are not filtered
    by accountCode.
    """
    # Normalize string input defensively
    if isinstance(account_codes, str):
        account_codes = [account_codes]
    
    period = get_current_period()
    month = period["month"]
    year = period["year"]

    query = (
        "SELECT "
        "id, "
        "adTypeCode, "
        "amount "
        "FROM SpendShere_RollBreakdowns "
        "WHERE month = %s "
        "AND year = %s"
    )

    params = [month, year]

    if account_codes:
        placeholders = ", ".join(["%s"] * len(account_codes))
        query += f" AND accountCode IN ({placeholders})"
        params.extend(account_codes)

    return fetch_all(query, tuple(params))

def get_rollovers(
    account_codes: Optional[Sequence[str]] = None
) -> List[dict]:
    SPREADSHEET_ID = "1wbKImoY7_fv_dcn_bA9pL7g9htrWMKXB0EqVu09-KEQ"
    RANGE_NAME = "0.0 LowcoderRollover"

    data = read_spreadsheet(
        spreadsheet_id=SPREADSHEET_ID,
        range_name=RANGE_NAME
    )

    if not data:
        return []

    # Normalize string input defensively
    if isinstance(account_codes, str):
        account_codes = [account_codes]
    
    period = get_current_period()
    current_month = period["month"]
    current_year = period["year"]

    # Normalize account codes once
    normalized_accounts = (
        {code.strip().upper() for code in account_codes}
        if account_codes
        else None
    )

    filtered = [
        row for row in data
        if int(row.get("month", 0)) == current_month
        and int(row.get("year", 0)) == current_year
        and (
            normalized_accounts is None
            or row.get("accountCode", "").strip().upper() in normalized_accounts
        )
    ]

    return filtered



