# functions/repository.py
from typing import Optional, Tuple, List
from functions.db import get_connection
from functions.constants import SERVICE_BUDGETS
from functions.util import get_current_period
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

def get_master_budgets() -> List[dict]:
    """
    Get budgets for the current month/year filtered by service IDs
    """
    period = get_current_period()
    month = period["month"]
    year = period["year"]

    # Build placeholders for IN clause safely
    in_placeholders = ", ".join(["%s"] * len(SERVICE_BUDGETS))

    query = (
        "SELECT "
        "b.accountCode, "
        "b.serviceId, "
        "s.name AS serviceName, "
        "b.subService, b.month, b.year,"
        "b.netAmount "
        "FROM Budgets AS b "
        "JOIN Services AS s ON s.id = b.serviceId "
        "WHERE b.month = %s "
        "AND b.year = %s "
        f"AND b.serviceId IN ({in_placeholders})"
    )

    params = (
        month,
        year,
        *SERVICE_BUDGETS
    )

    return fetch_all(query, params)
def get_master_budgets_by_account(account_code: str) -> List[dict]:
    """
    Get budgets for an account for the current month/year
    """
    period = get_current_period()
    month = period["month"]
    year = period["year"]

    # Build placeholders for IN clause safely
    in_placeholders = ", ".join(["%s"] * len(SERVICE_BUDGETS))

    query = (
        "SELECT "
        "b.accountCode, "
        "b.serviceId, "
        "s.name AS serviceName, "
        "b.subService, "
        "b.netAmount "
        "FROM Budgets AS b "
        "JOIN Services AS s ON s.id = b.serviceId "
        "WHERE b.accountCode = %s "
        "AND b.month = %s "
        "AND b.year = %s "
        f"AND b.serviceId IN ({in_placeholders})"
    )

    params = (
        account_code,
        month,
        year,
        *SERVICE_BUDGETS
    )

    return fetch_all(query, params)

def get_allocations_by_account(account_code: str) -> List[dict]:
    """
    Get SpendShare allocations for an account for the current month/year
    """
    period = get_current_period()
    month = period["month"]
    year = period["year"]

    query = (
        "SELECT "
        "id, "
        "ggBudgetId, "
        "allocation "
        "FROM SpendShere_Allocations "
        "WHERE accountCode = %s "
        "AND month = %s "
        "AND year = %s"
    )

    params = (
        account_code,
        month,
        year
    )

    return fetch_all(query, params)

def get_rollbreakdowns_by_account(account_code: str) -> List[dict]:
    """
    Get SpendShare roll breakdowns for an account for the current month/year
    """
    period = get_current_period()
    month = period["month"]
    year = period["year"]

    query = (
        "SELECT "
        "id, "
        "adTypeCode, "
        "amount "
        "FROM SpendShere_RollBreakdowns "
        "WHERE accountCode = %s "
        "AND month = %s "
        "AND year = %s"
    )

    params = (
        account_code,
        month,
        year
    )

    return fetch_all(query, params)

def getRollovers(account_code: str):
    SPREADSHEET_ID = "1wbKImoY7_fv_dcn_bA9pL7g9htrWMKXB0EqVu09-KEQ"
    RANGE_NAME = "0.0 LowcoderRollover"

    data = read_spreadsheet(
        spreadsheet_id=SPREADSHEET_ID,
        range_name=RANGE_NAME
    )

    if not data:
        return []

    # Normalize input
    account_code = account_code.strip().upper()
    period = get_current_period()
    current_month = period["month"]
    current_year = period["year"]

    filtered = [
        row for row in data
        if row.get("accountCode", "").strip().upper() == account_code
        and int(row.get("month", 0)) == current_month
        and int(row.get("year", 0)) == current_year
    ]

    return filtered



