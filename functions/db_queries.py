# functions/db_queries.py

from functions.db import get_connection
from functions.constants import SERVICE_BUDGETS
from functions.utils import get_current_period


# =====================
# GENERIC DB HELPERS
# =====================

def fetch_all(query: str, params: tuple | None = None) -> list[dict]:
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


# =====================
# DOMAIN QUERIES
# =====================

def get_accounts(limit: int = 10) -> list[dict]:
    query = """
        SELECT code, name
        FROM Accounts
        LIMIT %s
    """
    return fetch_all(query, (limit,))


def get_masterbudgets(
    account_codes: list[str] | None = None,
) -> list[dict]:
    """
    Get master budgets for the current month/year.

    - Filters by SERVICE_BUDGETS (always)
    - Optionally filters by one or more account codes
    """
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

    params: list = [month, year, *SERVICE_BUDGETS]

    if account_codes:
        placeholders = ", ".join(["%s"] * len(account_codes))
        query += f" AND b.accountCode IN ({placeholders})"
        params.extend(account_codes)

    return fetch_all(query, tuple(params))


def get_allocations(
    account_codes: list[str] | None = None,
) -> list[dict]:
    """
    Get SpendShare allocations for the current month/year.
    """
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

    params: list = [month, year]

    if account_codes:
        placeholders = ", ".join(["%s"] * len(account_codes))
        query += f" AND accountCode IN ({placeholders})"
        params.extend(account_codes)

    return fetch_all(query, tuple(params))


def get_rollbreakdowns(
    account_codes: list[str] | None = None,
) -> list[dict]:
    """
    Get SpendShare roll breakdowns for one or more accounts
    for the current month/year.
    """
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

    params: list = [month, year]

    if account_codes:
        placeholders = ", ".join(["%s"] * len(account_codes))
        query += f" AND accountCode IN ({placeholders})"
        params.extend(account_codes)

    return fetch_all(query, tuple(params))

