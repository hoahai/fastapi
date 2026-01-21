from shared.db import fetch_all
from shared.utils import get_current_period
from apps.spendsphere.api.v1.helpers.config import get_service_budgets


# ============================================================
# ACCOUNTS
# ============================================================


def get_accounts(account_codes: str | list[str] | None = None) -> list[dict]:
    """
    Fetch accounts from DB.

    - account_codes is None or empty -> return ALL accounts
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
    if not codes:
        query = """
            SELECT code, name
            FROM Accounts
        """
        params = ()
    else:
        placeholders = ",".join(["%s"] * len(codes))
        query = f"""
            SELECT code, name
            FROM Accounts
            WHERE UPPER(code) IN ({placeholders})
        """
        params = tuple(codes)

    return fetch_all(query, params)


# ============================================================
# BUDGETS
# ============================================================

def get_masterbudgets(
    account_codes: list[str] | None = None,
) -> list[dict]:
    """
    Get master budgets for the current month/year.

    - Filters by configured service budgets (always)
    - Optionally filters by one or more account codes
    """
    if isinstance(account_codes, str):
        account_codes = [account_codes]

    period = get_current_period()
    month = period["month"]
    year = period["year"]

    service_budgets = get_service_budgets()
    if not service_budgets:
        return []

    service_placeholders = ", ".join(["%s"] * len(service_budgets))

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
        "accountCode, "
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


# ============================================================
# ROLLBREAKDOWNS
# ============================================================

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
        "accountCode, "
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
