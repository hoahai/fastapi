from apps.spendsphere.api.v1.helpers import db_queries as v1_db_queries


# ============================================================
# ACCOUNTS
# ============================================================

def get_accounts(
    account_codes: str | list[str] | None = None,
    *,
    include_all: bool = False,
) -> list[dict]:
    return v1_db_queries.get_accounts(account_codes, include_all=include_all)


# ============================================================
# BUDGETS
# ============================================================

def get_masterbudgets(
    account_codes: list[str] | None = None,
) -> list[dict]:
    """
    V2 override example.

    Update this function when V2 needs different budget queries.
    """
    return v1_db_queries.get_masterbudgets(account_codes)


# ============================================================
# ALLOCATIONS
# ============================================================

def get_allocations(
    account_codes: list[str] | None = None,
) -> list[dict]:
    return v1_db_queries.get_allocations(account_codes)


# ============================================================
# ROLLBREAKDOWNS
# ============================================================

def get_rollbreakdowns(
    account_codes: list[str] | None = None,
) -> list[dict]:
    return v1_db_queries.get_rollbreakdowns(account_codes)
