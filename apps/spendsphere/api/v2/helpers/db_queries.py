from apps.spendsphere.api.v1.helpers import db_queries as v1_db_queries


def get_accounts(account_codes: str | list[str] | None = None) -> list[dict]:
    return v1_db_queries.get_accounts(account_codes)


def get_masterbudgets(
    account_codes: list[str] | None = None,
) -> list[dict]:
    """
    V2 override example.

    Update this function when V2 needs different budget queries.
    """
    return v1_db_queries.get_masterbudgets(account_codes)


def get_allocations(
    account_codes: list[str] | None = None,
) -> list[dict]:
    return v1_db_queries.get_allocations(account_codes)


def get_rollbreakdowns(
    account_codes: list[str] | None = None,
) -> list[dict]:
    return v1_db_queries.get_rollbreakdowns(account_codes)
