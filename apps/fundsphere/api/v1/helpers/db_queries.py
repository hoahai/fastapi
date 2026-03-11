from shared.db import fetch_all


# ============================================================
# ACCOUNTS
# ============================================================


def get_accounts() -> list[dict]:
    query = (
        "SELECT code, name, active "
        "FROM Accounts "
        "ORDER BY active DESC, code ASC"
    )
    return fetch_all(query)
