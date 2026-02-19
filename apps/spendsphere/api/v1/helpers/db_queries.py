from datetime import datetime, date
import pytz

from shared.db import execute_many, execute_write, fetch_all
from shared.utils import get_current_period
from shared.tenant import get_timezone
from apps.spendsphere.api.v1.helpers.config import get_db_tables, get_service_budgets


def _resolve_period(
    month: int | None,
    year: int | None,
) -> tuple[int, int]:
    if month is not None and year is not None:
        return month, year
    period = get_current_period()
    return period["month"], period["year"]


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
    account_codes: list[str],
    overwrite: bool = False,
) -> int:
    """
    Duplicate allocations from one month/year to another.
    - Duplicates only for accounts active in the target period.
    - Skips rows that already exist in the target month/year unless overwrite=True.
    - Never duplicates rows with zero allocation.
    """
    from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
        validate_account_codes,
    )

    tables = get_db_tables()
    allocations_table = tables["ALLOCATIONS"]
    normalized_codes = [
        str(code).strip().upper()
        for code in account_codes
        if isinstance(code, str) and str(code).strip()
    ]
    if not normalized_codes:
        return 0

    active_accounts = validate_account_codes(
        None,
        month=to_month,
        year=to_year,
    )
    active_codes = {
        str(account.get("code", "")).strip().upper()
        for account in active_accounts
        if isinstance(account, dict) and str(account.get("code", "")).strip()
    }
    eligible_codes = [code for code in normalized_codes if code in active_codes]
    if not eligible_codes:
        return 0

    query = (
        f"INSERT INTO {allocations_table} "
        "(id, accountCode, ggBudgetId, allocation, month, year) "
        "SELECT UUID(), a.accountCode, a.ggBudgetId, a.allocation, %s, %s "
        f"FROM {allocations_table} AS a "
        f"LEFT JOIN {allocations_table} AS t "
        "ON t.accountCode = a.accountCode "
        "AND t.ggBudgetId = a.ggBudgetId "
        "AND t.month = %s "
        "AND t.year = %s "
        "WHERE a.month = %s "
        "AND a.year = %s "
    )

    params: list = [to_month, to_year, to_month, to_year, from_month, from_year]

    if eligible_codes:
        placeholders = ", ".join(["%s"] * len(eligible_codes))
        query += f"AND a.accountCode IN ({placeholders}) "
        params.extend(eligible_codes)

    query += "AND a.allocation <> 0 "

    if not overwrite:
        query += "AND t.id IS NULL"
    else:
        query += "ON DUPLICATE KEY UPDATE allocation = VALUES(allocation)"

    return execute_write(query, tuple(params))


def upsert_allocations(
    rows: list[dict],
    *,
    month: int,
    year: int,
) -> dict[str, int]:
    if not rows:
        return {"updated": 0, "inserted": 0}

    tables = get_db_tables()
    allocations_table = tables["ALLOCATIONS"]

    update_rows = [r for r in rows if r.get("id")]
    insert_rows = [r for r in rows if not r.get("id")]

    updated = 0
    inserted = 0

    if update_rows:
        update_query = (
            f"UPDATE {allocations_table} "
            "SET allocation = %s "
            "WHERE id = %s AND accountCode = %s AND month = %s AND year = %s"
        )
        update_params = [
            (
                row.get("allocation"),
                row.get("id"),
                row.get("accountCode"),
                month,
                year,
            )
            for row in update_rows
        ]
        updated = execute_many(update_query, update_params)

    if insert_rows:
        insert_query = (
            f"INSERT INTO {allocations_table} "
            "(id, accountCode, ggBudgetId, allocation, month, year) "
            "VALUES (UUID(), %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE allocation = VALUES(allocation)"
        )
        insert_params = [
            (
                row.get("accountCode"),
                row.get("ggBudgetId"),
                row.get("allocation"),
                month,
                year,
            )
            for row in insert_rows
        ]
        inserted = execute_many(insert_query, insert_params)

    return {"updated": updated, "inserted": inserted}


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


def upsert_rollbreakdowns(
    rows: list[dict],
    *,
    month: int,
    year: int,
) -> dict[str, int]:
    if not rows:
        return {"updated": 0, "inserted": 0}

    tables = get_db_tables()
    rollbreakdowns_table = tables["ROLLBREAKDOWNS"]

    update_rows = [r for r in rows if r.get("id")]
    insert_rows = [r for r in rows if not r.get("id")]

    updated = 0
    inserted = 0

    if update_rows:
        update_query = (
            f"UPDATE {rollbreakdowns_table} "
            "SET amount = %s "
            "WHERE id = %s AND accountCode = %s AND month = %s AND year = %s"
        )
        update_params = [
            (
                row.get("amount"),
                row.get("id"),
                row.get("accountCode"),
                month,
                year,
            )
            for row in update_rows
        ]
        updated = execute_many(update_query, update_params)

    if insert_rows:
        insert_query = (
            f"INSERT INTO {rollbreakdowns_table} "
            "(id, accountCode, adTypeCode, amount, month, year) "
            "VALUES (UUID(), %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE amount = VALUES(amount)"
        )
        insert_params = [
            (
                row.get("accountCode"),
                row.get("adTypeCode"),
                row.get("amount"),
                month,
                year,
            )
            for row in insert_rows
        ]
        inserted = execute_many(insert_query, insert_params)

    return {"updated": updated, "inserted": inserted}


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
        "scopeLevel, "
        "scopeValue, "
        "startDate, "
        "endDate, "
        "multiplier, "
        "note, "
        "active, "
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


def get_accelerations_by_ids(ids: list[object]) -> list[dict]:
    return _get_accelerations_by_ids(ids)


def get_accelerations_by_ids_active(ids: list[object]) -> list[dict]:
    return _get_accelerations_by_ids(ids, only_active=True)


def _get_accelerations_by_ids(
    ids: list[object],
    *,
    only_active: bool = False,
) -> list[dict]:
    if not ids:
        return []

    tables = get_db_tables()
    accelerations_table = tables["ACCELERATIONS"]

    placeholders = ", ".join(["%s"] * len(ids))
    where_clause = f"WHERE id IN ({placeholders})"
    if only_active:
        where_clause += " AND active = 1"

    query = (
        "SELECT "
        "id, "
        "accountCode, "
        "scopeLevel, "
        "scopeValue, "
        "startDate, "
        "endDate, "
        "multiplier, "
        "note, "
        "active, "
        "dateCreated, "
        "dateUpdated "
        f"FROM {accelerations_table} "
        f"{where_clause}"
    )

    return fetch_all(query, tuple(ids))


def get_accelerations_by_keys(rows: list[dict]) -> list[dict]:
    if not rows:
        return []

    tables = get_db_tables()
    accelerations_table = tables["ACCELERATIONS"]

    key_expr = "(accountCode, scopeLevel, scopeValue, startDate, endDate)"
    placeholders = ", ".join(["(%s, %s, %s, %s, %s)"] * len(rows))
    query = (
        "SELECT "
        "id, "
        "accountCode, "
        "scopeLevel, "
        "scopeValue, "
        "startDate, "
        "endDate, "
        "multiplier, "
        "note, "
        "active, "
        "dateCreated, "
        "dateUpdated "
        f"FROM {accelerations_table} "
        f"WHERE {key_expr} IN ({placeholders})"
    )

    params: list = []
    for r in rows:
        params.extend(
            [
                r.get("accountCode"),
                r.get("scopeLevel"),
                r.get("scopeValue"),
                r.get("startDate"),
                r.get("endDate"),
            ]
        )

    return fetch_all(query, tuple(params))


def insert_accelerations(rows: list[dict]) -> int:
    if not rows:
        return 0

    tables = get_db_tables()
    accelerations_table = tables["ACCELERATIONS"]

    row_placeholder = "(%s, %s, %s, %s, %s, %s, %s, 1)"
    placeholders = ", ".join([row_placeholder] * len(rows))
    query = (
        f"INSERT INTO {accelerations_table} "
        "(accountCode, scopeLevel, scopeValue, startDate, endDate, multiplier, note, active) "
        f"VALUES {placeholders}"
    )

    params: list = []
    for r in rows:
        params.extend(
            [
                r.get("accountCode"),
                r.get("scopeLevel"),
                r.get("scopeValue"),
                r.get("startDate"),
                r.get("endDate"),
                r.get("multiplier"),
                r.get("note"),
            ]
        )

    return execute_write(query, tuple(params))


def update_acceleration_by_id(row: dict) -> int:
    if not row:
        return 0

    acceleration_id = row.get("id")
    if not acceleration_id:
        return 0

    tables = get_db_tables()
    accelerations_table = tables["ACCELERATIONS"]

    set_parts = [
        "scopeLevel = %s",
        "scopeValue = %s",
        "startDate = %s",
        "endDate = %s",
        "multiplier = %s",
    ]
    params: list = [
        row.get("scopeLevel"),
        row.get("scopeValue"),
        row.get("startDate"),
        row.get("endDate"),
        row.get("multiplier"),
    ]

    if row.get("_note_provided"):
        set_parts.append("note = %s")
        params.append(row.get("note"))

    params.append(acceleration_id)
    query = (
        f"UPDATE {accelerations_table} "
        f"SET {', '.join(set_parts)} "
        "WHERE id = %s"
    )

    return execute_write(query, tuple(params))


def update_accelerations(rows: list[dict]) -> int:
    if not rows:
        return 0

    tables = get_db_tables()
    accelerations_table = tables["ACCELERATIONS"]

    key_expr = "(accountCode, scopeLevel, scopeValue, startDate, endDate)"
    case_parts: list[str] = []
    note_case_parts: list[str] = []
    params: list = []
    note_params: list = []

    for r in rows:
        case_parts.append(f"WHEN {key_expr} = (%s, %s, %s, %s, %s) THEN %s")
        params.extend(
            [
                r.get("accountCode"),
                r.get("scopeLevel"),
                r.get("scopeValue"),
                r.get("startDate"),
                r.get("endDate"),
                r.get("multiplier"),
            ]
        )
        if r.get("_note_provided"):
            note_case_parts.append(
                f"WHEN {key_expr} = (%s, %s, %s, %s, %s) THEN %s"
            )
            note_params.extend(
                [
                    r.get("accountCode"),
                    r.get("scopeLevel"),
                    r.get("scopeValue"),
                    r.get("startDate"),
                    r.get("endDate"),
                    r.get("note"),
                ]
            )

    in_placeholders = ", ".join(["(%s, %s, %s, %s, %s)"] * len(rows))
    params.extend(note_params)
    for r in rows:
        params.extend(
            [
                r.get("accountCode"),
                r.get("scopeLevel"),
                r.get("scopeValue"),
                r.get("startDate"),
                r.get("endDate"),
            ]
        )

    set_parts = [
        f"multiplier = CASE {' '.join(case_parts)} ELSE multiplier END"
    ]
    if note_case_parts:
        set_parts.append(f"note = CASE {' '.join(note_case_parts)} ELSE note END")

    query = (
        f"UPDATE {accelerations_table} "
        f"SET {', '.join(set_parts)} "
        f"WHERE {key_expr} IN ({in_placeholders})"
    )

    return execute_write(query, tuple(params))


def get_existing_acceleration_keys(rows: list[dict]) -> set[tuple]:
    if not rows:
        return set()

    tables = get_db_tables()
    accelerations_table = tables["ACCELERATIONS"]

    key_expr = "(accountCode, scopeLevel, scopeValue, startDate, endDate)"
    placeholders = ", ".join(["(%s, %s, %s, %s, %s)"] * len(rows))
    query = (
        "SELECT accountCode, scopeLevel, scopeValue, startDate, endDate "
        f"FROM {accelerations_table} "
        f"WHERE {key_expr} IN ({placeholders})"
    )

    params: list = []
    for r in rows:
        params.extend(
            [
                r.get("accountCode"),
                r.get("scopeLevel"),
                r.get("scopeValue"),
                r.get("startDate"),
                r.get("endDate"),
            ]
        )

    results = fetch_all(query, tuple(params))
    return {
        (
            row.get("accountCode"),
            row.get("scopeLevel"),
            row.get("scopeValue"),
            row.get("startDate"),
            row.get("endDate"),
        )
        for row in results
    }


def soft_delete_accelerations(rows: list[dict]) -> int:
    if not rows:
        return 0

    tables = get_db_tables()
    accelerations_table = tables["ACCELERATIONS"]

    key_expr = "(accountCode, scopeLevel, scopeValue, startDate, endDate)"
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
                r.get("scopeLevel"),
                r.get("scopeValue"),
                r.get("startDate"),
                r.get("endDate"),
            ]
        )

    return execute_write(query, tuple(params))


def soft_delete_accelerations_by_ids(ids: list[object]) -> int:
    if not ids:
        return 0

    tables = get_db_tables()
    accelerations_table = tables["ACCELERATIONS"]

    placeholders = ", ".join(["%s"] * len(ids))
    query = (
        f"UPDATE {accelerations_table} "
        "SET active = 0 "
        f"WHERE active = 1 AND id IN ({placeholders})"
    )

    return execute_write(query, tuple(ids))
