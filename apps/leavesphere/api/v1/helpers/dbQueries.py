from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from apps.leavesphere.api.v1.helpers.config import get_db_tables
from apps.leavesphere.api.v1.helpers.ptoAccounting import is_request_action_code, request_action_sql, signed_pto_hours
from shared.db import execute_write, fetch_all, run_transaction


def _build_where_clauses(filters: list[tuple[str, object]]) -> tuple[str, tuple[object, ...]]:
    active = [(clause, value) for clause, value in filters if value is not None]
    if not active:
        return "", tuple()
    where = " WHERE " + " AND ".join(clause for clause, _ in active)
    params = tuple(value for _, value in active)
    return where, params


def _build_in_clause(column: str, values: list[object]) -> tuple[str, tuple[object, ...]]:
    active = [value for value in values if value is not None and str(value).strip()]
    if not active:
        return "", tuple()
    placeholders = ", ".join(["%s"] * len(active))
    return f" WHERE {column} IN ({placeholders})", tuple(active)


def _build_pto_transaction_select_clause() -> str:
    columns = [
        "dateCreated",
        "dateUpdated",
        "id",
        "employeeId",
        "ptoTypeCode",
        "ptoActionCode",
        "hours",
        "year",
        "startDate",
        "endDate",
        "status",
        "description",
        "approverNote",
        "approverId",
        "calendarId",
    ]
    return ", ".join(columns)


def _build_pto_transaction_insert_query(item: dict) -> tuple[str, tuple[object, ...]]:
    tables = get_db_tables()
    columns = [
        "id",
        "employeeId",
        "ptoTypeCode",
        "ptoActionCode",
        "hours",
        "year",
        "startDate",
        "endDate",
        "status",
        "description",
        "approverNote",
        "approverId",
        "calendarId",
    ]
    params: list[object] = [
        item["id"],
        item["employeeId"],
        item["ptoTypeCode"],
        item["ptoActionCode"],
        item["hours"],
        item["year"],
        item.get("startDate"),
        item.get("endDate"),
        item["status"],
        item.get("description"),
        item.get("approverNote"),
        item.get("approverId"),
        item.get("calendarId"),
    ]
    placeholders = ", ".join(["%s"] * len(columns))
    query = f"INSERT INTO {tables['PTOTRANSACTIONS']} (" + ", ".join(columns) + f") VALUES ({placeholders})"
    return query, tuple(params)


def _build_pto_transaction_update_query(*, updates: dict) -> tuple[str, tuple[object, ...]]:
    tables = get_db_tables()
    fields: list[str] = []
    params: list[object] = []
    for key in (
        "employeeId",
        "ptoTypeCode",
        "ptoActionCode",
        "hours",
        "year",
        "startDate",
        "endDate",
        "status",
        "description",
        "approverNote",
        "approverId",
        "calendarId",
    ):
        if key not in updates:
            continue
        fields.append(f"{key} = %s")
        params.append(updates[key])
    if not fields:
        return "", tuple()
    fields.append("dateUpdated = %s")
    params.append(datetime.utcnow())
    query = f"UPDATE {tables['PTOTRANSACTIONS']} SET " + ", ".join(fields)
    return query, tuple(params)


def get_employees(*, employee_id: str | None = None) -> list[dict]:
    tables = get_db_tables()
    where, params = _build_where_clauses([("id = %s", employee_id)])
    query = (
        "SELECT "
        "dateCreated, dateUpdated, id, identityKey, firstName, lastName, email, "
        "phone, dob, pictureUrl, region, startDate, title, isAE, active "
        f"FROM {tables['EMPLOYEES']}{where} "
        "ORDER BY lastName ASC, firstName ASC"
    )
    return fetch_all(query, params)


def get_employees_by_email(*, email: str) -> list[dict]:
    tables = get_db_tables()
    query = (
        "SELECT "
        "dateCreated, dateUpdated, id, identityKey, firstName, lastName, email, "
        "phone, dob, pictureUrl, region, startDate, title, isAE, active "
        f"FROM {tables['EMPLOYEES']} "
        "WHERE LOWER(TRIM(email)) = LOWER(TRIM(%s)) "
        "ORDER BY active DESC, dateUpdated DESC, lastName ASC, firstName ASC, id ASC"
    )
    return fetch_all(query, (email,))


def get_employees_by_ids(*, employee_ids: list[str]) -> list[dict]:
    tables = get_db_tables()
    where, params = _build_in_clause("id", employee_ids)
    query = (
        "SELECT "
        "dateCreated, dateUpdated, id, identityKey, firstName, lastName, email, "
        "phone, dob, pictureUrl, region, startDate, title, isAE, active "
        f"FROM {tables['EMPLOYEES']}{where} "
        "ORDER BY lastName ASC, firstName ASC"
    )
    return fetch_all(query, params)


def get_employees_by_identity_key(*, identity_key: str) -> list[dict]:
    tables = get_db_tables()
    query = (
        "SELECT "
        "dateCreated, dateUpdated, id, identityKey, firstName, lastName, email, "
        "phone, dob, pictureUrl, region, startDate, title, isAE, active "
        f"FROM {tables['EMPLOYEES']} "
        "WHERE identityKey = %s "
        "ORDER BY lastName ASC, firstName ASC"
    )
    return fetch_all(query, (identity_key,))


def insert_employee(item: dict) -> int:
    tables = get_db_tables()
    query = (
        f"INSERT INTO {tables['EMPLOYEES']} ("
        "id, identityKey, firstName, lastName, email, phone, dob, pictureUrl, "
        "region, startDate, title, isAE, active"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    params = (
        item["id"],
        item["identityKey"],
        item["firstName"],
        item["lastName"],
        item["email"],
        item.get("phone"),
        item.get("dob"),
        item.get("pictureUrl"),
        item["region"],
        item.get("startDate"),
        item.get("title"),
        item["isAE"],
        item["active"],
    )
    return execute_write(query, params)


def update_employee(*, employee_id: str, updates: dict) -> int:
    tables = get_db_tables()
    fields: list[str] = []
    params: list[object] = []
    for key in (
        "identityKey",
        "firstName",
        "lastName",
        "email",
        "phone",
        "dob",
        "pictureUrl",
        "region",
        "startDate",
        "title",
        "isAE",
        "active",
    ):
        if key not in updates:
            continue
        fields.append(f"{key} = %s")
        params.append(updates[key])
    if not fields:
        return 0
    fields.append("dateUpdated = %s")
    params.append(datetime.utcnow())
    params.append(employee_id)
    query = f"UPDATE {tables['EMPLOYEES']} SET " + ", ".join(fields) + " WHERE id = %s"
    return execute_write(query, tuple(params))


def get_employee_managers(
    *,
    mapping_id: str | None = None,
    employee_id: str | None = None,
    manager_id: str | None = None,
) -> list[dict]:
    tables = get_db_tables()
    where, params = _build_where_clauses(
        [
            ("id = %s", mapping_id),
            ("employeeId = %s", employee_id),
            ("managerId = %s", manager_id),
        ]
    )
    query = (
        "SELECT dateCreated, dateUpdated, id, employeeId, managerId "
        f"FROM {tables['EMPLOYEEMANAGERS']}{where} "
        "ORDER BY employeeId ASC, managerId ASC"
    )
    return fetch_all(query, params)


def insert_employee_manager(*, item: dict) -> int:
    tables = get_db_tables()
    query = (
        f"INSERT INTO {tables['EMPLOYEEMANAGERS']} (id, employeeId, managerId) "
        "VALUES (%s, %s, %s)"
    )
    params = (item["id"], item["employeeId"], item["managerId"])
    return execute_write(query, params)


def delete_employee_manager(*, mapping_id: str) -> int:
    tables = get_db_tables()
    query = f"DELETE FROM {tables['EMPLOYEEMANAGERS']} WHERE id = %s"
    return execute_write(query, (mapping_id,))


def get_pto_types(*, code: str | None = None) -> list[dict]:
    tables = get_db_tables()
    where, params = _build_where_clauses([("code = %s", code)])
    query = (
        "SELECT "
        "dateCreated, dateUpdated, code, name, rolloverable, payoutable, "
        "listingOrder, usaDefaultHour, phlDefaultHour "
        f"FROM {tables['PTOTYPES']}{where} "
        "ORDER BY listingOrder ASC, code ASC"
    )
    return fetch_all(query, params)


def insert_pto_type(item: dict) -> int:
    tables = get_db_tables()
    query = (
        f"INSERT INTO {tables['PTOTYPES']} ("
        "code, name, rolloverable, payoutable, listingOrder, usaDefaultHour, phlDefaultHour"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )
    params = (
        item["code"],
        item["name"],
        item["rolloverable"],
        item["payoutable"],
        item.get("listingOrder"),
        item["usaDefaultHour"],
        item["phlDefaultHour"],
    )
    return execute_write(query, params)


def update_pto_type(*, code: str, updates: dict) -> int:
    tables = get_db_tables()
    fields: list[str] = []
    params: list[object] = []
    for key in (
        "name",
        "rolloverable",
        "payoutable",
        "listingOrder",
        "usaDefaultHour",
        "phlDefaultHour",
    ):
        if key not in updates:
            continue
        fields.append(f"{key} = %s")
        params.append(updates[key])
    if not fields:
        return 0
    fields.append("dateUpdated = %s")
    params.append(datetime.utcnow())
    params.append(code)
    query = f"UPDATE {tables['PTOTYPES']} SET " + ", ".join(fields) + " WHERE code = %s"
    return execute_write(query, tuple(params))


def get_pto_actions(*, code: str | None = None) -> list[dict]:
    tables = get_db_tables()
    where, params = _build_where_clauses([("code = %s", code)])
    query = (
        "SELECT dateCreated, dateUpdated, code, name, color "
        f"FROM {tables['PTOACTIONS']}{where} "
        "ORDER BY code ASC"
    )
    return fetch_all(query, params)


def get_holidays(*, year: int | None = None) -> list[dict]:
    tables = get_db_tables()
    filters: list[tuple[str, object]] = []
    if year is not None:
        filters.append(("date >= %s", f"{year}-01-01"))
        filters.append(("date <= %s", f"{year}-12-31"))
    where, params = _build_where_clauses(filters)
    query = (
        "SELECT dateCreated, dateUpdated, id, name, date, region AS teamRegion, year, hours, active "
        f"FROM {tables['HOLIDAYS']}{where} "
        "ORDER BY date ASC, name ASC, id ASC"
    )
    return fetch_all(query, params)


def upsert_holiday(item: dict) -> int:
    tables = get_db_tables()
    region = item.get("region") or item.get("teamRegion")
    query = (
        f"INSERT INTO {tables['HOLIDAYS']} (id, name, date, region) "
        "VALUES (%s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "name = VALUES(name), "
        "date = VALUES(date), "
        "region = VALUES(region), "
        "dateUpdated = CURRENT_TIMESTAMP"
    )
    params = (
        item["id"],
        item["name"],
        item["date"],
        region,
    )
    return execute_write(query, params)


def insert_pto_action(item: dict) -> int:
    tables = get_db_tables()
    query = f"INSERT INTO {tables['PTOACTIONS']} (code, name, color) VALUES (%s, %s, %s)"
    return execute_write(query, (item["code"], item["name"], item.get("color")))


def update_pto_action(*, code: str, updates: dict) -> int:
    tables = get_db_tables()
    fields: list[str] = []
    params: list[object] = []
    for key in ("name", "color"):
        if key not in updates:
            continue
        fields.append(f"{key} = %s")
        params.append(updates[key])
    if not fields:
        return 0
    fields.append("dateUpdated = %s")
    params.append(datetime.utcnow())
    params.append(code)
    query = f"UPDATE {tables['PTOACTIONS']} SET " + ", ".join(fields) + " WHERE code = %s"
    return execute_write(query, tuple(params))


def get_pto_transactions(
    *,
    transaction_id: str | None = None,
    employee_id: str | None = None,
    employee_ids: list[str] | None = None,
    pto_type_code: str | None = None,
    year: int | None = None,
    status: str | None = None,
    start_date_from: str | None = None,
    start_date_to: str | None = None,
    end_date_from: str | None = None,
    end_date_to: str | None = None,
) -> list[dict]:
    tables = get_db_tables()
    clauses: list[tuple[str, object]] = [
        ("id = %s", transaction_id),
        ("employeeId = %s", employee_id),
        ("ptoTypeCode = %s", pto_type_code),
        ("year = %s", year),
        ("status = %s", status),
        ("startDate >= %s", start_date_from),
        ("startDate <= %s", start_date_to),
        ("endDate >= %s", end_date_from),
        ("endDate <= %s", end_date_to),
    ]
    where, params = _build_where_clauses(clauses)
    if employee_ids:
        employee_where, employee_params = _build_in_clause("employeeId", employee_ids)
        if employee_where:
            if where:
                where = where + " AND " + employee_where[len(" WHERE ") :]
                params = params + employee_params
            else:
                where = employee_where
                params = employee_params
    select_clause = _build_pto_transaction_select_clause()
    query = (
        f"SELECT {select_clause} "
        f"FROM {tables['PTOTRANSACTIONS']}{where} "
        "ORDER BY year DESC, startDate DESC, dateCreated DESC"
    )
    return fetch_all(query, params)


def insert_pto_transaction(item: dict) -> int:
    query, params = _build_pto_transaction_insert_query(item)
    return execute_write(query, params)


def execute_pto_transaction_insert(cursor, item: dict) -> int:
    query, params = _build_pto_transaction_insert_query(item)
    cursor.execute(query, params)
    return int(cursor.rowcount or 0)


def update_pto_transaction(*, transaction_id: str, updates: dict) -> int:
    query, params = _build_pto_transaction_update_query(updates=updates)
    if not query:
        return 0
    query = f"{query} WHERE id = %s"
    params = params + (transaction_id,)
    return execute_write(query, tuple(params))


def create_pto_request_transaction(*, item: dict, requested_hours: Decimal) -> int:
    tables = get_db_tables()

    def _work(cursor) -> int:
        cursor.execute(
            f"SELECT id FROM {tables['EMPLOYEES']} WHERE id = %s LIMIT 1 FOR UPDATE",
            (item["employeeId"],),
        )
        if cursor.fetchone() is None:
            raise ValueError("employeeId not found")

        cursor.execute(
            "SELECT ptoActionCode, hours, status "
            f"FROM {tables['PTOTRANSACTIONS']} "
            "WHERE employeeId = %s AND ptoTypeCode = %s AND year = %s "
            "FOR UPDATE",
            (item["employeeId"], item["ptoTypeCode"], item["year"]),
        )
        rows = cursor.fetchall() or []

        approved = Decimal("0.00")
        pending = Decimal("0.00")
        for row in rows:
            action_code = row[0]
            hours = Decimal(str(row[1] or 0)).quantize(Decimal("0.01"))
            status = str(row[2] or "").strip().capitalize()
            if status == "Approved":
                approved += signed_pto_hours(action_code, hours)
            elif status == "Pending" and is_request_action_code(action_code):
                pending += abs(hours)
        available = approved - pending
        if available - requested_hours < 0:
            raise ValueError("Requested hours exceed available balance")

        return execute_pto_transaction_insert(cursor, item)

    return int(run_transaction(_work) or 0)


def cancel_pending_pto_transaction(*, transaction_id: str) -> int:
    tables = get_db_tables()

    def _work(cursor) -> int:
        cursor.execute(
            f"SELECT status FROM {tables['PTOTRANSACTIONS']} WHERE id = %s LIMIT 1 FOR UPDATE",
            (transaction_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError("PTO transaction not found")

        status = str(row[0] or "").strip()
        if status != "Pending":
            raise ValueError("Only Pending PTO transactions can be canceled")

        cursor.execute(
            f"UPDATE {tables['PTOTRANSACTIONS']} "
            "SET status = %s, dateUpdated = %s "
            "WHERE id = %s",
            ("Canceled", datetime.utcnow(), transaction_id),
        )
        return int(cursor.rowcount or 0)

    return int(run_transaction(_work) or 0)


def approve_pending_pto_request(
    *,
    transaction_id: str,
    approver_id: str | None,
    approverNote: str | None,
) -> int:
    tables = get_db_tables()

    def _work(cursor) -> int:
        cursor.execute(
            "SELECT employeeId, ptoTypeCode, year, ptoActionCode, hours, status "
            f"FROM {tables['PTOTRANSACTIONS']} "
            "WHERE id = %s "
            "LIMIT 1 FOR UPDATE",
            (transaction_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError("PTO transaction not found")

        employee_id, pto_type_code, year, action_code_raw, hours_raw, status = row
        hours = Decimal(str(hours_raw or 0)).quantize(Decimal("0.01"))
        status_text = str(status or "").strip().capitalize()
        if status_text != "Pending":
            raise ValueError("Only Pending PTO transactions can be approved")
        if not is_request_action_code(action_code_raw):
            raise ValueError("Only PTO requests can be approved")

        cursor.execute(
            "SELECT ptoActionCode, hours, status "
            f"FROM {tables['PTOTRANSACTIONS']} "
            "WHERE employeeId = %s AND ptoTypeCode = %s AND year = %s "
            "FOR UPDATE",
            (employee_id, pto_type_code, year),
        )
        balance_rows = cursor.fetchall() or []
        approved = Decimal("0.00")
        pending = Decimal("0.00")
        for balance_row in balance_rows:
            balance_action_code = balance_row[0]
            balance_hours = Decimal(str(balance_row[1] or 0)).quantize(Decimal("0.01"))
            balance_status = str(balance_row[2] or "").strip().capitalize()
            if balance_status == "Approved":
                approved += signed_pto_hours(balance_action_code, balance_hours)
            elif balance_status == "Pending" and is_request_action_code(balance_action_code):
                pending += abs(balance_hours)
        available = approved - pending
        if available < 0:
            raise ValueError("Cannot approve request because available balance is below zero")

        fields = ["status = %s", "approverId = %s", "dateUpdated = %s"]
        params: list[object] = ["Approved", approver_id, datetime.utcnow()]
        if approverNote is not None:
            fields.append("approverNote = %s")
            params.append(approverNote)
        params.append(transaction_id)
        cursor.execute(
            f"UPDATE {tables['PTOTRANSACTIONS']} "
            f"SET {', '.join(fields)} "
            "WHERE id = %s",
            tuple(params),
        )
        return int(cursor.rowcount or 0)

    return int(run_transaction(_work) or 0)


def reject_pending_pto_request(
    *,
    transaction_id: str,
    approver_id: str | None,
    approverNote: str | None,
) -> int:
    tables = get_db_tables()

    def _work(cursor) -> int:
        cursor.execute(
            "SELECT ptoActionCode, hours, status "
            f"FROM {tables['PTOTRANSACTIONS']} "
            "WHERE id = %s "
            "LIMIT 1 FOR UPDATE",
            (transaction_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError("PTO transaction not found")

        action_code = row[0]
        status_text = str(row[2] or "").strip().capitalize()
        if status_text != "Pending":
            raise ValueError("Only Pending PTO transactions can be rejected")
        if not is_request_action_code(action_code):
            raise ValueError("Only PTO requests can be rejected")

        fields = ["status = %s", "approverId = %s", "dateUpdated = %s"]
        params: list[object] = ["Rejected", approver_id, datetime.utcnow()]
        if approverNote is not None:
            fields.append("approverNote = %s")
            params.append(approverNote)
        params.append(transaction_id)
        cursor.execute(
            f"UPDATE {tables['PTOTRANSACTIONS']} "
            f"SET {', '.join(fields)} "
            "WHERE id = %s",
            tuple(params),
        )
        return int(cursor.rowcount or 0)

    return int(run_transaction(_work) or 0)


def get_pto_balances(
    *,
    employee_id: str | None = None,
    pto_type_code: str | None = None,
    year: int | None = None,
) -> list[dict]:
    tables = get_db_tables()
    filters: list[tuple[str, object]] = []
    if employee_id is not None:
        filters.append(("employeeId = %s", employee_id))
    if pto_type_code is not None:
        filters.append(("ptoTypeCode = %s", pto_type_code))
    if year is not None:
        filters.append(("year = %s", year))

    where, params = _build_where_clauses(filters)
    query = (
        "SELECT "
        "employeeId, "
        "ptoTypeCode, "
        "year, "
        f"CAST(SUM(CASE WHEN status = 'Approved' AND {request_action_sql()} THEN -ABS(hours) "
        "WHEN status = 'Approved' THEN hours ELSE 0 END) AS DECIMAL(10,2)) AS approvedBalanceHours, "
        f"CAST(SUM(CASE WHEN status = 'Pending' AND {request_action_sql()} THEN ABS(hours) ELSE 0 END) AS DECIMAL(10,2)) AS pendingRequestHours, "
        "CAST("
        f"SUM(CASE WHEN status = 'Approved' AND {request_action_sql()} THEN -ABS(hours) "
        "WHEN status = 'Approved' THEN hours ELSE 0 END) - "
        f"SUM(CASE WHEN status = 'Pending' AND {request_action_sql()} THEN ABS(hours) ELSE 0 END) "
        "AS DECIMAL(10,2)"
        ") AS availableBalanceHours "
        f"FROM {tables['PTOTRANSACTIONS']}{where} "
        "GROUP BY employeeId, ptoTypeCode, year "
        "ORDER BY employeeId ASC, ptoTypeCode ASC, year ASC"
    )
    return fetch_all(query, params)
