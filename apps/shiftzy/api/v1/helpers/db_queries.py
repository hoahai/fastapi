from __future__ import annotations

from datetime import date, datetime, time as time_type, timedelta
from uuid import uuid4

from apps.shiftzy.api.v1.helpers.config import get_db_tables, get_schedule_sections
from apps.shiftzy.api.v1.helpers.weeks import build_week_info
from shared.db import execute_many, fetch_all, run_transaction


# ============================================================
# HELPERS
# ============================================================

def _ensure_list(items: list[dict] | dict, *, name: str) -> list[dict]:
    if isinstance(items, dict):
        return [items]
    if not isinstance(items, list):
        raise TypeError(f"{name} must be dict or list[dict]")
    return items


def _normalize_bool(value: object, *, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        value = value.strip().lower()
        if value in {"1", "true", "yes", "y"}:
            return True
        if value in {"0", "false", "no", "n"}:
            return False
    raise ValueError("active must be a boolean-like value")


def _parse_date(value: date | datetime | str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value.strip())
    raise TypeError("date must be date, datetime, or ISO string")


def _parse_time(value: time_type | datetime | timedelta | str) -> time_type:
    if isinstance(value, time_type) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.time()
    if isinstance(value, timedelta):
        total_seconds = int(value.total_seconds())
        if total_seconds < 0:
            raise ValueError("time must be non-negative")
        seconds = total_seconds % 86400
        hours, remainder = divmod(seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        return time_type(hour=hours, minute=minutes, second=secs)
    if isinstance(value, str):
        text = value.strip()
        try:
            return datetime.strptime(text, "%H:%M:%S").time()
        except ValueError:
            return datetime.strptime(text, "%H:%M").time()
    raise TypeError("time must be time, datetime, or string")


def _format_duration(seconds: int) -> str:
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02}:{minutes:02}:{secs:02}"


def _format_time_value(value: object) -> str | None:
    if value is None:
        return None
    try:
        parsed = _parse_time(value)
    except (TypeError, ValueError):
        return str(value)
    return parsed.strftime("%H:%M")


def _compute_duration(start_time: object, end_time: object) -> str | None:
    if start_time is None or end_time is None:
        return None
    start = _parse_time(start_time)
    end = _parse_time(end_time)
    start_dt = datetime.combine(date.today(), start)
    end_dt = datetime.combine(date.today(), end)
    if end_dt < start_dt:
        end_dt += timedelta(days=1)
    seconds = int((end_dt - start_dt).total_seconds())
    return _format_duration(seconds)


# ============================================================
# POSITIONS
# ============================================================

def get_positions(code: str | None = None, include_all: bool = False) -> list[dict]:
    tables = get_db_tables()
    positions_table = tables["POSITIONS"]
    where_clauses: list[str] = []
    params: list[object] = []

    if code:
        where_clauses.append("code = %s")
        params.append(code)

    if not include_all:
        where_clauses.append("active = 1")

    query = f"SELECT code, name, icon, active FROM {positions_table}"
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    return fetch_all(query, tuple(params))


def insert_positions(positions: list[dict] | dict) -> int:
    tables = get_db_tables()
    positions_table = tables["POSITIONS"]
    rows = _ensure_list(positions, name="positions")
    values: list[tuple] = []
    for item in rows:
        code = (item.get("code") or "").strip()
        name = (item.get("name") or "").strip()
        icon = item.get("icon")
        if isinstance(icon, str):
            icon = icon.strip() or None
        if not code or not name:
            raise ValueError("code and name are required for positions")
        active = _normalize_bool(item.get("active"), default=True)
        values.append((code, name, icon, active))

    query = (
        f"INSERT INTO {positions_table} (code, name, icon, active) "
        "VALUES (%s, %s, %s, %s)"
    )
    return execute_many(query, values)


def update_positions(positions: list[dict] | dict) -> int:
    tables = get_db_tables()
    positions_table = tables["POSITIONS"]
    rows = _ensure_list(positions, name="positions")
    if not rows:
        return 0
    statements: list[tuple[str, tuple]] = []
    for item in rows:
        if not isinstance(item, dict):
            raise TypeError("positions must be a dict or list[dict]")
        code = (item.get("code") or "").strip()
        if not code:
            raise ValueError("code is required for positions update")

        fields: list[str] = []
        params: list[object] = []

        if "name" in item:
            name = (item.get("name") or "").strip()
            if not name:
                raise ValueError("name cannot be empty")
            fields.append("name = %s")
            params.append(name)

        if "icon" in item:
            icon = item.get("icon")
            if isinstance(icon, str):
                icon = icon.strip() or None
            fields.append("icon = %s")
            params.append(icon)

        if "active" in item:
            active_value = item.get("active")
            if active_value is None:
                raise ValueError("active cannot be null")
            active = _normalize_bool(active_value, default=True)
            fields.append("active = %s")
            params.append(active)

        if not fields:
            raise ValueError(f"No updatable fields provided for position code {code}")

        params.append(code)
        query = f"UPDATE {positions_table} SET " + ", ".join(fields) + " WHERE code = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += cursor.rowcount
        return updated

    return run_transaction(_work)


def delete_positions(positions: list[dict] | dict) -> int:
    tables = get_db_tables()
    positions_table = tables["POSITIONS"]
    rows = _ensure_list(positions, name="positions")
    if not rows:
        return 0
    codes: list[str] = []
    for item in rows:
        if isinstance(item, dict):
            raw = item.get("code") or item.get("position_code") or item.get("id")
        else:
            raw = item
        if raw is None:
            raise ValueError("code is required for positions delete")
        code = str(raw).strip()
        if not code:
            raise ValueError("code is required for positions delete")
        codes.append(code)

    placeholders = ", ".join(["%s"] * len(codes))
    query = f"UPDATE {positions_table} SET active = 0 WHERE code IN ({placeholders})"

    def _work(cursor) -> int:
        cursor.execute(query, tuple(codes))
        return cursor.rowcount

    return run_transaction(_work)


# ============================================================
# EMPLOYEES
# ============================================================

def get_employees(
    employee_id: str | None = None, include_all: bool = False
) -> list[dict]:
    tables = get_db_tables()
    employees_table = tables["EMPLOYEES"]
    where_clauses: list[str] = []
    params: list[object] = []

    if employee_id:
        where_clauses.append("id = %s")
        params.append(employee_id)

    if not include_all:
        where_clauses.append("active = 1")

    query = (
        "SELECT id, name, schedule_section, note, ref_positionCode, active "
        f"FROM {employees_table}"
    )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    return fetch_all(query, tuple(params))


def insert_employees(employees: list[dict] | dict) -> int:
    tables = get_db_tables()
    employees_table = tables["EMPLOYEES"]
    rows = _ensure_list(employees, name="employees")
    allowed_sections = {v.strip() for v in get_schedule_sections()}
    values: list[tuple] = []
    for item in rows:
        employee_id = (item.get("id") or "").strip() or str(uuid4())
        name = (item.get("name") or "").strip()
        section = (item.get("schedule_section") or "").strip()
        note = item.get("note")
        ref_position_code = item.get("ref_positionCode")
        if isinstance(ref_position_code, str):
            ref_position_code = ref_position_code.strip() or None
        if not name or not section:
            raise ValueError("name and schedule_section are required for employees")
        if section not in allowed_sections:
            raise ValueError(f"Invalid schedule_section: {section}")
        active = _normalize_bool(item.get("active"), default=True)
        values.append(
            (employee_id, name, section, note, ref_position_code, active)
        )

    query = (
        f"INSERT INTO {employees_table} "
        "(id, name, schedule_section, note, ref_positionCode, active) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )
    return execute_many(query, values)


def update_employees(employees: list[dict] | dict) -> int:
    tables = get_db_tables()
    employees_table = tables["EMPLOYEES"]
    rows = _ensure_list(employees, name="employees")
    if not rows:
        return 0
    allowed_sections = {v.strip() for v in get_schedule_sections()}
    statements: list[tuple[str, tuple]] = []
    for item in rows:
        if not isinstance(item, dict):
            raise TypeError("employees must be a dict or list[dict]")
        employee_id = (item.get("id") or "").strip()
        if not employee_id:
            raise ValueError("id is required for employees update")

        fields: list[str] = []
        params: list[object] = []

        if "name" in item:
            name = (item.get("name") or "").strip()
            if not name:
                raise ValueError("name cannot be empty")
            fields.append("name = %s")
            params.append(name)

        if "schedule_section" in item:
            section = (item.get("schedule_section") or "").strip()
            if not section:
                raise ValueError("schedule_section cannot be empty")
            if section not in allowed_sections:
                raise ValueError(f"Invalid schedule_section: {section}")
            fields.append("schedule_section = %s")
            params.append(section)

        if "note" in item:
            fields.append("note = %s")
            params.append(item.get("note"))

        if "ref_positionCode" in item:
            ref_position_code = item.get("ref_positionCode")
            if isinstance(ref_position_code, str):
                ref_position_code = ref_position_code.strip() or None
            fields.append("ref_positionCode = %s")
            params.append(ref_position_code)

        if "active" in item:
            active_value = item.get("active")
            if active_value is None:
                raise ValueError("active cannot be null")
            active = _normalize_bool(active_value, default=True)
            fields.append("active = %s")
            params.append(active)

        if not fields:
            raise ValueError(
                f"No updatable fields provided for employee id {employee_id}"
            )

        params.append(employee_id)
        query = f"UPDATE {employees_table} SET " + ", ".join(fields) + " WHERE id = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += cursor.rowcount
        return updated

    return run_transaction(_work)


def delete_employees(employees: list[dict] | dict) -> int:
    tables = get_db_tables()
    employees_table = tables["EMPLOYEES"]
    rows = _ensure_list(employees, name="employees")
    if not rows:
        return 0
    employee_ids: list[str] = []
    for item in rows:
        if isinstance(item, dict):
            raw = item.get("id")
        else:
            raw = item
        if raw is None:
            raise ValueError("id is required for employees delete")
        employee_id = str(raw).strip()
        if not employee_id:
            raise ValueError("id is required for employees delete")
        employee_ids.append(employee_id)

    placeholders = ", ".join(["%s"] * len(employee_ids))
    query = f"UPDATE {employees_table} SET active = 0 WHERE id IN ({placeholders})"

    def _work(cursor) -> int:
        cursor.execute(query, tuple(employee_ids))
        return cursor.rowcount

    return run_transaction(_work)


# ============================================================
# SHIFTS
# ============================================================

def get_shifts(
    shift_id: int | str | None = None, include_all: bool = False
) -> list[dict]:
    tables = get_db_tables()
    shifts_table = tables["SHIFTS"]
    where_clauses: list[str] = []
    params: list[object] = []

    if shift_id is not None:
        where_clauses.append("id = %s")
        params.append(shift_id)

    if not include_all:
        where_clauses.append("active = 1")

    query = f"SELECT id, name, start_time, end_time, active FROM {shifts_table}"
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    rows = fetch_all(query, tuple(params))

    for row in rows:
        start_time = row.get("start_time")
        end_time = row.get("end_time")
        row["duration"] = _compute_duration(start_time, end_time)
        row["start_time"] = _format_time_value(start_time)
        row["end_time"] = _format_time_value(end_time)
    return rows


def insert_shifts(shifts: list[dict] | dict) -> int:
    tables = get_db_tables()
    shifts_table = tables["SHIFTS"]
    rows = _ensure_list(shifts, name="shifts")
    values: list[tuple] = []
    for item in rows:
        name = (item.get("name") or "").strip()
        start_time = item.get("start_time")
        end_time = item.get("end_time")
        if not name or start_time is None or end_time is None:
            raise ValueError("name, start_time, and end_time are required for shifts")
        active = _normalize_bool(item.get("active"), default=True)
        values.append((name, start_time, end_time, active))

    query = (
        f"INSERT INTO {shifts_table} (name, start_time, end_time, active) "
        "VALUES (%s, %s, %s, %s)"
    )
    return execute_many(query, values)


def update_shifts(shifts: list[dict] | dict) -> int:
    tables = get_db_tables()
    shifts_table = tables["SHIFTS"]
    rows = _ensure_list(shifts, name="shifts")
    if not rows:
        return 0
    statements: list[tuple[str, tuple]] = []
    for item in rows:
        if not isinstance(item, dict):
            raise TypeError("shifts must be a dict or list[dict]")
        shift_id = item.get("id")
        if shift_id is None or (isinstance(shift_id, str) and not shift_id.strip()):
            raise ValueError("id is required for shifts update")

        fields: list[str] = []
        params: list[object] = []

        if "name" in item:
            name = (item.get("name") or "").strip()
            if not name:
                raise ValueError("name cannot be empty")
            fields.append("name = %s")
            params.append(name)

        if "start_time" in item:
            start_time = item.get("start_time")
            if start_time is None:
                raise ValueError("start_time cannot be null")
            fields.append("start_time = %s")
            params.append(_parse_time(start_time))

        if "end_time" in item:
            end_time = item.get("end_time")
            if end_time is None:
                raise ValueError("end_time cannot be null")
            fields.append("end_time = %s")
            params.append(_parse_time(end_time))

        if "active" in item:
            active_value = item.get("active")
            if active_value is None:
                raise ValueError("active cannot be null")
            active = _normalize_bool(active_value, default=True)
            fields.append("active = %s")
            params.append(active)

        if not fields:
            raise ValueError(f"No updatable fields provided for shift id {shift_id}")

        params.append(shift_id)
        query = f"UPDATE {shifts_table} SET " + ", ".join(fields) + " WHERE id = %s"
        statements.append((query, tuple(params)))

    def _work(cursor) -> int:
        updated = 0
        for query, params in statements:
            cursor.execute(query, params)
            updated += cursor.rowcount
        return updated

    return run_transaction(_work)


def delete_shifts(shifts: list[dict] | dict) -> int:
    tables = get_db_tables()
    shifts_table = tables["SHIFTS"]
    rows = _ensure_list(shifts, name="shifts")
    if not rows:
        return 0
    shift_ids: list[object] = []
    for item in rows:
        if isinstance(item, dict):
            raw = item.get("id")
        else:
            raw = item
        if raw is None:
            raise ValueError("id is required for shifts delete")
        if isinstance(raw, str):
            raw = raw.strip()
        if raw == "":
            raise ValueError("id is required for shifts delete")
        shift_ids.append(raw)

    placeholders = ", ".join(["%s"] * len(shift_ids))
    query = f"UPDATE {shifts_table} SET active = 0 WHERE id IN ({placeholders})"

    def _work(cursor) -> int:
        cursor.execute(query, tuple(shift_ids))
        return cursor.rowcount

    return run_transaction(_work)


# ============================================================
# SCHEDULES
# ============================================================

def get_schedules(
    *,
    schedule_id: str | None = None,
    employee_id: str | None = None,
    date_value: date | datetime | str | None = None,
    start_date: date | datetime | str | None = None,
    end_date: date | datetime | str | None = None,
    start_time: time_type | datetime | str | None = None,
    end_time: time_type | datetime | str | None = None,
    week_no: int | None = None,
) -> list[dict]:
    tables = get_db_tables()
    schedules_table = tables["SCHEDULES"]
    employees_table = tables["EMPLOYEES"]
    positions_table = tables["POSITIONS"]
    shifts_table = tables["SHIFTS"]
    if week_no is not None and (date_value or start_date or end_date):
        raise ValueError("Use week_no or date/date range, not both")
    if date_value and (start_date or end_date):
        raise ValueError("Use date or date range, not both")

    if week_no is not None:
        week_info = build_week_info(week_no)
        start_date = week_info["start_date"]
        end_date = week_info["end_date"]
    elif date_value is not None:
        start_date = date_value
        end_date = date_value

    apply_time_filters = not (week_no or start_date or end_date)

    where_clauses: list[str] = []
    params: list[object] = []

    if schedule_id:
        where_clauses.append("s.id = %s")
        params.append(schedule_id)

    if employee_id:
        where_clauses.append("s.employee_id = %s")
        params.append(employee_id)

    if start_date is not None:
        start = _parse_date(start_date)
        where_clauses.append("s.date >= %s")
        params.append(start)

    if end_date is not None:
        end = _parse_date(end_date)
        where_clauses.append("s.date <= %s")
        params.append(end)

    if apply_time_filters and start_time is not None:
        start_t = _parse_time(start_time)
        where_clauses.append("s.start_time >= %s")
        params.append(start_t)

    if apply_time_filters and end_time is not None:
        end_t = _parse_time(end_time)
        where_clauses.append("s.end_time <= %s")
        params.append(end_t)

    query = (
        "SELECT "
        "s.id, "
        "s.employee_id, "
        "s.position_code, "
        "s.shift_id, "
        "s.date, "
        "s.start_time, "
        "s.end_time, "
        "s.note, "
        "e.name AS employee_name, "
        "e.schedule_section AS schedule_section, "
        "p.name AS position_name, "
        "sh.name AS shift_name "
        f"FROM {schedules_table} AS s "
        f"LEFT JOIN {employees_table} AS e ON e.id = s.employee_id "
        f"LEFT JOIN {positions_table} AS p ON p.code = s.position_code "
        f"LEFT JOIN {shifts_table} AS sh ON sh.id = s.shift_id "
    )

    if where_clauses:
        query += "WHERE " + " AND ".join(where_clauses) + " "

    query += "ORDER BY s.date, s.start_time"

    rows = fetch_all(query, tuple(params))
    for row in rows:
        row["start_time"] = _format_time_value(row.get("start_time"))
        row["end_time"] = _format_time_value(row.get("end_time"))
    return rows


def duplicate_week_schedules(
    *,
    source_start: date,
    source_end: date,
    target_start: date,
    target_end: date,
    delta_days: int,
    overwrite: bool,
) -> int:
    tables = get_db_tables()
    schedules_table = tables["SCHEDULES"]
    def _work(cursor) -> int:
        if overwrite:
            cursor.execute(
                f"DELETE FROM {schedules_table} WHERE date >= %s AND date <= %s",
                (target_start, target_end),
            )
            insert_prefix = f"INSERT INTO {schedules_table} "
        else:
            insert_prefix = f"INSERT IGNORE INTO {schedules_table} "

        cursor.execute(
            insert_prefix
            + "(id, employee_id, position_code, shift_id, date, start_time, end_time, note) "
            "SELECT "
            "UUID(), s.employee_id, s.position_code, s.shift_id, "
            "DATE_ADD(s.date, INTERVAL %s DAY), s.start_time, s.end_time, s.note "
            f"FROM {schedules_table} AS s "
            "WHERE s.date >= %s AND s.date <= %s",
            (delta_days, source_start, source_end),
        )
        return cursor.rowcount

    return run_transaction(_work)


def insert_schedules(schedules: list[dict] | dict) -> int:
    rows = _ensure_list(schedules, name="schedules")
    values = _build_schedule_insert_values(rows)
    return execute_many(_schedule_insert_query(), values)


def update_schedules(schedules: list[dict] | dict) -> int:
    rows = _ensure_list(schedules, name="schedules")
    if not rows:
        return 0
    statements = _build_schedule_update_statements(rows)
    return run_transaction(
        lambda cursor: _execute_schedule_updates(cursor, statements)
    )


def delete_schedules(schedules: list[dict] | dict) -> int:
    rows = _ensure_list(schedules, name="schedules")
    if not rows:
        return 0
    delete_payload = _build_schedule_delete_payload(rows)
    if delete_payload is None:
        return 0
    return run_transaction(lambda cursor: _execute_schedule_delete(cursor, delete_payload))


def delete_schedules_by_week(week_no: int) -> int:
    week_info = build_week_info(week_no)
    start_date = _parse_date(week_info["start_date"])
    end_date = _parse_date(week_info["end_date"])
    tables = get_db_tables()
    schedules_table = tables["SCHEDULES"]

    def _work(cursor) -> int:
        cursor.execute(
            f"DELETE FROM {schedules_table} WHERE date >= %s AND date <= %s",
            (start_date, end_date),
        )
        return cursor.rowcount

    return run_transaction(_work)


def apply_schedule_changes(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dict")

    create_rows = _coerce_optional_list(payload.get("toCreate"), name="toCreate")
    update_rows = _coerce_optional_list(payload.get("toUpdate"), name="toUpdate")
    delete_rows = _coerce_optional_list(payload.get("toDelete"), name="toDelete")

    _validate_schedule_change_conflicts(
        create_rows=create_rows,
        update_rows=update_rows,
        delete_rows=delete_rows,
    )

    create_values = _build_schedule_insert_values(create_rows)
    update_statements = _build_schedule_update_statements(update_rows)
    delete_payload = _build_schedule_delete_payload(delete_rows)

    def _work(cursor) -> dict:
        deleted = _execute_schedule_delete(cursor, delete_payload)
        updated = _execute_schedule_updates(cursor, update_statements)
        inserted = _execute_schedule_inserts(cursor, create_values)
        return {"inserted": inserted, "updated": updated, "deleted": deleted}

    return run_transaction(_work)


def _schedule_insert_query() -> str:
    tables = get_db_tables()
    schedules_table = tables["SCHEDULES"]
    return (
        f"INSERT INTO {schedules_table} "
        "(id, employee_id, position_code, shift_id, date, start_time, end_time, note) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )


def _coerce_optional_list(value: object, *, name: str) -> list:
    if value is None:
        return []
    return _ensure_list(value, name=name)


def _extract_schedule_id(value: object) -> str:
    if isinstance(value, dict):
        raw = value.get("id") or value.get("schedule_id") or ""
    else:
        raw = value
    if raw is None:
        return ""
    return str(raw).strip()


def _validate_schedule_change_conflicts(
    *,
    create_rows: list[dict],
    update_rows: list[dict],
    delete_rows: list,
) -> None:
    create_ids = [_extract_schedule_id(item) for item in create_rows]
    update_ids = [_extract_schedule_id(item) for item in update_rows]
    delete_ids = [_extract_schedule_id(item) for item in delete_rows]

    create_set = {value for value in create_ids if value}
    update_set = {value for value in update_ids if value}
    delete_set = {value for value in delete_ids if value}

    if create_ids and len(create_set) != len([v for v in create_ids if v]):
        raise ValueError("Duplicate ids found in toCreate")
    if update_ids and len(update_set) != len([v for v in update_ids if v]):
        raise ValueError("Duplicate ids found in toUpdate")
    if delete_ids and len(delete_set) != len([v for v in delete_ids if v]):
        raise ValueError("Duplicate ids found in toDelete")

    overlap_create_update = create_set & update_set
    overlap_create_delete = create_set & delete_set
    overlap_update_delete = update_set & delete_set

    if overlap_create_update:
        raise ValueError(
            "Schedule ids cannot appear in both toCreate and toUpdate: "
            + ", ".join(sorted(overlap_create_update))
        )
    if overlap_create_delete:
        raise ValueError(
            "Schedule ids cannot appear in both toCreate and toDelete: "
            + ", ".join(sorted(overlap_create_delete))
        )
    if overlap_update_delete:
        raise ValueError(
            "Schedule ids cannot appear in both toUpdate and toDelete: "
            + ", ".join(sorted(overlap_update_delete))
        )


def _build_schedule_insert_values(rows: list[dict]) -> list[tuple]:
    values: list[tuple] = []
    for item in rows:
        if not isinstance(item, dict):
            raise TypeError("schedules must be a dict or list[dict]")
        schedule_id = (item.get("id") or "").strip() or str(uuid4())
        employee_id = (item.get("employee_id") or "").strip()
        position_code = (item.get("position_code") or "").strip()
        shift_id = item.get("shift_id")
        date_value = item.get("date")
        start_time = item.get("start_time")
        end_time = item.get("end_time")
        note = item.get("note")
        if (
            not employee_id
            or not position_code
            or date_value is None
            or start_time is None
            or end_time is None
        ):
            raise ValueError(
                "employee_id, position_code, date, start_time, end_time are required"
            )
        values.append(
            (
                schedule_id,
                employee_id,
                position_code,
                shift_id,
                _parse_date(date_value),
                start_time,
                end_time,
                note,
            )
        )
    return values


def _execute_schedule_inserts(cursor, values: list[tuple]) -> int:
    if not values:
        return 0
    cursor.executemany(_schedule_insert_query(), values)
    return cursor.rowcount


def _build_schedule_update_statements(rows: list[dict]) -> list[tuple[str, tuple]]:
    tables = get_db_tables()
    schedules_table = tables["SCHEDULES"]
    statements: list[tuple[str, tuple]] = []
    for item in rows:
        if not isinstance(item, dict):
            raise TypeError("schedules must be a dict or list[dict]")
        schedule_id = _extract_schedule_id(item)
        if not schedule_id:
            raise ValueError("id is required for schedules update")

        fields: list[str] = []
        params: list[object] = []

        if "employee_id" in item:
            employee_id = (item.get("employee_id") or "").strip()
            if not employee_id:
                raise ValueError("employee_id cannot be empty")
            fields.append("employee_id = %s")
            params.append(employee_id)

        if "position_code" in item:
            position_code = (item.get("position_code") or "").strip()
            if not position_code:
                raise ValueError("position_code cannot be empty")
            fields.append("position_code = %s")
            params.append(position_code)

        if "shift_id" in item:
            shift_id = item.get("shift_id")
            if isinstance(shift_id, str) and not shift_id.strip():
                shift_id = None
            fields.append("shift_id = %s")
            params.append(shift_id)

        if "date" in item:
            date_value = item.get("date")
            if date_value is None:
                raise ValueError("date cannot be null")
            fields.append("date = %s")
            params.append(_parse_date(date_value))

        if "start_time" in item:
            start_time = item.get("start_time")
            if start_time is None:
                raise ValueError("start_time cannot be null")
            fields.append("start_time = %s")
            params.append(_parse_time(start_time))

        if "end_time" in item:
            end_time = item.get("end_time")
            if end_time is None:
                raise ValueError("end_time cannot be null")
            fields.append("end_time = %s")
            params.append(_parse_time(end_time))

        if "note" in item:
            fields.append("note = %s")
            params.append(item.get("note"))

        if not fields:
            raise ValueError(
                f"No updatable fields provided for schedule id {schedule_id}"
            )

        params.append(schedule_id)
        query = f"UPDATE {schedules_table} SET " + ", ".join(fields) + " WHERE id = %s"
        statements.append((query, tuple(params)))

    return statements


def _execute_schedule_updates(cursor, statements: list[tuple[str, tuple]]) -> int:
    updated = 0
    for query, params in statements:
        cursor.execute(query, params)
        updated += cursor.rowcount
    return updated


def _build_schedule_delete_payload(rows: list) -> tuple[str, tuple] | None:
    tables = get_db_tables()
    schedules_table = tables["SCHEDULES"]
    if not rows:
        return None
    schedule_ids: list[str] = []
    for item in rows:
        schedule_id = _extract_schedule_id(item)
        if not schedule_id:
            raise ValueError("id is required for schedules delete")
        schedule_ids.append(schedule_id)

    placeholders = ", ".join(["%s"] * len(schedule_ids))
    query = f"DELETE FROM {schedules_table} WHERE id IN ({placeholders})"
    return query, tuple(schedule_ids)


def _execute_schedule_delete(cursor, payload: tuple[str, tuple] | None) -> int:
    if payload is None:
        return 0
    query, params = payload
    cursor.execute(query, params)
    return cursor.rowcount
