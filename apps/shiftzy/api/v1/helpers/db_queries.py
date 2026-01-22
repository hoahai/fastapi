from __future__ import annotations

from datetime import date, datetime, time as time_type, timedelta
from uuid import uuid4

from apps.shiftzy.api.v1.helpers.config import (
    get_position_areas,
    get_schedule_sections,
)
from apps.shiftzy.api.v1.helpers.weeks import build_week_info
from shared.db import fetch_all, get_connection


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


def _execute_many(query: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.executemany(query, rows)
        conn.commit()
        return cursor.rowcount
    finally:
        cursor.close()
        conn.close()


# ============================================================
# POSITIONS
# ============================================================

def get_positions(code: str | None = None) -> list[dict]:
    if code:
        query = "SELECT code, name, area, active FROM positions WHERE code = %s"
        return fetch_all(query, (code,))
    query = "SELECT code, name, area, active FROM positions"
    return fetch_all(query, ())


def insert_positions(positions: list[dict] | dict) -> int:
    rows = _ensure_list(positions, name="positions")
    allowed_areas = {v.strip() for v in get_position_areas()}
    values: list[tuple] = []
    for item in rows:
        code = (item.get("code") or "").strip()
        name = (item.get("name") or "").strip()
        area = (item.get("area") or "").strip()
        if not code or not name or not area:
            raise ValueError("code, name, and area are required for positions")
        if area not in allowed_areas:
            raise ValueError(f"Invalid area: {area}")
        active = _normalize_bool(item.get("active"), default=True)
        values.append((code, name, area, active))

    query = "INSERT INTO positions (code, name, area, active) VALUES (%s, %s, %s, %s)"
    return _execute_many(query, values)


# ============================================================
# EMPLOYEES
# ============================================================

def get_employees(employee_id: str | None = None) -> list[dict]:
    if employee_id:
        query = (
            "SELECT id, name, schedule_section, note, active "
            "FROM employees WHERE id = %s"
        )
        return fetch_all(query, (employee_id,))
    query = "SELECT id, name, schedule_section, note, active FROM employees"
    return fetch_all(query, ())


def insert_employees(employees: list[dict] | dict) -> int:
    rows = _ensure_list(employees, name="employees")
    allowed_sections = {v.strip() for v in get_schedule_sections()}
    values: list[tuple] = []
    for item in rows:
        employee_id = (item.get("id") or "").strip() or str(uuid4())
        name = (item.get("name") or "").strip()
        section = (item.get("schedule_section") or "").strip()
        note = item.get("note")
        if not name or not section:
            raise ValueError("name and schedule_section are required for employees")
        if section not in allowed_sections:
            raise ValueError(f"Invalid schedule_section: {section}")
        active = _normalize_bool(item.get("active"), default=True)
        values.append((employee_id, name, section, note, active))

    query = (
        "INSERT INTO employees (id, name, schedule_section, note, active) "
        "VALUES (%s, %s, %s, %s, %s)"
    )
    return _execute_many(query, values)


# ============================================================
# SHIFTS
# ============================================================

def get_shifts(shift_id: int | str | None = None) -> list[dict]:
    if shift_id is not None:
        query = "SELECT id, name, start_time, end_time FROM shifts WHERE id = %s"
        rows = fetch_all(query, (shift_id,))
    else:
        query = "SELECT id, name, start_time, end_time FROM shifts"
        rows = fetch_all(query, ())

    for row in rows:
        row["duration"] = _compute_duration(
            row.get("start_time"),
            row.get("end_time"),
        )
    return rows


def insert_shifts(shifts: list[dict] | dict) -> int:
    rows = _ensure_list(shifts, name="shifts")
    values: list[tuple] = []
    for item in rows:
        name = (item.get("name") or "").strip()
        start_time = item.get("start_time")
        end_time = item.get("end_time")
        if not name or start_time is None or end_time is None:
            raise ValueError("name, start_time, and end_time are required for shifts")
        values.append((name, start_time, end_time))

    query = "INSERT INTO shifts (name, start_time, end_time) VALUES (%s, %s, %s)"
    return _execute_many(query, values)


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
        "p.name AS position_name, "
        "sh.name AS shift_name "
        "FROM schedules AS s "
        "LEFT JOIN employees AS e ON e.id = s.employee_id "
        "LEFT JOIN positions AS p ON p.code = s.position_code "
        "LEFT JOIN shifts AS sh ON sh.id = s.shift_id "
    )

    if where_clauses:
        query += "WHERE " + " AND ".join(where_clauses) + " "

    query += "ORDER BY s.date, s.start_time"

    return fetch_all(query, tuple(params))


def insert_schedules(schedules: list[dict] | dict) -> int:
    rows = _ensure_list(schedules, name="schedules")
    values: list[tuple] = []
    for item in rows:
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

    query = (
        "INSERT INTO schedules "
        "(id, employee_id, position_code, shift_id, date, start_time, end_time, note) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )
    return _execute_many(query, values)
