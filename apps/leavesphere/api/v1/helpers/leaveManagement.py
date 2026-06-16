from __future__ import annotations

from collections import OrderedDict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from apps.leavesphere.api.v1.helpers.dbQueries import (
    get_employee_managers,
    get_employees,
    get_employees_by_ids,
    get_db_tables,
    get_holidays,
    get_pto_actions,
    get_pto_transactions,
    get_pto_types,
    insert_pto_transaction,
    update_pto_transaction,
    upsert_holiday,
)
from apps.leavesphere.api.v1.helpers.employees import create_employee
from apps.leavesphere.api.v1.helpers.myPto import (
    _build_employee_map,
    _build_pto_action_catalog,
    _build_pto_type_catalog,
    _build_request_rows,
    _employee_full_name,
    _extract_email_from_auth_payload,
    _normalize_email,
    _normalize_team_region,
    _normalize_text,
    _normalize_year,
    _resolve_current_employee,
    _resolve_default_action_code,
    _resolve_primary_manager_id,
    _split_ui_type_tokens,
    _to_date_string,
)
from apps.leavesphere.api.v1.helpers.config import (
    resolve_employee_email,
    resolve_employee_email_candidates,
)
from apps.leavesphere.api.v1.helpers.ptoActions import create_pto_action, modify_pto_action
from apps.leavesphere.api.v1.helpers.ptoTypes import create_pto_type, modify_pto_type
from apps.leavesphere.api.v1.helpers.ptoTransactions import create_adjustment, create_request
from shared.auth.dependencies import get_auth_principal
from shared.db import execute_write

_REQUEST_ACTION_TOKENS = ("request", "req")
_LOAD_ACTION_TOKENS = ("load", "grant", "accrual", "carry")
_ADJUSTMENT_ACTION_TOKENS = ("adjust", "manual", "correction", "fix")


def _normalize_optional_iso_date(value: object | None) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError as exc:
        raise ValueError("Date must use YYYY-MM-DD format") from exc


def _normalize_holiday_date(value: object | None) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    text = _normalize_text(value)
    if not text:
        return ""

    candidates = [text, text[:10]]
    formats = (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S.%f",
        "%m-%d-%Y",
    )
    for candidate in candidates:
        for fmt in formats:
            try:
                return datetime.strptime(candidate, fmt).date().isoformat()
            except ValueError:
                continue
        try:
            return date.fromisoformat(candidate).isoformat()
        except ValueError:
            continue
    return ""


def _normalize_decimal_hours(value: object) -> Decimal:
    try:
        hours = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("hours must be a decimal number") from exc
    if hours < 0:
        raise ValueError("hours must not be negative")
    return hours.quantize(Decimal("0.01"))


def _normalize_positive_requested_hours(value: object) -> Decimal:
    try:
        hours = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("hours must be a decimal number") from exc
    if hours <= 0:
        raise ValueError("hours must be greater than zero")
    return hours.quantize(Decimal("0.01"))


def _normalize_optional_text(value: object | None) -> str | None:
    text = _normalize_text(value)
    return text or None


def _normalize_optional_iso_date(value: object | None) -> str | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError as exc:
        raise ValueError("Date must use YYYY-MM-DD format") from exc


def _normalize_optional_month_key(value: object | None) -> str | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return datetime.strptime(text[:7], "%Y-%m").strftime("%Y-%m")
    except ValueError as exc:
        raise ValueError("Month must use YYYY-MM format") from exc


def _build_month_bounds(month_key: str) -> tuple[str, str]:
    month_start = datetime.strptime(month_key, "%Y-%m").date().replace(day=1)
    if month_start.month == 12:
        next_month_start = date(month_start.year + 1, 1, 1)
    else:
        next_month_start = date(month_start.year, month_start.month + 1, 1)
    month_end = date.fromordinal(next_month_start.toordinal() - 1)
    return month_start.isoformat(), month_end.isoformat()


def _merge_request_rows(*groups: list[dict]) -> list[dict]:
    requests_by_id: OrderedDict[str, dict] = OrderedDict()
    for group in groups:
        for row in group:
            request_id = _normalize_text(row.get("id"))
            if not request_id:
                continue
            requests_by_id[request_id] = row
    return list(requests_by_id.values())


def _build_action_haystack(row: dict, action_by_code: dict[str, dict]) -> str:
    action_code = _normalize_text(row.get("ptoActionCode")).upper()
    action = action_by_code.get(action_code)
    name = _normalize_text(action.get("name")) if action else ""
    return f"{action_code} {name}".strip().lower()


def _is_token_match(haystack: str, tokens: tuple[str, ...]) -> bool:
    return any(token in haystack for token in tokens)


def _is_request_action(row: dict, action_by_code: dict[str, dict]) -> bool:
    return _is_token_match(_build_action_haystack(row, action_by_code), _REQUEST_ACTION_TOKENS)


def _is_load_action(row: dict, action_by_code: dict[str, dict]) -> bool:
    return _is_token_match(_build_action_haystack(row, action_by_code), _LOAD_ACTION_TOKENS)


def _is_adjustment_action(row: dict, action_by_code: dict[str, dict]) -> bool:
    return _is_token_match(_build_action_haystack(row, action_by_code), _ADJUSTMENT_ACTION_TOKENS)


def _build_employee_manager_map(manager_rows: list[dict]) -> dict[str, str]:
    manager_map: dict[str, str] = {}
    for row in manager_rows:
        employee_id = _normalize_text(row.get("employeeId"))
        manager_id = _normalize_text(row.get("managerId"))
        if employee_id and manager_id and employee_id not in manager_map:
            manager_map[employee_id] = manager_id
    return manager_map


def _build_employee_rows(
    employees: list[dict],
    employee_map: dict[str, dict],
    manager_map: dict[str, str],
    current_employee_id: str,
    current_employee_name: str,
) -> list[dict]:
    employee_rows: list[dict] = []
    for row in employees:
        employee_id = _normalize_text(row.get("id"))
        if not employee_id:
            continue
        manager_id = manager_map.get(employee_id)
        manager = employee_map.get(manager_id) if manager_id else None
        employee_rows.append(
            {
                "employeeId": employee_id,
                "employeeName": _employee_full_name(row),
                "pictureUrl": _normalize_text(row.get("pictureUrl")) or None,
                "title": _normalize_text(row.get("title")),
                "managerId": manager_id or None,
                "managerName": _employee_full_name(manager) if manager else (current_employee_name if employee_id == current_employee_id else ""),
                "teamRegion": _normalize_team_region(row.get("region")),
                "active": int(row.get("active") or 0) == 1,
            }
        )
    employee_rows.sort(key=lambda item: (item["employeeName"].lower(), item["employeeId"]))
    return employee_rows


def _resolve_pto_type_code(
    *,
    value: object | None,
    pto_type_by_code: dict[str, dict],
) -> str:
    normalized = _normalize_text(value).lower()
    if not normalized:
        return ""
    for code, item in pto_type_by_code.items():
        item_code = _normalize_text(code).lower()
        item_type = _normalize_text(item.get("type")).lower()
        item_label = _normalize_text(item.get("label")).lower()
        if normalized in {item_code, item_type}:
            return code
        if normalized and normalized in item_label:
            return code
    return normalized.upper()


def _resolve_action_code(
    *,
    action_catalog: list[dict],
    include_tokens: tuple[str, ...],
    fallback_index: int = 0,
) -> str:
    for row in action_catalog:
        haystack = f"{_normalize_text(row.get('code'))} {_normalize_text(row.get('name'))}".lower()
        if _is_token_match(haystack, include_tokens):
            return _normalize_text(row.get("code")).upper()
    if 0 <= fallback_index < len(action_catalog):
        return _normalize_text(action_catalog[fallback_index].get("code")).upper()
    return ""


def _build_employee_balances(
    *,
    employees: list[dict],
    employee_map: dict[str, dict],
    pto_types: list[dict],
    pto_type_by_code: dict[str, dict],
    pto_action_by_code: dict[str, dict],
    request_rows: list[dict],
    balance_rows: list[dict],
) -> list[dict]:
    balance_lookup: dict[str, dict[str, dict[str, float | str]]] = OrderedDict()
    type_order = [str(row.get("code") or "").strip().upper() for row in pto_types if str(row.get("code") or "").strip()]
    if not type_order:
        type_order = sorted(pto_type_by_code.keys())

    for employee in employees:
        employee_id = _normalize_text(employee.get("id"))
        employee_name = _employee_full_name(employee)
        if not employee_id:
            continue
        balance_lookup[employee_id] = OrderedDict()
        for code in type_order:
            item = pto_type_by_code.get(code, {})
            balance_lookup[employee_id][code] = {
                "type": code.lower() if code else "",
                "code": code,
                "label": _normalize_text(item.get("label")) or code,
                "totalHours": 0.0,
                "usedHours": 0.0,
                "scheduledHours": 0.0,
                "remainingHours": 0.0,
            }

    def _ensure_bucket(employee_id: str, pto_type_code: str) -> dict[str, float | str]:
        employee_bucket = balance_lookup.setdefault(employee_id, OrderedDict())
        if pto_type_code not in employee_bucket:
            item = pto_type_by_code.get(pto_type_code, {})
            employee_bucket[pto_type_code] = {
                "type": pto_type_code.lower() if pto_type_code else "",
                "code": pto_type_code,
                "label": _normalize_text(item.get("label")) or pto_type_code,
                "totalHours": 0.0,
                "usedHours": 0.0,
                "scheduledHours": 0.0,
                "remainingHours": 0.0,
            }
        return employee_bucket[pto_type_code]

    for row in request_rows:
        employee_id = _normalize_text(row.get("employeeId"))
        pto_type_code = _normalize_text(row.get("ptoTypeCode")).upper()
        if not employee_id or not pto_type_code:
            continue
        bucket = _ensure_bucket(employee_id, pto_type_code)
        hours = abs(float(Decimal(str(row.get("hours") or 0)).quantize(Decimal("0.01"))))
        status = _normalize_text(row.get("status")).lower()
        if status == "approved":
            bucket["usedHours"] = float(bucket["usedHours"]) + hours
        elif status == "pending":
            bucket["scheduledHours"] = float(bucket["scheduledHours"]) + hours

    for row in balance_rows:
        employee_id = _normalize_text(row.get("employeeId"))
        pto_type_code = _normalize_text(row.get("ptoTypeCode")).upper()
        if not employee_id or not pto_type_code:
            continue
        bucket = _ensure_bucket(employee_id, pto_type_code)
        hours = abs(float(Decimal(str(row.get("hours") or 0)).quantize(Decimal("0.01"))))
        status = _normalize_text(row.get("status")).lower()
        if status != "approved":
            continue
        if _is_load_action(row, pto_action_by_code):
            bucket["totalHours"] = float(bucket["totalHours"]) + hours

    employee_balances: list[dict] = []
    for employee in employees:
        employee_id = _normalize_text(employee.get("id"))
        employee_name = _employee_full_name(employee)
        if not employee_id:
            continue
        employee_bucket = balance_lookup.get(employee_id, {})
        balances: list[dict] = []
        for code, item in employee_bucket.items():
            total = float(item["totalHours"])
            used = float(item["usedHours"])
            scheduled = float(item["scheduledHours"])
            balances.append(
                {
                    "type": item["type"] or code.lower(),
                    "code": code,
                    "label": item["label"] or code,
                    "totalHours": round(total, 2),
                    "usedHours": round(used, 2),
                    "scheduledHours": round(scheduled, 2),
                    "remainingHours": round(total - used - scheduled, 2),
                }
            )
        balances.sort(key=lambda item: (str(item["label"]).lower(), str(item["code"]).lower()))
        employee_balances.append(
            {
                "employeeId": employee_id,
                "employeeName": employee_name,
                "balances": balances,
            }
        )
    employee_balances.sort(key=lambda item: (item["employeeName"].lower(), item["employeeId"]))
    return employee_balances


def _build_balance_transaction_rows(
    *,
    transaction_rows: list[dict],
    employee_map: dict[str, dict],
    pto_action_by_code: dict[str, dict],
) -> list[dict]:
    rows: list[dict] = []
    for row in transaction_rows:
        status = _normalize_text(row.get("status")).capitalize()
        if status != "Approved":
            continue
        if not (_is_load_action(row, pto_action_by_code) or _is_adjustment_action(row, pto_action_by_code)):
            continue
        employee_id = _normalize_text(row.get("employeeId"))
        pto_type_code = _normalize_text(row.get("ptoTypeCode")).upper()
        if not employee_id or not pto_type_code:
            continue
        action_code = "adjustment" if _is_adjustment_action(row, pto_action_by_code) else "load_grant"
        approver_id = _normalize_text(row.get("approverId"))
        approver = employee_map.get(approver_id) if approver_id else None
        hours_value = Decimal(str(row.get("hours") or 0)).quantize(Decimal("0.01"))
        rows.append(
            {
                "id": _normalize_text(row.get("id")),
                "employeeId": employee_id,
                "ptoTypeCode": pto_type_code,
                "ptoActionCode": action_code,
                "hours": float(abs(hours_value)) if action_code == "load_grant" else float(hours_value),
                "year": int(row.get("year") or date.today().year),
                "status": "Approved",
                "approverNote": _normalize_text(row.get("approverNote")) or None,
                "createdAt": _to_date_string(row.get("dateCreated")) or _to_date_string(row.get("dateUpdated")) or date.today().isoformat(),
                "createdByName": _employee_full_name(approver) if approver else None,
            }
        )
    rows.sort(key=lambda item: (item["createdAt"], item["id"]), reverse=True)
    return rows


def _split_employee_name(full_name: str) -> tuple[str, str]:
    parts = [part for part in _normalize_text(full_name).split(" ") if part]
    if not parts:
        return "New", "Employee"
    if len(parts) == 1:
        return parts[0], "Employee"
    return parts[0], " ".join(parts[1:])


def _make_synthetic_identity(employee_name: str) -> tuple[str, str]:
    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in _normalize_text(employee_name))
    slug = "-".join(part for part in slug.split("-") if part) or "employee"
    suffix = uuid4().hex[:8]
    identity_key = f"lm-{slug[:20]}-{suffix}"
    email = f"{slug}.{suffix}@example.invalid"
    return identity_key, email


def _build_synthetic_current_employee(request) -> dict:
    principal = get_auth_principal(request)
    principal_email = ""
    principal_id = ""
    if principal is not None:
        principal_email = _normalize_email(principal.email) or _extract_email_from_auth_payload(principal.raw_user)
        principal_id = _normalize_text(getattr(principal, "user_id", ""))

    header_email = _normalize_email(getattr(request, "headers", {}).get("x-user-email"))
    current_email = resolve_employee_email(principal_email or header_email)
    current_name = current_email.split("@", 1)[0].replace(".", " ").replace("_", " ").strip() if current_email else ""
    current_name = " ".join(part.capitalize() for part in current_name.split() if part) or "LeaveSphere Admin"
    identity_key, synthetic_email = _make_synthetic_identity(current_name)
    synthetic_id = principal_id or identity_key
    return {
        "id": synthetic_id,
        "identityKey": identity_key,
        "firstName": current_name.split(" ", 1)[0],
        "lastName": current_name.split(" ", 1)[1] if " " in current_name else "Admin",
        "email": current_email or synthetic_email,
        "region": "US",
        "title": "Leave Management Admin",
        "active": 1,
    }


def _get_holiday_rows() -> list[dict]:
    try:
        return get_holidays()
    except Exception:
        return []


def _resolve_current_employee_from_rows(request, employees: list[dict]) -> dict:
    principal = get_auth_principal(request)

    candidate_emails = resolve_employee_email_candidates(
        getattr(principal, "email", None),
        _extract_email_from_auth_payload(getattr(principal, "raw_user", None)) if principal is not None else "",
        _normalize_text(getattr(request, "headers", {}).get("x-user-email")),
        _normalize_text(getattr(request, "headers", {}).get("x-user-name"))
        if "@" in _normalize_text(getattr(request, "headers", {}).get("x-user-name"))
        else "",
    )

    identity_key = _normalize_text(getattr(principal, "user_id", "")).lower() if principal is not None else ""
    candidate_identity_keys = [identity_key] if identity_key else []

    matched_rows: list[dict] = []
    for row in employees:
        if not isinstance(row, dict):
            continue
        row_email = _normalize_email(row.get("email"))
        row_identity_key = _normalize_text(row.get("identityKey")).lower()
        if row_email and row_email in candidate_emails:
            matched_rows.append(row)
            continue
        if row_identity_key and row_identity_key in candidate_emails:
            matched_rows.append(row)
            continue
        if row_identity_key and row_identity_key in candidate_identity_keys:
            matched_rows.append(row)

    if not matched_rows:
        raise ValueError("Authenticated user is not mapped to a LeaveSphere employee")

    active_rows = [row for row in matched_rows if int(row.get("active") or 0) == 1]
    return active_rows[0] if active_rows else matched_rows[0]


def _resolve_workspace_year_from_transaction(transaction: dict | None, fallback_year: int | None = None) -> int:
    if isinstance(transaction, dict):
        try:
            year = int(transaction.get("year") or 0)
        except (TypeError, ValueError):
            year = 0
        if year >= 1901:
            return year
    if fallback_year is not None:
        return fallback_year
    return date.today().year


def _resolve_current_employee_record(request) -> dict:
    employees = get_employees()
    try:
        employee = _resolve_current_employee_from_rows(request, employees)
    except ValueError:
        employee = _build_synthetic_current_employee(request)
    if not isinstance(employee, dict):
        raise ValueError("Authenticated user is not mapped to a LeaveSphere employee")
    return employee


def _build_workspace(
    *,
    request,
    year: int,
    include_year_requests: bool = True,
    include_pending: bool = True,
    history_start_date: str | None = None,
    history_end_date: str | None = None,
    overlap_month: str | None = None,
) -> dict:
    selected_year = _normalize_year(year)
    employees = get_employees()
    try:
        current_employee = _resolve_current_employee_from_rows(request, employees)
    except ValueError:
        current_employee = _build_synthetic_current_employee(request)
    current_employee_id = _normalize_text(current_employee.get("id"))
    current_employee_name = _employee_full_name(current_employee)
    current_employee_email = _normalize_email(current_employee.get("email"))
    current_employee_region = _normalize_team_region(current_employee.get("region"))

    employee_map = _build_employee_map(employees)
    employee_map.setdefault(current_employee_id, current_employee)

    manager_rows = get_employee_managers()
    manager_map = _build_employee_manager_map(manager_rows)

    employee_rows = _build_employee_rows(
        employees=employees,
        employee_map=employee_map,
        manager_map=manager_map,
        current_employee_id=current_employee_id,
        current_employee_name=current_employee_name,
    )
    direct_reports = [
        {
            "employeeId": row["employeeId"],
            "employeeName": row["employeeName"],
            "title": row["title"],
        }
        for row in employee_rows
        if row["managerId"] == current_employee_id and row["employeeId"] != current_employee_id
    ]

    pto_types, pto_type_by_code = _build_pto_type_catalog()
    pto_actions, pto_action_by_code = _build_pto_action_catalog()
    balance_transaction_rows = get_pto_transactions(year=selected_year)
    year_request_rows = [row for row in balance_transaction_rows if _is_request_action(row, pto_action_by_code)]
    request_rows: list[dict] = []
    if include_year_requests:
        request_rows = [row for row in balance_transaction_rows if _is_request_action(row, pto_action_by_code)]

    if include_pending:
        year_start = f"{selected_year}-01-01"
        year_end = f"{selected_year}-12-31"
        pending_rows = get_pto_transactions(
            status="Pending",
            start_date_to=year_end,
            end_date_from=year_start,
        )
        request_rows = _merge_request_rows(request_rows, pending_rows)

    normalized_history_start_date = _normalize_optional_iso_date(history_start_date)
    normalized_history_end_date = _normalize_optional_iso_date(history_end_date)
    if normalized_history_start_date or normalized_history_end_date:
        historical_rows = get_pto_transactions(
            start_date_from=normalized_history_start_date,
            start_date_to=normalized_history_end_date,
        )
        request_rows = _merge_request_rows(request_rows, historical_rows)

    normalized_overlap_month = _normalize_optional_month_key(overlap_month)
    if normalized_overlap_month:
        overlap_start_date, overlap_end_date = _build_month_bounds(normalized_overlap_month)
        overlap_rows = get_pto_transactions(
            start_date_to=overlap_end_date,
            end_date_from=overlap_start_date,
        )
        request_rows = _merge_request_rows(request_rows, overlap_rows)

    requests = _build_request_rows(
        rows=request_rows,
        employee_map=employee_map,
        pto_type_by_code=pto_type_by_code,
        current_employee_id=current_employee_id,
        manager_id_by_employee_id=manager_map,
    )
    employee_balances = _build_employee_balances(
        employees=employees,
        employee_map=employee_map,
        pto_types=pto_types,
        pto_type_by_code=pto_type_by_code,
        pto_action_by_code=pto_action_by_code,
        request_rows=year_request_rows,
        balance_rows=balance_transaction_rows,
    )
    balance_transactions = _build_balance_transaction_rows(
        transaction_rows=balance_transaction_rows,
        employee_map=employee_map,
        pto_action_by_code=pto_action_by_code,
    )

    holidays = []
    for holiday in _get_holiday_rows():
        holiday_date = _normalize_holiday_date(holiday.get("date"))
        if not holiday_date or not holiday_date.startswith(f"{selected_year}-"):
            continue
        holidays.append(
            {
                "id": _normalize_text(holiday.get("id")),
                "name": _normalize_text(holiday.get("name")),
                "date": holiday_date,
                "teamRegion": _normalize_team_region(holiday.get("teamRegion") or holiday.get("region")),
            }
        )
    holidays.sort(key=lambda item: (item["date"], item["name"], item["id"]))

    current_employee_balances = next(
        (row["balances"] for row in employee_balances if row["employeeId"] == current_employee_id),
        [],
    )

    return {
        "currentUserId": current_employee_id,
        "currentUserName": current_employee_name,
        "currentUserEmail": current_employee_email,
        "managerId": manager_map.get(current_employee_id),
        "currentUserTeamRegion": current_employee_region,
        "isManager": bool(direct_reports),
        "ptoTypes": pto_types,
        "ptoActions": pto_actions,
        "defaultRequestActionCode": _resolve_default_action_code(
            pto_actions,
            include_tokens=_REQUEST_ACTION_TOKENS,
        ),
        "defaultCancelActionCode": _resolve_default_action_code(
            pto_actions,
            include_tokens=("request", "cancel"),
            fallback_index=0,
        ),
        "balances": current_employee_balances,
        "employeeBalances": employee_balances,
        "balanceTransactions": balance_transactions,
        "requests": requests,
        "holidays": holidays,
        "employees": employee_rows,
        "directReports": direct_reports,
    }


def load_leave_management_workspace(
    *,
    request,
    year: int | None = None,
    include_pending: bool = True,
    history_start_date: str | None = None,
    history_end_date: str | None = None,
    overlap_month: str | None = None,
) -> dict:
    selected_year = _normalize_year(year)
    return _build_workspace(
        request=request,
        year=selected_year,
        include_year_requests=False,
        include_pending=include_pending,
        history_start_date=history_start_date,
        history_end_date=history_end_date,
        overlap_month=overlap_month,
    )


def _resolve_request_year(payload: dict, transaction: dict | None = None) -> int:
    raw_year = payload.get("year")
    if raw_year is not None and str(raw_year).strip():
        return _normalize_year(raw_year)
    if transaction is not None:
        return _resolve_workspace_year_from_transaction(transaction)
    start_date = _normalize_text(payload.get("startDate"))
    if len(start_date) >= 4 and start_date[:4].isdigit():
        try:
            return _normalize_year(int(start_date[:4]))
        except ValueError:
            pass
    return date.today().year


def create_leave_management_request(*, request, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    current_employee = _resolve_current_employee_record(request)
    current_employee_id = _normalize_text(current_employee.get("id"))
    pto_types, pto_type_by_code = _build_pto_type_catalog()
    pto_actions, _ = _build_pto_action_catalog()

    employee_id = _normalize_text(payload.get("employeeId")) or current_employee_id
    if not employee_id:
        raise ValueError("employeeId is required")

    pto_type_code = _resolve_pto_type_code(
        value=payload.get("ptoTypeCode") or payload.get("type"),
        pto_type_by_code=pto_type_by_code,
    )
    if not pto_type_code:
        raise ValueError("ptoTypeCode is required")

    pto_action_code = _resolve_action_code(
        action_catalog=pto_actions,
        include_tokens=_REQUEST_ACTION_TOKENS,
    )
    if not pto_action_code:
        raise ValueError("ptoActionCode is required")

    requested_hours = _normalize_positive_requested_hours(payload.get("hours"))
    start_date = _normalize_optional_iso_date(payload.get("startDate"))
    end_date = _normalize_optional_iso_date(payload.get("endDate"))
    if start_date and end_date and start_date > end_date:
        raise ValueError("startDate must be on or before endDate")

    year = _resolve_request_year(payload)
    item = {
        "id": str(uuid4()),
        "employeeId": employee_id,
        "ptoTypeCode": pto_type_code,
        "ptoActionCode": pto_action_code,
        "hours": (requested_hours * Decimal("-1")).quantize(Decimal("0.01")),
        "year": year,
        "startDate": start_date or None,
        "endDate": end_date or None,
        "status": "Pending",
        "description": _normalize_text(payload.get("description")) or None,
        "approverNote": None,
        "approverId": None,
        "calendarId": _normalize_text(payload.get("calendarId")) or None,
    }
    if item["calendarId"] and len(item["calendarId"]) > 30:
        raise ValueError("calendarId must be <= 30 characters")

    inserted = create_request(
        {
            "employeeId": employee_id,
            "ptoTypeCode": pto_type_code,
            "ptoActionCode": pto_action_code,
            "hours": requested_hours,
            "year": year,
            "startDate": start_date or None,
            "endDate": end_date or None,
            "description": item["description"],
            "approverNote": item["approverNote"],
            "calendarId": item["calendarId"],
        }
    ).get("inserted")
    return {
        "workspace": _build_workspace(request=request, year=year),
        "source": "network",
        "createdRequestId": item["id"],
        "inserted": inserted,
        "status": "Pending",
    }


def update_leave_management_request(*, request, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    transaction_id = _normalize_text(payload.get("transactionId"))
    if not transaction_id:
        raise ValueError("transactionId is required")

    transaction_rows = get_pto_transactions(transaction_id=transaction_id)
    if not transaction_rows:
        raise ValueError("PTO transaction not found")
    transaction = transaction_rows[0]

    pto_types, pto_type_by_code = _build_pto_type_catalog()
    pto_type_code = _resolve_pto_type_code(
        value=payload.get("ptoTypeCode") or payload.get("type") or transaction.get("ptoTypeCode"),
        pto_type_by_code=pto_type_by_code,
    )
    if not pto_type_code:
        raise ValueError("ptoTypeCode is required")

    is_request = _is_request_action(transaction, _build_pto_action_catalog()[1])
    hours = _normalize_positive_requested_hours(payload.get("hours"))
    stored_hours = (hours * Decimal("-1")).quantize(Decimal("0.01")) if is_request or Decimal(str(transaction.get("hours") or 0)) < 0 else hours

    updates = {
        "ptoTypeCode": pto_type_code,
        "hours": stored_hours,
        "year": _resolve_request_year(payload, transaction),
        "startDate": _normalize_optional_iso_date(payload.get("startDate")) or transaction.get("startDate"),
        "endDate": _normalize_optional_iso_date(payload.get("endDate")) or transaction.get("endDate"),
        "description": _normalize_text(payload.get("description")) or transaction.get("description"),
    }
    updated = update_pto_transaction(transaction_id=transaction_id, updates=updates)
    return {
        "workspace": _build_workspace(request=request, year=_resolve_workspace_year_from_transaction(transaction)),
        "source": "network",
        "updated": updated,
    }


def review_leave_management_request(*, request, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    transaction_id = _normalize_text(payload.get("requestId") or payload.get("transactionId"))
    if not transaction_id:
        raise ValueError("requestId is required")

    action = _normalize_text(payload.get("action")).lower()
    approver_note = _normalize_optional_text(payload.get("approverNote"))
    transaction_rows = get_pto_transactions(transaction_id=transaction_id)
    if not transaction_rows:
        raise ValueError("PTO transaction not found")
    transaction = transaction_rows[0]

    if action == "approve":
        result = approve_request(request=request, transaction_id=transaction_id, approverNote=approver_note)
    elif action == "reject":
        result = reject_request(request=request, transaction_id=transaction_id, approverNote=approver_note)
    else:
        raise ValueError("action must be approve or reject")

    return {
        "workspace": _build_workspace(request=request, year=_resolve_workspace_year_from_transaction(transaction)),
        "source": "network",
        "updated": result.get("updated"),
        "status": result.get("status"),
    }


def adjust_leave_management_balance(*, request, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    employee_id = _normalize_text(payload.get("employeeId"))
    if not employee_id:
        raise ValueError("employeeId is required")

    pto_types, pto_type_by_code = _build_pto_type_catalog()
    pto_actions, _ = _build_pto_action_catalog()
    action_code = _normalize_text(payload.get("ptoActionCode")).lower()
    if action_code == "load_grant":
        resolved_action_code = _resolve_action_code(action_catalog=pto_actions, include_tokens=_LOAD_ACTION_TOKENS)
    elif action_code == "adjustment":
        resolved_action_code = _resolve_action_code(action_catalog=pto_actions, include_tokens=_ADJUSTMENT_ACTION_TOKENS)
    else:
        raise ValueError("ptoActionCode is required")

    pto_type_code = _resolve_pto_type_code(
        value=payload.get("ptoTypeCode") or payload.get("type"),
        pto_type_by_code=pto_type_by_code,
    )
    if not pto_type_code:
        raise ValueError("ptoTypeCode is required")

    hours = _normalize_decimal_hours(payload.get("hours"))
    year = _normalize_year(payload.get("year"))
    status = _normalize_text(payload.get("status")).capitalize() or "Approved"
    if status != "Approved":
        raise ValueError("status must be Approved")

    approver_note = _normalize_optional_text(payload.get("approverNote"))
    transaction_id = _normalize_text(payload.get("transactionId"))
    if transaction_id:
        existing = get_pto_transactions(transaction_id=transaction_id)
        if not existing:
            raise ValueError("PTO transaction not found")
        updated = update_pto_transaction(
            transaction_id=transaction_id,
            updates={
                "employeeId": employee_id,
                "ptoTypeCode": pto_type_code,
                "ptoActionCode": resolved_action_code,
                "hours": hours,
                "year": year,
                "status": "Approved",
                "approverNote": approver_note,
            },
        )
    else:
        updated = create_adjustment(
            {
                "employeeId": employee_id,
                "ptoTypeCode": pto_type_code,
                "ptoActionCode": resolved_action_code,
                "hours": hours,
                "year": year,
                "approverNote": approver_note,
                "status": "Approved",
            }
        ).get("inserted")

    return {
        "workspace": _build_workspace(request=request, year=year),
        "source": "network",
        "updated": updated,
        "status": "Approved",
    }


def _replace_employee_manager_mapping(*, employee_id: str, manager_id: str) -> int:
    tables = get_db_tables()
    deleted = execute_write(
        f"DELETE FROM {tables['EMPLOYEEMANAGERS']} WHERE employeeId = %s",
        (employee_id,),
    )
    inserted = execute_write(
        f"INSERT INTO {tables['EMPLOYEEMANAGERS']} (id, employeeId, managerId) VALUES (%s, %s, %s)",
        (str(uuid4()), employee_id, manager_id),
    )
    return int(deleted or 0) + int(inserted or 0)


def _seed_employee_opening_balances(*, employee_id: str, region: str, year: int, current_user_name: str) -> list[dict]:
    pto_types = get_pto_types()
    pto_actions, _ = _build_pto_action_catalog()
    load_action_code = _resolve_action_code(action_catalog=pto_actions, include_tokens=_LOAD_ACTION_TOKENS)
    if not load_action_code:
        load_action_code = "LOAD"

    transactions: list[dict] = []
    for item in pto_types:
        pto_type_code = _normalize_text(item.get("code")).upper()
        if not pto_type_code:
            continue
        default_hour = int(item.get("usaDefaultHour") or 0)
        if _normalize_team_region(region) == "Philippines":
            default_hour = int(item.get("phlDefaultHour") or default_hour)
        elif _normalize_team_region(region) == "Mexico":
            default_hour = int(item.get("usaDefaultHour") or default_hour)
        if default_hour <= 0:
            continue
        transaction = {
            "id": str(uuid4()),
            "employeeId": employee_id,
            "ptoTypeCode": pto_type_code,
            "ptoActionCode": load_action_code,
            "hours": Decimal(str(default_hour)).quantize(Decimal("0.01")),
            "year": year,
            "startDate": None,
            "endDate": None,
            "status": "Approved",
            "description": "Opening PTO balance load",
            "approverNote": None,
            "approverId": None,
            "calendarId": None,
        }
        insert_pto_transaction(transaction)
        transactions.append(transaction)
    return transactions


def update_leave_management_setup_data(*, request, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    kind = _normalize_text(payload.get("kind"))
    current_employee = _resolve_current_employee_record(request)
    current_user_name = _employee_full_name(current_employee)
    current_year = date.today().year

    if kind == "pto_type":
        code = _normalize_text(payload.get("code")).upper()
        label = _normalize_text(payload.get("label"))
        active = bool(payload.get("active", True))
        if not code:
            raise ValueError("code is required")
        if not label:
            raise ValueError("label is required")
        existing = get_pto_types(code=code)
        if existing:
            modify_pto_type(code=code, payload={"name": label, "rolloverable": False, "payoutable": False})
        else:
            create_pto_type(
                {
                    "code": code,
                    "name": label,
                    "rolloverable": False,
                    "payoutable": False,
                    "usaDefaultHour": 0,
                    "phlDefaultHour": 0,
                }
            )
        if not active:
            modify_pto_type(code=code, payload={"name": label})
    elif kind == "pto_action":
        code = _normalize_text(payload.get("code")).upper()
        label = _normalize_text(payload.get("label"))
        detail = _normalize_text(payload.get("detail"))
        if not label:
            raise ValueError("label is required")
        existing = get_pto_actions(code=code) if code else []
        if existing:
            modify_pto_action(code=code, payload={"name": label, "color": detail or None})
        else:
            create_pto_action({"code": code or f"ACT{uuid4().hex[:5]}", "name": label, "color": detail or None})
    elif kind == "employee":
        employee_name = _normalize_text(payload.get("employeeName"))
        if not employee_name:
            raise ValueError("employeeName is required")
        title = _normalize_optional_text(payload.get("title"))
        manager_id = _normalize_text(payload.get("managerId")) or current_employee["id"]
        team_region = _normalize_team_region(payload.get("teamRegion"))
        first_name, last_name = _split_employee_name(employee_name)
        identity_key, synthetic_email = _make_synthetic_identity(employee_name)
        employee_result = create_employee(
            {
                "identityKey": identity_key,
                "firstName": first_name,
                "lastName": last_name,
                "email": synthetic_email,
                "region": team_region,
                "title": title or "Team Member",
                "active": bool(payload.get("active", True)),
            }
        )
        employee_id = employee_result["id"]
        _replace_employee_manager_mapping(employee_id=employee_id, manager_id=manager_id or current_employee["id"])
        _seed_employee_opening_balances(
            employee_id=employee_id,
            region=team_region,
            year=current_year,
            current_user_name=current_user_name,
        )
    elif kind == "employee_manager":
        employee_id = _normalize_text(payload.get("employeeId"))
        manager_id = _normalize_text(payload.get("managerId"))
        if not employee_id or not manager_id:
            raise ValueError("employeeId and managerId are required")
        _replace_employee_manager_mapping(employee_id=employee_id, manager_id=manager_id)
    elif kind == "holiday":
        holiday_name = _normalize_text(payload.get("name"))
        holiday_date = _normalize_optional_iso_date(payload.get("date"))
        team_region = _normalize_team_region(payload.get("teamRegion"))
        if not holiday_name or not holiday_date:
            raise ValueError("Holiday name and date are required")
        upsert_holiday(
            {
                "id": f"holiday-{team_region.lower()}-{holiday_date}",
                "name": holiday_name,
                "date": holiday_date,
                "teamRegion": team_region,
            }
        )
    else:
        raise ValueError("kind is required")

    return {
        "workspace": _build_workspace(request=request, year=current_year),
        "source": "network",
    }
