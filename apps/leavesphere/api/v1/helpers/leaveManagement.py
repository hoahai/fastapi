from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from apps.leavesphere.api.v1.helpers.dbQueries import (
    get_employee_managers,
    get_employees,
    get_employees_by_ids,
    get_db_tables,
    get_holidays,
    cancel_pending_pto_transaction,
    get_pto_actions,
    get_pto_transactions,
    get_pto_types,
    insert_pto_transaction,
    update_pto_transaction,
    upsert_holiday,
)
from apps.leavesphere.api.v1.helpers.employees import create_employee
from apps.leavesphere.api.v1.helpers.myPto import (
    _employee_full_name,
    _extract_email_from_auth_payload,
    _normalize_email,
    _normalize_team_region,
    _normalize_text,
    _normalize_year,
    _is_before_start_date,
    _resolve_current_employee,
    _resolve_primary_manager_id,
    _split_ui_type_tokens,
    _to_date_string,
)
from apps.leavesphere.api.v1.helpers.config import (
    resolve_employee_email,
    resolve_employee_email_candidates,
)
from apps.leavesphere.api.v1.helpers.ptoAccounting import (
    action_matches_tokens,
    ADJUSTMENT_ACTION_TOKENS,
    is_load_action_row,
    is_request_action_code,
    is_request_action_row,
    LOAD_ACTION_TOKENS,
    REQUEST_ACTION_TOKENS,
    resolve_action_code,
    signed_pto_hours,
)
from apps.leavesphere.api.v1.helpers.ptoWorkspaceShared import (
    build_leave_sphere_workspace_common_payload,
    build_pto_employee_map,
    build_pto_request_rows,
    clear_leave_sphere_pto_workspace_catalog_cache,
    load_leave_sphere_pto_workspace_catalogs,
    resolve_pto_type_code_from_catalog,
)
from apps.leavesphere.api.v1.helpers.readCache import clear_leave_sphere_read_cache
from apps.leavesphere.api.v1.helpers.ptoActions import create_pto_action, modify_pto_action
from apps.leavesphere.api.v1.helpers.ptoTypes import create_pto_type, modify_pto_type
from apps.leavesphere.api.v1.helpers.ptoTransactions import approve_request, create_adjustment, create_request, reject_request
from apps.leavesphere.api.v1.helpers.workspaceCache import (
    clear_leave_sphere_workspace_cache_by_page,
    read_leave_sphere_workspace_cache,
    read_leave_sphere_workspace_latest_cache,
    write_leave_sphere_workspace_cache,
    write_leave_sphere_workspace_cache_value,
)
from shared.auth.dependencies import get_auth_principal
from shared.db import execute_write, run_transaction

LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE = "leave-management"
LEAVESPHERE_LEAVE_MANAGEMENT_LEGACY_CACHE_PAGE_CODE = "admin-pto"


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
    return resolve_pto_type_code_from_catalog(value=value, pto_type_by_code=pto_type_by_code)


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
        if is_load_action_row(row, pto_action_by_code):
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
        if not (
            is_load_action_row(row, pto_action_by_code)
            or action_matches_tokens(row.get("ptoActionCode"), tokens=ADJUSTMENT_ACTION_TOKENS)
        ):
            continue
        employee_id = _normalize_text(row.get("employeeId"))
        pto_type_code = _normalize_text(row.get("ptoTypeCode")).upper()
        if not employee_id or not pto_type_code:
            continue
        action_code = "adjustment" if action_matches_tokens(row.get("ptoActionCode"), tokens=ADJUSTMENT_ACTION_TOKENS) else "load_grant"
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
                "description": _normalize_text(row.get("description")) or None,
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

    employee_map = build_pto_employee_map(employees)
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

    catalogs = load_leave_sphere_pto_workspace_catalogs()
    pto_type_by_code = catalogs.pto_type_by_code
    pto_actions = catalogs.pto_actions
    pto_action_by_code = catalogs.pto_action_by_code
    balance_transaction_rows = get_pto_transactions(year=selected_year)
    year_request_rows = [row for row in balance_transaction_rows if is_request_action_row(row, pto_action_by_code)]
    request_rows: list[dict] = []
    if include_year_requests:
        request_rows = [row for row in balance_transaction_rows if is_request_action_row(row, pto_action_by_code)]

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

    requests = build_pto_request_rows(
        rows=request_rows,
        employee_map=employee_map,
        pto_type_by_code=pto_type_by_code,
        current_employee_id=current_employee_id,
        manager_id_by_employee_id=manager_map,
        employee_full_name_fn=_employee_full_name,
    )
    employee_balances = _build_employee_balances(
        employees=employees,
        employee_map=employee_map,
        pto_types=catalogs.pto_types,
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
        **build_leave_sphere_workspace_common_payload(catalogs),
        "balances": current_employee_balances,
        "employeeBalances": employee_balances,
        "balanceTransactions": balance_transactions,
        "requests": requests,
        "holidays": holidays,
        "employees": employee_rows,
        "directReports": direct_reports,
    }


def _resolve_workspace_cache_user_key(request) -> str:
    principal = get_auth_principal(request)
    if principal is not None:
        principal_email = _normalize_email(getattr(principal, "email", ""))
        if principal_email:
            return principal_email
        principal_id = _normalize_text(getattr(principal, "user_id", "")).lower()
        if principal_id:
            return principal_id
        raw_email = _extract_email_from_auth_payload(getattr(principal, "raw_user", None))
        if raw_email:
            return raw_email

    headers = getattr(request, "headers", {})
    header_email = _normalize_email(headers.get("x-user-email"))
    if header_email:
        return header_email
    header_name = _normalize_text(headers.get("x-user-name")).lower()
    if header_name:
        return header_name
    return "unknown"


def _workspace_page_code_candidates(page_code: str) -> tuple[str, ...]:
    normalized_page_code = _normalize_text(page_code)
    if normalized_page_code == LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE:
        return (
            LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
            LEAVESPHERE_LEAVE_MANAGEMENT_LEGACY_CACHE_PAGE_CODE,
        )
    return (normalized_page_code or LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,)


def _load_cached_workspace_snapshot(
    *,
    request,
    page_code: str,
    year: int,
    params: dict[str, object] | None = None,
) -> dict | None:
    user_key = _resolve_workspace_cache_user_key(request)
    for candidate_page_code in _workspace_page_code_candidates(page_code):
        snapshot = read_leave_sphere_workspace_cache(
            page_code=candidate_page_code,
            user_key=user_key,
            year=year,
            params=params,
        )
        if snapshot and int(snapshot.get("year") or 0) == int(year):
            workspace = snapshot.get("workspace")
            if isinstance(workspace, dict):
                return workspace
    return None


def _store_cached_workspace_snapshot(
    *,
    request,
    page_code: str,
    year: int,
    workspace: dict,
    params: dict[str, object] | None = None,
) -> None:
    user_key = _resolve_workspace_cache_user_key(request)
    write_leave_sphere_workspace_cache(
        page_code=page_code,
        user_key=user_key,
        year=year,
        workspace=workspace,
        params=params,
    )


def _sort_leave_management_request_rows(request_rows: list[dict]) -> list[dict]:
    return sorted(
        request_rows,
        key=lambda item: (
            _normalize_text(item.get("submittedAt")),
            _normalize_text(item.get("id")),
        ),
        reverse=True,
    )


def _sort_leave_management_balance_transaction_rows(transaction_rows: list[dict]) -> list[dict]:
    return sorted(
        transaction_rows,
        key=lambda item: (
            _normalize_text(item.get("createdAt")),
            _normalize_text(item.get("id")),
        ),
        reverse=True,
    )


def _build_leave_management_pto_type_by_code(workspace: dict) -> dict[str, dict]:
    pto_types = workspace.get("ptoTypes") if isinstance(workspace.get("ptoTypes"), list) else []
    return {
        _normalize_text(row.get("code")).upper(): row
        for row in pto_types
        if isinstance(row, dict) and _normalize_text(row.get("code"))
    }


def _rebuild_leave_management_workspace_employee_balances(workspace: dict) -> None:
    employees = workspace.get("employees") if isinstance(workspace.get("employees"), list) else []
    pto_types = workspace.get("ptoTypes") if isinstance(workspace.get("ptoTypes"), list) else []
    pto_actions = workspace.get("ptoActions") if isinstance(workspace.get("ptoActions"), list) else []
    requests = workspace.get("requests") if isinstance(workspace.get("requests"), list) else []
    balance_transactions = workspace.get("balanceTransactions") if isinstance(workspace.get("balanceTransactions"), list) else []

    employee_map = build_pto_employee_map([row for row in employees if isinstance(row, dict)])
    pto_type_by_code = {
        _normalize_text(row.get("code")).upper(): row
        for row in pto_types
        if isinstance(row, dict) and _normalize_text(row.get("code"))
    }
    pto_action_by_code = {
        _normalize_text(row.get("code")).upper(): row
        for row in pto_actions
        if isinstance(row, dict) and _normalize_text(row.get("code"))
    }
    employee_balances = _build_employee_balances(
        employees=[row for row in employees if isinstance(row, dict)],
        employee_map=employee_map,
        pto_types=[row for row in pto_types if isinstance(row, dict)],
        pto_type_by_code=pto_type_by_code,
        pto_action_by_code=pto_action_by_code,
        request_rows=[row for row in requests if isinstance(row, dict)],
        balance_rows=[row for row in balance_transactions if isinstance(row, dict)],
    )
    workspace["employeeBalances"] = employee_balances

    current_user_id = _normalize_text(workspace.get("currentUserId"))
    current_user_balances = next(
        (row.get("balances") for row in employee_balances if row.get("employeeId") == current_user_id),
        [],
    )
    workspace["balances"] = current_user_balances if isinstance(current_user_balances, list) else []


def _upsert_leave_management_request_row(
    *,
    workspace: dict,
    request_row: dict,
) -> None:
    requests = [row for row in workspace.get("requests", []) if isinstance(row, dict)]
    request_id = _normalize_text(request_row.get("id"))
    updated = False
    for index, row in enumerate(requests):
        if _normalize_text(row.get("id")) != request_id:
            continue
        requests[index] = request_row
        updated = True
        break
    if not updated:
        requests.append(request_row)
    workspace["requests"] = _sort_leave_management_request_rows(requests)


def _replace_leave_management_request_row(
    *,
    workspace: dict,
    transaction_id: str,
    updates: dict,
) -> bool:
    requests = [row for row in workspace.get("requests", []) if isinstance(row, dict)]
    for index, row in enumerate(requests):
        if _normalize_text(row.get("id")) != _normalize_text(transaction_id):
            continue
        next_row = {**row, **updates}
        requests[index] = next_row
        workspace["requests"] = _sort_leave_management_request_rows(requests)
        return True
    return False


def _upsert_leave_management_balance_transaction_row(
    *,
    workspace: dict,
    transaction_row: dict,
) -> None:
    rows = [row for row in workspace.get("balanceTransactions", []) if isinstance(row, dict)]
    transaction_id = _normalize_text(transaction_row.get("id"))
    updated = False
    for index, row in enumerate(rows):
        if _normalize_text(row.get("id")) != transaction_id:
            continue
        rows[index] = transaction_row
        updated = True
        break
    if not updated:
        rows.append(transaction_row)
    workspace["balanceTransactions"] = _sort_leave_management_balance_transaction_rows(rows)


def _build_leave_management_request_row_from_payload(
    *,
    workspace: dict,
    request_id: str,
    payload: dict,
    status: str,
    current_user_name: str,
) -> dict:
    pto_types = [row for row in workspace.get("ptoTypes", []) if isinstance(row, dict)]
    pto_type_by_code = {
        _normalize_text(row.get("code")).upper(): row
        for row in pto_types
        if _normalize_text(row.get("code"))
    }
    pto_type_code = _resolve_pto_type_code(
        value=payload.get("ptoTypeCode") or payload.get("type"),
        pto_type_by_code=pto_type_by_code,
    )
    pto_type = pto_type_by_code.get(pto_type_code, {})
    employee_id = _normalize_text(payload.get("employeeId"))
    employee_row = next(
        (row for row in workspace.get("employees", []) if isinstance(row, dict) and _normalize_text(row.get("employeeId")) == employee_id),
        {},
    )
    manager_id = _normalize_text(employee_row.get("managerId")) or _normalize_text(workspace.get("managerId")) or _normalize_text(workspace.get("currentUserId"))
    today = date.today().isoformat()
    request_type = _normalize_text(pto_type.get("type"))
    if not request_type:
        request_type = pto_type_code.lower() if pto_type_code else _normalize_text(payload.get("type")) or "vacation"
    return {
        "id": request_id,
        "employeeId": employee_id,
        "managerId": manager_id or None,
        "type": request_type,
        "ptoTypeCode": pto_type_code,
        "startDate": _normalize_optional_iso_date(payload.get("startDate")) or today,
        "endDate": _normalize_optional_iso_date(payload.get("endDate")) or today,
        "hours": float(_normalize_positive_requested_hours(payload.get("hours"))),
        "description": _normalize_text(payload.get("description")) or None,
        "status": status.lower(),
        "submittedAt": today,
        "reviewedAt": today if status.lower() != "pending" else None,
        "reviewerName": current_user_name if status.lower() != "pending" else None,
        "approverNote": None,
    }


def _apply_leave_management_workspace_mutation_from_cache(
    *,
    request,
    year: int,
    mutate_workspace,
) -> dict | None:
    user_key = _resolve_workspace_cache_user_key(request)
    latest = None
    for candidate_page_code in _workspace_page_code_candidates(LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE):
        latest = read_leave_sphere_workspace_latest_cache(page_code=candidate_page_code, user_key=user_key)
        if isinstance(latest, dict) and int(latest.get("year") or 0) == int(year):
            break
        latest = None
    if not isinstance(latest, dict) or int(latest.get("year") or 0) != int(year):
        return None
    cached_workspace = latest.get("workspace")
    if not isinstance(cached_workspace, dict):
        return None
    cache_key = _normalize_text(latest.get("cacheKey"))
    if not cache_key:
        return None

    workspace = cached_workspace
    mutated = mutate_workspace(workspace)
    if not mutated:
        return None

    _rebuild_leave_management_workspace_employee_balances(workspace)
    write_leave_sphere_workspace_cache_value(
        cache_key=cache_key,
        page_code=LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
        user_key=user_key,
        year=year,
        workspace=workspace,
    )
    return workspace


def _copy_workspace_row(row: dict) -> dict:
    return deepcopy(row)


def _build_leave_management_workspace_patch(
    *,
    workspace: dict,
    request_ids: list[str] | None = None,
    balance_transaction_ids: list[str] | None = None,
    employee_ids: list[str] | None = None,
    holiday_ids: list[str] | None = None,
    pto_type_codes: list[str] | None = None,
    pto_action_codes: list[str] | None = None,
    include_current_balances: bool = False,
) -> dict:
    patch = {
        "currentUserId": workspace.get("currentUserId"),
        "currentUserName": workspace.get("currentUserName"),
        "currentUserEmail": workspace.get("currentUserEmail"),
        "managerId": workspace.get("managerId"),
        "currentUserTeamRegion": workspace.get("currentUserTeamRegion"),
        "isManager": workspace.get("isManager"),
    }
    if request_ids:
        request_id_set = {_normalize_text(item) for item in request_ids if _normalize_text(item)}
        patch["requests"] = [
            _copy_workspace_row(row)
            for row in workspace.get("requests", [])
            if isinstance(row, dict) and _normalize_text(row.get("id")) in request_id_set
        ]
    if balance_transaction_ids:
        transaction_id_set = {_normalize_text(item) for item in balance_transaction_ids if _normalize_text(item)}
        patch["balanceTransactions"] = [
            _copy_workspace_row(row)
            for row in workspace.get("balanceTransactions", [])
            if isinstance(row, dict) and _normalize_text(row.get("id")) in transaction_id_set
        ]
    if employee_ids:
        employee_id_set = {_normalize_text(item) for item in employee_ids if _normalize_text(item)}
        patch["employees"] = [
            _copy_workspace_row(row)
            for row in workspace.get("employees", [])
            if isinstance(row, dict) and _normalize_text(row.get("employeeId")) in employee_id_set
        ]
        patch["employeeBalances"] = [
            _copy_workspace_row(row)
            for row in workspace.get("employeeBalances", [])
            if isinstance(row, dict) and _normalize_text(row.get("employeeId")) in employee_id_set
        ]
    if holiday_ids:
        holiday_id_set = {_normalize_text(item) for item in holiday_ids if _normalize_text(item)}
        patch["holidays"] = [
            _copy_workspace_row(row)
            for row in workspace.get("holidays", [])
            if isinstance(row, dict) and _normalize_text(row.get("id")) in holiday_id_set
        ]
    if pto_type_codes:
        pto_type_code_set = {_normalize_text(item).upper() for item in pto_type_codes if _normalize_text(item)}
        patch["ptoTypes"] = [
            _copy_workspace_row(row)
            for row in workspace.get("ptoTypes", [])
            if isinstance(row, dict) and _normalize_text(row.get("code")).upper() in pto_type_code_set
        ]
    if pto_action_codes:
        pto_action_code_set = {_normalize_text(item).upper() for item in pto_action_codes if _normalize_text(item)}
        patch["ptoActions"] = [
            _copy_workspace_row(row)
            for row in workspace.get("ptoActions", [])
            if isinstance(row, dict) and _normalize_text(row.get("code")).upper() in pto_action_code_set
        ]
    if include_current_balances:
        current_user_id = _normalize_text(workspace.get("currentUserId"))
        current_row = next(
            (
                row
                for row in workspace.get("employeeBalances", [])
                if isinstance(row, dict) and _normalize_text(row.get("employeeId")) == current_user_id
            ),
            None,
        )
        if isinstance(current_row, dict):
            balances = current_row.get("balances") if isinstance(current_row.get("balances"), list) else []
            patch["balances"] = [_copy_workspace_row(row) for row in balances if isinstance(row, dict)]
    return patch


def _build_leave_management_request_patch_from_transaction(
    *,
    request,
    transaction_id: str,
) -> dict | None:
    transaction_rows = get_pto_transactions(transaction_id=transaction_id)
    if not transaction_rows:
        return None

    employees = get_employees()
    try:
        current_employee = _resolve_current_employee_from_rows(request, employees)
    except ValueError:
        current_employee = _build_synthetic_current_employee(request)
    current_employee_id = _normalize_text(current_employee.get("id"))
    if not current_employee_id:
        return None

    employee_map = build_pto_employee_map(employees)
    employee_map.setdefault(current_employee_id, current_employee)
    manager_map = _build_employee_manager_map(get_employee_managers())
    catalogs = load_leave_sphere_pto_workspace_catalogs()
    request_rows = build_pto_request_rows(
        rows=transaction_rows,
        employee_map=employee_map,
        pto_type_by_code=catalogs.pto_type_by_code,
        current_employee_id=current_employee_id,
        manager_id_by_employee_id=manager_map,
        employee_full_name_fn=_employee_full_name,
    )
    if not request_rows:
        return None
    return request_rows[0]


def load_leave_management_workspace(
    *,
    request,
    year: int | None = None,
    include_pending: bool = True,
    history_start_date: str | None = None,
    history_end_date: str | None = None,
    overlap_month: str | None = None,
    force_refresh: bool = False,
) -> dict:
    selected_year = _normalize_year(year)
    cache_params = {
        "include_pending": include_pending,
        "history_start_date": history_start_date,
        "history_end_date": history_end_date,
        "overlap_month": overlap_month,
    }
    if not force_refresh:
        cached_workspace = _load_cached_workspace_snapshot(
            request=request,
            page_code=LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
            year=selected_year,
            params=cache_params,
        )
        if isinstance(cached_workspace, dict):
            return cached_workspace

    workspace = _build_workspace(
        request=request,
        year=selected_year,
        include_year_requests=False,
        include_pending=include_pending,
        history_start_date=history_start_date,
        history_end_date=history_end_date,
        overlap_month=overlap_month,
    )
    _store_cached_workspace_snapshot(
        request=request,
        page_code=LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
        year=selected_year,
        workspace=workspace,
        params=cache_params,
    )
    return workspace


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


def _create_immediate_approved_request(
    *,
    employee_id: str,
    pto_type_code: str,
    pto_action_code: str,
    requested_hours: Decimal,
    year: int,
    start_date: str | None,
    end_date: str | None,
    description: str | None,
    approver_id: str | None,
    calendar_id: str | None,
) -> tuple[str, int]:
    transaction_id = str(uuid4())

    def _work(cursor) -> int:
        tables = get_db_tables()
        cursor.execute(
            "SELECT ptoActionCode, hours, status "
            f"FROM {tables['PTOTRANSACTIONS']} "
            "WHERE employeeId = %s AND ptoTypeCode = %s AND year = %s "
            "FOR UPDATE",
            (employee_id, pto_type_code, year),
        )
        rows = cursor.fetchall() or []
        approved = Decimal("0.00")
        pending = Decimal("0.00")
        for row in rows:
            action_code = str(row[0] or "").strip().upper()
            hours = Decimal(str(row[1] or 0)).quantize(Decimal("0.01"))
            status = str(row[2] or "").strip().lower()
            if status == "approved":
                approved += signed_pto_hours(action_code, hours)
            elif status == "pending" and is_request_action_code(action_code):
                pending += abs(hours)

        available_after_approval = approved - pending - requested_hours
        if available_after_approval < 0:
            raise ValueError("Cannot approve request because available balance is below zero")

        cursor.execute(
            f"INSERT INTO {tables['PTOTRANSACTIONS']} ("
            "id, employeeId, ptoTypeCode, ptoActionCode, hours, year, startDate, endDate, "
            "status, description, approverNote, approverId, calendarId"
            ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                transaction_id,
                employee_id,
                pto_type_code,
                pto_action_code,
                requested_hours.quantize(Decimal("0.01")),
                year,
                start_date,
                end_date,
                "Approved",
                description,
                None,
                approver_id,
                calendar_id,
            ),
        )
        return int(cursor.rowcount or 0)

    inserted = int(run_transaction(_work) or 0)
    return transaction_id, inserted


def create_leave_management_request(*, request, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    current_employee = _resolve_current_employee_record(request)
    current_employee_id = _normalize_text(current_employee.get("id"))
    current_employee_name = _employee_full_name(current_employee)
    catalogs = load_leave_sphere_pto_workspace_catalogs()
    pto_type_by_code = catalogs.pto_type_by_code
    pto_actions = catalogs.pto_actions

    employee_id = _normalize_text(payload.get("employeeId"))
    if not employee_id:
        raise ValueError("employeeId is required")

    pto_type_code = _resolve_pto_type_code(
        value=payload.get("ptoTypeCode") or payload.get("type"),
        pto_type_by_code=pto_type_by_code,
    )
    if not pto_type_code:
        raise ValueError("ptoTypeCode is required")

    pto_action_code = resolve_action_code(
        action_catalog=pto_actions,
        include_tokens=REQUEST_ACTION_TOKENS,
    )
    if not pto_action_code:
        raise ValueError("ptoActionCode is required")

    requested_hours = _normalize_positive_requested_hours(payload.get("hours"))
    start_date = _normalize_optional_iso_date(payload.get("startDate"))
    end_date = _normalize_optional_iso_date(payload.get("endDate"))
    if start_date and end_date and start_date > end_date:
        raise ValueError("startDate must be on or before endDate")

    year = _resolve_request_year(payload)
    description = _normalize_text(payload.get("description")) or None
    calendar_id = _normalize_text(payload.get("calendarId")) or None
    if calendar_id and len(calendar_id) > 30:
        raise ValueError("calendarId must be <= 30 characters")

    approve_immediately = bool(payload.get("approveImmediately"))
    if approve_immediately:
        created_request_id, inserted = _create_immediate_approved_request(
            employee_id=employee_id,
            pto_type_code=pto_type_code,
            pto_action_code=pto_action_code,
            requested_hours=requested_hours,
            year=year,
            start_date=start_date or None,
            end_date=end_date or None,
            description=description,
            approver_id=current_employee_id,
            calendar_id=calendar_id,
        )
    else:
        created_request = create_request(
            {
                "employeeId": employee_id,
                "ptoTypeCode": pto_type_code,
                "ptoActionCode": pto_action_code,
                "hours": requested_hours,
                "year": year,
                "startDate": start_date or None,
                "endDate": end_date or None,
                "description": description,
                "approverNote": None,
                "calendarId": calendar_id,
            }
        )
        created_request_id = str(created_request.get("id") or "").strip()
        inserted = created_request.get("inserted")
        if not created_request_id:
            raise ValueError("Failed to create PTO request")

    workspace = _apply_leave_management_workspace_mutation_from_cache(
        request=request,
        year=year,
        mutate_workspace=lambda cached_workspace: (
            _upsert_leave_management_request_row(
                workspace=cached_workspace,
                request_row=_build_leave_management_request_row_from_payload(
                    workspace=cached_workspace,
                    request_id=created_request_id,
                    payload=payload,
                    status="Approved" if approve_immediately else "Pending",
                    current_user_name=current_employee_name,
                ),
            )
            or True
        ),
    )
    workspace_patch = None
    if workspace is not None:
        workspace_patch = _build_leave_management_workspace_patch(
            workspace=workspace,
            request_ids=[created_request_id],
            employee_ids=[employee_id],
            include_current_balances=_normalize_text(employee_id) == _normalize_text(workspace.get("currentUserId")),
        )
    if workspace is None:
        workspace = _build_workspace(request=request, year=year)
        _store_cached_workspace_snapshot(
            request=request,
            page_code=LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
            year=year,
            workspace=workspace,
        )
        workspace_patch = _build_leave_management_workspace_patch(
            workspace=workspace,
            request_ids=[created_request_id],
            employee_ids=[employee_id],
            include_current_balances=_normalize_text(employee_id) == _normalize_text(workspace.get("currentUserId")),
        )
    response = {
        "source": "network",
        "createdRequestId": created_request_id,
        "inserted": inserted,
        "status": "Approved" if approve_immediately else "Pending",
    }
    if workspace_patch is not None:
        response["workspacePatch"] = workspace_patch
    else:
        request_patch_row = _build_leave_management_request_patch_from_transaction(
            request=request,
            transaction_id=created_request_id,
        )
        if request_patch_row is not None:
            response["workspacePatch"] = {"requests": [request_patch_row]}
        else:
            response["workspace"] = workspace
    return response


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

    catalogs = load_leave_sphere_pto_workspace_catalogs()
    pto_type_by_code = catalogs.pto_type_by_code
    pto_type_code = _resolve_pto_type_code(
        value=payload.get("ptoTypeCode") or payload.get("type") or transaction.get("ptoTypeCode"),
        pto_type_by_code=pto_type_by_code,
    )
    if not pto_type_code:
        raise ValueError("ptoTypeCode is required")

    hours = _normalize_positive_requested_hours(payload.get("hours"))
    stored_hours = hours.quantize(Decimal("0.01"))

    updates = {
        "ptoTypeCode": pto_type_code,
        "hours": stored_hours,
        "year": _resolve_request_year(payload, transaction),
        "startDate": _normalize_optional_iso_date(payload.get("startDate")) or transaction.get("startDate"),
        "endDate": _normalize_optional_iso_date(payload.get("endDate")) or transaction.get("endDate"),
        "description": _normalize_text(payload.get("description")) or transaction.get("description"),
    }
    updated = update_pto_transaction(transaction_id=transaction_id, updates=updates)
    requested_status = _normalize_text(transaction.get("status")).lower()
    year_key = _resolve_workspace_year_from_transaction(transaction)
    workspace = _apply_leave_management_workspace_mutation_from_cache(
        request=request,
        year=year_key,
        mutate_workspace=lambda cached_workspace: (
            _replace_leave_management_request_row(
                workspace=cached_workspace,
                transaction_id=transaction_id,
                updates={
                    "ptoTypeCode": pto_type_code,
                    "type": _normalize_text(_build_leave_management_pto_type_by_code(cached_workspace).get(pto_type_code, {}).get("type"))
                    or _normalize_text(payload.get("type"))
                    or "vacation",
                    "hours": float(stored_hours),
                    "startDate": updates["startDate"],
                    "endDate": updates["endDate"],
                    "description": updates["description"],
                    "status": requested_status,
                },
            )
            or True
        ),
    )
    workspace_patch = None
    if workspace is not None:
        workspace_patch = _build_leave_management_workspace_patch(
            workspace=workspace,
            request_ids=[transaction_id],
            employee_ids=[_normalize_text(transaction.get("employeeId"))],
            include_current_balances=_normalize_text(transaction.get("employeeId")) == _normalize_text(workspace.get("currentUserId")),
        )
    if workspace is None:
        workspace = _build_workspace(request=request, year=year_key)
        _store_cached_workspace_snapshot(
            request=request,
            page_code=LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
            year=year_key,
            workspace=workspace,
        )
        workspace_patch = _build_leave_management_workspace_patch(
            workspace=workspace,
            request_ids=[transaction_id],
            employee_ids=[_normalize_text(transaction.get("employeeId"))],
            include_current_balances=_normalize_text(transaction.get("employeeId")) == _normalize_text(workspace.get("currentUserId")),
        )
    response = {
        "source": "network",
        "updated": updated,
    }
    if workspace_patch is not None:
        response["workspacePatch"] = workspace_patch
    else:
        request_patch_row = _build_leave_management_request_patch_from_transaction(
            request=request,
            transaction_id=transaction_id,
        )
        if request_patch_row is not None:
            response["workspacePatch"] = {"requests": [request_patch_row]}
        else:
            response["workspace"] = workspace
    return response


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
        result = approve_request(
            request=request,
            transaction_id=transaction_id,
            approverNote=approver_note,
            force_admin_override=True,
        )
    elif action == "reject":
        result = reject_request(
            request=request,
            transaction_id=transaction_id,
            approverNote=approver_note,
            force_admin_override=True,
        )
    elif action == "cancel":
        if _normalize_text(transaction.get("status")).lower() not in {"pending", "approved", "rejected"}:
            raise ValueError("Only Pending, Approved, or Rejected PTO transactions can be canceled")
        if not _is_before_start_date(start_date=_to_date_string(transaction.get("startDate"))):
            raise ValueError("Only future PTO requests can be canceled")
        if _normalize_text(transaction.get("status")).lower() == "pending":
            updated = cancel_pending_pto_transaction(transaction_id=transaction_id)
        else:
            updated = update_pto_transaction(
                transaction_id=transaction_id,
                updates={
                    "status": "Canceled",
                    "approverId": None,
                },
            )
        result = {"updated": updated, "status": "Canceled"}
    elif action == "revert":
        if _normalize_text(transaction.get("status")).lower() not in {"approved", "rejected", "canceled"}:
            raise ValueError("Only Approved, Rejected, or Canceled PTO transactions can be reverted")
        if not _is_before_start_date(start_date=_to_date_string(transaction.get("startDate"))):
            raise ValueError("Only future PTO requests can be reverted")
        updated = update_pto_transaction(
            transaction_id=transaction_id,
            updates={
                "status": "Pending",
                "approverId": None,
            },
        )
        result = {"updated": updated, "status": "Pending"}
    else:
        raise ValueError("action must be approve, reject, cancel, or revert")

    workspace = _apply_leave_management_workspace_mutation_from_cache(
        request=request,
        year=_resolve_workspace_year_from_transaction(transaction),
        mutate_workspace=lambda cached_workspace: (
            _replace_leave_management_request_row(
                workspace=cached_workspace,
                transaction_id=transaction_id,
                updates={
                    "status": result.get("status", "Pending").lower(),
                    "approverNote": approver_note,
                    "reviewedAt": date.today().isoformat(),
                    "reviewerName": _normalize_text(cached_workspace.get("currentUserName")),
                },
            )
            or True
        ),
    )
    workspace_patch = None
    if workspace is not None:
        workspace_patch = _build_leave_management_workspace_patch(
            workspace=workspace,
            request_ids=[transaction_id],
            employee_ids=[_normalize_text(transaction.get("employeeId"))],
            include_current_balances=_normalize_text(transaction.get("employeeId")) == _normalize_text(workspace.get("currentUserId")),
        )
    if workspace is None:
        workspace = _build_workspace(request=request, year=_resolve_workspace_year_from_transaction(transaction))
        _store_cached_workspace_snapshot(
            request=request,
            page_code=LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
            year=_resolve_workspace_year_from_transaction(transaction),
            workspace=workspace,
        )
        workspace_patch = _build_leave_management_workspace_patch(
            workspace=workspace,
            request_ids=[transaction_id],
            employee_ids=[_normalize_text(transaction.get("employeeId"))],
            include_current_balances=_normalize_text(transaction.get("employeeId")) == _normalize_text(workspace.get("currentUserId")),
        )
    response = {
        "source": "network",
        "updated": result.get("updated"),
        "status": result.get("status"),
    }
    if workspace_patch is not None:
        response["workspacePatch"] = workspace_patch
    else:
        request_patch_row = _build_leave_management_request_patch_from_transaction(
            request=request,
            transaction_id=transaction_id,
        )
        if request_patch_row is not None:
            response["workspacePatch"] = {"requests": [request_patch_row]}
        else:
            response["workspace"] = workspace
    return response


def adjust_leave_management_balance(*, request, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    current_employee = _resolve_current_employee_record(request)
    current_employee_id = _normalize_text(current_employee.get("id"))
    if not current_employee_id:
        raise ValueError("Authenticated user is not mapped to a LeaveSphere employee")

    employee_id = _normalize_text(payload.get("employeeId"))
    if not employee_id:
        raise ValueError("employeeId is required")

    catalogs = load_leave_sphere_pto_workspace_catalogs()
    pto_type_by_code = catalogs.pto_type_by_code
    pto_actions = catalogs.pto_actions
    action_code = _normalize_text(payload.get("ptoActionCode")).lower()
    if action_code == "load_grant":
        resolved_action_code = resolve_action_code(action_catalog=pto_actions, include_tokens=LOAD_ACTION_TOKENS)
    elif action_code == "adjustment":
        resolved_action_code = resolve_action_code(action_catalog=pto_actions, include_tokens=ADJUSTMENT_ACTION_TOKENS)
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

    description = _normalize_optional_text(payload.get("description"))
    approver_note = _normalize_optional_text(payload.get("approverNote"))
    transaction_id = _normalize_text(payload.get("transactionId"))
    if transaction_id:
        existing = get_pto_transactions(transaction_id=transaction_id)
        if not existing:
            raise ValueError("PTO transaction not found")
        existing_row = existing[0]
        updated = update_pto_transaction(
            transaction_id=transaction_id,
            updates={
                "employeeId": employee_id,
                "ptoTypeCode": pto_type_code,
                "ptoActionCode": resolved_action_code,
                "hours": hours,
                "year": year,
                "status": "Approved",
                "description": description if description is not None else _normalize_optional_text(existing_row.get("description")),
                "approverNote": approver_note,
                "approverId": current_employee_id,
            },
        )
    else:
        generated_transaction_id = str(uuid4())
        created_adjustment = create_adjustment(
            {
                "transactionId": generated_transaction_id,
                "employeeId": employee_id,
                "ptoTypeCode": pto_type_code,
                "ptoActionCode": resolved_action_code,
                "hours": hours,
                "year": year,
                "description": description,
                "approverNote": approver_note,
                "approverId": current_employee_id,
                "status": "Approved",
            }
        )
        updated = created_adjustment.get("inserted")
        transaction_id = str(created_adjustment.get("id") or generated_transaction_id or transaction_id or "").strip()

    workspace = _apply_leave_management_workspace_mutation_from_cache(
        request=request,
        year=year,
        mutate_workspace=lambda cached_workspace: (
            _upsert_leave_management_balance_transaction_row(
                workspace=cached_workspace,
                transaction_row={
                    "id": transaction_id or f"adjustment-{year}-{employee_id}-{pto_type_code}-{hours}",
                    "employeeId": employee_id,
                    "ptoTypeCode": pto_type_code,
                    "ptoActionCode": resolved_action_code,
                    "hours": float(hours),
                    "year": year,
                    "status": "Approved",
                    "description": description,
                    "approverNote": approver_note,
                    "approverId": current_employee_id,
                    "createdAt": date.today().isoformat(),
                    "createdByName": _normalize_text(cached_workspace.get("currentUserName")),
                },
            )
            or True
        ),
    )
    workspace_patch = None
    if workspace is not None:
        workspace_patch = _build_leave_management_workspace_patch(
            workspace=workspace,
            balance_transaction_ids=[transaction_id],
            employee_ids=[employee_id],
            include_current_balances=employee_id == _normalize_text(workspace.get("currentUserId")),
        )
    if workspace is None:
        workspace = _build_workspace(request=request, year=year)
        _store_cached_workspace_snapshot(
            request=request,
            page_code=LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
            year=year,
            workspace=workspace,
        )
        workspace_patch = _build_leave_management_workspace_patch(
            workspace=workspace,
            balance_transaction_ids=[transaction_id],
            employee_ids=[employee_id],
            include_current_balances=employee_id == _normalize_text(workspace.get("currentUserId")),
        )
    response = {
        "source": "network",
        "updated": updated,
        "status": "Approved",
    }
    if workspace_patch is not None:
        response["workspacePatch"] = workspace_patch
    else:
        response["workspace"] = workspace
    return response


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
    catalogs = load_leave_sphere_pto_workspace_catalogs()
    pto_actions = catalogs.pto_actions
    load_action_code = resolve_action_code(action_catalog=pto_actions, include_tokens=LOAD_ACTION_TOKENS)
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
    workspace_patch_kwargs: dict[str, object] = {}

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
        workspace_patch_kwargs["pto_type_codes"] = [code]
    elif kind == "pto_action":
        code = _normalize_text(payload.get("code")).upper()
        label = _normalize_text(payload.get("label"))
        detail = _normalize_text(payload.get("detail"))
        if not label:
            raise ValueError("label is required")
        existing = get_pto_actions(code=code) if code else []
        action_code = code or f"ACT{uuid4().hex[:5]}"
        if existing:
            modify_pto_action(code=action_code, payload={"name": label, "color": detail or None})
        else:
            create_pto_action({"code": action_code, "name": label, "color": detail or None})
        workspace_patch_kwargs["pto_action_codes"] = [action_code]
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
        workspace_patch_kwargs["employee_ids"] = [employee_id]
    elif kind == "employee_manager":
        employee_id = _normalize_text(payload.get("employeeId"))
        manager_id = _normalize_text(payload.get("managerId"))
        if not employee_id or not manager_id:
            raise ValueError("employeeId and managerId are required")
        _replace_employee_manager_mapping(employee_id=employee_id, manager_id=manager_id)
        workspace_patch_kwargs["employee_ids"] = [employee_id]
    elif kind == "holiday":
        holiday_name = _normalize_text(payload.get("name"))
        holiday_date = _normalize_optional_iso_date(payload.get("date"))
        team_region = _normalize_team_region(payload.get("teamRegion"))
        if not holiday_name or not holiday_date:
            raise ValueError("Holiday name and date are required")
        holiday_id = f"holiday-{team_region.lower()}-{holiday_date}"
        upsert_holiday(
            {
                "id": holiday_id,
                "name": holiday_name,
                "date": holiday_date,
                "teamRegion": team_region,
            }
        )
        workspace_patch_kwargs["holiday_ids"] = [holiday_id]
    else:
        raise ValueError("kind is required")

    clear_leave_sphere_pto_workspace_catalog_cache()
    clear_leave_sphere_read_cache()
    clear_leave_sphere_workspace_cache_by_page(page_code=LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE)
    clear_leave_sphere_workspace_cache_by_page(page_code=LEAVESPHERE_LEAVE_MANAGEMENT_LEGACY_CACHE_PAGE_CODE)
    clear_leave_sphere_workspace_cache_by_page(page_code="my-pto")

    workspace = _build_workspace(request=request, year=current_year)
    _store_cached_workspace_snapshot(
        request=request,
        page_code=LEAVESPHERE_LEAVE_MANAGEMENT_PAGE_CODE,
        year=current_year,
        workspace=workspace,
    )
    response = {
        "source": "network",
    }
    if workspace_patch_kwargs:
        response["workspacePatch"] = _build_leave_management_workspace_patch(workspace=workspace, **workspace_patch_kwargs)
    else:
        response["workspace"] = workspace
    return response
