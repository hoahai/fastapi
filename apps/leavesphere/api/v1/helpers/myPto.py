from __future__ import annotations

from collections import OrderedDict
from datetime import date, datetime
from decimal import Decimal

from apps.leavesphere.api.v1.helpers.dbQueries import (
    approve_pending_pto_request,
    cancel_pending_pto_transaction,
    get_db_tables,
    get_employee_managers,
    get_employees_by_email,
    get_employees_by_identity_key,
    get_employees_by_ids,
    get_holidays,
    get_pto_actions,
    get_pto_transactions,
    get_pto_types,
    reject_pending_pto_request,
    update_pto_transaction,
)
from shared.auth.dependencies import get_auth_principal, get_tenant_access
from shared.db import run_transaction

_UI_PTO_TYPE_ORDER = ("vacation", "sick", "personal", "floating")
_PTO_TYPE_TOKEN_MAP = (
    ("vac", "vacation"),
    ("sick", "sick"),
    ("personal", "personal"),
    ("float", "floating"),
)


def _normalize_year(value: object | None) -> int:
    try:
        year = int(str(value or "").strip() or date.today().year)
    except (TypeError, ValueError) as exc:
        raise ValueError("year must be an integer") from exc
    if year < 1901 or year > 2155:
        raise ValueError("year must be between 1901 and 2155")
    return year


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _normalize_email(value: object | None) -> str:
    return _normalize_text(value).lower()


def _extract_email_from_auth_payload(payload: object | None) -> str:
    if not isinstance(payload, dict):
        return ""

    direct_email = _normalize_email(payload.get("email"))
    if direct_email:
        return direct_email

    user_metadata = payload.get("user_metadata")
    if isinstance(user_metadata, dict):
        nested_email = _normalize_email(user_metadata.get("email"))
        if nested_email:
            return nested_email

    app_metadata = payload.get("app_metadata")
    if isinstance(app_metadata, dict):
        nested_email = _normalize_email(app_metadata.get("email"))
        if nested_email:
            return nested_email

    nested_user = payload.get("user")
    if isinstance(nested_user, dict):
        nested_email = _extract_email_from_auth_payload(nested_user)
        if nested_email:
            return nested_email

    return ""


def _normalize_team_region(value: object | None) -> str:
    normalized = _normalize_text(value).lower()
    if normalized in {"mexico", "mx"}:
        return "Mexico"
    if normalized in {"philippines", "ph", "phl"}:
        return "Philippines"
    return "US"


def _normalize_iso_date(value: object | None) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError as exc:
        raise ValueError("Date must use YYYY-MM-DD format") from exc


def _is_before_start_date(*, start_date: str | None) -> bool:
    if not start_date:
        return True
    today = date.today().isoformat()
    return today < start_date


def _employee_full_name(employee: dict | None) -> str:
    if not isinstance(employee, dict):
        return ""
    first_name = _normalize_text(employee.get("firstName"))
    last_name = _normalize_text(employee.get("lastName"))
    full_name = " ".join(part for part in (first_name, last_name) if part)
    if full_name:
        return full_name
    return _normalize_text(employee.get("email"))


def _to_date_string(value: object | None) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = _normalize_text(value)
    if not text:
        return ""
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return text[:10]


def _split_ui_type_tokens(*values: object | None) -> str | None:
    text = " ".join(_normalize_text(value).lower() for value in values if _normalize_text(value))
    if not text:
        return None
    for token, ui_type in _PTO_TYPE_TOKEN_MAP:
        if token in text:
            return ui_type
    return None


def _build_pto_type_catalog() -> tuple[list[dict], dict[str, dict]]:
    rows = get_pto_types()
    catalog: list[dict] = []
    by_code: dict[str, dict] = {}
    for row in rows:
        code = _normalize_text(row.get("code")).upper()
        if not code:
            continue
        label = _normalize_text(row.get("name")) or code
        ui_type = _split_ui_type_tokens(code, label) or code.lower()
        item = {
            "code": code,
            "type": ui_type,
            "label": label,
            "active": True,
            "listingOrder": int(row.get("listingOrder") or 0),
            "rolloverable": bool(int(row.get("rolloverable") or 0)),
            "payoutable": bool(int(row.get("payoutable") or 0)),
            "usaDefaultHour": int(row.get("usaDefaultHour") or 0),
            "phlDefaultHour": int(row.get("phlDefaultHour") or 0),
        }
        catalog.append(item)
        by_code[code] = item

    catalog.sort(key=lambda item: (item["listingOrder"], item["label"].lower(), item["code"]))
    if not catalog:
        for ui_type in _UI_PTO_TYPE_ORDER:
            catalog.append(
                {
                    "code": ui_type.upper(),
                    "type": ui_type,
                    "label": ui_type.title(),
                    "active": True,
                    "listingOrder": 0,
                    "rolloverable": False,
                    "payoutable": False,
                    "usaDefaultHour": 0,
                    "phlDefaultHour": 0,
                }
            )
            by_code[ui_type.upper()] = catalog[-1]
    return catalog, by_code


def _build_pto_action_catalog() -> tuple[list[dict], dict[str, dict]]:
    rows = get_pto_actions()
    catalog: list[dict] = []
    by_code: dict[str, dict] = {}
    for row in rows:
        code = _normalize_text(row.get("code")).upper()
        if not code:
            continue
        item = {
            "code": code,
            "name": _normalize_text(row.get("name")) or code,
            "color": _normalize_text(row.get("color")) or None,
        }
        catalog.append(item)
        by_code[code] = item
    catalog.sort(key=lambda item: (item["code"], item["name"].lower()))
    return catalog, by_code


def _resolve_default_action_code(
    action_catalog: list[dict],
    *,
    include_tokens: tuple[str, ...],
    fallback_index: int = 0,
) -> str:
    for row in action_catalog:
        haystack = f"{row['code']} {row['name']}".lower()
        if any(token in haystack for token in include_tokens):
            return row["code"]
    if 0 <= fallback_index < len(action_catalog):
        return action_catalog[fallback_index]["code"]
    return ""


def _resolve_current_principal_email(request) -> str:
    principal = get_auth_principal(request)
    if principal is not None and principal.email:
        return principal.email.strip().lower()

    if principal is not None:
        raw_user_email = _extract_email_from_auth_payload(principal.raw_user)
        if raw_user_email:
            return raw_user_email

    legacy_user_name = _normalize_text(getattr(request, "headers", {}).get("x-user-name"))
    legacy_user_email = _normalize_text(getattr(request, "headers", {}).get("x-user-email"))
    if legacy_user_email:
        return legacy_user_email.lower()
    if "@" in legacy_user_name:
        return legacy_user_name.lower()

    raise ValueError("Authenticated user email is required")


def _resolve_current_employee_candidates(request) -> list[str]:
    candidates: list[str] = []
    principal = get_auth_principal(request)
    if principal is None:
        return candidates

    legacy_user_name = _normalize_text(getattr(request, "headers", {}).get("x-user-name"))
    for value in (
        principal.email,
        _extract_email_from_auth_payload(principal.raw_user),
        _normalize_text(getattr(request, "headers", {}).get("x-user-email")),
        legacy_user_name if "@" in legacy_user_name else "",
    ):
        normalized = _normalize_email(value)
        if normalized and normalized not in candidates:
            candidates.append(normalized)
    return candidates


def _resolve_current_employee(request, *, require_active: bool = False) -> dict:
    rows: list[dict] = []
    for email in _resolve_current_employee_candidates(request):
        rows = get_employees_by_email(email=email)
        if rows:
            break

    if not rows:
        principal = get_auth_principal(request)
        identity_key = _normalize_text(getattr(principal, "user_id", None))
        if identity_key:
            rows = get_employees_by_identity_key(identity_key=identity_key)

    if not rows:
        raise ValueError("Authenticated user is not mapped to a LeaveSphere employee")
    active_rows = [row for row in rows if int(row.get("active") or 0) == 1]
    employee = active_rows[0] if active_rows else rows[0]
    if require_active and int(employee.get("active") or 0) != 1:
        raise ValueError("Authenticated employee must be active")
    return employee


def _resolve_primary_manager_id(employee_id: str) -> str | None:
    rows = get_employee_managers(employee_id=employee_id)
    if not rows:
        return None
    manager_id = _normalize_text(rows[0].get("managerId"))
    return manager_id or None


def _resolve_direct_reports(manager_id: str) -> list[dict]:
    rows = get_employee_managers(manager_id=manager_id)
    employee_ids = [str(row.get("employeeId") or "").strip() for row in rows if str(row.get("employeeId") or "").strip()]
    if not employee_ids:
        return []
    employees = get_employees_by_ids(employee_ids=employee_ids)
    employees_by_id = {str(row.get("id") or "").strip(): row for row in employees}
    direct_reports: list[dict] = []
    for employee_id in employee_ids:
        employee = employees_by_id.get(employee_id)
        if not employee:
            continue
        direct_reports.append(
            {
                "employeeId": employee_id,
                "employeeName": _employee_full_name(employee),
                "pictureUrl": _normalize_text(employee.get("pictureUrl")) or None,
                "title": _normalize_text(employee.get("title")),
            }
        )
    direct_reports.sort(key=lambda item: (item["employeeName"].lower(), item["employeeId"]))
    return direct_reports


def _build_holidays(year: int, _region: str) -> list[dict]:
    holidays: list[dict] = []
    for row in get_holidays(year=year):
        holiday_date = _to_date_string(row.get("date"))
        if not holiday_date.startswith(f"{year}-"):
            continue
        holidays.append(
            {
                "id": _normalize_text(row.get("id")),
                "name": _normalize_text(row.get("name")),
                "date": holiday_date,
                "teamRegion": _normalize_team_region(row.get("teamRegion") or row.get("region")),
            }
        )
    holidays.sort(key=lambda item: (item["date"], item["name"], item["id"]))
    return holidays


def _build_request_rows(
    *,
    rows: list[dict],
    employee_map: dict[str, dict],
    pto_type_by_code: dict[str, dict],
    current_employee_id: str,
    manager_id_by_employee_id: dict[str, str | None],
) -> list[dict]:
    requests: list[dict] = []
    for row in rows:
        employee_id = _normalize_text(row.get("employeeId"))
        employee = employee_map.get(employee_id)
        if employee is None:
            continue
        pto_type_code = _normalize_text(row.get("ptoTypeCode")).upper()
        pto_type = pto_type_by_code.get(pto_type_code)
        ui_type = pto_type["type"] if pto_type else _split_ui_type_tokens(pto_type_code, pto_type_code)
        if ui_type is None:
            continue

        status = _normalize_text(row.get("status")).lower()
        hours_raw = Decimal(str(row.get("hours") or 0))
        hours = abs(hours_raw)
        approver_id = _normalize_text(row.get("approverId"))
        approver = employee_map.get(approver_id) if approver_id else None
        submitted_at = _to_date_string(row.get("dateCreated"))
        reviewed_at = _to_date_string(row.get("dateUpdated")) if status != "pending" else ""
        description = _normalize_text(row.get("description"))
        request = {
            "id": _normalize_text(row.get("id")),
            "employeeId": employee_id,
            "managerId": manager_id_by_employee_id.get(employee_id) or current_employee_id,
            "type": ui_type,
            "ptoTypeCode": pto_type_code,
            "startDate": _to_date_string(row.get("startDate")),
            "endDate": _to_date_string(row.get("endDate")),
            "hours": float(hours.quantize(Decimal("0.01"))),
            "description": description,
            "status": status,
            "submittedAt": submitted_at,
            "reviewedAt": reviewed_at or None,
            "reviewerName": _employee_full_name(approver) if approver else None,
            "approverNote": _normalize_text(row.get("approverNote")) or None,
        }
        requests.append(request)

    requests.sort(key=lambda item: (item["submittedAt"] or "", item["id"]), reverse=True)
    return requests


def _build_balance_rows(
    *,
    rows: list[dict],
    pto_type_by_code: dict[str, dict],
) -> list[dict]:
    totals: dict[str, dict[str, Decimal | str | int]] = OrderedDict()
    for row in rows:
        pto_type_code = _normalize_text(row.get("ptoTypeCode")).upper()
        if not pto_type_code:
            continue
        pto_type = pto_type_by_code.get(pto_type_code)
        label = _normalize_text(pto_type.get("label")) if pto_type else ""
        bucket = totals.setdefault(
            pto_type_code,
            {
                "label": label or pto_type_code,
                "listingOrder": int(pto_type.get("listingOrder") or 0) if pto_type else 0,
                "granted": Decimal("0.00"),
                "used": Decimal("0.00"),
                "scheduled": Decimal("0.00"),
            },
        )
        if not bucket.get("label"):
            bucket["label"] = label or pto_type_code
        hours = Decimal(str(row.get("hours") or 0)).quantize(Decimal("0.01"))
        status = _normalize_text(row.get("status")).lower()
        action_code = _normalize_text(row.get("ptoActionCode")).upper()
        is_load_action = "LOAD" in action_code or "GRANT" in action_code
        is_request_action = "REQ" in action_code or "REQUEST" in action_code
        if status == "approved" and is_load_action:
            bucket["granted"] = Decimal(str(bucket["granted"])) + abs(hours)
        elif status == "approved" and is_request_action:
            bucket["used"] = Decimal(str(bucket["used"])) + abs(hours)
        elif status == "pending":
            bucket["scheduled"] = Decimal(str(bucket["scheduled"])) + abs(hours)

    balances: list[dict] = []
    sorted_totals = sorted(
        totals.items(),
        key=lambda item: (
            int(item[1].get("listingOrder") or 0),
            _normalize_text(item[1].get("label")).lower(),
            _normalize_text(item[0]),
        ),
    )
    for pto_type_code, bucket in sorted_totals:
        granted = Decimal(str(bucket["granted"])).quantize(Decimal("0.01"))
        used = Decimal(str(bucket["used"])).quantize(Decimal("0.01"))
        scheduled = Decimal(str(bucket["scheduled"])).quantize(Decimal("0.01"))
        remaining = granted - used - scheduled
        balances.append(
            {
                "type": pto_type_code,
                "code": pto_type_code,
                "label": _normalize_text(bucket.get("label")) or pto_type_code,
                "totalHours": float(granted),
                "usedHours": float(used),
                "scheduledHours": float(scheduled),
                "remainingHours": float(remaining.quantize(Decimal("0.01"))),
            }
        )
    return balances


def _build_employee_map(employees: list[dict]) -> dict[str, dict]:
    return {
        _normalize_text(employee.get("id")): employee
        for employee in employees
        if _normalize_text(employee.get("id"))
    }


def _build_employee_rows(employees: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for employee in employees:
        employee_id = _normalize_text(employee.get("id")) or _normalize_text(employee.get("employeeId"))
        employee_name = _employee_full_name(employee) or _normalize_text(employee.get("employeeName"))
        if not employee_id or not employee_name:
            continue
        rows.append(
            {
                "employeeId": employee_id,
                "employeeName": employee_name,
                "pictureUrl": _normalize_text(employee.get("pictureUrl")) or None,
                "title": _normalize_text(employee.get("title")) or None,
            }
        )
    rows.sort(key=lambda item: (item["employeeName"].lower(), item["employeeId"]))
    return rows


def load_my_pto_workspace(*, request, year: int | None = None) -> dict:
    selected_year = _normalize_year(year)
    employee = _resolve_current_employee(request, require_active=False)
    employee_id = _normalize_text(employee.get("id"))
    current_employee_name = _employee_full_name(employee)
    current_employee_region = _normalize_team_region(employee.get("region"))
    pto_types, pto_type_by_code = _build_pto_type_catalog()
    pto_actions, action_by_code = _build_pto_action_catalog()

    direct_reports = _resolve_direct_reports(employee_id)
    direct_report_ids = [row["employeeId"] for row in direct_reports]
    manager_id_by_employee_id: dict[str, str | None] = {employee_id: _resolve_primary_manager_id(employee_id)}
    for row in direct_reports:
        manager_id_by_employee_id[row["employeeId"]] = employee_id

    approver_ids: list[str] = [row["managerId"] for row in direct_reports if _normalize_text(row.get("managerId"))]
    approver_ids.extend(
        _normalize_text(row.get("managerId"))
        for row in get_employee_managers(employee_id=employee_id)
        if _normalize_text(row.get("managerId"))
    )
    approver_ids.extend(
        _normalize_text(row.get("approverId"))
        for row in get_pto_transactions(
            employee_ids=[employee_id, *direct_report_ids],
            year=selected_year,
        )
        if _normalize_text(row.get("approverId"))
    )

    employee_ids = [employee_id, *direct_report_ids, *approver_ids]
    employees = get_employees_by_ids(employee_ids=list(dict.fromkeys([item for item in employee_ids if item])))
    employee_map = _build_employee_map(employees)
    employee_map.setdefault(employee_id, employee)

    own_transactions = get_pto_transactions(employee_id=employee_id, year=selected_year)
    team_transactions = get_pto_transactions(employee_ids=direct_report_ids, year=selected_year) if direct_report_ids else []
    all_requests = _build_request_rows(
        rows=[*own_transactions, *team_transactions],
        employee_map=employee_map,
        pto_type_by_code=pto_type_by_code,
        current_employee_id=employee_id,
        manager_id_by_employee_id=manager_id_by_employee_id,
    )

    own_balance_rows = _build_balance_rows(rows=own_transactions, pto_type_by_code=pto_type_by_code)
    holidays = _build_holidays(selected_year, current_employee_region)

    return {
        "currentUserId": employee_id,
        "currentUserName": current_employee_name,
        "currentUserEmail": _normalize_email(employee.get("email")),
        "managerId": manager_id_by_employee_id.get(employee_id),
        "currentUserTeamRegion": current_employee_region,
        "isManager": bool(direct_reports),
        "employees": _build_employee_rows([employee, *direct_reports]),
        "ptoTypes": pto_types,
        "ptoActions": pto_actions,
        "defaultRequestActionCode": _resolve_default_action_code(
            pto_actions,
            include_tokens=("request",),
        ),
        "defaultCancelActionCode": _resolve_default_action_code(
            pto_actions,
            include_tokens=("request", "cancel"),
            fallback_index=0,
        ),
        "balances": own_balance_rows,
        "requests": all_requests,
        "holidays": holidays,
        "directReports": direct_reports,
    }


def _resolve_transaction_for_current_employee(
    *,
    request,
    transaction_id: str,
    require_active: bool = True,
) -> tuple[dict, dict]:
    employee = _resolve_current_employee(request, require_active=require_active)
    employee_id = _normalize_text(employee.get("id"))
    rows = get_pto_transactions(transaction_id=transaction_id)
    if not rows:
      raise ValueError("PTO transaction not found")
    transaction = rows[0]
    if _normalize_text(transaction.get("employeeId")) != employee_id:
        raise ValueError("PTO transaction not found")
    return employee, transaction


def create_my_pto_request(*, request, payload: dict) -> dict:
    employee = _resolve_current_employee(request, require_active=True)
    employee_id = _normalize_text(employee.get("id"))
    selected_year = _normalize_year(payload.get("year"))
    start_date = _normalize_iso_date(payload.get("startDate"))
    end_date = _normalize_iso_date(payload.get("endDate"))
    if start_date and end_date and start_date > end_date:
        raise ValueError("startDate must be on or before endDate")
    if not payload.get("hours"):
        raise ValueError("hours must be greater than zero")

    pto_types, pto_type_by_code = _build_pto_type_catalog()
    pto_actions, action_by_code = _build_pto_action_catalog()
    requested_type = _split_ui_type_tokens(payload.get("type"), payload.get("ptoTypeCode"))
    if requested_type is None:
        raise ValueError("ptoTypeCode is required")

    pto_type = next((row for row in pto_types if row["type"] == requested_type), None)
    if pto_type is None:
        raise ValueError("ptoTypeCode not found")

    request_action_code = _resolve_default_action_code(pto_actions, include_tokens=("request",))
    if not request_action_code:
        raise ValueError("ptoActionCode not found")

    requested_hours = Decimal(str(payload.get("hours") or 0))
    if requested_hours <= 0:
        raise ValueError("hours must be greater than zero")

    item = {
        "id": str(payload.get("transactionId") or "").strip() or f"pto-{datetime.utcnow().timestamp()}",
        "employeeId": employee_id,
        "ptoTypeCode": pto_type["code"],
        "ptoActionCode": request_action_code,
        "hours": (requested_hours * Decimal("-1")).quantize(Decimal("0.01")),
        "year": selected_year,
        "startDate": start_date or None,
        "endDate": end_date or None,
        "status": "Pending",
        "description": _normalize_text(payload.get("description")) or None,
        "approverNote": None,
        "approverId": None,
        "calendarId": _normalize_text(payload.get("calendarId")) or None,
    }
    if item["calendarId"] == "":
        item["calendarId"] = None

    from apps.leavesphere.api.v1.helpers.ptoTransactions import create_request as create_request_transaction

    created = create_request_transaction(item)
    return {
        "workspace": load_my_pto_workspace(request=request, year=selected_year),
        "source": "network",
        "createdRequestId": item["id"],
        "inserted": created.get("inserted", 0),
        "status": created.get("status", "Pending"),
    }


def update_my_pto_request(*, request, payload: dict) -> dict:
    employee = _resolve_current_employee(request, require_active=True)
    employee_id = _normalize_text(employee.get("id"))
    transaction_id = _normalize_text(payload.get("transactionId"))
    if not transaction_id:
        raise ValueError("transactionId is required")

    _employee, transaction = _resolve_transaction_for_current_employee(
        request=request,
        transaction_id=transaction_id,
        require_active=True,
    )
    if _normalize_text(transaction.get("status")).lower() != "pending":
        raise ValueError("Only Pending PTO transactions can be updated")
    if not _is_before_start_date(start_date=_to_date_string(transaction.get("startDate"))):
        raise ValueError("Only future PTO requests can be updated")

    pto_types, pto_type_by_code = _build_pto_type_catalog()
    requested_type = _split_ui_type_tokens(payload.get("type"), payload.get("ptoTypeCode"))
    if requested_type is None:
        raise ValueError("ptoTypeCode is required")
    pto_type = next((row for row in pto_types if row["type"] == requested_type), None)
    if pto_type is None:
        raise ValueError("ptoTypeCode not found")

    requested_year = _normalize_year(payload.get("year"))
    start_date = _normalize_iso_date(payload.get("startDate"))
    end_date = _normalize_iso_date(payload.get("endDate"))
    if start_date and end_date and start_date > end_date:
        raise ValueError("startDate must be on or before endDate")

    requested_hours = Decimal(str(payload.get("hours") or 0))
    if requested_hours <= 0:
        raise ValueError("hours must be greater than zero")

    request_action_code = _resolve_default_action_code(_build_pto_action_catalog()[0], include_tokens=("request",))
    if not request_action_code:
        raise ValueError("ptoActionCode not found")

    def _work(cursor) -> int:
        tables = get_db_tables()
        cursor.execute(
            "SELECT employeeId, ptoTypeCode, year, status, hours, startDate "
            f"FROM {tables['PTOTRANSACTIONS']} "
            "WHERE id = %s FOR UPDATE",
            (transaction_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError("PTO transaction not found")

        existing_employee_id, existing_pto_type_code, existing_year, status, existing_hours_raw, existing_start_date = row
        if _normalize_text(existing_employee_id) != employee_id:
            raise ValueError("PTO transaction not found")
        if _normalize_text(status).lower() != "pending":
            raise ValueError("Only Pending PTO transactions can be updated")
        if not _is_before_start_date(start_date=_to_date_string(existing_start_date)):
            raise ValueError("Only future PTO requests can be updated")

        cursor.execute(
            "SELECT hours, status "
            f"FROM {tables['PTOTRANSACTIONS']} "
            "WHERE employeeId = %s AND ptoTypeCode = %s AND year = %s AND id <> %s "
            "FOR UPDATE",
            (employee_id, pto_type["code"], requested_year, transaction_id),
        )
        rows = cursor.fetchall() or []
        approved = Decimal("0.00")
        pending = Decimal("0.00")
        for balance_row in rows:
            balance_hours = Decimal(str(balance_row[0] or 0)).quantize(Decimal("0.01"))
            balance_status = _normalize_text(balance_row[1]).lower()
            if balance_status == "approved":
                approved += balance_hours
            elif balance_status == "pending" and balance_hours < 0:
                pending += balance_hours
        available = approved + pending
        if available - requested_hours < 0:
            raise ValueError("Requested hours exceed available balance")

        cursor.execute(
            f"UPDATE {tables['PTOTRANSACTIONS']} "
            "SET ptoTypeCode = %s, ptoActionCode = %s, hours = %s, year = %s, startDate = %s, endDate = %s, "
            "description = %s, dateUpdated = %s "
            "WHERE id = %s",
            (
                pto_type["code"],
                request_action_code,
                (requested_hours * Decimal("-1")).quantize(Decimal("0.01")),
                requested_year,
                start_date or None,
                end_date or None,
                _normalize_text(payload.get("description")) or None,
                datetime.utcnow(),
                transaction_id,
            ),
        )
        return int(cursor.rowcount or 0)

    updated = int(run_transaction(_work) or 0)
    return {
        "workspace": load_my_pto_workspace(request=request, year=requested_year),
        "source": "network",
        "updated": updated,
    }


def cancel_my_pto_request(*, request, transaction_id: str) -> dict:
    _employee, transaction = _resolve_transaction_for_current_employee(
        request=request,
        transaction_id=transaction_id,
        require_active=True,
    )
    status = _normalize_text(transaction.get("status")).lower()
    if status not in {"pending", "approved", "rejected"}:
        raise ValueError("Only Pending, Approved, or Rejected PTO transactions can be canceled")
    if not _is_before_start_date(start_date=_to_date_string(transaction.get("startDate"))):
        raise ValueError("Only future PTO requests can be canceled")

    year = int(transaction.get("year") or date.today().year)

    if status == "pending":
        canceled = cancel_pending_pto_transaction(transaction_id=transaction_id)
        if canceled == 0:
            raise ValueError("PTO transaction not found")
        return {
            "workspace": load_my_pto_workspace(request=request, year=year),
            "source": "network",
            "id": transaction_id,
            "status": "Canceled",
            "updated": canceled,
        }

    updated = update_pto_transaction(
        transaction_id=transaction_id,
        updates={
            "status": "Canceled",
            "approverId": None,
        },
    )
    if updated == 0:
        raise ValueError("PTO transaction not found")
    return {
        "workspace": load_my_pto_workspace(request=request, year=year),
        "source": "network",
        "id": transaction_id,
        "status": "Canceled",
        "updated": updated,
    }


def review_my_pto_request(*, request, payload: dict) -> dict:
    transaction_id = _normalize_text(payload.get("transactionId") or payload.get("requestId"))
    if not transaction_id:
        raise ValueError("transactionId is required")

    action = _normalize_text(payload.get("action")).lower()
    if action not in {"approve", "reject", "cancel", "revert"}:
        raise ValueError("action must be approve, reject, cancel, or revert")

    employee = _resolve_current_employee(request, require_active=True)
    current_employee_id = _normalize_text(employee.get("id"))
    transaction_rows = get_pto_transactions(transaction_id=transaction_id)
    if not transaction_rows:
        raise ValueError("PTO transaction not found")

    transaction = transaction_rows[0]
    employee_id = _normalize_text(transaction.get("employeeId"))
    if not employee_id:
        raise ValueError("PTO transaction not found")

    access = get_tenant_access(request)
    has_admin_override = bool(
        access and ("workspace.super_admin" in access.permissions or "leavesphere.admin" in access.permissions)
    )
    direct_report_rows = get_employee_managers(employee_id=employee_id, manager_id=current_employee_id)
    is_direct_manager = bool(direct_report_rows)
    if not has_admin_override and not is_direct_manager:
        raise ValueError("Only a direct manager can approve or reject this PTO request")

    status = _normalize_text(transaction.get("status")).lower()
    if action in {"approve", "reject"}:
        if status != "pending":
            raise ValueError("Only Pending PTO transactions can be approved or rejected")
        if Decimal(str(transaction.get("hours") or 0)) >= 0:
            raise ValueError("Only debit PTO requests (hours < 0) can be approved or rejected")
        if action == "approve":
            updated = approve_pending_pto_request(
                transaction_id=transaction_id,
                approver_id=current_employee_id if current_employee_id else None,
                approverNote=_normalize_text(payload.get("approverNote")) or None,
            )
            return {
                "workspace": load_my_pto_workspace(request=request, year=int(transaction.get("year") or date.today().year)),
                "source": "network",
                "id": transaction_id,
                "status": "Approved",
                "updated": updated,
            }
        updated = reject_pending_pto_request(
            transaction_id=transaction_id,
            approver_id=current_employee_id if current_employee_id else None,
            approverNote=_normalize_text(payload.get("approverNote")) or None,
        )
        return {
            "workspace": load_my_pto_workspace(request=request, year=int(transaction.get("year") or date.today().year)),
            "source": "network",
            "id": transaction_id,
            "status": "Rejected",
            "updated": updated,
        }

    if action == "cancel":
        if status not in {"pending", "approved", "rejected"}:
            raise ValueError("Only Pending, Approved, or Rejected PTO transactions can be canceled")
        if not _is_before_start_date(start_date=_to_date_string(transaction.get("startDate"))):
            raise ValueError("Only future PTO requests can be canceled")
        if status == "pending":
            updated = cancel_pending_pto_transaction(transaction_id=transaction_id)
        else:
            updated = update_pto_transaction(
                transaction_id=transaction_id,
                updates={
                    "status": "Canceled",
                    "approverId": None,
                },
            )
        return {
            "workspace": load_my_pto_workspace(request=request, year=int(transaction.get("year") or date.today().year)),
            "source": "network",
            "id": transaction_id,
            "status": "Canceled",
            "updated": updated,
        }

    if action == "revert":
        if status not in {"approved", "rejected", "canceled"}:
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
        return {
            "workspace": load_my_pto_workspace(request=request, year=int(transaction.get("year") or date.today().year)),
            "source": "network",
            "id": transaction_id,
            "status": "Pending",
            "updated": updated,
        }

    raise ValueError("Unsupported action")
