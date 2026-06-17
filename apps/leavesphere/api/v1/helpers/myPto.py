from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
import re

from apps.leavesphere.api.v1.helpers.dbQueries import (
    approve_pending_pto_request,
    cancel_pending_pto_transaction,
    get_db_tables,
    get_employee_managers,
    get_employees_by_email,
    get_employees_by_identity_key,
    get_employees_by_ids,
    get_holidays,
    get_pto_transactions,
    reject_pending_pto_request,
    update_pto_transaction,
)
from apps.leavesphere.api.v1.helpers.config import resolve_employee_email_candidates
from apps.leavesphere.api.v1.helpers.ptoAccounting import (
    is_load_action_row,
    is_request_action_row,
    is_request_action_code,
    signed_pto_hours,
)
from apps.leavesphere.api.v1.helpers.ptoWorkspaceShared import (
    build_leave_sphere_workspace_common_payload,
    build_pto_employee_map,
    build_pto_request_rows,
    load_leave_sphere_pto_workspace_catalogs,
    resolve_pto_type_code_from_catalog,
)
from apps.leavesphere.api.v1.helpers.workspaceCache import (
    read_leave_sphere_workspace_cache,
    read_leave_sphere_workspace_latest_cache,
    write_leave_sphere_workspace_cache,
    write_leave_sphere_workspace_cache_value,
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


def _normalize_lookup_key(value: object | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", _normalize_text(value).lower())


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
    principal = get_auth_principal(request)
    if principal is None:
        return []

    legacy_user_name = _normalize_text(getattr(request, "headers", {}).get("x-user-name"))
    return resolve_employee_email_candidates(
        principal.email,
        _extract_email_from_auth_payload(principal.raw_user),
        _normalize_text(getattr(request, "headers", {}).get("x-user-email")),
        legacy_user_name if "@" in legacy_user_name else "",
    )


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


def _build_balance_rows(
    *,
    rows: list[dict],
    pto_type_by_code: dict[str, dict],
    pto_action_by_code: dict[str, dict],
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
        if status == "approved" and is_load_action_row(row, pto_action_by_code):
            bucket["granted"] = Decimal(str(bucket["granted"])) + abs(hours)
        elif status == "approved" and is_request_action_row(row, pto_action_by_code):
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


def _resolve_workspace_cache_user_key(request) -> str:
    principal = get_auth_principal(request)
    if principal is not None:
        if principal.email:
            return _normalize_email(principal.email)
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


def _load_cached_workspace_snapshot(
    *,
    request,
    year: int,
) -> dict | None:
    user_key = _resolve_workspace_cache_user_key(request)
    snapshot = read_leave_sphere_workspace_cache(
        page_code="my-pto",
        user_key=user_key,
        year=year,
        params={},
    )
    if not snapshot or int(snapshot.get("year") or 0) != int(year):
        return None
    workspace = snapshot.get("workspace")
    return workspace if isinstance(workspace, dict) else None


def _store_cached_workspace_snapshot(
    *,
    request,
    year: int,
    workspace: dict,
) -> None:
    user_key = _resolve_workspace_cache_user_key(request)
    write_leave_sphere_workspace_cache(
        page_code="my-pto",
        user_key=user_key,
        year=year,
        workspace=workspace,
        params={},
    )


def _apply_my_pto_workspace_mutation_from_cache(
    *,
    request,
    year: int,
    mutate_workspace,
) -> dict | None:
    user_key = _resolve_workspace_cache_user_key(request)
    latest = read_leave_sphere_workspace_latest_cache(page_code="my-pto", user_key=user_key)
    if not isinstance(latest, dict) or int(latest.get("year") or 0) != int(year):
        return None
    cached_workspace = latest.get("workspace")
    cache_key = _normalize_text(latest.get("cacheKey"))
    if not isinstance(cached_workspace, dict) or not cache_key:
        return None

    mutated = mutate_workspace(cached_workspace)
    if not mutated:
        return None

    write_leave_sphere_workspace_cache_value(
        cache_key=cache_key,
        page_code="my-pto",
        user_key=user_key,
        year=year,
        workspace=cached_workspace,
        params={},
    )
    return cached_workspace


def _sort_my_pto_requests(requests: list[dict]) -> list[dict]:
    return sorted(
        requests,
        key=lambda item: (
            _normalize_text(item.get("submittedAt")),
            _normalize_text(item.get("id")),
        ),
        reverse=True,
    )


def _find_my_pto_request_row(workspace: dict, transaction_id: str) -> dict | None:
    for row in workspace.get("requests", []):
        if isinstance(row, dict) and _normalize_text(row.get("id")) == _normalize_text(transaction_id):
            return row
    return None


def _upsert_my_pto_request_row(
    *,
    workspace: dict,
    request_row: dict,
) -> None:
    requests = [row for row in workspace.get("requests", []) if isinstance(row, dict)]
    request_id = _normalize_text(request_row.get("id"))
    for index, row in enumerate(requests):
        if _normalize_text(row.get("id")) != request_id:
            continue
        requests[index] = request_row
        workspace["requests"] = _sort_my_pto_requests(requests)
        return
    requests.append(request_row)
    workspace["requests"] = _sort_my_pto_requests(requests)


def _adjust_my_pto_balance_row(
    *,
    workspace: dict,
    pto_type_code: str,
    used_delta: float = 0.0,
    scheduled_delta: float = 0.0,
) -> None:
    balances = [row for row in workspace.get("balances", []) if isinstance(row, dict)]
    normalized_code = _normalize_text(pto_type_code).upper()
    for row in balances:
        row_code = _normalize_text(row.get("code") or row.get("type")).upper()
        if row_code != normalized_code:
            continue
        total = float(row.get("totalHours") or 0)
        used = float(row.get("usedHours") or 0) + float(used_delta)
        scheduled = float(row.get("scheduledHours") or 0) + float(scheduled_delta)
        row["usedHours"] = round(used, 2)
        row["scheduledHours"] = round(scheduled, 2)
        row["remainingHours"] = round(total - used - scheduled, 2)
        workspace["balances"] = balances
        return

    total = 0.0
    used = max(0.0, float(used_delta))
    scheduled = max(0.0, float(scheduled_delta))
    balances.append(
        {
            "type": normalized_code.lower() or normalized_code,
            "code": normalized_code,
            "label": normalized_code,
            "totalHours": total,
            "usedHours": round(used, 2),
            "scheduledHours": round(scheduled, 2),
            "remainingHours": round(total - used - scheduled, 2),
        }
    )
    workspace["balances"] = balances


def _request_balance_effect(request_row: dict | None) -> dict[str, tuple[float, float]]:
    if not isinstance(request_row, dict):
        return {}

    pto_type_code = _normalize_text(request_row.get("ptoTypeCode") or request_row.get("type")).upper()
    if not pto_type_code:
        return {}

    status = _normalize_text(request_row.get("status")).lower()
    hours = float(abs(Decimal(str(request_row.get("hours") or 0)).quantize(Decimal("0.01"))))
    if hours <= 0:
        return {}

    if status == "pending":
        return {pto_type_code: (0.0, hours)}
    if status == "approved":
        return {pto_type_code: (hours, 0.0)}
    return {}


def _apply_my_pto_request_balance_delta(
    *,
    workspace: dict,
    old_row: dict | None,
    new_row: dict | None,
) -> None:
    current_user_id = _normalize_text(workspace.get("currentUserId"))
    employee_id = _normalize_text((new_row or old_row or {}).get("employeeId"))
    if not current_user_id or employee_id != current_user_id:
        return

    old_effects = _request_balance_effect(old_row)
    new_effects = _request_balance_effect(new_row)
    for pto_type_code in set(old_effects) | set(new_effects):
        old_used, old_scheduled = old_effects.get(pto_type_code, (0.0, 0.0))
        new_used, new_scheduled = new_effects.get(pto_type_code, (0.0, 0.0))
        _adjust_my_pto_balance_row(
            workspace=workspace,
            pto_type_code=pto_type_code,
            used_delta=new_used - old_used,
            scheduled_delta=new_scheduled - old_scheduled,
        )


def _copy_workspace_row(row: dict) -> dict:
    return deepcopy(row)


def _build_my_pto_workspace_patch(
    *,
    workspace: dict,
    request_ids: list[str] | None = None,
    include_balances: bool = False,
) -> dict:
    patch = {
        "currentUserId": workspace.get("currentUserId"),
        "currentUserName": workspace.get("currentUserName"),
        "currentUserEmail": workspace.get("currentUserEmail"),
        "managerId": workspace.get("managerId"),
        "currentUserTeamRegion": workspace.get("currentUserTeamRegion"),
        "isManager": workspace.get("isManager"),
        "defaultRequestActionCode": workspace.get("defaultRequestActionCode"),
        "defaultCancelActionCode": workspace.get("defaultCancelActionCode"),
    }
    if request_ids:
        request_id_set = {_normalize_text(item) for item in request_ids if _normalize_text(item)}
        patch["requests"] = [
            _copy_workspace_row(row)
            for row in workspace.get("requests", [])
            if isinstance(row, dict) and _normalize_text(row.get("id")) in request_id_set
        ]
    if include_balances:
        patch["balances"] = [
            _copy_workspace_row(row)
            for row in workspace.get("balances", [])
            if isinstance(row, dict)
        ]
    return patch


def load_my_pto_workspace(*, request, year: int | None = None) -> dict:
    selected_year = _normalize_year(year)
    cached_workspace = _load_cached_workspace_snapshot(request=request, year=selected_year)
    if isinstance(cached_workspace, dict):
        return cached_workspace

    employee = _resolve_current_employee(request, require_active=False)
    employee_id = _normalize_text(employee.get("id"))
    current_employee_name = _employee_full_name(employee)
    current_employee_region = _normalize_team_region(employee.get("region"))
    catalogs = load_leave_sphere_pto_workspace_catalogs()
    pto_type_by_code = catalogs.pto_type_by_code
    pto_action_by_code = catalogs.pto_action_by_code

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
    employee_map = build_pto_employee_map(employees)
    employee_map.setdefault(employee_id, employee)

    own_transactions = get_pto_transactions(employee_id=employee_id, year=selected_year)
    team_transactions = get_pto_transactions(employee_ids=direct_report_ids, year=selected_year) if direct_report_ids else []
    all_requests = build_pto_request_rows(
        rows=[*own_transactions, *team_transactions],
        employee_map=employee_map,
        pto_type_by_code=pto_type_by_code,
        current_employee_id=employee_id,
        manager_id_by_employee_id=manager_id_by_employee_id,
        employee_full_name_fn=_employee_full_name,
    )

    own_balance_rows = _build_balance_rows(
        rows=own_transactions,
        pto_type_by_code=pto_type_by_code,
        pto_action_by_code=pto_action_by_code,
    )
    holidays = _build_holidays(selected_year, current_employee_region)

    workspace = {
        "currentUserId": employee_id,
        "currentUserName": current_employee_name,
        "currentUserEmail": _normalize_email(employee.get("email")),
        "managerId": manager_id_by_employee_id.get(employee_id),
        "currentUserTeamRegion": current_employee_region,
        "isManager": bool(direct_reports),
        "employees": _build_employee_rows([employee, *direct_reports]),
        **build_leave_sphere_workspace_common_payload(catalogs),
        "balances": own_balance_rows,
        "requests": all_requests,
        "holidays": holidays,
        "directReports": direct_reports,
    }
    _store_cached_workspace_snapshot(request=request, year=selected_year, workspace=workspace)
    return workspace


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

    catalogs = load_leave_sphere_pto_workspace_catalogs()
    requested_pto_type_code = resolve_pto_type_code_from_catalog(
        value=payload.get("ptoTypeCode") or payload.get("type"),
        pto_type_by_code=catalogs.pto_type_by_code,
    )
    if not requested_pto_type_code:
        raise ValueError("ptoTypeCode is required")

    pto_type = catalogs.pto_type_by_code.get(requested_pto_type_code)
    if pto_type is None:
        raise ValueError("ptoTypeCode not found")

    request_action_code = catalogs.default_request_action_code
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
        "hours": requested_hours.quantize(Decimal("0.01")),
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
    workspace = _apply_my_pto_workspace_mutation_from_cache(
        request=request,
        year=selected_year,
        mutate_workspace=lambda cached_workspace: (
            (
                lambda new_row: (
                    _upsert_my_pto_request_row(
                        workspace=cached_workspace,
                        request_row=new_row,
                    )
                    or _apply_my_pto_request_balance_delta(
                        workspace=cached_workspace,
                        old_row=None,
                        new_row=new_row,
                    )
                    or True
                )
            )(
                {
                    "id": item["id"],
                    "employeeId": employee_id,
                    "managerId": _normalize_text(cached_workspace.get("managerId")) or employee_id,
                    "type": _normalize_text(pto_type.get("type")) or pto_type["code"].lower(),
                    "ptoTypeCode": pto_type["code"],
                    "startDate": start_date or None,
                    "endDate": end_date or None,
                    "hours": float(requested_hours.quantize(Decimal("0.01"))),
                    "description": _normalize_text(payload.get("description")) or None,
                    "status": "pending",
                    "submittedAt": date.today().isoformat(),
                    "reviewedAt": None,
                    "reviewerName": None,
                    "approverNote": None,
                }
            )
        ),
    )
    workspace_patch = None
    if workspace is not None:
        workspace_patch = _build_my_pto_workspace_patch(
            workspace=workspace,
            request_ids=[item["id"]],
            include_balances=employee_id == _normalize_text(workspace.get("currentUserId")),
        )
    if workspace is None:
        workspace = load_my_pto_workspace(request=request, year=selected_year)
    response = {
        "source": "network",
        "createdRequestId": item["id"],
        "inserted": created.get("inserted", 0),
        "status": created.get("status", "Pending"),
    }
    if workspace_patch is not None:
        response["workspacePatch"] = workspace_patch
    else:
        response["workspace"] = workspace
    return response


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

    catalogs = load_leave_sphere_pto_workspace_catalogs()
    requested_pto_type_code = resolve_pto_type_code_from_catalog(
        value=payload.get("ptoTypeCode") or payload.get("type"),
        pto_type_by_code=catalogs.pto_type_by_code,
    )
    if not requested_pto_type_code:
        raise ValueError("ptoTypeCode is required")
    pto_type = catalogs.pto_type_by_code.get(requested_pto_type_code)
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

    request_action_code = catalogs.default_request_action_code
    if not request_action_code:
        raise ValueError("ptoActionCode not found")

    def _work(cursor) -> int:
        tables = get_db_tables()
        cursor.execute(
            "SELECT employeeId, ptoTypeCode, year, ptoActionCode, status, hours, startDate "
            f"FROM {tables['PTOTRANSACTIONS']} "
            "WHERE id = %s FOR UPDATE",
            (transaction_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ValueError("PTO transaction not found")

        existing_employee_id, existing_pto_type_code, _existing_year, _existing_action_code, status, _existing_hours_raw, existing_start_date = row
        if _normalize_text(existing_employee_id) != employee_id:
            raise ValueError("PTO transaction not found")
        if _normalize_text(status).lower() != "pending":
            raise ValueError("Only Pending PTO transactions can be updated")
        if not _is_before_start_date(start_date=_to_date_string(existing_start_date)):
            raise ValueError("Only future PTO requests can be updated")

        cursor.execute(
            "SELECT ptoActionCode, hours, status "
            f"FROM {tables['PTOTRANSACTIONS']} "
            "WHERE employeeId = %s AND ptoTypeCode = %s AND year = %s AND id <> %s "
            "FOR UPDATE",
            (employee_id, pto_type["code"], requested_year, transaction_id),
        )
        rows = cursor.fetchall() or []
        approved = Decimal("0.00")
        pending = Decimal("0.00")
        for balance_row in rows:
            balance_action_code = _normalize_text(balance_row[0]).upper()
            balance_hours = Decimal(str(balance_row[1] or 0)).quantize(Decimal("0.01"))
            balance_status = _normalize_text(balance_row[2]).lower()
            if balance_status == "approved":
                approved += signed_pto_hours(balance_action_code, balance_hours)
            elif balance_status == "pending" and is_request_action_code(balance_action_code):
                pending += abs(balance_hours)
        available = approved - pending
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
                requested_hours.quantize(Decimal("0.01")),
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
    def _apply_update(cached_workspace: dict) -> bool:
        old_row = _find_my_pto_request_row(cached_workspace, transaction_id)
        new_row = {
            **(old_row or {}),
            "type": _normalize_text(pto_type.get("type")) or pto_type["code"].lower(),
            "ptoTypeCode": pto_type["code"],
            "startDate": start_date or transaction.get("startDate") or None,
            "endDate": end_date or transaction.get("endDate") or None,
            "hours": float(requested_hours.quantize(Decimal("0.01"))),
            "description": _normalize_text(payload.get("description")) or transaction.get("description") or None,
            "status": "pending",
        }
        _upsert_my_pto_request_row(
            workspace=cached_workspace,
            request_row=new_row,
        )
        _apply_my_pto_request_balance_delta(
            workspace=cached_workspace,
            old_row=old_row,
            new_row=new_row,
        )
        return True

    workspace = _apply_my_pto_workspace_mutation_from_cache(
        request=request,
        year=requested_year,
        mutate_workspace=_apply_update,
    )
    workspace_patch = None
    if workspace is not None:
        workspace_patch = _build_my_pto_workspace_patch(
            workspace=workspace,
            request_ids=[transaction_id],
            include_balances=employee_id == _normalize_text(workspace.get("currentUserId")),
        )
    if workspace is None:
        workspace = load_my_pto_workspace(request=request, year=requested_year)
    response = {
        "source": "network",
        "updated": updated,
    }
    if workspace_patch is not None:
        response["workspacePatch"] = workspace_patch
    else:
        response["workspace"] = workspace
    return response


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
        def _apply_cancel(cached_workspace: dict) -> bool:
            old_row = _find_my_pto_request_row(cached_workspace, transaction_id)
            new_row = {
                **(old_row or {}),
                "status": "canceled",
            }
            _upsert_my_pto_request_row(
                workspace=cached_workspace,
                request_row=new_row,
            )
            _apply_my_pto_request_balance_delta(
                workspace=cached_workspace,
                old_row=old_row,
                new_row=new_row,
            )
            return True

        workspace = _apply_my_pto_workspace_mutation_from_cache(
            request=request,
            year=year,
            mutate_workspace=_apply_cancel,
        )
        workspace_patch = None
        if workspace is not None:
            workspace_patch = _build_my_pto_workspace_patch(
                workspace=workspace,
                request_ids=[transaction_id],
                include_balances=status == "pending" and _normalize_text(transaction.get("employeeId")) == _normalize_text(workspace.get("currentUserId")),
            )
        if workspace is None:
            workspace = load_my_pto_workspace(request=request, year=year)
        response = {
            "source": "network",
            "id": transaction_id,
            "status": "Canceled",
            "updated": canceled,
        }
        if workspace_patch is not None:
            response["workspacePatch"] = workspace_patch
        else:
            response["workspace"] = workspace
        return response

    updated = update_pto_transaction(
        transaction_id=transaction_id,
        updates={
            "status": "Canceled",
            "approverId": None,
        },
    )
    if updated == 0:
        raise ValueError("PTO transaction not found")
    def _apply_cancel_approved(cached_workspace: dict) -> bool:
        old_row = _find_my_pto_request_row(cached_workspace, transaction_id)
        new_row = {
            **(old_row or {}),
            "status": "canceled",
        }
        _upsert_my_pto_request_row(
            workspace=cached_workspace,
            request_row=new_row,
        )
        _apply_my_pto_request_balance_delta(
            workspace=cached_workspace,
            old_row=old_row,
            new_row=new_row,
        )
        return True

    workspace = _apply_my_pto_workspace_mutation_from_cache(
        request=request,
        year=year,
        mutate_workspace=_apply_cancel_approved,
    )
    workspace_patch = None
    if workspace is not None:
        workspace_patch = _build_my_pto_workspace_patch(
            workspace=workspace,
            request_ids=[transaction_id],
            include_balances=False,
        )
    if workspace is None:
        workspace = load_my_pto_workspace(request=request, year=year)
    response = {
        "source": "network",
        "id": transaction_id,
        "status": "Canceled",
        "updated": updated,
    }
    if workspace_patch is not None:
        response["workspacePatch"] = workspace_patch
    else:
        response["workspace"] = workspace
    return response


def review_my_pto_request(*, request, payload: dict) -> dict:
    transaction_id = _normalize_text(payload.get("transactionId") or payload.get("requestId"))
    if not transaction_id:
        raise ValueError("transactionId is required")

    action = _normalize_text(payload.get("action")).lower()
    if action not in {"approve", "reject", "cancel", "revert"}:
        raise ValueError("action must be approve, reject, cancel, or revert")

    employee = _resolve_current_employee(request, require_active=True)
    current_employee_id = _normalize_text(employee.get("id"))
    current_employee_name = _employee_full_name(employee)
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
    workspace_year = int(transaction.get("year") or date.today().year)
    if action in {"approve", "reject"}:
        if status != "pending":
            raise ValueError("Only Pending PTO transactions can be approved or rejected")
        if action == "approve":
            updated = approve_pending_pto_request(
                transaction_id=transaction_id,
                approver_id=current_employee_id if current_employee_id else None,
                approverNote=_normalize_text(payload.get("approverNote")) or None,
            )

            def _apply_approve(cached_workspace: dict) -> bool:
                old_row = _find_my_pto_request_row(cached_workspace, transaction_id)
                new_row = {
                    **(old_row or {}),
                    "status": "approved",
                    "approverNote": _normalize_text(payload.get("approverNote")) or None,
                    "reviewedAt": date.today().isoformat(),
                    "reviewerName": _normalize_text(cached_workspace.get("currentUserName")) or current_employee_name,
                }
                _upsert_my_pto_request_row(workspace=cached_workspace, request_row=new_row)
                _apply_my_pto_request_balance_delta(workspace=cached_workspace, old_row=old_row, new_row=new_row)
                return True

            workspace = _apply_my_pto_workspace_mutation_from_cache(
                request=request,
                year=workspace_year,
                mutate_workspace=_apply_approve,
            )
            workspace_patch = None
            if workspace is not None:
                workspace_patch = _build_my_pto_workspace_patch(
                    workspace=workspace,
                    request_ids=[transaction_id],
                    include_balances=False,
                )
            if workspace is None:
                workspace = load_my_pto_workspace(request=request, year=workspace_year)
            response = {
                "source": "network",
                "id": transaction_id,
                "status": "Approved",
                "updated": updated,
            }
            if workspace_patch is not None:
                response["workspacePatch"] = workspace_patch
            else:
                response["workspace"] = workspace
            return response

        updated = reject_pending_pto_request(
            transaction_id=transaction_id,
            approver_id=current_employee_id if current_employee_id else None,
            approverNote=_normalize_text(payload.get("approverNote")) or None,
        )

        def _apply_reject(cached_workspace: dict) -> bool:
            old_row = _find_my_pto_request_row(cached_workspace, transaction_id)
            new_row = {
                **(old_row or {}),
                "status": "rejected",
                "approverNote": _normalize_text(payload.get("approverNote")) or None,
                "reviewedAt": date.today().isoformat(),
                "reviewerName": _normalize_text(cached_workspace.get("currentUserName")) or current_employee_name,
            }
            _upsert_my_pto_request_row(workspace=cached_workspace, request_row=new_row)
            _apply_my_pto_request_balance_delta(workspace=cached_workspace, old_row=old_row, new_row=new_row)
            return True

        workspace = _apply_my_pto_workspace_mutation_from_cache(
            request=request,
            year=workspace_year,
            mutate_workspace=_apply_reject,
        )
        workspace_patch = None
        if workspace is not None:
            workspace_patch = _build_my_pto_workspace_patch(
                workspace=workspace,
                request_ids=[transaction_id],
                include_balances=False,
            )
        if workspace is None:
            workspace = load_my_pto_workspace(request=request, year=workspace_year)
        response = {
            "source": "network",
            "id": transaction_id,
            "status": "Rejected",
            "updated": updated,
        }
        if workspace_patch is not None:
            response["workspacePatch"] = workspace_patch
        else:
            response["workspace"] = workspace
        return response

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

        def _apply_cancel(cached_workspace: dict) -> bool:
            old_row = _find_my_pto_request_row(cached_workspace, transaction_id)
            new_row = {
                **(old_row or {}),
                "status": "canceled",
                "reviewedAt": date.today().isoformat(),
                "reviewerName": _normalize_text(cached_workspace.get("currentUserName")) or current_employee_name,
            }
            _upsert_my_pto_request_row(workspace=cached_workspace, request_row=new_row)
            _apply_my_pto_request_balance_delta(workspace=cached_workspace, old_row=old_row, new_row=new_row)
            return True

        workspace = _apply_my_pto_workspace_mutation_from_cache(
            request=request,
            year=workspace_year,
            mutate_workspace=_apply_cancel,
        )
        workspace_patch = None
        if workspace is not None:
            workspace_patch = _build_my_pto_workspace_patch(
                workspace=workspace,
                request_ids=[transaction_id],
                include_balances=False,
            )
        if workspace is None:
            workspace = load_my_pto_workspace(request=request, year=workspace_year)
        response = {
            "source": "network",
            "id": transaction_id,
            "status": "Canceled",
            "updated": updated,
        }
        if workspace_patch is not None:
            response["workspacePatch"] = workspace_patch
        else:
            response["workspace"] = workspace
        return response

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

        def _apply_revert(cached_workspace: dict) -> bool:
            old_row = _find_my_pto_request_row(cached_workspace, transaction_id)
            new_row = {
                **(old_row or {}),
                "status": "pending",
                "reviewedAt": None,
                "reviewerName": None,
            }
            _upsert_my_pto_request_row(workspace=cached_workspace, request_row=new_row)
            _apply_my_pto_request_balance_delta(workspace=cached_workspace, old_row=old_row, new_row=new_row)
            return True

        workspace = _apply_my_pto_workspace_mutation_from_cache(
            request=request,
            year=workspace_year,
            mutate_workspace=_apply_revert,
        )
        workspace_patch = None
        if workspace is not None:
            workspace_patch = _build_my_pto_workspace_patch(
                workspace=workspace,
                request_ids=[transaction_id],
                include_balances=False,
            )
        if workspace is None:
            workspace = load_my_pto_workspace(request=request, year=workspace_year)
        response = {
            "source": "network",
            "id": transaction_id,
            "status": "Pending",
            "updated": updated,
        }
        if workspace_patch is not None:
            response["workspacePatch"] = workspace_patch
        else:
            response["workspace"] = workspace
        return response

    raise ValueError("Unsupported action")
