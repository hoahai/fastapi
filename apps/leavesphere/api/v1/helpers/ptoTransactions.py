from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from apps.leavesphere.api.v1.helpers.dbQueries import (
    approve_pending_pto_request,
    cancel_pending_pto_transaction,
    create_pto_request_transaction,
    get_employees,
    get_employees_by_identity_key,
    get_employee_managers,
    get_pto_actions,
    get_pto_balances,
    get_pto_transactions,
    get_pto_types,
    insert_pto_transaction,
    reject_pending_pto_request,
)
from shared.auth.config import get_auth_mode, is_legacy_api_key_fallback_enabled
from shared.auth.dependencies import get_auth_principal, get_tenant_access

_VALID_STATUSES = {"PENDING", "APPROVED", "REJECTED", "CANCELED"}


def _normalize_code(value: str | None) -> str | None:
    normalized = str(value or "").strip().upper()
    return normalized or None


def _normalize_status(value: str | None) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if normalized.upper() not in _VALID_STATUSES:
        raise ValueError("status must be one of Pending, Approved, Rejected, Canceled")
    return normalized.capitalize()


def _normalize_year(value: object | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError("year must be an integer") from exc
    if parsed < 1901 or parsed > 2155:
        raise ValueError("year must be between 1901 and 2155")
    return int(parsed)


def list_pto_transactions(
    *,
    employee_id: str | None = None,
    pto_type_code: str | None = None,
    year: int | None = None,
    status: str | None = None,
) -> list[dict]:
    return get_pto_transactions(
        employee_id=employee_id,
        pto_type_code=_normalize_code(pto_type_code),
        year=_normalize_year(year),
        status=_normalize_status(status),
    )


def get_pto_transaction(transaction_id: str) -> dict | None:
    rows = get_pto_transactions(transaction_id=transaction_id)
    return rows[0] if rows else None


def list_pto_balances(
    *,
    employee_id: str | None = None,
    pto_type_code: str | None = None,
    year: int | None = None,
) -> list[dict]:
    return get_pto_balances(
        employee_id=employee_id,
        pto_type_code=_normalize_code(pto_type_code),
        year=_normalize_year(year),
    )


def _normalize_datetime(value: object | None, *, field: str) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be a valid ISO datetime") from exc


def _normalize_decimal_hours(value: object) -> Decimal:
    try:
        hours = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("hours must be a decimal number") from exc
    if hours == 0:
        raise ValueError("hours must not be zero")
    return hours.quantize(Decimal("0.01"))


def _normalize_positive_requested_hours(value: object) -> Decimal:
    try:
        hours = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("hours must be a decimal number") from exc
    if hours <= 0:
        raise ValueError("hours must be greater than zero")
    return hours.quantize(Decimal("0.01"))


def _require_active_employee(employee_id: str) -> None:
    rows = get_employees(employee_id=employee_id)
    if not rows:
        raise ValueError("employeeId not found")
    active = int(rows[0].get("active") or 0)
    if active != 1:
        raise ValueError("employeeId must be active")


def _require_pto_type(code: str) -> None:
    if not get_pto_types(code=code):
        raise ValueError("ptoTypeCode not found")


def _require_pto_action(code: str) -> None:
    if not get_pto_actions(code=code):
        raise ValueError("ptoActionCode not found")


def create_adjustment(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    employee_id = str(payload.get("employeeId") or "").strip()
    pto_type_code = _normalize_code(str(payload.get("ptoTypeCode") or "").strip())
    pto_action_code = _normalize_code(str(payload.get("ptoActionCode") or "").strip())
    if not employee_id:
        raise ValueError("employeeId is required")
    if not pto_type_code:
        raise ValueError("ptoTypeCode is required")
    if not pto_action_code:
        raise ValueError("ptoActionCode is required")

    _require_active_employee(employee_id)
    _require_pto_type(pto_type_code)
    _require_pto_action(pto_action_code)

    item = {
        "id": str(payload.get("transactionId") or payload.get("id") or "").strip() or str(uuid4()),
        "employeeId": employee_id,
        "ptoTypeCode": pto_type_code,
        "ptoActionCode": pto_action_code,
        "hours": _normalize_decimal_hours(payload.get("hours")),
        "year": _normalize_year(payload.get("year")),
        "startDate": _normalize_datetime(payload.get("startDate"), field="startDate"),
        "endDate": _normalize_datetime(payload.get("endDate"), field="endDate"),
        "status": "Approved",
        "description": (str(payload.get("description") or "").strip() or None),
        "approverNote": (str(payload.get("approverNote") or "").strip() or None),
        "approverId": None,
        "calendarId": (str(payload.get("calendarId") or "").strip() or None),
    }
    if item["year"] is None:
        raise ValueError("year is required")
    if item["description"] and len(item["description"]) > 255:
        raise ValueError("description must be <= 255 characters")
    if item["approverNote"] and len(item["approverNote"]) > 2048:
        raise ValueError("approverNote must be <= 2048 characters")
    if item["calendarId"] and len(item["calendarId"]) > 30:
        raise ValueError("calendarId must be <= 30 characters")

    inserted = insert_pto_transaction(item)
    return {"id": item["id"], "status": item["status"], "inserted": inserted}


def create_request(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    employee_id = str(payload.get("employeeId") or "").strip()
    pto_type_code = _normalize_code(str(payload.get("ptoTypeCode") or "").strip())
    pto_action_code = _normalize_code(str(payload.get("ptoActionCode") or "").strip())
    if not employee_id:
        raise ValueError("employeeId is required")
    if not pto_type_code:
        raise ValueError("ptoTypeCode is required")
    if not pto_action_code:
        raise ValueError("ptoActionCode is required")

    _require_active_employee(employee_id)
    _require_pto_type(pto_type_code)
    _require_pto_action(pto_action_code)

    requested_hours = _normalize_positive_requested_hours(payload.get("hours"))
    start_date = _normalize_datetime(payload.get("startDate"), field="startDate")
    end_date = _normalize_datetime(payload.get("endDate"), field="endDate")
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("startDate must be on or before endDate")

    item = {
        "id": str(uuid4()),
        "employeeId": employee_id,
        "ptoTypeCode": pto_type_code,
        "ptoActionCode": pto_action_code,
        "hours": requested_hours.quantize(Decimal("0.01")),
        "year": _normalize_year(payload.get("year")),
        "startDate": start_date,
        "endDate": end_date,
        "status": "Pending",
        "description": (str(payload.get("description") or "").strip() or None),
        "approverNote": (str(payload.get("approverNote") or "").strip() or None),
        "approverId": None,
        "calendarId": (str(payload.get("calendarId") or "").strip() or None),
    }
    if item["year"] is None:
        raise ValueError("year is required")
    if item["description"] and len(item["description"]) > 255:
        raise ValueError("description must be <= 255 characters")
    if item["approverNote"] and len(item["approverNote"]) > 2048:
        raise ValueError("approverNote must be <= 2048 characters")
    if item["calendarId"] and len(item["calendarId"]) > 30:
        raise ValueError("calendarId must be <= 30 characters")

    inserted = create_pto_request_transaction(item=item, requested_hours=requested_hours)
    return {"id": item["id"], "status": item["status"], "inserted": inserted}


def cancel_request(*, transaction_id: str) -> dict:
    normalized_id = str(transaction_id or "").strip()
    if not normalized_id:
        raise ValueError("transaction_id is required")
    canceled = cancel_pending_pto_transaction(transaction_id=normalized_id)
    return {"id": normalized_id, "status": "Canceled", "updated": canceled}


def _normalize_optional_approver_note(value: object | None) -> str | None:
    if value is None:
        return None
    note = str(value).strip()
    if not note:
        return None
    if len(note) > 2048:
        raise ValueError("approverNote must be <= 2048 characters")
    return note


def _has_admin_override(request) -> bool:
    access = get_tenant_access(request)
    if access is None:
        return False
    permissions = set(access.permissions or set())
    return "workspace.super_admin" in permissions or "leavesphere.admin" in permissions


def _is_legacy_compat_bypass(request) -> bool:
    auth_mode = str(getattr(request.state, "auth_mode", "")).strip().lower()
    return (
        auth_mode == "legacy_api_key"
        and get_auth_mode() == "compat"
        and is_legacy_api_key_fallback_enabled()
    )


def _resolve_actor_employee_id(request, *, allow_unmapped: bool) -> str | None:
    principal = get_auth_principal(request)
    if principal is None:
        if allow_unmapped:
            return None
        raise ValueError("Authenticated user principal is required for manager approval")

    identity_key = str(principal.user_id or "").strip()
    if not identity_key:
        if allow_unmapped:
            return None
        raise ValueError("Authenticated user id is required for manager approval")

    rows = get_employees_by_identity_key(identity_key=identity_key)
    if not rows:
        if allow_unmapped:
            return None
        raise ValueError("Authenticated user is not mapped to a LeaveSphere employee")

    actor = rows[0]
    actor_id = str(actor.get("id") or "").strip()
    if not actor_id:
        if allow_unmapped:
            return None
        raise ValueError("Authenticated manager employee record is invalid")

    active = int(actor.get("active") or 0)
    if active != 1:
        if allow_unmapped:
            return None
        raise ValueError("Authenticated manager employee must be active")
    return actor_id


def _require_direct_manager(*, employee_id: str, manager_id: str) -> None:
    rows = get_employee_managers(employee_id=employee_id, manager_id=manager_id)
    if not rows:
        raise ValueError("Only a direct manager can approve or reject this PTO request")


def approve_request(
    *,
    request,
    transaction_id: str,
    approverNote: object | None = None,
    force_admin_override: bool = False,
) -> dict:
    normalized_id = str(transaction_id or "").strip()
    if not normalized_id:
        raise ValueError("transaction_id is required")

    transaction = get_pto_transaction(normalized_id)
    if transaction is None:
        raise ValueError("PTO transaction not found")

    admin_override = force_admin_override or _has_admin_override(request) or _is_legacy_compat_bypass(request)
    actor_employee_id = _resolve_actor_employee_id(request, allow_unmapped=admin_override)
    if not admin_override:
        employee_id = str(transaction.get("employeeId") or "").strip()
        _require_direct_manager(employee_id=employee_id, manager_id=str(actor_employee_id or "").strip())

    updated = approve_pending_pto_request(
        transaction_id=normalized_id,
        approver_id=actor_employee_id,
        approverNote=_normalize_optional_approver_note(approverNote),
    )
    return {"id": normalized_id, "status": "Approved", "updated": updated}


def reject_request(
    *,
    request,
    transaction_id: str,
    approverNote: object | None = None,
    force_admin_override: bool = False,
) -> dict:
    normalized_id = str(transaction_id or "").strip()
    if not normalized_id:
        raise ValueError("transaction_id is required")

    transaction = get_pto_transaction(normalized_id)
    if transaction is None:
        raise ValueError("PTO transaction not found")

    admin_override = force_admin_override or _has_admin_override(request) or _is_legacy_compat_bypass(request)
    actor_employee_id = _resolve_actor_employee_id(request, allow_unmapped=admin_override)
    if not admin_override:
        employee_id = str(transaction.get("employeeId") or "").strip()
        _require_direct_manager(employee_id=employee_id, manager_id=str(actor_employee_id or "").strip())

    updated = reject_pending_pto_request(
        transaction_id=normalized_id,
        approver_id=actor_employee_id,
        approverNote=_normalize_optional_approver_note(approverNote),
    )
    return {"id": normalized_id, "status": "Rejected", "updated": updated}
