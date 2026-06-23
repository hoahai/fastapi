from __future__ import annotations

from datetime import date, timedelta

from apps.leavesphere.api.v1.helpers.dbQueries import (
    get_employees,
    get_pto_transactions,
    get_pto_types,
    update_pto_transaction,
)
from apps.leavesphere.api.v1.helpers.googleCalendarOAuth import (
    create_google_calendar_event,
    delete_google_calendar_event,
    get_google_calendar_id,
    get_google_calendar_connection_status,
    patch_google_calendar_event,
)
from shared.tenant import get_tenant_id


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _build_employee_name(employee: dict | None) -> str:
    if not isinstance(employee, dict):
        return ""
    first_name = _normalize_text(employee.get("firstName"))
    last_name = _normalize_text(employee.get("lastName"))
    full_name = " ".join(part for part in (first_name, last_name) if part)
    return full_name or _normalize_text(employee.get("email")) or _normalize_text(employee.get("id"))


def _parse_date(value: object | None) -> date | None:
    text = _normalize_text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _build_event_dates(transaction: dict) -> tuple[dict[str, str] | None, dict[str, str] | None]:
    start_date = _parse_date(transaction.get("startDate"))
    end_date = _parse_date(transaction.get("endDate")) or start_date
    if start_date is None and end_date is None:
        return None, None
    effective_start = start_date or end_date
    effective_end = end_date or start_date or effective_start
    if effective_start is None or effective_end is None:
        return None, None
    return (
        {"date": effective_start.isoformat()},
        {"date": (effective_end + timedelta(days=1)).isoformat()},
    )


def _build_event_summary(*, employee_name: str, pto_type_name: str) -> str:
    employee_label = _normalize_text(employee_name) or "Employee"
    return f"LeaveSphere: {employee_label} OOO"


def _build_event_body(*, transaction: dict, employee_name: str, pto_type_name: str) -> dict[str, object] | None:
    start, end = _build_event_dates(transaction)
    if start is None or end is None:
        return None

    transaction_description = _normalize_text(transaction.get("description"))
    pto_label = _normalize_text(pto_type_name) or "Leave"
    description = pto_label
    if transaction_description:
        description = f"{pto_label}: {transaction_description}"

    return {
        "summary": _build_event_summary(employee_name=employee_name, pto_type_name=pto_type_name),
        "description": description,
        "start": start,
        "end": end,
    }


def sync_leave_sphere_google_calendar_event(
    *,
    transaction_id: str,
    tenant_id: str | None = None,
    send_updates: str | None = None,
) -> dict[str, object]:
    normalized_transaction_id = _normalize_text(transaction_id)
    normalized_tenant_id = _normalize_text(tenant_id or get_tenant_id())
    if not normalized_transaction_id:
        raise ValueError("transaction_id is required")
    if not normalized_tenant_id:
        return {"synced": False, "reason": "missing_tenant"}

    transaction_rows = get_pto_transactions(transaction_id=normalized_transaction_id)
    if not transaction_rows:
        return {"synced": False, "reason": "transaction_not_found"}

    transaction = transaction_rows[0]
    shared_calendar_id = get_google_calendar_id()
    if not shared_calendar_id:
        return {"synced": False, "reason": "missing_calendar_id"}

    connection = get_google_calendar_connection_status(tenant_id=normalized_tenant_id)
    if not bool(connection.get("connected")):
        return {"synced": False, "reason": "calendar_not_connected"}

    employee_rows = get_employees(employee_id=_normalize_text(transaction.get("employeeId")))
    employee_name = _build_employee_name(employee_rows[0]) if employee_rows else _normalize_text(transaction.get("employeeId"))
    pto_type_rows = get_pto_types(code=_normalize_text(transaction.get("ptoTypeCode")).upper())
    pto_type_name = _normalize_text(pto_type_rows[0].get("name")) if pto_type_rows else _normalize_text(transaction.get("ptoTypeCode"))
    event_body = _build_event_body(transaction=transaction, employee_name=employee_name, pto_type_name=pto_type_name)

    current_status = _normalize_text(transaction.get("status")).lower()
    existing_event_id = _normalize_text(transaction.get("calendarId"))

    if current_status == "approved":
        if event_body is None:
            return {"synced": False, "reason": "missing_dates", "transactionId": normalized_transaction_id}

        if existing_event_id:
            try:
                event = patch_google_calendar_event(
                    tenant_id=normalized_tenant_id,
                    calendar_id=shared_calendar_id,
                    event_id=existing_event_id,
                    event=event_body,
                    send_updates=send_updates,
                )
                event_id = _normalize_text(event.get("id")) or existing_event_id
                operation = "updated"
            except Exception:
                event = create_google_calendar_event(
                    tenant_id=normalized_tenant_id,
                    calendar_id=shared_calendar_id,
                    event=event_body,
                    send_updates=send_updates,
                )
                event_id = _normalize_text(event.get("id"))
                operation = "created"
        else:
            event = create_google_calendar_event(
                tenant_id=normalized_tenant_id,
                calendar_id=shared_calendar_id,
                event=event_body,
                send_updates=send_updates,
            )
            event_id = _normalize_text(event.get("id"))
            operation = "created"

        if event_id:
            update_pto_transaction(
                transaction_id=normalized_transaction_id,
                updates={"calendarId": event_id},
            )

        return {
            "synced": True,
            "operation": operation,
            "transactionId": normalized_transaction_id,
            "calendarId": shared_calendar_id,
            "eventId": event_id,
        }

    if existing_event_id:
        try:
            delete_google_calendar_event(
                tenant_id=normalized_tenant_id,
                calendar_id=shared_calendar_id,
                event_id=existing_event_id,
                send_updates=send_updates,
            )
        finally:
            update_pto_transaction(
                transaction_id=normalized_transaction_id,
                updates={"calendarId": None},
            )
        return {
            "synced": True,
            "operation": "deleted",
            "transactionId": normalized_transaction_id,
            "calendarId": shared_calendar_id,
            "eventId": existing_event_id,
            "status": current_status,
        }

    return {
        "synced": False,
        "reason": f"status_{current_status}_no_link",
        "transactionId": normalized_transaction_id,
        "calendarId": shared_calendar_id,
    }
