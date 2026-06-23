from __future__ import annotations

from datetime import date, datetime

from apps.leavesphere.api.v1.helpers.dbQueries import (
    approve_pending_pto_request,
    cancel_pending_pto_transaction,
    get_pto_transactions,
    reject_pending_pto_request,
    update_pto_transaction,
)
from apps.leavesphere.api.v1.helpers.googleCalendarSync import sync_leave_sphere_google_calendar_event
from shared.tenant import get_tenant_id

def finalize_pending_pto_action(
    *,
    transaction_id: str,
    action: str,
    approver_id: str | None,
    approver_note: str | None,
    transaction: dict | None = None,
    send_status_email: bool = True,
    note_label_override: str | None = None,
) -> dict[str, object]:
    normalized_id = str(transaction_id or "").strip()
    if not normalized_id:
        raise ValueError("transaction_id is required")

    normalized_action = str(action or "").strip().lower()
    if normalized_action in {"approve", "approved"}:
        normalized_action = "approved"
    elif normalized_action in {"reject", "rejected"}:
        normalized_action = "rejected"
    elif normalized_action in {"cancel", "canceled"}:
        normalized_action = "canceled"
    elif normalized_action in {"revert", "reverted"}:
        normalized_action = "reverted"
    else:
        raise ValueError("action must be approved, rejected, canceled, or reverted")

    current_transaction = transaction
    if not isinstance(current_transaction, dict):
        rows = get_pto_transactions(transaction_id=normalized_id)
        if not rows:
            raise ValueError("PTO transaction not found")
        current_transaction = rows[0]

    if normalized_action == "approved":
        updated = approve_pending_pto_request(
            transaction_id=normalized_id,
            approver_id=approver_id,
            approverNote=approver_note,
        )
        email_status = "approved"
        return_status = "Approved"
    elif normalized_action == "rejected":
        updated = reject_pending_pto_request(
            transaction_id=normalized_id,
            approver_id=approver_id,
            approverNote=approver_note,
        )
        email_status = "rejected"
        return_status = "Rejected"
    else:
        current_status = str((current_transaction or {}).get("status") or "").strip().lower()
        start_date = str((current_transaction or {}).get("startDate") or "").strip()
        if start_date:
            start_date = start_date[:10]
        if start_date and date.today().isoformat() >= start_date:
            raise ValueError("Only future PTO requests can be canceled or reverted")

        if normalized_action == "canceled":
            if current_status not in {"pending", "approved", "rejected"}:
                raise ValueError("Only Pending, Approved, or Rejected PTO transactions can be canceled")
            if current_status == "pending":
                updated = cancel_pending_pto_transaction(transaction_id=normalized_id)
            else:
                updated = update_pto_transaction(
                    transaction_id=normalized_id,
                    updates={
                        "status": "Canceled",
                        "approverId": None,
                        "approverNote": approver_note,
                    },
                )
            email_status = "canceled"
            return_status = "Canceled"
        else:
            if current_status not in {"approved", "rejected", "canceled"}:
                raise ValueError("Only Approved, Rejected, or Canceled PTO transactions can be reverted")
            updated = update_pto_transaction(
                transaction_id=normalized_id,
                updates={
                    "status": "Pending",
                    "approverId": None,
                },
            )
            email_status = "updated"
            return_status = "Pending"

        if not updated:
            raise ValueError("PTO transaction not found")

    calendar_sync = None
    try:
        calendar_sync = sync_leave_sphere_google_calendar_event(transaction_id=normalized_id)
    except Exception as exc:  # pragma: no cover - defensive; approval flow must not fail on calendar sync
        calendar_sync = {
            "synced": False,
            "reason": str(exc),
            "transactionId": normalized_id,
        }

    if send_status_email and isinstance(current_transaction, dict):
        from apps.leavesphere.api.v1.helpers.notification_emails import send_leave_sphere_status_email

        send_leave_sphere_status_email(
            transaction=current_transaction,
            status=email_status,
            admin_note=approver_note,
            note_label_override=note_label_override,
        )

    tenant_id = str(get_tenant_id() or "").strip()
    if tenant_id:
        from apps.leavesphere.api.v1.helpers.quickApproval import (
            clear_quick_approval_refresh_cache,
            clear_quick_approval_transaction_cache,
        )

        clear_quick_approval_refresh_cache(tenant_id=tenant_id, request_id=normalized_id)
        clear_quick_approval_transaction_cache(tenant_id=tenant_id, request_id=normalized_id)

    return {
        "id": normalized_id,
        "status": return_status,
        "updated": updated,
        "calendarSync": calendar_sync,
    }
