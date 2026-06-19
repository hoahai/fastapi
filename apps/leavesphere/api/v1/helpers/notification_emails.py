from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from shared.email_templates import (
    build_callout_html,
    build_email_shell_html,
    build_key_value_text,
    escape_html,
    normalize_text,
)

LeaveSphereNotificationKind = Literal[
    "confirmation",
    "approval",
    "approved",
    "rejected",
    "canceled",
    "updated",
]


@dataclass(frozen=True)
class LeaveSphereNotificationEmail:
    subject: str
    text_body: str
    html_body: str


def build_leave_sphere_notification_email(
    *,
    kind: LeaveSphereNotificationKind,
    employee_name: str,
    pto_type_label: str,
    start_date: str,
    end_date: str,
    hours: object,
    request_id: str | None = None,
    reason: str | None = None,
    submitted_at: str | None = None,
    recipient_name: str | None = None,
    manager_name: str | None = None,
    admin_note: str | None = None,
    quick_approval_url: str | None = None,
    update_summary: list[str] | None = None,
) -> LeaveSphereNotificationEmail:
    normalized_kind = normalize_text(kind).lower()
    if normalized_kind not in {"confirmation", "approval", "approved", "rejected", "canceled", "updated"}:
        raise ValueError("kind must be confirmation, approval, approved, rejected, canceled, or updated")

    normalized_employee_name = normalize_text(employee_name) or "Employee"
    normalized_pto_type = normalize_text(pto_type_label) or "PTO"
    normalized_start_date = normalize_text(start_date)
    normalized_end_date = normalize_text(end_date)
    normalized_hours = _format_hours(hours)
    normalized_request_id = normalize_text(request_id)
    normalized_reason = normalize_text(reason)
    normalized_submitted_at = _format_date(submitted_at)
    normalized_recipient_name = normalize_text(recipient_name)
    normalized_manager_name = normalize_text(manager_name)
    normalized_admin_note = normalize_text(admin_note)
    normalized_update_summary = [normalize_text(item) for item in (update_summary or []) if normalize_text(item)]

    title, subtitle, preheader, status_label, status_tone, subject = _build_variant_metadata(
        kind=normalized_kind,
        employee_name=normalized_employee_name,
        pto_type_label=normalized_pto_type,
    )

    summary_rows: list[tuple[str, object | None]] = [
        ("Employee", normalized_employee_name),
        ("PTO type", normalized_pto_type),
        ("Dates", _format_date_range(normalized_start_date, normalized_end_date)),
        ("Hours", normalized_hours),
    ]
    if normalized_request_id:
        summary_rows.append(("Request ID", normalized_request_id))
    if normalized_submitted_at:
        summary_rows.append(("Submitted", normalized_submitted_at))
    if normalized_recipient_name and normalized_kind in {"confirmation", "approved", "rejected", "canceled", "updated"}:
        summary_rows.append(("Sent to", normalized_recipient_name))
    if normalized_manager_name and normalized_kind == "approval":
        summary_rows.append(("Approver", normalized_manager_name))
    if normalized_reason and normalized_kind in {"confirmation", "approval", "updated"}:
        summary_rows.append(("Reason", normalized_reason))

    sections_html: list[str] = []
    text_sections: list[str] = []

    if normalized_kind == "approval":
        explanation = (
            "Review the request details below, then use the quick approval button to open the approval page."
        )
        sections_html.append(
            build_callout_html(
                title="Review request",
                body_html=normalize_text(explanation).replace("\n", "<br />"),
                tone="info",
            )
        )
        text_sections.append("Review the request details below, then use the quick approval link to open the approval page.")
    elif normalized_kind == "confirmation":
        sections_html.append(
            build_callout_html(
                title="Request submitted",
                body_html=(
                    f"We received this request for {escape_html(normalized_employee_name)}. "
                    "It is now waiting for manager or admin review."
                ),
                tone="info",
            )
        )
        text_sections.append(
            f"We received this request for {normalized_employee_name}. It is now waiting for manager or admin review."
        )
    else:
        sections_html.append(
            build_callout_html(
                title="Status update",
                body_html=(
                    f"The request for {escape_html(normalized_employee_name)} is now marked as "
                    f"<strong>{escape_html(normalized_kind.capitalize())}</strong>."
                ),
                tone=_tone_for_status(normalized_kind),
            )
        )
        text_sections.append(
            f"The request for {normalized_employee_name} is now marked as {normalized_kind.capitalize()}."
        )

    if normalized_update_summary:
        sections_html.append(
            build_callout_html(
                title="What changed",
                body_html=build_bullets_html(normalized_update_summary),
                tone="neutral",
            )
        )
        text_sections.append("What changed:")
        text_sections.extend(f"- {item}" for item in normalized_update_summary)

    if normalized_admin_note:
        sections_html.append(
            build_callout_html(
                title="Admin note",
                body_html=_paragraph_html(normalized_admin_note),
                tone="neutral" if normalized_kind != "rejected" else "danger",
            )
        )
        text_sections.append("")
        text_sections.append("Admin note:")
        text_sections.append(normalized_admin_note)
    elif normalized_kind in {"approved", "rejected", "canceled", "updated"}:
        sections_html.append(
            build_callout_html(
                title="Admin note",
                body_html="No admin note was provided.",
                tone="neutral",
            )
        )
        text_sections.append("")
        text_sections.append("Admin note: No admin note was provided.")

    footer_note = "If you have questions, reply to this email or review the request in LeaveSphere."
    if normalized_kind == "approval" and quick_approval_url:
        footer_note = "The quick approval button opens the exact request in LeaveSphere for review."

    html_body = build_email_shell_html(
        app_label="LeaveSphere",
        title=title,
        subtitle=subtitle,
        preheader=preheader,
        status_label=status_label,
        status_tone=status_tone,
        greeting=f"Hi {normalized_recipient_name}," if normalized_recipient_name else None,
        summary_rows=summary_rows,
        sections_html=sections_html,
        cta_label="Open Quick Approval" if normalized_kind == "approval" and quick_approval_url else None,
        cta_href=quick_approval_url if normalized_kind == "approval" else None,
        footer_note=footer_note,
        accent_color="#1d4ed8",
    )

    text_lines: list[str] = [
        "LeaveSphere",
        title,
        "",
        subtitle,
    ]
    if normalized_recipient_name:
        text_lines.extend(["", f"Hi {normalized_recipient_name},"])
    summary_text = build_key_value_text(summary_rows)
    if summary_text:
        text_lines.extend([""] + summary_text.splitlines())
    for section in text_sections:
        if section == "":
            text_lines.append("")
            continue
        text_lines.append(section)
    if normalized_kind == "approval" and quick_approval_url:
        text_lines.extend(["", f"Open Quick Approval: {normalize_text(quick_approval_url)}"])
    text_lines.extend(["", footer_note])

    text_body = _squash_blank_lines(text_lines)
    return LeaveSphereNotificationEmail(
        subject=subject,
        text_body=text_body,
        html_body=html_body,
    )


def build_leave_sphere_confirmation_email(
    *,
    employee_name: str,
    pto_type_label: str,
    start_date: str,
    end_date: str,
    hours: object,
    request_id: str | None = None,
    reason: str | None = None,
    submitted_at: str | None = None,
) -> LeaveSphereNotificationEmail:
    return build_leave_sphere_notification_email(
        kind="confirmation",
        employee_name=employee_name,
        pto_type_label=pto_type_label,
        start_date=start_date,
        end_date=end_date,
        hours=hours,
        request_id=request_id,
        reason=reason,
        submitted_at=submitted_at,
        recipient_name=employee_name,
    )


def build_leave_sphere_approval_email(
    *,
    employee_name: str,
    pto_type_label: str,
    start_date: str,
    end_date: str,
    hours: object,
    request_id: str | None = None,
    reason: str | None = None,
    submitted_at: str | None = None,
    manager_name: str | None = None,
    quick_approval_url: str,
) -> LeaveSphereNotificationEmail:
    return build_leave_sphere_notification_email(
        kind="approval",
        employee_name=employee_name,
        pto_type_label=pto_type_label,
        start_date=start_date,
        end_date=end_date,
        hours=hours,
        request_id=request_id,
        reason=reason,
        submitted_at=submitted_at,
        recipient_name=manager_name or None,
        manager_name=manager_name,
        quick_approval_url=quick_approval_url,
    )


def build_leave_sphere_status_email(
    *,
    status: Literal["approved", "rejected", "canceled", "updated"],
    employee_name: str,
    pto_type_label: str,
    start_date: str,
    end_date: str,
    hours: object,
    request_id: str | None = None,
    reason: str | None = None,
    submitted_at: str | None = None,
    admin_note: str | None = None,
    update_summary: list[str] | None = None,
) -> LeaveSphereNotificationEmail:
    return build_leave_sphere_notification_email(
        kind=status,
        employee_name=employee_name,
        pto_type_label=pto_type_label,
        start_date=start_date,
        end_date=end_date,
        hours=hours,
        request_id=request_id,
        reason=reason,
        submitted_at=submitted_at,
        recipient_name=employee_name,
        admin_note=admin_note,
        update_summary=update_summary,
    )


def _build_variant_metadata(
    *,
    kind: LeaveSphereNotificationKind,
    employee_name: str,
    pto_type_label: str,
) -> tuple[str, str, str, str, str, str]:
    if kind == "confirmation":
        title = "Your PTO request has been submitted"
        subtitle = f"We received the request for {employee_name} and it is waiting for review."
        preheader = f"{employee_name}'s PTO request is in the queue."
        status_label = "Submitted"
        status_tone = "info"
        subject = f"LeaveSphere: PTO request submitted for {employee_name}"
    elif kind == "approval":
        title = "PTO request needs your review"
        subtitle = f"{employee_name} submitted a {pto_type_label} request."
        preheader = f"Review the PTO request from {employee_name}."
        status_label = "Needs approval"
        status_tone = "warning"
        subject = f"LeaveSphere: approval needed for {employee_name}"
    elif kind == "approved":
        title = "Your PTO request was approved"
        subtitle = f"The request for {employee_name} is approved and recorded."
        preheader = f"{employee_name}'s PTO request was approved."
        status_label = "Approved"
        status_tone = "success"
        subject = f"LeaveSphere: PTO request approved for {employee_name}"
    elif kind == "rejected":
        title = "Your PTO request was rejected"
        subtitle = f"The request for {employee_name} was rejected after review."
        preheader = f"{employee_name}'s PTO request was rejected."
        status_label = "Rejected"
        status_tone = "danger"
        subject = f"LeaveSphere: PTO request rejected for {employee_name}"
    elif kind == "canceled":
        title = "Your PTO request was canceled"
        subtitle = f"The request for {employee_name} is no longer active."
        preheader = f"{employee_name}'s PTO request was canceled."
        status_label = "Canceled"
        status_tone = "neutral"
        subject = f"LeaveSphere: PTO request canceled for {employee_name}"
    else:
        title = "Your PTO request was updated"
        subtitle = f"The request for {employee_name} was updated by an admin or manager."
        preheader = f"{employee_name}'s PTO request was updated."
        status_label = "Updated"
        status_tone = "info"
        subject = f"LeaveSphere: PTO request updated for {employee_name}"
    return title, subtitle, preheader, status_label, status_tone, subject


def _format_hours(value: object) -> str:
    try:
        hours = float(str(value).strip())
    except (TypeError, ValueError):
        return normalize_text(value) or "-"
    if hours.is_integer():
        return f"{int(hours)} hours"
    return f"{hours:.1f} hours"


def _format_date(value: str | None) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    try:
        parsed = datetime.fromisoformat(text[:19].replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.fromisoformat(f"{text}T00:00:00")
        except ValueError:
            return text
    return parsed.strftime("%b %d, %Y")


def _format_date_range(start_date: str, end_date: str) -> str:
    if start_date and end_date and start_date == end_date:
        return _format_date(start_date)
    if start_date and end_date:
        return f"{_format_date(start_date)} to {_format_date(end_date)}"
    return start_date or end_date or "-"


def _tone_for_status(status: str) -> str:
    if status == "approved":
        return "success"
    if status == "rejected":
        return "danger"
    if status == "canceled":
        return "neutral"
    return "info"


def _paragraph_html(text: str) -> str:
    return escape_html(normalize_text(text)).replace("\n", "<br />")


def build_bullets_html(items: list[str]) -> str:
    rows = [normalize_text(item) for item in items if normalize_text(item)]
    if not rows:
        return "No details provided."
    return (
        "<ul style=\"margin:0;padding:0 0 0 18px;\">"
        + "".join(f"<li style=\"margin:0 0 8px 0;\">{escape_html(item)}</li>" for item in rows)
        + "</ul>"
    )


def _squash_blank_lines(lines: list[str]) -> str:
    output: list[str] = []
    previous_blank = False
    for line in lines:
        normalized = normalize_text(line)
        if not normalized:
            if not previous_blank:
                output.append("")
            previous_blank = True
            continue
        output.append(normalized)
        previous_blank = False
    while output and output[0] == "":
        output.pop(0)
    while output and output[-1] == "":
        output.pop()
    return "\n".join(output)
