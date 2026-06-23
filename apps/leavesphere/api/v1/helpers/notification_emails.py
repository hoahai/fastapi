from __future__ import annotations

import ast
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

from apps.leavesphere.api.v1.helpers.quickApproval import (
    issue_quick_approval_deliveries,
    resolve_quick_approval_delivery,
    revoke_quick_approval_grant,
)
from shared.smtp import SmtpSendError, SmtpSettings, send_smtp_email
from apps.leavesphere.api.v1.helpers.config import get_action_cc_emails, get_reminder_cc_emails
from shared.tenant import get_app_scoped_env
from shared.email_templates import escape_html, normalize_text

LeaveSphereNotificationKind = Literal[
    "confirmation",
    "approval",
    "approved",
    "rejected",
    "canceled",
    "updated",
]

LEAVESPHERE_EMAIL_HEADER_IMAGE_URL = (
    "https://res.cloudinary.com/dpmjwuqfl/image/upload/v1781879686/"
    "leavesphere-emailheader_qcgdxy.png"
)
LEAVESPHERE_EMAIL_FOOTER_IMAGE_URL = (
    "https://res.cloudinary.com/dpmjwuqfl/image/upload/v1781879688/"
    "leavesphere-emailfooter_a4f49e.png"
)
LEAVESPHERE_TENANT_LOGO_URL = (
    "https://res.cloudinary.com/dpmjwuqfl/image/upload/v1781880434/"
    "scfgs5yjhapadizhl1py_ktuep1.png"
)
LEAVESPHERE_EMAIL_PREVIEW_TEST_RECIPIENT = "hai@theautoadagency.com"

EMAIL_BACKGROUND = "#f9f9fb"
EMAIL_SURFACE = "#ffffff"
EMAIL_SURFACE_SUBTLE = "#f5f3f9"
EMAIL_CARD_BORDER = "#e5e1ec"
EMAIL_PRIMARY = "#15003d"
EMAIL_PRIMARY_SOFT = "#2d0a6a"
EMAIL_SECONDARY = "#632ce5"
EMAIL_TEXT = "#1a1c1d"
EMAIL_MUTED = "#5e5a68"
EMAIL_MUTED_STRONG = "#494551"
EMAIL_OUTLINE = "#cbc4d3"
FONT_HEADLINE = "Manrope, Arial, Helvetica, sans-serif"
FONT_BODY = "Inter, Arial, Helvetica, sans-serif"
REQUEST_DETAILS_ICON_URL = (
    "https://res.cloudinary.com/dpmjwuqfl/image/upload/v1782137160/"
    "request-details_dgnrwe.svg"
)
NOTE_AVATAR_ICON_URL = (
    "https://res.cloudinary.com/dpmjwuqfl/image/upload/v1782137161/"
    "note-avatar_yyhp2j.svg"
)
INFO_ICON_URL = (
    "https://res.cloudinary.com/dpmjwuqfl/image/upload/v1782137160/"
    "info_qkn9id.svg"
)


@dataclass(frozen=True)
class LeaveSphereNotificationEmail:
    subject: str
    text_body: str
    html_body: str


@dataclass(frozen=True)
class LeaveSpherePendingRequest:
    employee_name: str
    pto_type_label: str
    start_date: str
    end_date: str
    hours: object
    request_id: str | None = None
    reason: str | None = None
    submitted_at: str | None = None
    request_url: str | None = None
    picture_url: str | None = None


_LOGGER = logging.getLogger(__name__)

_SMTP_KEYS = {
    "host": "host",
    "server": "host",
    "smtp_host": "host",
    "port": "port",
    "smtp_port": "port",
    "username": "username",
    "user": "username",
    "smtp_username": "username",
    "password": "password",
    "pass": "password",
    "smtp_password": "password",
    "from": "from_email",
    "from_email": "from_email",
    "email_from": "from_email",
    "fromemail": "from_email",
    "from_name": "from_name",
    "fromname": "from_name",
    "reply_to": "reply_to",
    "replyto": "reply_to",
    "use_tls": "use_tls",
    "usetls": "use_tls",
    "use_ssl": "use_ssl",
    "usessl": "use_ssl",
    "timeout_seconds": "timeout_seconds",
    "timeoutseconds": "timeout_seconds",
    "timeout": "timeout_seconds",
}


def _parse_config_payload(raw: str) -> dict[str, object]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = ast.literal_eval(raw)
    if not isinstance(payload, dict):
        raise ValueError("SMTP config must be an object")
    return payload


def _normalize_bool(value: object | None, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = normalize_text(value).lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "on"}


def _normalize_port(value: object | None, *, default: int) -> int:
    text = normalize_text(value)
    if not text:
        return default
    port = int(text)
    if port < 1 or port > 65535:
        raise ValueError("SMTP port must be between 1 and 65535")
    return port


def _normalize_timeout(value: object | None, *, default: float) -> float:
    text = normalize_text(value)
    if not text:
        return default
    timeout = float(text)
    if timeout <= 0:
        raise ValueError("SMTP timeout must be greater than zero")
    return timeout


def _build_icon_badge_html(
    *,
    label: str,
    background: str,
    color: str,
    size: int = 18,
    font_size: int = 12,
    border: str | None = None,
) -> str:
    border_style = f"border:1px solid {border};" if border else ""
    return (
        f'<span style="display:inline-block;width:{size}px;height:{size}px;'
        f'border-radius:999px;background:{background};{border_style}'
        f'color:{color};font-family:{FONT_HEADLINE};font-size:{font_size}px;'
        f'line-height:{size}px;font-weight:800;text-align:center;vertical-align:middle;">'
        f"{escape_html(label)}"
        "</span>"
    )


def _build_icon_image_html(*, src: str, alt: str, size: int, top: int = 0) -> str:
    return (
        f'<img src="{escape_html(src)}" alt="{escape_html(alt)}" width="{size}" height="{size}" '
        f'style="display:block;width:{size}px;height:{size}px;border:0;outline:none;'
        f'text-decoration:none;line-height:0;vertical-align:middle;position:relative;top:{top}px;" />'
    )


def _get_employees_by_ids(*, employee_ids: list[str]) -> list[dict]:
    from apps.leavesphere.api.v1.helpers.dbQueries import get_employees_by_ids

    return get_employees_by_ids(employee_ids=employee_ids)


def _get_employee_managers(*, employee_id: str | None = None, manager_id: str | None = None) -> list[dict]:
    from apps.leavesphere.api.v1.helpers.dbQueries import get_employee_managers

    return get_employee_managers(employee_id=employee_id, manager_id=manager_id)


def _get_pto_types(*, code: str) -> list[dict]:
    from apps.leavesphere.api.v1.helpers.dbQueries import get_pto_types

    return get_pto_types(code=code)


def _get_leave_sphere_smtp_settings() -> SmtpSettings | None:
    raw = get_app_scoped_env("leavesphere", "SMTP")
    if raw is None or not str(raw).strip():
        return None

    payload = _parse_config_payload(str(raw))
    canonical: dict[str, object] = {}
    for key, value in payload.items():
        canonical_key = _SMTP_KEYS.get(str(key).strip().lower().replace("-", "_"))
        if canonical_key:
            canonical[canonical_key] = value

    host = normalize_text(canonical.get("host"))
    from_email = normalize_text(canonical.get("from_email"))
    if not host or not from_email or "@" not in from_email:
        raise ValueError("leavesphere.smtp.host and leavesphere.smtp.from_email are required")

    use_ssl = _normalize_bool(canonical.get("use_ssl"), default=False)
    use_tls = _normalize_bool(canonical.get("use_tls"), default=not use_ssl)
    if use_ssl and use_tls:
        raise ValueError("leavesphere.smtp.use_ssl and leavesphere.smtp.use_tls cannot both be true")

    username = normalize_text(canonical.get("username")) or None
    password = normalize_text(canonical.get("password")) or None
    if username and not password:
        raise ValueError("leavesphere.smtp.password is required when leavesphere.smtp.username is provided")

    return SmtpSettings(
        host=host,
        port=_normalize_port(canonical.get("port"), default=465 if use_ssl else 587),
        from_email=from_email,
        username=username,
        password=password,
        from_name=normalize_text(canonical.get("from_name")) or None,
        reply_to=normalize_text(canonical.get("reply_to")) or None,
        use_tls=use_tls,
        use_ssl=use_ssl,
        timeout_seconds=_normalize_timeout(canonical.get("timeout_seconds"), default=20.0),
    )


def _resolve_employee_contact(*, employee_id: str) -> tuple[str, str] | None:
    employee_rows = _get_employees_by_ids(employee_ids=[employee_id])
    if not employee_rows:
        return None

    employee = employee_rows[0]
    recipient_email = normalize_text(employee.get("email"))
    if not recipient_email or "@" not in recipient_email:
        return None

    first_name = normalize_text(employee.get("firstName"))
    last_name = normalize_text(employee.get("lastName"))
    display_name = " ".join(part for part in (first_name, last_name) if part)
    if not display_name:
        display_name = normalize_text(employee.get("name")) or recipient_email

    return display_name, recipient_email


def _resolve_employee_name(*, employee_id: str) -> str | None:
    employee_rows = _get_employees_by_ids(employee_ids=[employee_id])
    if not employee_rows:
        return None

    employee = employee_rows[0]
    first_name = normalize_text(employee.get("firstName"))
    last_name = normalize_text(employee.get("lastName"))
    display_name = " ".join(part for part in (first_name, last_name) if part)
    if display_name:
        return display_name
    return normalize_text(employee.get("name")) or None


def _resolve_manager_contacts(*, employee_id: str) -> list[dict[str, str | None]]:
    manager_rows = _get_employee_managers(employee_id=employee_id)
    manager_ids: list[str] = []
    for row in manager_rows:
        manager_id = normalize_text(row.get("managerId"))
        if manager_id and manager_id not in manager_ids:
            manager_ids.append(manager_id)

    if not manager_ids:
        return []

    manager_employee_rows = _get_employees_by_ids(employee_ids=manager_ids)
    manager_by_id = {
        normalize_text(row.get("id")): row
        for row in manager_employee_rows
        if normalize_text(row.get("id"))
    }

    contacts: list[dict[str, str | None]] = []
    for manager_id in manager_ids:
        manager = manager_by_id.get(manager_id)
        if not manager:
            continue
        recipient_email = normalize_text(manager.get("email"))
        if not recipient_email or "@" not in recipient_email:
            continue
        first_name = normalize_text(manager.get("firstName"))
        last_name = normalize_text(manager.get("lastName"))
        display_name = " ".join(part for part in (first_name, last_name) if part)
        if not display_name:
            display_name = normalize_text(manager.get("name")) or recipient_email
        contacts.append(
            {
                "managerId": manager_id,
                "managerName": display_name,
                "managerEmail": recipient_email,
                "managerPictureUrl": normalize_text(manager.get("pictureUrl")) or None,
            }
        )
    return contacts


def _resolve_pto_type_label(*, pto_type_code: str) -> str:
    type_rows = _get_pto_types(code=pto_type_code) if pto_type_code else []
    if type_rows:
        resolved_name = normalize_text(type_rows[0].get("name"))
        if resolved_name:
            return resolved_name
    return normalize_text(pto_type_code) or "PTO"


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
    note_label_override: str | None = None,
    note_avatar_url: str | None = None,
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
    normalized_request_id = normalize_text(request_id)
    normalized_reason = normalize_text(reason)
    normalized_submitted_at = _format_date(submitted_at)
    normalized_recipient_name = normalize_text(recipient_name)
    normalized_manager_name = normalize_text(manager_name)
    normalized_admin_note = normalize_text(admin_note)
    normalized_update_summary = [
        normalize_text(item)
        for item in (update_summary or [])
        if normalize_text(item)
    ]
    normalized_hours = _format_hours(hours)

    spec = _build_variant_spec(
        kind=normalized_kind,
        employee_name=normalized_employee_name,
        pto_type_label=normalized_pto_type,
        start_date=normalized_start_date,
        end_date=normalized_end_date,
    )

    summary_rows: list[tuple[str, str]] = [
        ("Type", normalized_pto_type),
        ("Total Hours", normalized_hours),
        ("Start Date", _format_date(normalized_start_date)),
        ("End Date", _format_date(normalized_end_date)),
    ]

    note_tone, note_label, note_author, note_avatar, note_body = _resolve_note_block(
        kind=normalized_kind,
        employee_name=normalized_employee_name,
        recipient_name=normalized_recipient_name,
        manager_name=normalized_manager_name,
        reason=normalized_reason,
        admin_note=normalized_admin_note,
        note_label_override=normalize_text(note_label_override) or None,
    )

    html_body = _build_html_email(
        kind=normalized_kind,
        employee_name=normalized_employee_name,
        recipient_name=normalized_recipient_name,
        pto_type_label=normalized_pto_type,
        request_id=normalized_request_id,
        reason=normalized_reason,
        submitted_at=normalized_submitted_at,
        manager_name=normalized_manager_name,
        note_tone=note_tone,
        note_label=note_label,
        note_author=note_author,
        note_avatar=note_avatar,
        note_avatar_url=note_avatar_url,
        note_body=note_body,
        update_summary=normalized_update_summary,
        quick_approval_url=quick_approval_url if normalized_kind == "approval" else None,
        spec=spec,
        summary_rows=summary_rows,
    )

    text_body = _build_text_body(
        kind=normalized_kind,
        employee_name=normalized_employee_name,
        recipient_name=normalized_recipient_name,
        pto_type_label=normalized_pto_type,
        request_id=normalized_request_id,
        reason=normalized_reason,
        submitted_at=normalized_submitted_at,
        manager_name=normalized_manager_name,
        admin_note=normalized_admin_note,
        note_label=note_label,
        update_summary=normalized_update_summary,
        quick_approval_url=quick_approval_url if normalized_kind == "approval" else None,
        spec=spec,
        summary_rows=summary_rows,
    )

    return LeaveSphereNotificationEmail(
        subject=spec["subject"],
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
    manager_picture_url: str | None = None,
    quick_approval_url: str | None = None,
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
        note_avatar_url=manager_picture_url,
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
    note_label_override: str | None = None,
    approver_picture_url: str | None = None,
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
        note_label_override=note_label_override,
        note_avatar_url=approver_picture_url,
        update_summary=update_summary,
    )


def build_leave_sphere_reminder_email(
    *,
    manager_name: str,
    pending_requests: list[dict],
    reminder_note: str | None = None,
) -> LeaveSphereNotificationEmail:
    normalized_manager_name = normalize_text(manager_name) or "Manager"
    normalized_requests = [
        request
        for request in (_normalize_pending_request(item) for item in pending_requests)
        if request is not None
    ]
    if not normalized_requests:
        raise ValueError("pending_requests must include at least one request")

    pending_count = len(normalized_requests)
    plural_suffix = "s" if pending_count != 1 else ""
    subject = (
        f"PTO approval reminder for {normalized_manager_name} | "
        f"{pending_count} request{plural_suffix} pending"
    )
    intro = (
        f"You have {pending_count} PTO request{plural_suffix} waiting for your approval."
    )

    summary_card = _build_request_details_card(
        title="Pending PTO requests",
        badge="",
        badge_tone="amber",
        rows=[
            ("Manager", normalized_manager_name),
            ("Pending Requests", str(pending_count)),
            ("Total Hours", _format_hours(sum(_parse_hours(request.hours) for request in normalized_requests))),
        ],
        description=None,
    )
    request_cards = _build_pending_request_cards_html(
        pending_requests=normalized_requests,
        fallback_url=None,
    )
    note_card = _build_info_card(
        title="Instruction",
        body_html=_paragraph_html(
            reminder_note
            or "Click any request card to open the request details and complete quick approval in LeaveSphere."
        ),
    )

    body_sections = [
        _build_section_row(summary_card, padding="24px 40px 0"),
        _build_section_row(request_cards, padding="16px 40px 0"),
        _build_section_row(note_card, padding="18px 40px 0"),
    ]

    html_body = _build_email_shell(
        subject=subject,
        title="Pending PTO requests",
        recipient_name=normalized_manager_name,
        intro=intro,
        body_sections_html="".join(body_sections),
        footer_note="This is an automated approval reminder. Please do not reply directly to this email.",
    )

    text_body = _build_reminder_text_body(
        manager_name=normalized_manager_name,
        pending_requests=normalized_requests,
        reminder_note=reminder_note,
        footer_note="This is an automated approval reminder. Please do not reply directly to this email.",
    )

    return LeaveSphereNotificationEmail(
        subject=subject,
        text_body=text_body,
        html_body=html_body,
    )


def send_leave_sphere_status_email(
    *,
    transaction: dict,
    status: Literal["approved", "rejected", "canceled", "updated"],
    admin_note: str | None = None,
    note_label_override: str | None = None,
    approver_picture_url: str | None = None,
    update_summary: list[str] | None = None,
) -> bool:
    if not isinstance(transaction, dict):
        return False

    smtp_settings = _get_leave_sphere_smtp_settings()
    if smtp_settings is None:
        return False

    employee_id = normalize_text(transaction.get("employeeId"))
    if not employee_id:
        return False

    contact = _resolve_employee_contact(employee_id=employee_id)
    if contact is None:
        return False
    employee_name, recipient_email = contact

    pto_type_code = normalize_text(transaction.get("ptoTypeCode")).upper()
    pto_type_label = _resolve_pto_type_label(pto_type_code=pto_type_code)
    start_date = normalize_text(transaction.get("startDate"))
    end_date = normalize_text(transaction.get("endDate"))
    hours = transaction.get("hours")
    request_id = normalize_text(transaction.get("id"))
    submitted_at = normalize_text(transaction.get("dateCreated"))
    reason = normalize_text(transaction.get("description")) or None

    email = build_leave_sphere_status_email(
        status=status,
        employee_name=employee_name,
        pto_type_label=pto_type_label,
        start_date=start_date,
        end_date=end_date,
        hours=hours,
        request_id=request_id,
        reason=reason,
        submitted_at=submitted_at,
        admin_note=admin_note,
        note_label_override=note_label_override,
        approver_picture_url=approver_picture_url,
        update_summary=update_summary,
    )

    try:
        send_smtp_email(
            settings=smtp_settings,
            to_addresses=[recipient_email],
            cc_addresses=get_action_cc_emails(),
            subject=email.subject,
            text_body=email.text_body,
            html_body=email.html_body,
        )
    except (SmtpSendError, ValueError, OSError) as exc:
        _LOGGER.warning(
            "LeaveSphere status email send failed for %s: %s",
            recipient_email,
            exc,
        )
        return False
    return True


def send_leave_sphere_reminder_email(
    *,
    manager_email: str,
    manager_name: str,
    manager_id: str | None = None,
    pending_requests: list[dict],
    reminder_note: str | None = None,
    recipient_email: str | None = None,
    cc_addresses: list[str] | None = None,
    quick_approval_base_url: str | None = None,
    quick_approval_tenant_id: str | None = None,
    quick_approval_tenant_slug: str | None = None,
) -> bool:
    smtp_settings = _get_leave_sphere_smtp_settings()
    if smtp_settings is None:
        return False

    normalized_recipient_email = normalize_text(recipient_email or manager_email)
    if not normalized_recipient_email or "@" not in normalized_recipient_email:
        return False

    enriched_requests = [dict(request) for request in pending_requests]
    if quick_approval_base_url and (quick_approval_tenant_id or quick_approval_tenant_slug):
        for request in enriched_requests:
            request_id = normalize_text(request.get("requestId") or request.get("request_id"))
            employee_id = normalize_text(request.get("employeeId") or request.get("employee_id"))
            if not request_id or not employee_id:
                continue
            delivery = resolve_quick_approval_delivery(
                transaction={
                    "id": request_id,
                    "employeeId": employee_id,
                    "ptoTypeCode": request.get("ptoTypeCode") or request.get("pto_type_code") or request.get("ptoTypeLabel"),
                    "startDate": request.get("startDate") or request.get("start_date"),
                    "endDate": request.get("endDate") or request.get("end_date"),
                    "hours": request.get("hours"),
                    "description": request.get("description") or request.get("reason"),
                    "dateCreated": request.get("submittedAt") or request.get("submitted_at"),
                },
                tenant_id=quick_approval_tenant_id,
                tenant_slug=quick_approval_tenant_slug,
                recipient_email=manager_email,
                recipient_role="manager",
                recipient_employee_id=manager_id,
                recipient_name=manager_name,
                quick_approval_base_url=quick_approval_base_url,
            )
            if delivery and normalize_text(delivery.get("url")):
                request["requestUrl"] = normalize_text(delivery.get("url"))

    try:
        email = build_leave_sphere_reminder_email(
            manager_name=manager_name,
            pending_requests=enriched_requests,
            reminder_note=reminder_note,
        )
        resolved_cc_addresses = (
            list(cc_addresses)
            if cc_addresses is not None
            else get_reminder_cc_emails()
        )
        send_smtp_email(
            settings=smtp_settings,
            to_addresses=[normalized_recipient_email],
            cc_addresses=resolved_cc_addresses,
            subject=email.subject,
            text_body=email.text_body,
            html_body=email.html_body,
        )
    except (ValueError, SmtpSendError, OSError) as exc:
        _LOGGER.warning(
            "LeaveSphere reminder email send failed for %s: %s",
            normalized_recipient_email,
            exc,
        )
        return False
    return True


def send_leave_sphere_confirmation_email(*, transaction: dict) -> bool:
    if not isinstance(transaction, dict):
        return False

    smtp_settings = _get_leave_sphere_smtp_settings()
    if smtp_settings is None:
        return False

    employee_id = normalize_text(transaction.get("employeeId"))
    if not employee_id:
        return False

    contact = _resolve_employee_contact(employee_id=employee_id)
    if contact is None:
        return False
    employee_name, recipient_email = contact

    pto_type_code = normalize_text(transaction.get("ptoTypeCode")).upper()
    pto_type_label = _resolve_pto_type_label(pto_type_code=pto_type_code)
    start_date = normalize_text(transaction.get("startDate"))
    end_date = normalize_text(transaction.get("endDate"))
    hours = transaction.get("hours")
    request_id = normalize_text(transaction.get("id"))
    submitted_at = normalize_text(transaction.get("dateCreated"))
    reason = normalize_text(transaction.get("description")) or None

    email = build_leave_sphere_confirmation_email(
        employee_name=employee_name,
        pto_type_label=pto_type_label,
        start_date=start_date,
        end_date=end_date,
        hours=hours,
        request_id=request_id,
        reason=reason,
        submitted_at=submitted_at,
    )

    try:
        send_smtp_email(
            settings=smtp_settings,
            to_addresses=[recipient_email],
            cc_addresses=get_action_cc_emails(),
            subject=email.subject,
            text_body=email.text_body,
            html_body=email.html_body,
        )
    except (SmtpSendError, ValueError, OSError) as exc:
        _LOGGER.warning(
            "LeaveSphere confirmation email send failed for %s: %s",
            recipient_email,
            exc,
        )
        return False
    return True


def send_leave_sphere_approval_email(
    *,
    transaction: dict,
    quick_approval_url: str | None = None,
    quick_approval_base_url: str | None = None,
    quick_approval_tenant_id: str | None = None,
    quick_approval_tenant_slug: str | None = None,
) -> bool:
    if not isinstance(transaction, dict):
        return False

    smtp_settings = _get_leave_sphere_smtp_settings()
    if smtp_settings is None:
        return False

    employee_id = normalize_text(transaction.get("employeeId"))
    if not employee_id:
        return False

    employee_name = _resolve_employee_name(employee_id=employee_id)
    if not employee_name:
        return False

    manager_contacts = _resolve_manager_contacts(employee_id=employee_id)
    if not manager_contacts:
        return False

    pto_type_code = normalize_text(transaction.get("ptoTypeCode")).upper()
    pto_type_label = _resolve_pto_type_label(pto_type_code=pto_type_code)
    start_date = normalize_text(transaction.get("startDate"))
    end_date = normalize_text(transaction.get("endDate"))
    hours = transaction.get("hours")
    request_id = normalize_text(transaction.get("id"))
    submitted_at = normalize_text(transaction.get("dateCreated"))
    reason = normalize_text(transaction.get("description")) or None

    sent_any = False
    deliveries: list[dict[str, object]] = []
    if quick_approval_url:
        deliveries = [
            {
                "grant_id": None,
                "recipient_email": manager["managerEmail"],
                "recipient_name": manager["managerName"],
                "recipient_picture_url": manager["managerPictureUrl"],
                "url": quick_approval_url,
            }
            for manager in manager_contacts
        ]
    elif quick_approval_base_url:
        deliveries = issue_quick_approval_deliveries(
            transaction=transaction,
            tenant_id=quick_approval_tenant_id or "",
            tenant_slug=quick_approval_tenant_slug or "",
            quick_approval_base_url=quick_approval_base_url,
        )

    if not deliveries:
        deliveries = [
            {
                "grant_id": None,
                "recipient_email": manager["managerEmail"],
                "recipient_name": manager["managerName"],
                "recipient_picture_url": manager["managerPictureUrl"],
                "url": None,
            }
            for manager in manager_contacts
        ]

    for delivery in deliveries:
        recipient_email = normalize_text(delivery.get("recipient_email"))
        recipient_name = normalize_text(delivery.get("recipient_name"))
        recipient_picture_url = normalize_text(delivery.get("recipient_picture_url")) or None
        email = build_leave_sphere_approval_email(
            employee_name=employee_name,
            pto_type_label=pto_type_label,
            start_date=start_date,
            end_date=end_date,
            hours=hours,
            request_id=request_id,
            reason=reason,
            submitted_at=submitted_at,
            manager_name=recipient_name,
            manager_picture_url=recipient_picture_url,
            quick_approval_url=normalize_text(delivery.get("url")) or None,
        )
        try:
            send_smtp_email(
                settings=smtp_settings,
                to_addresses=[recipient_email],
                cc_addresses=get_action_cc_emails(),
                subject=email.subject,
                text_body=email.text_body,
                html_body=email.html_body,
            )
            sent_any = True
        except (SmtpSendError, ValueError, OSError) as exc:
            grant_id = normalize_text(delivery.get("grant_id"))
            if grant_id:
                revoke_quick_approval_grant(grant_id=grant_id, tenant_id=str(delivery.get("tenant_id") or ""))
            _LOGGER.warning(
                "LeaveSphere approval email send failed for %s: %s",
                recipient_email,
                exc,
            )
    return sent_any


def send_leave_sphere_status_preview_email(
    *,
    recipient_email: str = LEAVESPHERE_EMAIL_PREVIEW_TEST_RECIPIENT,
) -> dict[str, str]:
    smtp_settings = _get_leave_sphere_smtp_settings()
    if smtp_settings is None:
        raise ValueError("LeaveSphere SMTP config is required")

    normalized_recipient_email = normalize_text(recipient_email)
    if not normalized_recipient_email or "@" not in normalized_recipient_email:
        raise ValueError("recipient email is required")

    email = build_leave_sphere_status_email(
        status="approved",
        employee_name="Alex Chen",
        pto_type_label="Vacation",
        start_date="2026-06-10",
        end_date="2026-06-12",
        hours=24,
        request_id="pto-preview",
        reason="Family trip",
        submitted_at="2026-05-29T10:00:00",
        admin_note="Preview test email sent from the LeaveSphere email preview.",
        approver_picture_url="https://picsum.photos/seed/leave-approver/96/96",
        update_summary=["Date changed to June 11", "Reason updated"],
    )

    send_smtp_email(
        settings=smtp_settings,
        to_addresses=[normalized_recipient_email],
        subject=email.subject,
        text_body=email.text_body,
        html_body=email.html_body,
    )
    return {
        "recipient_email": normalized_recipient_email,
        "subject": email.subject,
        "status": "sent",
    }


def _build_variant_spec(
    *,
    kind: LeaveSphereNotificationKind,
    employee_name: str,
    pto_type_label: str,
    start_date: str,
    end_date: str,
) -> dict[str, str]:
    date_suffix = _format_subject_date_range(start_date, end_date)
    date_part = f" | {date_suffix}" if date_suffix else ""
    if kind == "confirmation":
        return {
            "subject": f"PTO request submitted for {employee_name}{date_part}",
            "title": "Confirmation of PTO Request",
            "intro": (
                "This email is to confirm that your request for paid time off has been received. "
                "Here are the details of your submission."
            ),
            "badge": "REQUEST CONFIRMED",
            "badge_tone": "purple",
            "button_label": "View Request in LeaveSphere",
            "footer_note": "This is an automated confirmation. Please do not reply directly to this email.",
            "review_card_title": "Review Process",
            "review_card_body": (
                "Your PTO request is currently under review, and a decision will be communicated as soon as possible. "
                "Should there be any issues or if further discussions are needed regarding your request, you will be contacted directly.\n\n"
                "Thank you for planning ahead and coordinating your responsibilities in preparation for your absence, which helps maintain smooth operations."
            ),
        }
    if kind == "approval":
        return {
            "subject": f"New PTO Approval needed for {employee_name}{date_part}",
            "title": "PTO Request Needs Your Approval",
            "intro": (
                "A new Paid Time Off (PTO) request has been submitted by "
                f"{employee_name} and requires your approval. Please review the details at your earliest "
                "convenience in the PTO management system."
            ),
            "badge": "AWAITING APPROVAL",
            "badge_tone": "blue",
            "button_label": "Open Quick Approval",
            "footer_note": "This is an automated approval email. Please review the request in LeaveSphere.",
            "review_card_title": "",
            "review_card_body": "",
        }
    if kind == "approved":
        return {
            "subject": f"PTO request approved for {employee_name}{date_part}",
            "title": "PTO Request Approved",
            "intro": (
                "We are pleased to inform you that your request for Paid Time Off below has been approved."
            ),
            "badge": "APPROVED",
            "badge_tone": "green",
            "button_label": "View Request in LeaveSphere",
            "footer_note": "This is an automated approval notice. Please do not reply directly to this email.",
            "review_card_title": "Next Steps",
            "review_card_body": (
                "Please ensure that all your current responsibilities are managed appropriately during your absence. "
                "If you have not already done so, coordinate with your team or manager to ensure a smooth transition of duties."
            ),
        }
    if kind == "rejected":
        return {
            "subject": f"PTO request rejected for {employee_name}{date_part}",
            "title": "PTO Request Rejected",
            "intro": "We regret to inform you that your request for Paid Time Off below has not been approved.",
            "badge": "REJECTED",
            "badge_tone": "red",
            "button_label": "",
            "footer_note": "This is an automated rejection notice. Please do not reply directly to this email.",
            "review_card_title": "Next Steps",
            "review_card_body": (
                "We understand this decision may be disappointing. If you believe there has been a misunderstanding or if you have additional information that you think should be considered, please feel free to discuss this with your manager at your earliest convenience.\n\n"
                "Alternatively, you are welcome to submit a new PTO request for a different period, and we will review it based on the current staffing needs and company policy."
            ),
        }
    if kind == "canceled":
        return {
            "subject": f"PTO request canceled for {employee_name}{date_part}",
            "title": "PTO Request Canceled",
            "intro": "We regret to inform you that your recent request for Paid Time Off below has been canceled.",
            "badge": "CANCELED",
            "badge_tone": "gray",
            "button_label": "View Request in LeaveSphere",
            "footer_note": "This is an automated cancellation notice. Please do not reply directly to this email.",
            "review_card_title": "Next Steps",
            "review_card_body": (
                "We understand that this may cause inconvenience and we apologize for any disruption this may cause to your plans. "
                "We encourage you to discuss any concerns you may have with your manager or submit a new PTO request at your earliest convenience.\n\n"
                "Our team is here to assist you with the process and address any questions you may have."
            ),
        }
    return {
        "subject": f"PTO request updated for {employee_name}{date_part}",
        "title": "PTO Request Updated",
        "intro": "This email is to inform you that there has been an update to your paid time off request originally submitted.",
        "badge": "UPDATED",
        "badge_tone": "violet",
        "button_label": "View Request in LeaveSphere",
        "footer_note": "This is an automated update notice. Please do not reply directly to this email.",
        "review_card_title": "Next Steps",
        "review_card_body": (
            "Please review these changes at your earliest convenience. If these updates were made in error or if you have any questions, please contact your manager."
        ),
    }


def _build_html_email(
    *,
    kind: LeaveSphereNotificationKind,
    employee_name: str,
    recipient_name: str,
    pto_type_label: str,
    request_id: str,
    reason: str,
    submitted_at: str,
    manager_name: str,
    note_tone: str,
    note_label: str,
    note_author: str,
    note_avatar: str,
    note_avatar_url: str | None,
    note_body: str,
    update_summary: list[str],
    quick_approval_url: str | None,
    spec: dict[str, str],
    summary_rows: list[tuple[str, str]],
) -> str:
    intro = spec["intro"]
    button_label = spec["button_label"]
    footer_note = spec["footer_note"]
    review_card_title = spec["review_card_title"]
    review_card_body = spec["review_card_body"]
    badge = spec["badge"]
    badge_tone = spec["badge_tone"]

    status_tone = _tone_class(badge_tone)
    request_details_card = _build_request_details_card(
        title="Request Details",
        badge=badge,
        badge_tone=status_tone,
        rows=summary_rows,
        description=reason,
    )
    note_card = _build_note_card(
        label=note_label,
        author=note_author,
        avatar_text=note_avatar,
        avatar_url=note_avatar_url,
        body=note_body,
        tone=note_tone,
    )
    info_card = ""
    if review_card_title and review_card_body:
        info_card = _build_info_card(
            title=review_card_title,
            body_html=_paragraph_html(review_card_body),
        )
    changes_card = ""
    if update_summary and kind not in {"approved", "updated"}:
        changes_card = _build_info_card(
            title="What Changed",
            body_html=_build_bullets_html(update_summary),
        )
    button_html = ""
    if kind == "approval" and quick_approval_url:
        button_html = _build_button_html(
            label=button_label,
            href=quick_approval_url,
        )
    body_sections = [
        _build_section_row(request_details_card, padding="24px 40px 0"),
        _build_section_row(note_card, padding="20px 40px 0") if note_card else "",
        _build_section_row(changes_card, padding="20px 40px 0") if changes_card else "",
        _build_section_row(info_card, padding="20px 40px 0"),
        _build_section_row(button_html, padding="36px 40px 44px", align="center"),
    ]

    return _build_email_shell(
        subject=spec["subject"],
        title=spec["title"],
        recipient_name=recipient_name or employee_name,
        intro=intro,
        body_sections_html="".join(section for section in body_sections if section),
        footer_note=footer_note,
    )


def _build_request_details_card(
    *,
    title: str,
    badge: str,
    badge_tone: str,
    rows: list[tuple[str, str]],
    description: str | None,
) -> str:
    cells = []
    for index, (label, value) in enumerate(rows):
        label_html = escape_html(label)
        value_html = escape_html(value)
        cells.append(
            f"""
            <div style="padding:0 0 18px 0;">
              <div style="font-size:10.5px;line-height:1.3;letter-spacing:0.16em;text-transform:uppercase;color:{EMAIL_MUTED};margin-bottom:7px;font-weight:300;">{label_html}</div>
              <div style="font-size:15px;line-height:1.5;font-weight:600;color:{EMAIL_TEXT};">{value_html}</div>
            </div>
            """
        )

    grid_rows = []
    for i in range(0, len(cells), 2):
        left = cells[i]
        right = cells[i + 1] if i + 1 < len(cells) else '<div style="padding:0 0 18px 0;"></div>'
        grid_rows.append(
            f"""
            <tr>
              <td width="50%" valign="top" style="padding-right:18px;">{left}</td>
              <td width="50%" valign="top">{right}</td>
            </tr>
            """
        )

    return f"""
    <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:separate;border-spacing:0;border:1px solid rgba(203,196,211,0.20);border-radius:18px;background:#fcfbfe;box-shadow:0 1px 6px rgba(21,0,61,0.045);overflow:hidden;">
      <tr>
        <td style="padding:24px 26px 26px;">
          <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;">
            <tr>
              <td valign="middle" style="width:18px;padding-right:8px;color:{EMAIL_SECONDARY};line-height:0;text-align:center;">
                {_build_icon_image_html(src=REQUEST_DETAILS_ICON_URL, alt="", size=18, top=-1)}
              </td>
              <td valign="middle" style="font-family:{FONT_HEADLINE};font-size:18px;line-height:1.16;font-weight:600;letter-spacing:-0.01em;color:{EMAIL_PRIMARY};padding-right:10px;text-align:left;">
                {escape_html(title)}
              </td>
              {f'<td align="right" valign="middle" style="white-space:nowrap;line-height:0;padding-top:1px;">{_build_chip_html(badge, badge_tone)}</td>' if normalize_text(badge) else ''}
            </tr>
          </table>
          <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;margin-top:16px;">
            {''.join(grid_rows)}
            {f'''
            <tr>
              <td colspan="2" style="padding-top:12px;">
                  <div style="font-family:{FONT_HEADLINE};font-size:10.5px;line-height:1.3;letter-spacing:0.16em;text-transform:uppercase;color:{EMAIL_MUTED};margin-bottom:8px;font-weight:300;">Description</div>
                <div style="font-family:{FONT_BODY};font-size:15px;line-height:1.65;font-weight:600;color:{EMAIL_MUTED_STRONG};">{_paragraph_html(description or "")}</div>
              </td>
            </tr>
            ''' if normalize_text(description) else ''}
          </table>
        </td>
      </tr>
    </table>
    """


def _build_note_card(
    *,
    label: str,
    author: str,
    avatar_text: str,
    avatar_url: str | None,
    body: str,
    tone: str,
) -> str:
    if not body:
        return ""

    tone_map = {
        "request_note": ("#ebe6f6", "#2d0a6a", "#ffffff", "#cfc6df", "#2d0a6a"),
        "admin_note": ("#eeebf4", "#15003d", "#ffffff", "#cbc3db", "#632ce5"),
        "manager_note": ("#ece8f7", "#15003d", "#ffffff", "#c7c0d8", "#632ce5"),
        "approval_note": ("#ece8f7", "#15003d", "#ffffff", "#c7c0d8", "#632ce5"),
    }
    background, chip_bg, avatar_bg, border, icon_color = tone_map.get(tone, tone_map["admin_note"])
    body_html = _paragraph_html(body)
    avatar_markup = (
        f'<img src="{escape_html(avatar_url)}" alt="{escape_html(author)}" width="48" height="48" '
        'style="display:block;width:48px;height:48px;border:0;outline:none;text-decoration:none;'
        'border-radius:999px;object-fit:cover;" />'
        if normalize_text(avatar_url)
        else (
            f'<table role="presentation" cellpadding="0" cellspacing="0" width="48" height="48" '
            f'style="border-collapse:collapse;width:48px;height:48px;border-radius:999px;background:{avatar_bg};'
            f'border:1px solid {border};box-shadow:inset 0 0 0 1px rgba(255,255,255,0.35);overflow:hidden;">'
            f'<tr><td align="center" valign="middle" style="width:48px;height:48px;">'
            f'{_build_icon_image_html(src=NOTE_AVATAR_ICON_URL, alt="", size=22, top=0)}'
            f'</td></tr></table>'
        )
    )
    return f"""
    <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:separate;border-spacing:0;">
      <tr>
        <td style="padding:0;">
          <div style="position:relative;">
            <div style="position:relative;z-index:2;display:block;margin:0 0 -22px 16px;">
              <table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">
                <tr>
                  <td valign="middle" style="padding-right:10px;line-height:0;">
                    <div style="width:48px;height:48px;border-radius:999px;background:#ffffff;border:1px solid {border};display:flex;align-items:center;justify-content:center;box-shadow:0 8px 18px rgba(21,0,61,0.08);overflow:hidden;">
                      {avatar_markup}
                    </div>
                  </td>
                  <td valign="middle" style="padding-top:0;line-height:0;">
                    <div style="display:inline-block;padding:7px 14px;border-radius:999px;background:{chip_bg};color:#ffffff;font-family:{FONT_HEADLINE};font-size:10.5px;line-height:1.05;font-weight:700;letter-spacing:0.12em;text-transform:uppercase;box-shadow:0 2px 0 rgba(255,255,255,0.35) inset, 0 3px 8px rgba(21,0,61,0.12);">
                      {escape_html(author)}
                    </div>
                    <div style="display:none;">{escape_html(label)}</div>
                  </td>
                </tr>
              </table>
            </div>
            <div style="padding:30px 22px 22px 22px;background:rgba(45,10,106,0.08);border:1px solid rgba(45,10,106,0.16);border-top-left-radius:0;border-radius:0 12px 12px 12px;box-shadow:0 1px 3px rgba(21,0,61,0.04);margin-top:-18px;">
              <div style="font-family:{FONT_BODY};font-size:15px;line-height:1.65;font-weight:400;color:{EMAIL_TEXT};">
                {body_html}
              </div>
            </div>
          </div>
        </td>
      </tr>
    </table>
    """


def _build_info_card(*, title: str, body_html: str) -> str:
    return f"""
    <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:separate;border-spacing:0;border:1px solid rgba(203,196,211,0.18);border-radius:16px;background:rgba(243,243,245,0.45);overflow:hidden;">
      <tr>
        <td style="padding:22px 24px;">
          <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;">
            <tr>
              <td valign="middle" style="padding-right:14px;width:20px;">
                {_build_icon_image_html(src=INFO_ICON_URL, alt="", size=18, top=0)}
              </td>
              <td valign="middle">
                <div style="font-family:{FONT_HEADLINE};font-size:11px;line-height:1.3;letter-spacing:0.16em;text-transform:uppercase;color:{EMAIL_MUTED};">{escape_html(title)}</div>
              </td>
            </tr>
            <tr>
              <td style="width:20px;padding-right:14px;"></td>
              <td style="padding-top:8px;">
                <div style="font-family:{FONT_BODY};font-size:14px;line-height:1.65;font-weight:300;color:{EMAIL_MUTED_STRONG};">
                  {body_html}
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
    """


def _build_section_row(content: str, *, padding: str = "20px 40px 0", align: str | None = None) -> str:
    align_attr = f' align="{align}"' if align else ""
    return f'<tr><td{align_attr} class="ls-section ls-pad" style="padding:{padding};">{content}</td></tr>'


def _build_email_shell(
    *,
    subject: str,
    title: str,
    recipient_name: str,
    intro: str,
    body_sections_html: str,
    footer_note: str,
) -> str:
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="color-scheme" content="light only" />
    <title>{escape_html(subject)}</title>
    <style>
      @media only screen and (max-width: 640px) {{
        .ls-wrap {{ width: 100% !important; }}
        .ls-pad {{ padding-left: 20px !important; padding-right: 20px !important; }}
        .ls-section {{ padding-left: 20px !important; padding-right: 20px !important; }}
        .ls-header-logo {{ width: 96px !important; max-width: 96px !important; }}
        .ls-header-wordmark {{ font-size: 26px !important; letter-spacing: 0.25em !important; }}
        .ls-body-title {{ font-size: 28px !important; line-height: 1.18 !important; }}
        .ls-button {{ width: 100% !important; }}
        .ls-footer-links a {{ display: inline-block !important; margin: 0 6px 8px !important; }}
      }}
    </style>
  </head>
  <body style="margin:0;padding:0;background:{EMAIL_BACKGROUND};font-family:{FONT_BODY};color:{EMAIL_TEXT};">
    <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;background:{EMAIL_BACKGROUND};">
      <tr>
        <td align="center" style="padding:38px 16px 48px;">
          <table role="presentation" cellpadding="0" cellspacing="0" width="640" class="ls-wrap" style="width:640px;max-width:640px;border-collapse:separate;border-spacing:0;box-shadow:0 16px 44px rgba(21,0,61,0.12);border-radius:12px;overflow:hidden;">
            <tr>
              <td style="background:{EMAIL_PRIMARY};border-radius:12px 12px 0 0;background-image:url('{LEAVESPHERE_EMAIL_HEADER_IMAGE_URL}');background-size:cover;background-position:center center;background-repeat:no-repeat;">
                <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;">
                  <tr>
                    <td align="center" style="padding:28px 40px 24px;">
                      <img
                        src="{LEAVESPHERE_TENANT_LOGO_URL}"
                        alt="LeaveSphere tenant logo"
                        class="ls-header-logo"
                        width="96"
                        style="display:block;width:96px;max-width:96px;height:auto;margin:0 auto 12px;border:0;outline:none;text-decoration:none;"
                      />
                      <div class="ls-header-wordmark" style="font-family:{FONT_HEADLINE};font-size:30px;line-height:1.15;font-weight:700;letter-spacing:0.25em;text-transform:uppercase;color:#ffffff;white-space:nowrap;text-shadow:0 2px 18px rgba(0,0,0,0.16);">
                        LEAVESPHERE
                      </div>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="background:{EMAIL_SURFACE};">
                <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;">
                  <tr>
                    <td class="ls-section ls-pad" style="padding:48px 40px 6px;">
                      <div class="ls-body-title" style="font-family:{FONT_HEADLINE};font-size:30px;line-height:1.16;font-weight:700;letter-spacing:-0.02em;color:{EMAIL_PRIMARY};">
                        {escape_html(title)}
                      </div>
                      <div style="margin-top:18px;font-family:{FONT_BODY};font-size:15px;line-height:1.65;color:{EMAIL_TEXT};">
                        Dear <strong>{escape_html(recipient_name or "Employee")}</strong>,
                      </div>
                      <div style="margin-top:8px;font-family:{FONT_BODY};font-size:15px;line-height:1.7;color:{EMAIL_MUTED};max-width:560px;">
                        {escape_html(intro)}
                      </div>
                    </td>
                  </tr>
                  {body_sections_html}
                </table>
              </td>
            </tr>
            <tr>
              <td style="background:{EMAIL_SURFACE};border-radius:0 0 16px 16px;">
                <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;">
                  <tr>
                    <td style="background:{EMAIL_SURFACE};">
                      <div style="height:20px;background:{EMAIL_SURFACE};"></div>
                    </td>
                  </tr>
                  <tr>
                    <td style="background:{EMAIL_SURFACE};padding:0 0 0;">
                      <div style="background-image:url('{LEAVESPHERE_EMAIL_FOOTER_IMAGE_URL}');background-size:cover;background-position:center bottom;background-repeat:no-repeat;padding:40px 40px 44px;border-radius:0 0 12px 12px;">
                        <div style="text-align:center;font-family:{FONT_HEADLINE};font-size:12px;line-height:1.4;letter-spacing:0.2em;text-transform:uppercase;color:{EMAIL_PRIMARY};font-weight:700;">
                          Leavesphere
                        </div>
                        <div class="ls-footer-links" style="margin-top:6px;text-align:center;font-family:{FONT_BODY};font-size:14px;line-height:1.4;color:{EMAIL_MUTED_STRONG};">
                          <a href="mailto:support@leavesphere.com" style="color:{EMAIL_MUTED_STRONG};text-decoration:none;margin:0 10px;">Support</a>
                          <a href="mailto:privacy@leavesphere.com" style="color:{EMAIL_MUTED_STRONG};text-decoration:none;margin:0 10px;">Privacy</a>
                          <a href="mailto:hr@leavesphere.com" style="color:{EMAIL_MUTED_STRONG};text-decoration:none;margin:0 10px;">Contact HR</a>
                        </div>
                        <div style="margin-top:10px;text-align:center;font-family:{FONT_BODY};font-size:14px;line-height:1.7;color:{EMAIL_MUTED_STRONG};">
                          © {datetime.now().year} LeaveSphere HR. All rights reserved.
                        </div>
                        <div style="margin-top:6px;text-align:center;font-family:{FONT_BODY};font-size:12px;line-height:1.6;color:{EMAIL_MUTED};font-style:italic;">
                          {escape_html(footer_note)}
                        </div>
                      </div>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
"""


def _normalize_pending_request(request: object) -> LeaveSpherePendingRequest | None:
    if not isinstance(request, dict):
        return None

    employee_name = normalize_text(
        request.get("employeeName")
        or request.get("employee_name")
        or request.get("employee")
        or request.get("name")
    )
    pto_type_label = normalize_text(
        request.get("ptoTypeLabel")
        or request.get("pto_type_label")
        or request.get("ptoTypeCode")
        or request.get("pto_type_code")
        or request.get("type")
    )
    start_date = normalize_text(request.get("startDate") or request.get("start_date"))
    end_date = normalize_text(request.get("endDate") or request.get("end_date"))
    hours = request.get("hours")
    request_id = normalize_text(
        request.get("requestId")
        or request.get("transactionId")
        or request.get("id")
    )
    reason = normalize_text(request.get("description") or request.get("reason")) or None
    submitted_at = normalize_text(
        request.get("submittedAt")
        or request.get("submitted_at")
        or request.get("dateCreated")
        or request.get("createdAt")
        or request.get("submittedOn")
    ) or None
    request_url = normalize_text(
        request.get("requestUrl")
        or request.get("request_url")
        or request.get("detailUrl")
        or request.get("detail_url")
        or request.get("href")
        or request.get("url")
    ) or None
    picture_url = normalize_text(
        request.get("pictureUrl")
        or request.get("picture_url")
        or request.get("employeePictureUrl")
        or request.get("employee_picture_url")
        or request.get("avatarUrl")
        or request.get("avatar_url")
    ) or None

    if not employee_name:
        employee_name = "Employee"
    if not pto_type_label:
        pto_type_label = "PTO"
    if not start_date:
        start_date = ""
    if not end_date:
        end_date = ""

    return LeaveSpherePendingRequest(
        employee_name=employee_name,
        pto_type_label=pto_type_label,
        start_date=start_date,
        end_date=end_date,
        hours=hours,
        request_id=request_id or None,
        reason=reason,
        submitted_at=submitted_at,
        request_url=request_url,
        picture_url=picture_url,
    )


def _build_pending_request_cards_html(
    *,
    pending_requests: list[LeaveSpherePendingRequest],
    fallback_url: str | None,
) -> str:
    cards: list[str] = []
    for request in pending_requests:
        request_href = request.request_url or fallback_url or "#"
        cards.append(
            _build_pending_request_card_html(
                request=request,
                href=request_href,
            )
        )
    return "".join(f'<div style="margin-top:14px;">{card}</div>' for card in cards)


def _build_pending_request_card_html(
    *,
    request: LeaveSpherePendingRequest,
    href: str,
) -> str:
    type_tone = _resolve_type_chip_tone(request.pto_type_label)
    status_chip = _build_chip_html("Pending", "amber")
    type_chip = _build_chip_html(request.pto_type_label, type_tone)
    title = escape_html(request.employee_name)
    card_href = href if href and href.strip() else "#"
    submitted_label = _format_date(request.submitted_at)
    date_label = _format_subject_date_range(request.start_date, request.end_date) or f"{_format_date(request.start_date)} - {_format_date(request.end_date)}"
    detail_label = request.reason or ""
    avatar_text = _initials(request.employee_name)
    avatar_markup = (
        f'<img src="{escape_html(request.picture_url)}" alt="{title}" width="28" height="28" '
        'style="display:block;width:100%;height:100%;border:0;outline:none;text-decoration:none;object-fit:cover;" />'
        if normalize_text(request.picture_url)
        else f'<div style="font-family:{FONT_HEADLINE};font-size:10px;line-height:1;font-weight:700;letter-spacing:0.04em;color:#92400e;">{escape_html(avatar_text)}</div>'
    )
    return f"""
    <a href="{escape_html(card_href)}" style="display:block;text-decoration:none;color:inherit;">
      <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:separate;border-spacing:0;border:1px solid #fde68a;border-radius:16px;background:linear-gradient(135deg, #ffffff 0%, #fffbeb 100%);box-shadow:0 8px 24px rgba(15,23,42,0.06);overflow:hidden;">
        <tr>
          <td style="padding:14px 14px 12px;">
            <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;">
              <tr>
                <td valign="top" style="padding-right:10px;width:28px;">
                  <div style="width:28px;height:28px;border-radius:999px;border:1px solid #fcd34d;background:#fef3c7;display:flex;align-items:center;justify-content:center;overflow:hidden;">
                    {avatar_markup}
                  </div>
                </td>
                <td valign="top" style="padding-right:10px;">
                  <div style="font-family:{FONT_BODY};font-size:14px;line-height:1.35;font-weight:600;color:{EMAIL_PRIMARY};">
                    {title}
                  </div>
                  <div style="margin-top:2px;font-family:{FONT_BODY};font-size:11px;line-height:1.35;color:{EMAIL_MUTED};">
                    Submitted {escape_html(submitted_label)}
                  </div>
                </td>
                <td valign="top" align="right" style="white-space:nowrap;">
                  <span style="display:inline-flex;flex-wrap:wrap;justify-content:flex-end;gap:6px;">
                    {type_chip}
                    {status_chip}
                  </span>
                </td>
              </tr>
            </table>
            <div style="margin-top:10px;font-family:{FONT_BODY};font-size:12px;line-height:1.45;color:{EMAIL_MUTED_STRONG};">
              {escape_html(date_label)}
            </div>
            <div style="margin-top:7px;display:flex;align-items:center;gap:6px;font-family:{FONT_BODY};font-size:11.5px;line-height:1.5;color:{EMAIL_MUTED_STRONG};">
              <span style="flex:0 0 auto;">{escape_html(_format_hours(request.hours))}</span>
              {f'<span style="flex:0 0 auto;">-</span><span style="min-width:0;flex:1 1 auto;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{escape_html(detail_label)}</span>' if detail_label else ""}
            </div>
          </td>
        </tr>
      </table>
    </a>
    """


def _resolve_type_chip_tone(type_label: str) -> str:
    normalized = normalize_text(type_label).lower()
    if "sick" in normalized:
        return "amber"
    if "personal" in normalized:
        return "violet"
    return "blue"


def _build_reminder_text_body(
    *,
    manager_name: str,
    pending_requests: list[LeaveSpherePendingRequest],
    reminder_note: str | None,
    footer_note: str,
) -> str:
    pending_count = len(pending_requests)
    total_hours = 0.0
    unique_employees = []
    for request in pending_requests:
        try:
            total_hours += float(str(request.hours).strip())
        except (TypeError, ValueError):
            pass
        if request.employee_name and request.employee_name not in unique_employees:
            unique_employees.append(request.employee_name)

    lines: list[str] = [
        "LeaveSphere",
        "Pending PTO requests",
        "",
        f"Dear {manager_name or 'Manager'},",
        "",
        f"You have {pending_count} PTO request{'s' if pending_count != 1 else ''} waiting for your approval.",
        "",
        "Pending PTO requests:",
        f"- Employees Waiting: {len(unique_employees) or pending_count}",
        f"- Pending Requests: {pending_count}",
        f"- Total Hours: {_format_hours(total_hours)}",
    ]
    if pending_requests:
        lines.extend(["", "Pending Requests:"])
        for index, request in enumerate(pending_requests, start=1):
            lines.extend(
                [
                    f"{index}. {request.employee_name} - {request.pto_type_label}",
                    f"   - Start Date: {_format_date(request.start_date)}",
                    f"   - End Date: {_format_date(request.end_date)}",
                    f"   - Total Hours: {_format_hours(request.hours)}",
                ]
            )
            if request.submitted_at:
                lines.append(f"   - Submitted: {_format_date(request.submitted_at)}")
            if request.request_id:
                lines.append(f"   - Request ID: {request.request_id}")
            if request.reason:
                lines.append(f"   - Note: {request.reason}")
    lines.extend([
        "",
        "Instruction:",
        f"- {reminder_note or 'Click any request card to open the request details and complete quick approval in LeaveSphere.'}",
    ])
    lines.extend(["", footer_note])
    return "\n".join(line for line in lines if line is not None)


def _parse_hours(value: object) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


def _build_button_html(*, label: str, href: str) -> str:
    button_href = href if href and href.strip() else "#"
    return f"""
    <table role="presentation" cellpadding="0" cellspacing="0" style="border-collapse:separate;border-spacing:0;">
      <tr>
        <td align="center" bgcolor="{EMAIL_PRIMARY}" style="border-radius:999px;box-shadow:0 10px 24px rgba(21,0,61,0.22);">
          <a href="{escape_html(button_href)}" style="display:inline-block;padding:15px 30px;border-radius:999px;background:{EMAIL_PRIMARY};color:#ffffff;text-decoration:none;font-family:{FONT_HEADLINE};font-size:15px;line-height:1;font-weight:700;">
            {escape_html(label)} <span style="padding-left:10px;">→</span>
          </a>
        </td>
      </tr>
    </table>
    """


def _build_chip_html(label: str, tone: str) -> str:
    tone_map = {
        "green": ("#e7f7ef", "#0f7a4f"),
        "amber": ("#fff4df", "#9a5b00"),
        "blue": ("#dbeafe", "#1d4ed8"),
        "red": ("#fde8ea", "#b42318"),
        "gray": ("#eef1f4", "#475569"),
        "neutral": ("#ece7f7", "#15003d"),
        "violet": ("#ece7fb", "#2d0a6a"),
        "purple": ("#ece7fb", "#2d0a6a"),
    }
    background, text_color = tone_map.get(tone, tone_map["neutral"])
    return f"""
    <span style="display:inline-block;padding:7px 15px;border-radius:999px;background:{background};color:{text_color};font-family:{FONT_HEADLINE};font-size:10px;line-height:1.05;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;border:1px solid rgba(71,85,105,0.18);position:relative;top:-1px;">
      {escape_html(label)}
    </span>
    """


def _build_bullets_html(items: list[str]) -> str:
    bullets = [normalize_text(item) for item in items if normalize_text(item)]
    if not bullets:
        return "<span>No details provided.</span>"
    return "<ul style=\"margin:0;padding:0 0 0 20px;\">" + "".join(
        f"<li style=\"margin:0 0 8px 0;\">{escape_html(item)}</li>" for item in bullets
    ) + "</ul>"


def _build_text_body(
    *,
    kind: LeaveSphereNotificationKind,
    employee_name: str,
    recipient_name: str,
    pto_type_label: str,
    request_id: str,
    reason: str,
    submitted_at: str,
    manager_name: str,
    admin_note: str,
    note_label: str | None,
    update_summary: list[str],
    quick_approval_url: str | None,
    spec: dict[str, str],
    summary_rows: list[tuple[str, str]],
) -> str:
    lines: list[str] = [
        "LeaveSphere",
        spec["title"],
        "",
        f"Dear {recipient_name or employee_name},",
        "",
        spec["intro"],
        "",
        "Request Details:",
    ]
    for label, value in summary_rows:
        if value:
            lines.append(f"- {label}: {value}")
    if kind == "approval" and quick_approval_url:
        lines.append("")
        lines.append(f"Open Quick Approval: {quick_approval_url}")
    if kind == "confirmation" and reason:
        lines.append("")
        lines.append("Request Note:")
        lines.append(f"- {reason}")
    if kind in {"approved", "rejected", "canceled", "updated"} and admin_note:
        lines.append("")
        lines.append(f"{note_label or 'Admin Note'}:")
        lines.append(f"- {admin_note}")
    if update_summary and kind not in {"approved", "updated"}:
        lines.append("")
        lines.append("What Changed:")
        lines.extend(f"- {item}" for item in update_summary)
    if spec["review_card_title"] and spec["review_card_body"]:
        lines.extend(["", spec["review_card_title"], spec["review_card_body"]])
    lines.extend(["", spec["footer_note"]])
    return "\n".join(line for line in lines if line is not None)


def _resolve_note_block(
    *,
    kind: LeaveSphereNotificationKind,
    employee_name: str,
    recipient_name: str,
    manager_name: str,
    reason: str,
    admin_note: str,
    note_label_override: str | None = None,
) -> tuple[str, str, str, str, str]:
    if kind == "confirmation":
        return "request_note", "REQUEST NOTE", employee_name, _initials(employee_name), ""
    if kind == "approval":
        return "approval_note", "QUICK APPROVAL NOTE", "", "", ""
    if kind in {"approved", "rejected", "canceled", "updated"}:
        body = admin_note or ""
        return "admin_note", note_label_override or "ADMIN NOTE", "LeaveSphere HR", _initials("LeaveSphere HR"), body
    return "admin_note", note_label_override or "ADMIN NOTE", "LeaveSphere HR", _initials("LeaveSphere HR"), admin_note or ""


def _initials(value: str) -> str:
    parts = [part for part in normalize_text(value).split(" ") if part]
    if not parts:
        return "HR"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return (parts[0][:1] + parts[-1][:1]).upper()


def _tone_class(tone: str) -> str:
    if tone in {"green", "amber"}:
        return "green"
    if tone == "blue":
        return "blue"
    if tone == "red":
        return "red"
    if tone == "gray":
        return "gray"
    if tone == "violet":
        return "violet"
    return "neutral"


def _format_hours(value: object) -> str:
    try:
        hours = float(str(value).strip())
    except (TypeError, ValueError):
        return normalize_text(value) or "-"
    return f"{hours:.1f} Hours"


def _format_subject_date(value: str | None) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    try:
        parsed = date.fromisoformat(text[:10])
    except ValueError:
        return text
    return parsed.strftime("%m/%d/%Y")


def _format_subject_date_range(start_date: str | None, end_date: str | None) -> str:
    start_text = _format_subject_date(start_date)
    end_text = _format_subject_date(end_date)
    if not start_text and not end_text:
        return ""
    if start_text and start_text == end_text:
        return start_text
    if start_text and end_text:
        return f"{start_text} - {end_text}"
    return start_text or end_text


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


def _paragraph_html(text: str) -> str:
    raw_text = str(text or "")
    paragraphs = []
    for paragraph in raw_text.split("\n\n"):
        clean_paragraph = " ".join(line.strip() for line in paragraph.splitlines() if line.strip())
        if clean_paragraph:
            paragraphs.append(escape_html(clean_paragraph))
    return "".join(f'<p style="margin:0 0 12px 0;">{paragraph}</p>' for paragraph in paragraphs)
