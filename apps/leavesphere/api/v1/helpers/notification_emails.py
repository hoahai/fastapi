from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

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
NOTE_AVATAR_ICON_SVG = """
<svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 22 22" fill="none" aria-hidden="true" role="img">
  <circle cx="11" cy="7" r="3" stroke="currentColor" stroke-width="2" />
  <path d="M5 19c0-3.314 2.686-6 6-6s6 2.686 6 6" stroke="currentColor" stroke-width="2" stroke-linecap="round" />
</svg>
""".strip()
REQUEST_DETAILS_ICON_SVG = """
<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 16 16" fill="none" aria-hidden="true" role="img">
  <path d="M3 2.5h10a.5.5 0 0 1 .5.5v10.5a.5.5 0 0 1-.5.5H3a.5.5 0 0 1-.5-.5V3a.5.5 0 0 1 .5-.5Z" stroke="currentColor" stroke-width="2"/>
  <path d="M5 1.5v2M11 1.5v2M4.5 6.5h7M4.5 9.5h7M4.5 12.5h5" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/>
</svg>
""".strip()
INFO_ICON_SVG = """
<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 18 18" fill="none" aria-hidden="true" role="img">
  <circle cx="9" cy="9" r="8" stroke="currentColor" stroke-width="2" />
  <path d="M9 8v5" stroke="currentColor" stroke-width="2" stroke-linecap="round" />
  <circle cx="9" cy="5.5" r="1" fill="currentColor" />
</svg>
""".strip()


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
        note_avatar_url=approver_picture_url,
        update_summary=update_summary,
    )


def _build_variant_spec(
    *,
    kind: LeaveSphereNotificationKind,
    employee_name: str,
    pto_type_label: str,
) -> dict[str, str]:
    if kind == "confirmation":
        return {
            "subject": f"LeaveSphere: PTO request submitted for {employee_name}",
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
            "subject": f"LeaveSphere: approval needed for {employee_name}",
            "title": "PTO Request Needs Your Approval",
            "intro": (
                "A new Paid Time Off (PTO) request has been submitted by "
                f"{employee_name} and requires your approval. Please review the details at your earliest "
                "convenience in the PTO management system."
            ),
            "badge": "AWAITING APPROVAL",
            "badge_tone": "amber",
            "button_label": "Open Quick Approval",
            "footer_note": "This is an automated approval email. Please review the request in LeaveSphere.",
            "review_card_title": "",
            "review_card_body": "",
        }
    if kind == "approved":
        return {
            "subject": f"LeaveSphere: PTO request approved for {employee_name}",
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
            "subject": f"LeaveSphere: PTO request rejected for {employee_name}",
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
            "subject": f"LeaveSphere: PTO request canceled for {employee_name}",
            "title": "PTO Request Canceled",
            "intro": "We regret to inform you that your recent request for Paid Time Off below has been canceled.",
            "badge": "CANCELED",
            "badge_tone": "neutral",
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
        "subject": f"LeaveSphere: PTO request updated for {employee_name}",
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
    if kind == "approval":
        button_html = _build_button_html(
            label=button_label,
            href=quick_approval_url or "#",
        )

    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <meta name="color-scheme" content="light only" />
    <title>{escape_html(spec["subject"])}</title>
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
                        {escape_html(spec["title"])}
                      </div>
                      <div style="margin-top:18px;font-family:{FONT_BODY};font-size:15px;line-height:1.65;color:{EMAIL_TEXT};">
                        Dear <strong>{escape_html(recipient_name or employee_name)}</strong>,
                      </div>
                      <div style="margin-top:8px;font-family:{FONT_BODY};font-size:15px;line-height:1.7;color:{EMAIL_MUTED};max-width:560px;">
                        {escape_html(intro)}
                      </div>
                    </td>
                  </tr>
                  <tr>
                    <td class="ls-section ls-pad" style="padding:24px 40px 0;">
                      {request_details_card}
                    </td>
                  </tr>
                  {f'<tr><td class="ls-section ls-pad" style="padding:20px 40px 0;">{note_card}</td></tr>' if note_card else ''}
                  {f'<tr><td class="ls-section ls-pad" style="padding:20px 40px 0;">{changes_card}</td></tr>' if changes_card else ''}
                  <tr>
                    <td class="ls-section ls-pad" style="padding:20px 40px 0;">
                      {info_card}
                    </td>
                  </tr>
                  <tr>
                    <td align="center" class="ls-section ls-pad" style="padding:36px 40px 44px;">
                      {button_html}
                    </td>
                  </tr>
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
                <span style="display:block;width:15px;height:15px;margin:0 auto;position:relative;top:-2px;color:{EMAIL_SECONDARY};">{REQUEST_DETAILS_ICON_SVG}</span>
              </td>
              <td valign="middle" style="font-family:{FONT_HEADLINE};font-size:18px;line-height:1.16;font-weight:600;letter-spacing:-0.01em;color:{EMAIL_PRIMARY};padding-right:10px;text-align:left;">
                {escape_html(title)}
              </td>
              <td align="right" valign="middle" style="white-space:nowrap;line-height:0;padding-top:1px;">
                {_build_chip_html(badge, badge_tone)}
              </td>
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
        else f'<div style="width:22px;height:22px;color:{icon_color};">{NOTE_AVATAR_ICON_SVG}</div>'
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
                <div style="width:20px;height:20px;color:{EMAIL_MUTED};display:block;line-height:0;">
                  <span style="display:block;width:18px;height:18px;">{INFO_ICON_SVG}</span>
                </div>
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
        "red": ("#fde8ea", "#b42318"),
        "neutral": ("#ece7f7", "#15003d"),
        "violet": ("#ece7fb", "#2d0a6a"),
        "purple": ("#ece7fb", "#2d0a6a"),
    }
    background, text_color = tone_map.get(tone, tone_map["neutral"])
    return f"""
    <span style="display:inline-block;padding:7px 15px;border-radius:999px;background:{background};color:{text_color};font-family:{FONT_HEADLINE};font-size:10px;line-height:1.05;font-weight:700;letter-spacing:0.14em;text-transform:uppercase;border:1px solid rgba(45,10,106,0.18);position:relative;top:-1px;">
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
    if kind == "approval":
        lines.append("")
        lines.append(f"Open Quick Approval: {quick_approval_url or '#'}")
    if kind == "confirmation" and reason:
        lines.append("")
        lines.append("Request Note:")
        lines.append(f"- {reason}")
    if kind in {"approved", "rejected", "canceled", "updated"} and admin_note:
        lines.append("")
        lines.append("Admin Note:")
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
) -> tuple[str, str, str, str, str]:
    if kind == "confirmation":
        return "request_note", "REQUEST NOTE", employee_name, _initials(employee_name), ""
    if kind == "approval":
        return "approval_note", "QUICK APPROVAL NOTE", "", "", ""
    if kind in {"approved", "rejected", "canceled", "updated"}:
        body = admin_note or ""
        return "admin_note", "ADMIN NOTE", "LeaveSphere HR", _initials("LeaveSphere HR"), body
    return "admin_note", "ADMIN NOTE", "LeaveSphere HR", _initials("LeaveSphere HR"), admin_note or ""


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
    if tone == "red":
        return "red"
    if tone == "violet":
        return "violet"
    return "neutral"


def _format_hours(value: object) -> str:
    try:
        hours = float(str(value).strip())
    except (TypeError, ValueError):
        return normalize_text(value) or "-"
    return f"{hours:.1f} Hours"


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
