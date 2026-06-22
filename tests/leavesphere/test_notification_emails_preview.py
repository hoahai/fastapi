from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from pathlib import Path

from apps.leavesphere.api.v1.helpers.notification_emails import (
    build_leave_sphere_approval_email,
    build_leave_sphere_confirmation_email,
    build_leave_sphere_reminder_email,
    build_leave_sphere_status_email,
)

PREVIEW_ENV = "LEAVESPHERE_EMAIL_PREVIEW"
PREVIEW_DIR_ENV = "LEAVESPHERE_EMAIL_PREVIEW_DIR"
PREVIEW_SEND_URL_ENV = "LEAVESPHERE_EMAIL_PREVIEW_SEND_URL"
DEFAULT_PREVIEW_SEND_URL = "http://localhost:8000/api/leavesphere/v1/admin/pto/email-preview/test-email"


def _resolve_preview_send_url() -> str:
    configured = str(os.getenv(PREVIEW_SEND_URL_ENV, "")).strip()
    if configured:
        return configured
    return DEFAULT_PREVIEW_SEND_URL


def _preview_enabled() -> bool:
    value = str(os.getenv(PREVIEW_ENV, "")).strip().lower()
    return value in {"1", "true", "yes", "on"}


def _resolve_preview_dir() -> Path:
    configured = str(os.getenv(PREVIEW_DIR_ENV, "")).strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(tempfile.gettempdir()) / "leavesphere-email-preview"


def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _build_index_html(items: list[tuple[str, str]]) -> str:
    links = []
    iframes = []
    for label, filename in items:
        safe_label = label.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        safe_file = filename.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        links.append(f'<a href="{safe_file}">{safe_label}</a>')
        iframes.append(
            f"""
            <section style="margin:0 0 28px 0;background:#ffffff;border:1px solid #dbe4ee;border-radius:20px;overflow:hidden;box-shadow:0 10px 26px rgba(15,23,42,0.06);">
              <div style="padding:18px 20px;border-bottom:1px solid #e2e8f0;">
                <div style="font:700 12px/1.4 Arial,Helvetica,sans-serif;letter-spacing:0.14em;text-transform:uppercase;color:#1d4ed8;">{safe_label}</div>
                <div style="margin-top:6px;font:400 13px/1.5 Arial,Helvetica,sans-serif;color:#64748b;">
                  File: <code>{safe_file}</code>
                </div>
              </div>
              <iframe
                src="{safe_file}"
                title="{safe_label}"
                style="width:100%;height:1080px;border:0;display:block;background:#f8fafc;"
              ></iframe>
            </section>
            """
        )

    send_url = _resolve_preview_send_url()
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>LeaveSphere Email Preview</title>
    <style>
      body {{
        margin: 0;
        background: #eef2f7;
        color: #0f172a;
        font-family: Arial, Helvetica, sans-serif;
      }}
      .wrap {{
        max-width: 1180px;
        margin: 0 auto;
        padding: 28px 18px 40px;
      }}
      h1 {{
        margin: 0 0 8px 0;
        font-size: 28px;
        line-height: 1.15;
        letter-spacing: -0.03em;
      }}
      p {{
        margin: 0;
        color: #475569;
        line-height: 1.6;
      }}
      code {{
        font-size: 12px;
      }}
      .links {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin: 16px 0 24px;
      }}
      .links a {{
        display: inline-block;
        padding: 8px 12px;
        border-radius: 999px;
        background: #dbeafe;
        color: #1d4ed8;
        text-decoration: none;
        font-size: 13px;
        font-weight: 700;
      }}
      .preview-send {{
        display: flex;
        flex-wrap: wrap;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        margin: 12px 0 22px;
        padding: 16px 18px;
        border-radius: 18px;
        border: 1px solid #cbd5e1;
        background: linear-gradient(135deg, #eff6ff 0%, #f8fafc 58%, #eef2ff 100%);
        box-shadow: 0 10px 26px rgba(15, 23, 42, 0.06);
      }}
      .preview-send h2 {{
        margin: 0;
        font-size: 16px;
        line-height: 1.4;
      }}
      .preview-send p {{
        margin-top: 4px;
        font-size: 13px;
        color: #475569;
      }}
      .preview-send button {{
        appearance: none;
        border: 0;
        border-radius: 999px;
        background: #1d4ed8;
        color: #ffffff;
        cursor: pointer;
        font-size: 13px;
        font-weight: 700;
        padding: 11px 16px;
        box-shadow: 0 8px 18px rgba(29, 78, 216, 0.22);
      }}
      .preview-send button:hover {{
        background: #1e40af;
      }}
      .preview-send code {{
        background: rgba(255, 255, 255, 0.65);
        border-radius: 999px;
        padding: 2px 6px;
      }}
    </style>
  </head>
  <body>
    <div class="wrap">
      <h1>LeaveSphere Email Preview</h1>
      <p>Each variant is rendered from the real builder and loaded below in its own iframe.</p>
      <form class="preview-send" action="{send_url}" method="post" target="leaveSphereEmailPreviewResult">
        <div>
          <h2>Send a test email</h2>
          <p>
            Sends the approved sample email to <code>hai@theautoadagency.com</code> using the configured LeaveSphere SMTP settings.
          </p>
        </div>
        <input type="hidden" name="toEmail" value="hai@theautoadagency.com" />
        <button type="submit">Send test email</button>
      </form>
      <iframe
        name="leaveSphereEmailPreviewResult"
        title="LeaveSphere email preview send result"
        style="display:none;width:0;height:0;border:0;"
      ></iframe>
      <div class="links">{''.join(links)}</div>
      {''.join(iframes)}
    </div>
  </body>
</html>
"""


def build_preview_bundle(output_dir: Path) -> Path:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    variants: list[tuple[str, str]] = []

    confirmation = build_leave_sphere_confirmation_email(
        employee_name="Alex Chen",
        pto_type_label="Vacation",
        start_date="2026-06-10",
        end_date="2026-06-12",
        hours=24,
        request_id="pto-123",
        reason="Family trip",
        submitted_at="2026-05-29T10:00:00",
    )
    approval = build_leave_sphere_approval_email(
        employee_name="Alex Chen",
        manager_name="Jordan Lee",
        manager_picture_url="https://picsum.photos/seed/jordan-lee/96/96",
        pto_type_label="Vacation",
        start_date="2026-06-10",
        end_date="2026-06-12",
        hours=24,
        request_id="pto-123",
        reason="Family trip",
        submitted_at="2026-05-29T10:00:00",
        quick_approval_url="https://workspace.example.com/leavesphere/quick-approval/token-123",
    )
    approved = build_leave_sphere_status_email(
        status="approved",
        employee_name="Alex Chen",
        pto_type_label="Vacation",
        start_date="2026-06-10",
        end_date="2026-06-12",
        hours=24,
        request_id="pto-123",
        submitted_at="2026-05-29T10:00:00",
        admin_note="Approved after coverage was confirmed.",
        approver_picture_url="https://picsum.photos/seed/leave-approver/96/96",
        update_summary=["Date changed to June 11", "Reason updated"],
    )
    rejected = build_leave_sphere_status_email(
        status="rejected",
        employee_name="Alex Chen",
        pto_type_label="Vacation",
        start_date="2026-06-10",
        end_date="2026-06-12",
        hours=24,
        request_id="pto-123",
        submitted_at="2026-05-29T10:00:00",
        admin_note="Coverage is tight during this period.",
        approver_picture_url="https://picsum.photos/seed/leave-approver/96/96",
    )
    canceled = build_leave_sphere_status_email(
        status="canceled",
        employee_name="Alex Chen",
        pto_type_label="Vacation",
        start_date="2026-06-10",
        end_date="2026-06-12",
        hours=24,
        request_id="pto-123",
        submitted_at="2026-05-29T10:00:00",
        admin_note="Canceled at the employee's request.",
        approver_picture_url="https://picsum.photos/seed/leave-approver/96/96",
    )
    updated = build_leave_sphere_status_email(
        status="updated",
        employee_name="Alex Chen",
        pto_type_label="Vacation",
        start_date="2026-06-11",
        end_date="2026-06-13",
        hours=24,
        request_id="pto-123",
        submitted_at="2026-05-29T10:00:00",
        admin_note="Dates shifted after manager review.",
        approver_picture_url="https://picsum.photos/seed/leave-approver/96/96",
        update_summary=["Start date moved", "End date moved"],
    )
    reminder = build_leave_sphere_reminder_email(
        manager_name="Jordan Lee",
        pending_requests=[
            {
                "employeeName": "Alex Chen",
                "ptoTypeLabel": "Vacation",
                "startDate": "2026-06-10",
                "endDate": "2026-06-12",
                "hours": 24,
                "requestId": "pto-123",
                "description": "Family trip",
                "submittedAt": "2026-05-29T10:00:00",
                "requestUrl": "https://workspace.example.com/leavesphere/my-pto/requests/pto-123",
                "pictureUrl": "https://picsum.photos/seed/alex-chen/96/96",
            },
            {
                "employeeName": "Taylor Morgan",
                "ptoTypeLabel": "Sick Leave",
                "startDate": "2026-06-15",
                "endDate": "2026-06-15",
                "hours": 8,
                "requestId": "pto-456",
                "description": "Medical appointment",
                "submittedAt": "2026-05-30T09:15:00",
                "requestUrl": "https://workspace.example.com/leavesphere/my-pto/requests/pto-456",
            },
        ],
    )

    all_variants = [
        ("confirmation", confirmation),
        ("approval", approval),
        ("approved", approved),
        ("rejected", rejected),
        ("canceled", canceled),
        ("updated", updated),
        ("reminder", reminder),
    ]

    for label, email in all_variants:
        filename = f"{label}.html"
        _write_text(output_dir / filename, email.html_body)
        variants.append((label.capitalize(), filename))

    _write_text(output_dir / "index.html", _build_index_html(variants))
    return output_dir / "index.html"


@unittest.skipUnless(_preview_enabled(), f"Set {PREVIEW_ENV}=1 to generate the HTML preview bundle")
class LeaveSphereNotificationEmailPreviewTests(unittest.TestCase):
    def test_write_preview_bundle(self) -> None:
        preview_dir = _resolve_preview_dir()
        index_path = build_preview_bundle(preview_dir)
        self.assertTrue(index_path.is_file())
        self.assertTrue((preview_dir / "confirmation.html").is_file())
        self.assertTrue((preview_dir / "approval.html").is_file())
        self.assertTrue((preview_dir / "approved.html").is_file())
        self.assertTrue((preview_dir / "rejected.html").is_file())
        self.assertTrue((preview_dir / "canceled.html").is_file())
        self.assertTrue((preview_dir / "updated.html").is_file())
        self.assertTrue((preview_dir / "reminder.html").is_file())
        index_html = index_path.read_text(encoding="utf-8")
        self.assertIn("Send a test email", index_html)
        self.assertIn("hai@theautoadagency.com", index_html)
        print(index_path)


if __name__ == "__main__":
    unittest.main()
