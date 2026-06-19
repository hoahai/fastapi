from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from pathlib import Path

from apps.leavesphere.api.v1.helpers.notification_emails import (
    build_leave_sphere_approval_email,
    build_leave_sphere_confirmation_email,
    build_leave_sphere_status_email,
)

PREVIEW_ENV = "LEAVESPHERE_EMAIL_PREVIEW"
PREVIEW_DIR_ENV = "LEAVESPHERE_EMAIL_PREVIEW_DIR"


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
    </style>
  </head>
  <body>
    <div class="wrap">
      <h1>LeaveSphere Email Preview</h1>
      <p>Each variant is rendered from the real builder and loaded below in its own iframe.</p>
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

    all_variants = [
        ("confirmation", confirmation),
        ("approval", approval),
        ("approved", approved),
        ("rejected", rejected),
        ("canceled", canceled),
        ("updated", updated),
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
        print(index_path)


if __name__ == "__main__":
    unittest.main()
