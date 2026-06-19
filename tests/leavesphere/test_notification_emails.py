from __future__ import annotations

import unittest

from apps.leavesphere.api.v1.helpers.notification_emails import (
    build_leave_sphere_approval_email,
    build_leave_sphere_confirmation_email,
    build_leave_sphere_status_email,
)


class LeaveSphereNotificationEmailTests(unittest.TestCase):
    def test_confirmation_email_uses_shared_shell_without_approval_cta(self) -> None:
        email = build_leave_sphere_confirmation_email(
            employee_name="Alex Chen",
            pto_type_label="Vacation",
            start_date="2026-06-10",
            end_date="2026-06-12",
            hours=24,
            request_id="pto-123",
            reason="Family trip",
            submitted_at="2026-05-29T10:00:00",
        )

        self.assertEqual(email.subject, "LeaveSphere: PTO request submitted for Alex Chen")
        self.assertIn("Your PTO request has been submitted", email.html_body)
        self.assertIn("Request submitted", email.html_body)
        self.assertIn("Employee: Alex Chen", email.text_body)
        self.assertIn("PTO type: Vacation", email.text_body)
        self.assertNotIn("Open Quick Approval", email.html_body)

    def test_approval_email_includes_quick_approval_button(self) -> None:
        email = build_leave_sphere_approval_email(
            employee_name="Alex Chen",
            manager_name="Jordan Lee",
            pto_type_label="Vacation",
            start_date="2026-06-10",
            end_date="2026-06-12",
            hours=24,
            request_id="pto-123",
            reason="Family trip",
            submitted_at="2026-05-29T10:00:00",
            quick_approval_url="https://workspace.example.com/leavesphere/quick-approval/token-123",
        )

        self.assertEqual(email.subject, "LeaveSphere: approval needed for Alex Chen")
        self.assertIn("PTO request needs your review", email.html_body)
        self.assertIn("Open Quick Approval", email.html_body)
        self.assertIn("token-123", email.html_body)
        self.assertIn("Approver: Jordan Lee", email.text_body)
        self.assertIn("Open Quick Approval: https://workspace.example.com/leavesphere/quick-approval/token-123", email.text_body)

    def test_status_email_includes_admin_note_and_change_summary(self) -> None:
        email = build_leave_sphere_status_email(
            status="approved",
            employee_name="Alex Chen",
            pto_type_label="Vacation",
            start_date="2026-06-10",
            end_date="2026-06-12",
            hours=24,
            request_id="pto-123",
            submitted_at="2026-05-29T10:00:00",
            admin_note="Approved after coverage was confirmed.",
            update_summary=["Date changed to June 11", "Reason updated"],
        )

        self.assertEqual(email.subject, "LeaveSphere: PTO request approved for Alex Chen")
        self.assertIn("Your PTO request was approved", email.html_body)
        self.assertIn("Admin note", email.html_body)
        self.assertIn("What changed", email.html_body)
        self.assertIn("Approved after coverage was confirmed.", email.text_body)
        self.assertIn("- Date changed to June 11", email.text_body)
        self.assertIn("- Reason updated", email.text_body)


if __name__ == "__main__":
    unittest.main()
