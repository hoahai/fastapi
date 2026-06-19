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
        self.assertIn("Confirmation of PTO Request", email.html_body)
        self.assertIn("REQUEST CONFIRMED", email.html_body)
        self.assertIn("scfgs5yjhapadizhl1py_ktuep1.png", email.html_body)
        self.assertIn("leavesphere-emailheader_qcgdxy.png", email.html_body)
        self.assertIn("leavesphere-emailfooter_a4f49e.png", email.html_body)
        self.assertIn("Request Details", email.html_body)
        self.assertIn("Family trip", email.html_body)
        self.assertIn("This email is to confirm that your request for paid time off has been received.", email.html_body)
        self.assertIn("Review Process", email.html_body)
        self.assertIn("Your PTO request is currently under review", email.html_body)
        self.assertIn("Thank you for planning ahead and coordinating your responsibilities", email.html_body)
        self.assertNotIn("REQUEST NOTE", email.html_body)
        self.assertNotIn("View Request in LeaveSphere", email.html_body)
        self.assertNotIn("Request ID", email.html_body)
        self.assertNotIn("Submitted", email.html_body)
        self.assertNotIn("pto-123", email.html_body)
        self.assertIn("Type: Vacation", email.text_body)
        self.assertIn("Total Hours: 24.0 Hours", email.text_body)
        self.assertNotIn("Open Quick Approval", email.html_body)

    def test_approval_email_includes_quick_approval_button(self) -> None:
        email = build_leave_sphere_approval_email(
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

        self.assertEqual(email.subject, "LeaveSphere: approval needed for Alex Chen")
        self.assertIn("PTO Request Needs Your Approval", email.html_body)
        self.assertIn("AWAITING APPROVAL", email.html_body)
        self.assertIn("A new Paid Time Off (PTO) request has been submitted by Alex Chen", email.html_body)
        self.assertIn("Open Quick Approval", email.html_body)
        self.assertIn("token-123", email.html_body)
        self.assertNotIn("QUICK APPROVAL NOTE", email.html_body)
        self.assertNotIn("Review Process", email.html_body)
        self.assertNotIn("picsum.photos/seed/jordan-lee/96/96", email.html_body)
        self.assertIn("This is an automated approval email", email.html_body)
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
            approver_picture_url="https://picsum.photos/seed/leave-approver/96/96",
            update_summary=["Date changed to June 11", "Reason updated"],
        )

        self.assertEqual(email.subject, "LeaveSphere: PTO request approved for Alex Chen")
        self.assertIn("PTO Request Approved", email.html_body)
        self.assertIn("APPROVED", email.html_body)
        self.assertIn("ADMIN NOTE", email.html_body)
        self.assertIn("LeaveSphere HR", email.html_body)
        self.assertIn("picsum.photos/seed/leave-approver/96/96", email.html_body)
        self.assertNotIn("View Request in LeaveSphere", email.html_body)
        self.assertIn("Approved after coverage was confirmed.", email.text_body)
        self.assertIn("Please ensure that all your current responsibilities are managed appropriately", email.text_body)

    def test_approved_email_omits_what_changed_section(self) -> None:
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

        self.assertIn("PTO Request Approved", email.html_body)
        self.assertIn("Next Steps", email.html_body)
        self.assertNotIn("What Changed", email.html_body)

    def test_status_emails_omit_cta_for_non_approval_variants(self) -> None:
        for status in ("rejected", "canceled", "updated"):
            email = build_leave_sphere_status_email(
                status=status,
                employee_name="Alex Chen",
                pto_type_label="Vacation",
                start_date="2026-06-10",
                end_date="2026-06-12",
                hours=24,
                request_id="pto-123",
                submitted_at="2026-05-29T10:00:00",
                admin_note="Reviewed by management.",
            )

            self.assertNotIn("View Request in LeaveSphere", email.html_body)
            self.assertIn("Next Steps", email.html_body)

    def test_rejected_email_uses_updated_copy(self) -> None:
        email = build_leave_sphere_status_email(
            status="rejected",
            employee_name="Alex Chen",
            pto_type_label="Vacation",
            start_date="2026-06-10",
            end_date="2026-06-12",
            hours=24,
            request_id="pto-123",
            submitted_at="2026-05-29T10:00:00",
            admin_note="Coverage is tight during this period.",
        )

        self.assertIn(
            "We regret to inform you that your request for Paid Time Off below has not been approved.",
            email.html_body,
        )
        self.assertIn("Next Steps", email.html_body)
        self.assertIn("We understand this decision may be disappointing.", email.html_body)
        self.assertIn("submit a new PTO request for a different period", email.html_body)
        self.assertNotIn("View Request in LeaveSphere", email.html_body)
        self.assertNotIn("Open Quick Approval", email.html_body)

    def test_canceled_email_uses_updated_copy(self) -> None:
        email = build_leave_sphere_status_email(
            status="canceled",
            employee_name="Alex Chen",
            pto_type_label="Vacation",
            start_date="2026-06-10",
            end_date="2026-06-12",
            hours=24,
            request_id="pto-123",
            submitted_at="2026-05-29T10:00:00",
            admin_note="Canceled at the employee's request.",
        )

        self.assertIn(
            "We regret to inform you that your recent request for Paid Time Off below has been canceled.",
            email.html_body,
        )
        self.assertIn("Next Steps", email.html_body)
        self.assertIn("We understand that this may cause inconvenience", email.html_body)
        self.assertIn("Our team is here to assist you with the process", email.html_body)
        self.assertNotIn("View Request in LeaveSphere", email.html_body)
        self.assertNotIn("Open Quick Approval", email.html_body)

    def test_updated_email_uses_updated_copy(self) -> None:
        email = build_leave_sphere_status_email(
            status="updated",
            employee_name="Alex Chen",
            pto_type_label="Vacation",
            start_date="2026-06-11",
            end_date="2026-06-13",
            hours=24,
            request_id="pto-123",
            submitted_at="2026-05-29T10:00:00",
            admin_note="Dates shifted after manager review.",
            update_summary=["Start date moved", "End date moved"],
        )

        self.assertIn(
            "This email is to inform you that there has been an update to your paid time off request originally submitted.",
            email.html_body,
        )
        self.assertIn("Next Steps", email.html_body)
        self.assertIn("Please review these changes at your earliest convenience.", email.html_body)
        self.assertIn("contact your manager", email.html_body)
        self.assertNotIn("What Changed", email.html_body)
        self.assertNotIn("View Request in LeaveSphere", email.html_body)
        self.assertNotIn("Open Quick Approval", email.html_body)

    def test_status_email_omits_note_card_when_admin_note_is_missing(self) -> None:
        email = build_leave_sphere_status_email(
            status="approved",
            employee_name="Alex Chen",
            pto_type_label="Vacation",
            start_date="2026-06-10",
            end_date="2026-06-12",
            hours=24,
            request_id="pto-123",
            submitted_at="2026-05-29T10:00:00",
        )

        self.assertNotIn("ADMIN NOTE", email.html_body)
        self.assertNotIn("No admin note was provided.", email.html_body)
        self.assertNotIn("Admin Note:", email.text_body)


if __name__ == "__main__":
    unittest.main()
