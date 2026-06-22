from __future__ import annotations

import unittest
from unittest.mock import patch

from apps.leavesphere.api.v1.helpers.notification_emails import (
    build_leave_sphere_approval_email,
    build_leave_sphere_confirmation_email,
    build_leave_sphere_reminder_email,
    build_leave_sphere_status_email,
    send_leave_sphere_approval_email,
    send_leave_sphere_confirmation_email,
    send_leave_sphere_reminder_email,
    send_leave_sphere_status_preview_email,
    send_leave_sphere_status_email,
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

        self.assertEqual(email.subject, "PTO request submitted for Alex Chen | 06/10/2026 - 06/12/2026")
        self.assertIn("Confirmation of PTO Request", email.html_body)
        self.assertIn("REQUEST CONFIRMED", email.html_body)
        self.assertIn("scfgs5yjhapadizhl1py_ktuep1.png", email.html_body)
        self.assertIn("leavesphere-emailheader_qcgdxy.png", email.html_body)
        self.assertIn("leavesphere-emailfooter_a4f49e.png", email.html_body)
        self.assertIn("request-details_dgnrwe.svg", email.html_body)
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

    def test_confirmation_subject_collapses_same_day_range(self) -> None:
        email = build_leave_sphere_confirmation_email(
            employee_name="Alex Chen",
            pto_type_label="Vacation",
            start_date="2026-06-10",
            end_date="2026-06-10",
            hours=8,
        )

        self.assertEqual(email.subject, "PTO request submitted for Alex Chen | 06/10/2026")

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

        self.assertEqual(email.subject, "New PTO Approval needed for Alex Chen | 06/10/2026 - 06/12/2026")
        self.assertIn("PTO Request Needs Your Approval", email.html_body)
        self.assertIn("AWAITING APPROVAL", email.html_body)
        self.assertIn("background:#dbeafe", email.html_body)
        self.assertIn("color:#1d4ed8", email.html_body)
        self.assertIn("request-details_dgnrwe.svg", email.html_body)
        self.assertIn("A new Paid Time Off (PTO) request has been submitted by Alex Chen", email.html_body)
        self.assertIn("Open Quick Approval", email.html_body)
        self.assertIn("token-123", email.html_body)
        self.assertNotIn("QUICK APPROVAL NOTE", email.html_body)
        self.assertNotIn("Review Process", email.html_body)
        self.assertNotIn("picsum.photos/seed/jordan-lee/96/96", email.html_body)
        self.assertIn("This is an automated approval email", email.html_body)
        self.assertIn("Open Quick Approval: https://workspace.example.com/leavesphere/quick-approval/token-123", email.text_body)

    def test_approval_email_can_omit_quick_approval_button(self) -> None:
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
        )

        self.assertIn("PTO Request Needs Your Approval", email.html_body)
        self.assertNotIn("Open Quick Approval", email.html_body)
        self.assertNotIn("Open Quick Approval:", email.text_body)

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

        self.assertEqual(email.subject, "PTO request approved for Alex Chen | 06/10/2026 - 06/12/2026")
        self.assertIn("PTO Request Approved", email.html_body)
        self.assertIn("APPROVED", email.html_body)
        self.assertIn("ADMIN NOTE", email.html_body)
        self.assertIn("LeaveSphere HR", email.html_body)
        self.assertIn("picsum.photos/seed/leave-approver/96/96", email.html_body)
        self.assertIn("info_qkn9id.svg", email.html_body)
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
        self.assertIn("background:#eef1f4", email.html_body)
        self.assertIn("color:#475569", email.html_body)
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

    def test_reminder_email_lists_pending_requests(self) -> None:
        email = build_leave_sphere_reminder_email(
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

        self.assertEqual(
            email.subject,
            "PTO approval reminder for Jordan Lee | 2 requests pending",
        )
        self.assertIn("Pending PTO requests", email.html_body)
        self.assertNotIn("AWAITING APPROVAL", email.html_body)
        self.assertIn("Alex Chen", email.html_body)
        self.assertIn("Taylor Morgan", email.html_body)
        self.assertIn("picsum.photos/seed/alex-chen/96/96", email.html_body)
        self.assertIn("request-details_dgnrwe.svg", email.html_body)
        self.assertIn("info_qkn9id.svg", email.html_body)
        self.assertIn("https://workspace.example.com/leavesphere/my-pto/requests/pto-123", email.html_body)
        self.assertIn("https://workspace.example.com/leavesphere/my-pto/requests/pto-456", email.html_body)
        self.assertIn("Instruction", email.html_body)
        self.assertIn("click any request card", email.html_body.lower())
        self.assertIn("Instruction:", email.text_body)
        self.assertIn("1. Alex Chen - Vacation", email.text_body)
        self.assertIn("2. Taylor Morgan - Sick Leave", email.text_body)

    @patch("apps.leavesphere.api.v1.helpers.notification_emails.send_smtp_email")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails.get_reminder_cc_emails")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails.get_app_scoped_env")
    def test_send_reminder_email_includes_cc_recipients(self, get_app_scoped_env, get_reminder_cc_emails, send_smtp_email) -> None:
        get_app_scoped_env.return_value = "{'host': 'smtp.example.com', 'port': 587, 'from_email': 'noreply@example.com'}"
        get_reminder_cc_emails.return_value = ["hr@example.com", "ops@example.com"]

        result = send_leave_sphere_reminder_email(
            manager_email="jordan@example.com",
            manager_name="Jordan Lee",
            pending_requests=[
                {
                    "employeeName": "Alex Chen",
                    "ptoTypeLabel": "Vacation",
                    "startDate": "2026-06-10",
                    "endDate": "2026-06-12",
                    "hours": 24,
                    "requestId": "pto-123",
                    "submittedAt": "2026-05-29T10:00:00",
                    "requestUrl": "https://workspace.example.com/leavesphere/my-pto/requests/pto-123",
                }
            ],
        )

        self.assertTrue(result)
        send_smtp_email.assert_called_once()
        call_kwargs = send_smtp_email.call_args.kwargs
        self.assertEqual(call_kwargs["to_addresses"], ["jordan@example.com"])
        self.assertEqual(call_kwargs["cc_addresses"], ["hr@example.com", "ops@example.com"])

    @patch("apps.leavesphere.api.v1.helpers.notification_emails.send_smtp_email")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails.get_app_scoped_env")
    def test_send_reminder_email_can_override_recipient_and_suppress_cc(self, get_app_scoped_env, send_smtp_email) -> None:
        get_app_scoped_env.return_value = "{'host': 'smtp.example.com', 'port': 587, 'from_email': 'noreply@example.com'}"

        result = send_leave_sphere_reminder_email(
            manager_email="jordan@example.com",
            manager_name="Jordan Lee",
            pending_requests=[
                {
                    "employeeName": "Alex Chen",
                    "ptoTypeLabel": "Vacation",
                    "startDate": "2026-06-10",
                    "endDate": "2026-06-12",
                    "hours": 24,
                    "requestId": "pto-123",
                    "submittedAt": "2026-05-29T10:00:00",
                    "requestUrl": "https://workspace.example.com/leavesphere/my-pto/requests/pto-123",
                }
            ],
            recipient_email="hai@theautoadagency.com",
            cc_addresses=[],
        )

        self.assertTrue(result)
        call_kwargs = send_smtp_email.call_args.kwargs
        self.assertEqual(call_kwargs["to_addresses"], ["hai@theautoadagency.com"])
        self.assertEqual(call_kwargs["cc_addresses"], [])

    def test_status_email_uses_note_avatar_icon_when_admin_avatar_is_missing(self) -> None:
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
        )

        self.assertIn("note-avatar_yyhp2j.svg", email.html_body)
        self.assertIn("Approved after coverage was confirmed.", email.html_body)

    @patch("apps.leavesphere.api.v1.helpers.notification_emails.send_smtp_email")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails._get_pto_types")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails._get_employees_by_ids")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails.get_app_scoped_env")
    def test_send_status_email_uses_template_and_smtp(self, get_app_scoped_env, get_employees_by_ids, get_pto_types, send_smtp_email) -> None:
        get_app_scoped_env.return_value = "{'host': 'smtp.example.com', 'port': 587, 'from_email': 'noreply@example.com'}"
        get_employees_by_ids.return_value = [
            {
                "id": "emp-1",
                "firstName": "Alex",
                "lastName": "Chen",
                "email": "alex@example.com",
            }
        ]
        get_pto_types.return_value = [
            {
                "code": "VAC",
                "name": "Vacation",
            }
        ]

        result = send_leave_sphere_status_email(
            transaction={
                "id": "pto-1",
                "employeeId": "emp-1",
                "ptoTypeCode": "VAC",
                "hours": 8,
                "startDate": "2026-06-10",
                "endDate": "2026-06-12",
                "dateCreated": "2026-05-29T10:00:00",
                "description": "Family trip",
            },
            status="approved",
            admin_note="Approved after coverage was confirmed.",
        )

        self.assertTrue(result)
        send_smtp_email.assert_called_once()
        call_kwargs = send_smtp_email.call_args.kwargs
        self.assertEqual(call_kwargs["to_addresses"], ["alex@example.com"])
        self.assertEqual(call_kwargs["subject"], "PTO request approved for Alex Chen | 06/10/2026 - 06/12/2026")
        self.assertIn("PTO Request Approved", call_kwargs["html_body"])
        self.assertIn("Approved after coverage was confirmed.", call_kwargs["html_body"])

    @patch("apps.leavesphere.api.v1.helpers.notification_emails.send_smtp_email")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails._get_pto_types")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails._get_employees_by_ids")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails.get_app_scoped_env")
    def test_send_confirmation_email_uses_employee_recipient(self, get_app_scoped_env, get_employees_by_ids, get_pto_types, send_smtp_email) -> None:
        get_app_scoped_env.return_value = "{'host': 'smtp.example.com', 'port': 587, 'from_email': 'noreply@example.com'}"
        get_employees_by_ids.return_value = [
            {
                "id": "emp-1",
                "firstName": "Alex",
                "lastName": "Chen",
                "email": "alex@example.com",
            }
        ]
        get_pto_types.return_value = [
            {
                "code": "VAC",
                "name": "Vacation",
            }
        ]

        result = send_leave_sphere_confirmation_email(
            transaction={
                "id": "pto-1",
                "employeeId": "emp-1",
                "ptoTypeCode": "VAC",
                "hours": 8,
                "startDate": "2026-06-10",
                "endDate": "2026-06-12",
                "dateCreated": "2026-05-29T10:00:00",
                "description": "Family trip",
            }
        )

        self.assertTrue(result)
        send_smtp_email.assert_called_once()
        call_kwargs = send_smtp_email.call_args.kwargs
        self.assertEqual(call_kwargs["to_addresses"], ["alex@example.com"])
        self.assertEqual(call_kwargs["subject"], "PTO request submitted for Alex Chen | 06/10/2026 - 06/12/2026")
        self.assertIn("Confirmation of PTO Request", call_kwargs["html_body"])

    @patch("apps.leavesphere.api.v1.helpers.notification_emails.send_smtp_email")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails._get_pto_types")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails._get_employees_by_ids")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails._get_employee_managers")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails.get_app_scoped_env")
    def test_send_approval_email_sends_to_all_managers(self, get_app_scoped_env, get_employee_managers, get_employees_by_ids, get_pto_types, send_smtp_email) -> None:
        get_app_scoped_env.return_value = "{'host': 'smtp.example.com', 'port': 587, 'from_email': 'noreply@example.com'}"
        get_employee_managers.return_value = [
            {"employeeId": "emp-2", "managerId": "mgr-1"},
            {"employeeId": "emp-2", "managerId": "mgr-2"},
        ]
        def _employees_side_effect(*, employee_ids):
            if employee_ids == ["emp-2"]:
                return [
                    {"id": "emp-2", "firstName": "Alex", "lastName": "Chen", "email": "alex@example.com"},
                ]
            if employee_ids == ["mgr-1", "mgr-2"]:
                return [
                    {"id": "mgr-1", "firstName": "Jordan", "lastName": "Lee", "email": "jordan@example.com"},
                    {"id": "mgr-2", "firstName": "Casey", "lastName": "Ng", "email": "casey@example.com"},
                ]
            return []

        get_employees_by_ids.side_effect = _employees_side_effect
        get_pto_types.return_value = [
            {
                "code": "VAC",
                "name": "Vacation",
            }
        ]

        result = send_leave_sphere_approval_email(
            transaction={
                "id": "pto-1",
                "employeeId": "emp-2",
                "ptoTypeCode": "VAC",
                "hours": 8,
                "startDate": "2026-06-10",
                "endDate": "2026-06-12",
                "dateCreated": "2026-05-29T10:00:00",
                "description": "Family trip",
            }
        )

        self.assertTrue(result)
        self.assertEqual(send_smtp_email.call_count, 2)
        recipients = [call.kwargs["to_addresses"][0] for call in send_smtp_email.call_args_list]
        self.assertEqual(recipients, ["jordan@example.com", "casey@example.com"])
        for call in send_smtp_email.call_args_list:
            self.assertEqual(call.kwargs["subject"], "New PTO Approval needed for Alex Chen | 06/10/2026 - 06/12/2026")
            self.assertIn("PTO Request Needs Your Approval", call.kwargs["html_body"])
            self.assertNotIn("Open Quick Approval", call.kwargs["html_body"])

    @patch("apps.leavesphere.api.v1.helpers.notification_emails.send_smtp_email")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails.get_app_scoped_env")
    def test_send_status_email_skips_when_smtp_is_missing(self, get_app_scoped_env, send_smtp_email) -> None:
        get_app_scoped_env.return_value = None

        result = send_leave_sphere_status_email(
            transaction={
                "id": "pto-1",
                "employeeId": "emp-1",
                "ptoTypeCode": "VAC",
                "hours": 8,
                "startDate": "2026-06-10",
                "endDate": "2026-06-12",
            },
            status="approved",
        )

        self.assertFalse(result)
        send_smtp_email.assert_not_called()

    @patch("apps.leavesphere.api.v1.helpers.notification_emails.send_smtp_email")
    @patch("apps.leavesphere.api.v1.helpers.notification_emails.get_app_scoped_env")
    def test_send_preview_email_uses_fixed_recipient_by_default(self, get_app_scoped_env, send_smtp_email) -> None:
        get_app_scoped_env.return_value = "{'host': 'smtp.example.com', 'port': 587, 'from_email': 'noreply@example.com'}"

        result = send_leave_sphere_status_preview_email()

        self.assertEqual(result["status"], "sent")
        self.assertEqual(result["recipient_email"], "hai@theautoadagency.com")
        send_smtp_email.assert_called_once()
        call_kwargs = send_smtp_email.call_args.kwargs
        self.assertEqual(call_kwargs["to_addresses"], ["hai@theautoadagency.com"])
        self.assertEqual(call_kwargs["subject"], "PTO request approved for Alex Chen | 06/10/2026 - 06/12/2026")
        self.assertIn("Preview test email sent from the LeaveSphere email preview.", call_kwargs["html_body"])


if __name__ == "__main__":
    unittest.main()
