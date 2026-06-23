import unittest
from unittest.mock import patch

from apps.leavesphere.api.v1.helpers.googleCalendarSync import sync_leave_sphere_google_calendar_event


class LeaveSphereGoogleCalendarSyncTests(unittest.TestCase):
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.update_pto_transaction")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.get_pto_types")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.get_employees")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.get_google_calendar_id", return_value="shared-cal")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.get_google_calendar_connection_status")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.get_pto_transactions")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.create_google_calendar_event")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.get_tenant_id", return_value="nucar")
    def test_sync_creates_event_for_approved_transaction(
        self,
        get_tenant_id_mock,
        create_event,
        get_transactions,
        get_connection,
        get_shared_calendar_id,
        get_employees_mock,
        get_pto_types_mock,
        update_pto_transaction_mock,
    ):
        get_transactions.return_value = [
            {
                "id": "pto-1",
                "employeeId": "emp-1",
                "ptoTypeCode": "VAC",
                "status": "Approved",
                "startDate": "2026-06-10",
                "endDate": "2026-06-12",
                "hours": "8.00",
                "description": "Family trip",
                "calendarId": None,
            }
        ]
        get_connection.return_value = {"connected": True}
        get_employees_mock.return_value = [{"firstName": "Alex", "lastName": "Chen", "email": "alex@example.com"}]
        get_pto_types_mock.return_value = [{"name": "Vacation"}]
        create_event.return_value = {"id": "evt-1"}

        result = sync_leave_sphere_google_calendar_event(transaction_id="pto-1")

        self.assertEqual(result["synced"], True)
        self.assertEqual(result["operation"], "created")
        create_event.assert_called_once()
        self.assertEqual(create_event.call_args.kwargs["calendar_id"], "shared-cal")
        self.assertEqual(create_event.call_args.kwargs["event"]["summary"], "LeaveSphere: Alex Chen - Vacation")
        self.assertEqual(create_event.call_args.kwargs["event"]["start"], {"date": "2026-06-10"})
        self.assertEqual(create_event.call_args.kwargs["event"]["end"], {"date": "2026-06-13"})
        update_pto_transaction_mock.assert_called_once_with(
            transaction_id="pto-1",
            updates={"calendarId": "evt-1"},
        )

    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.update_pto_transaction")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.get_pto_types")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.get_employees")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.get_google_calendar_id", return_value="shared-cal")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.get_google_calendar_connection_status")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.get_pto_transactions")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.delete_google_calendar_event")
    @patch("apps.leavesphere.api.v1.helpers.googleCalendarSync.get_tenant_id", return_value="nucar")
    def test_sync_deletes_event_when_transaction_is_not_approved(
        self,
        get_tenant_id_mock,
        delete_event,
        get_transactions,
        get_connection,
        get_shared_calendar_id,
        get_employees_mock,
        get_pto_types_mock,
        update_pto_transaction_mock,
    ):
        get_transactions.return_value = [
            {
                "id": "pto-1",
                "employeeId": "emp-1",
                "ptoTypeCode": "VAC",
                "status": "Canceled",
                "startDate": "2026-06-10",
                "endDate": "2026-06-12",
                "hours": "8.00",
                "description": "Family trip",
                "calendarId": "evt-1",
            }
        ]
        get_connection.return_value = {"connected": True}
        get_employees_mock.return_value = [{"firstName": "Alex", "lastName": "Chen", "email": "alex@example.com"}]
        get_pto_types_mock.return_value = [{"name": "Vacation"}]

        result = sync_leave_sphere_google_calendar_event(transaction_id="pto-1")

        self.assertEqual(result["synced"], True)
        self.assertEqual(result["operation"], "deleted")
        delete_event.assert_called_once()
        update_pto_transaction_mock.assert_called_once_with(
            transaction_id="pto-1",
            updates={"calendarId": None},
        )


if __name__ == "__main__":
    unittest.main()
