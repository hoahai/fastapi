import unittest
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from apps.leavesphere.api.v1.helpers import leaveManagement
from apps.leavesphere.api.v1.helpers import myPto
from shared.auth.types import AuthPrincipal, TenantAccessProfile


class LeaveManagementBackendTests(unittest.TestCase):
    @staticmethod
    def _build_request(*, email: str = "hai@theautoadagency.com") -> SimpleNamespace:
        principal = AuthPrincipal(user_id="supabase-user-1", email=email, raw_user={})
        access = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="admin",
            permissions=frozenset({"workspace.super_admin", "leavesphere.admin"}),
        )
        return SimpleNamespace(state=SimpleNamespace(auth_principal=principal, tenant_access=access))

    def test_load_workspace_uses_login_email_and_calculates_balances(self):
        request = self._build_request()
        employees = [
            {
                "id": "emp-1",
                "firstName": "Hai",
                "lastName": "Truong",
                "email": "hai@theautoadagency.com",
                "title": "Director",
                "region": "US",
                "active": 1,
            },
            {
                "id": "emp-2",
                "firstName": "Lee",
                "lastName": "Chen",
                "email": "lee@example.com",
                "title": "Designer",
                "region": "US",
                "active": 1,
            },
        ]
        year_transactions = [
            {
                "id": "txn-load",
                "employeeId": "emp-2",
                "ptoTypeCode": "VAC",
                "ptoActionCode": "LOAD",
                "hours": 40,
                "year": 2026,
                "status": "Approved",
                "dateCreated": "2026-01-01T00:00:00",
                "dateUpdated": "2026-01-01T00:00:00",
                "approverId": "emp-1",
                "description": "Opening load",
            },
            {
                "id": "txn-req-january",
                "employeeId": "emp-2",
                "ptoTypeCode": "VAC",
                "ptoActionCode": "REQUEST",
                "hours": -8,
                "year": 2026,
                "status": "Approved",
                "dateCreated": "2026-01-12T00:00:00",
                "dateUpdated": "2026-01-12T00:00:00",
                "approverId": "emp-1",
                "description": "January trip",
            },
            {
                "id": "txn-req-approved",
                "employeeId": "emp-2",
                "ptoTypeCode": "VAC",
                "ptoActionCode": "REQUEST",
                "hours": -4,
                "year": 2026,
                "status": "Approved",
                "dateCreated": "2026-06-01T00:00:00",
                "dateUpdated": "2026-06-02T00:00:00",
                "approverId": "emp-1",
                "description": "Trip",
                "approverNote": "Approved for travel",
            },
            {
                "id": "txn-req-pending",
                "employeeId": "emp-2",
                "ptoTypeCode": "VAC",
                "ptoActionCode": "REQUEST",
                "hours": -4,
                "year": 2026,
                "status": "Pending",
                "dateCreated": "2026-06-05T00:00:00",
                "dateUpdated": "2026-06-05T00:00:00",
                "description": "Doctor",
            },
        ]

        def _transactions_side_effect(
            *,
            transaction_id=None,
            employee_id=None,
            employee_ids=None,
            pto_type_code=None,
            year=None,
            status=None,
            start_date_from=None,
            start_date_to=None,
            end_date_from=None,
            end_date_to=None,
        ):
            if transaction_id:
                return []
            if status == "Pending":
                return [year_transactions[3]]
            if start_date_from is not None or start_date_to is not None or end_date_from is not None or end_date_to is not None:
                return [year_transactions[2]]
            if year == 2026:
                return year_transactions
            return []

        with patch.object(myPto, "get_employees_by_email", return_value=[employees[0]]), patch.object(
            leaveManagement, "get_employees", return_value=employees
        ), patch.object(
            leaveManagement, "get_employee_managers", return_value=[{"employeeId": "emp-2", "managerId": "emp-1"}]
        ), patch.object(leaveManagement, "get_pto_transactions", side_effect=_transactions_side_effect), patch.object(
            leaveManagement, "get_holidays", return_value=[{"id": "2026-us-new-year", "name": "New Year's Day", "date": "2026-01-01", "region": "US"}]
        ), patch.object(
            myPto, "get_pto_types", return_value=[{"code": "VAC", "name": "Vacation", "listingOrder": 1, "usaDefaultHour": 120, "phlDefaultHour": 0}]
        ), patch.object(
            myPto, "get_pto_actions", return_value=[{"code": "LOAD", "name": "Load"}, {"code": "REQUEST", "name": "Request"}]
        ):
            workspace = leaveManagement.load_leave_management_workspace(
                request=request,
                year=2026,
                include_pending=True,
                history_start_date="2026-05-01",
                history_end_date="2026-12-31",
                overlap_month="2026-06",
            )

        self.assertEqual(workspace["currentUserEmail"], "hai@theautoadagency.com")
        self.assertEqual(workspace["currentUserName"], "Hai Truong")
        self.assertEqual(len(workspace["requests"]), 2)
        self.assertEqual(len(workspace["holidays"]), 1)
        approved_request = next(item for item in workspace["requests"] if item["id"] == "txn-req-approved")
        self.assertEqual(approved_request["description"], "Trip")
        self.assertEqual(approved_request["approverNote"], "Approved for travel")
        self.assertNotIn("txn-req-january", {item["id"] for item in workspace["requests"]})
        employee_balance = next(row for row in workspace["employeeBalances"] if row["employeeId"] == "emp-2")
        vacation_balance = next(row["balances"] for row in [employee_balance] if row["employeeId"] == "emp-2")
        balance = next(item for item in vacation_balance if item["code"] == "VAC")
        self.assertEqual(balance["totalHours"], 40.0)
        self.assertEqual(balance["usedHours"], 8.0)
        self.assertEqual(balance["scheduledHours"], 4.0)
        self.assertEqual(balance["remainingHours"], 28.0)

    def test_load_workspace_normalizes_holiday_dates_before_year_filtering(self):
        request = self._build_request()

        with patch.object(myPto, "get_employees_by_email", return_value=[{
            "id": "emp-1",
            "firstName": "Hai",
            "lastName": "Truong",
            "email": "hai@theautoadagency.com",
            "title": "Director",
            "region": "US",
            "active": 1,
        }]), patch.object(
            leaveManagement, "get_employees", return_value=[{
                "id": "emp-1",
                "firstName": "Hai",
                "lastName": "Truong",
                "email": "hai@theautoadagency.com",
                "title": "Director",
                "region": "US",
                "active": 1,
            }]
        ), patch.object(
            leaveManagement, "get_employee_managers", return_value=[]
        ), patch.object(
            leaveManagement, "get_pto_transactions", return_value=[]
        ), patch.object(
            leaveManagement,
            "get_holidays",
            return_value=[
                {"id": "2026-us-oct", "name": "October Holiday", "date": "10/15/2026", "region": "US"},
                {"id": "2026-us-nov", "name": "November Holiday", "date": datetime(2026, 11, 20, 0, 0, 0), "region": "US"},
                {"id": "2026-us-dec", "name": "December Holiday", "date": "2026-12-31 00:00:00", "region": "US"},
            ],
        ), patch.object(
            myPto, "get_pto_types", return_value=[{"code": "VAC", "name": "Vacation", "listingOrder": 1, "usaDefaultHour": 120, "phlDefaultHour": 0}]
        ), patch.object(
            myPto, "get_pto_actions", return_value=[{"code": "LOAD", "name": "Load"}, {"code": "REQUEST", "name": "Request"}]
        ):
            workspace = leaveManagement.load_leave_management_workspace(request=request, year=2026)

        self.assertEqual([item["date"] for item in workspace["holidays"]], [
            "2026-10-15",
            "2026-11-20",
            "2026-12-31",
        ])
        self.assertEqual(len(workspace["holidays"]), 3)

    def test_load_workspace_keeps_pending_and_overlap_requests(self):
        request = self._build_request()
        employees = [
            {
                "id": "emp-1",
                "firstName": "Hai",
                "lastName": "Truong",
                "email": "hai@theautoadagency.com",
                "title": "Director",
                "region": "US",
                "active": 1,
            },
            {
                "id": "emp-2",
                "firstName": "Lee",
                "lastName": "Chen",
                "email": "lee@example.com",
                "title": "Designer",
                "region": "US",
                "active": 1,
            },
        ]
        year_request = {
            "id": "txn-2026",
            "employeeId": "emp-2",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": -8,
            "year": 2026,
            "status": "Approved",
            "dateCreated": "2026-02-01T00:00:00",
            "dateUpdated": "2026-02-02T00:00:00",
            "startDate": "2026-02-01",
            "endDate": "2026-02-01",
            "description": "Regular 2026 request",
        }
        pending_previous_year = {
            "id": "txn-pending-2025",
            "employeeId": "emp-2",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": -4,
            "year": 2025,
            "status": "Pending",
            "dateCreated": "2025-11-10T00:00:00",
            "dateUpdated": "2025-11-10T00:00:00",
            "description": "Pending from prior year",
        }
        pending_current_year = {
            "id": "txn-pending-2026",
            "employeeId": "emp-2",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": -4,
            "year": 2026,
            "status": "Pending",
            "dateCreated": "2026-05-10T00:00:00",
            "dateUpdated": "2026-05-10T00:00:00",
            "startDate": "2026-05-12",
            "endDate": "2026-05-12",
            "description": "Pending from selected year",
        }
        cross_year_overlap = {
            "id": "txn-cross-year",
            "employeeId": "emp-2",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": -16,
            "year": 2025,
            "status": "Approved",
            "dateCreated": "2025-12-20T00:00:00",
            "dateUpdated": "2025-12-21T00:00:00",
            "startDate": "2025-12-29",
            "endDate": "2026-01-03",
            "description": "Cross-year trip",
        }

        def _transactions_side_effect(*, transaction_id=None, employee_id=None, employee_ids=None, pto_type_code=None, year=None, status=None, start_date_from=None, start_date_to=None, end_date_from=None, end_date_to=None):
            if transaction_id:
                return []
            if status == "Pending":
                return [pending_previous_year, pending_current_year]
            if start_date_to is not None or end_date_from is not None or start_date_from is not None or end_date_to is not None:
                return [cross_year_overlap]
            if year == 2026:
                return [year_request]
            return []

        with patch.object(myPto, "get_employees_by_email", return_value=[employees[0]]), patch.object(
            leaveManagement, "get_employees", return_value=employees
        ), patch.object(
            leaveManagement, "get_employee_managers", return_value=[{"employeeId": "emp-2", "managerId": "emp-1"}]
        ), patch.object(leaveManagement, "get_pto_transactions", side_effect=_transactions_side_effect), patch.object(
            leaveManagement, "get_holidays", return_value=[]
        ), patch.object(
            myPto, "get_pto_types", return_value=[{"code": "VAC", "name": "Vacation", "listingOrder": 1, "usaDefaultHour": 120, "phlDefaultHour": 0}]
        ), patch.object(
            myPto, "get_pto_actions", return_value=[{"code": "LOAD", "name": "Load"}, {"code": "REQUEST", "name": "Request"}]
        ):
            workspace = leaveManagement.load_leave_management_workspace(
                request=request,
                year=2026,
                include_pending=True,
                history_start_date="2025-12-01",
                history_end_date="2026-12-31",
                overlap_month="2026-01",
            )

        request_ids = {item["id"] for item in workspace["requests"]}
        self.assertIn("txn-2026", request_ids)
        self.assertIn("txn-pending-2026", request_ids)
        self.assertNotIn("txn-pending-2025", request_ids)
        self.assertIn("txn-cross-year", request_ids)


if __name__ == "__main__":
    unittest.main()
