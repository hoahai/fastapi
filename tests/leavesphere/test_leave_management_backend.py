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
        transactions = [
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
                "id": "txn-req-approved",
                "employeeId": "emp-2",
                "ptoTypeCode": "VAC",
                "ptoActionCode": "REQUEST",
                "hours": -8,
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

        with patch.object(myPto, "get_employees_by_email", return_value=[employees[0]]), patch.object(
            leaveManagement, "get_employees", return_value=employees
        ), patch.object(
            leaveManagement, "get_employee_managers", return_value=[{"employeeId": "emp-2", "managerId": "emp-1"}]
        ), patch.object(leaveManagement, "get_pto_transactions", return_value=transactions), patch.object(
            leaveManagement, "get_holidays", return_value=[{"id": "2026-us-new-year", "name": "New Year's Day", "date": "2026-01-01", "teamRegion": "US"}]
        ), patch.object(
            myPto, "get_pto_types", return_value=[{"code": "VAC", "name": "Vacation", "listingOrder": 1, "usaDefaultHour": 120, "phlDefaultHour": 0}]
        ), patch.object(
            myPto, "get_pto_actions", return_value=[{"code": "LOAD", "name": "Load"}, {"code": "REQUEST", "name": "Request"}]
        ):
            workspace = leaveManagement.load_leave_management_workspace(request=request, year=2026)

        self.assertEqual(workspace["currentUserEmail"], "hai@theautoadagency.com")
        self.assertEqual(workspace["currentUserName"], "Hai Truong")
        self.assertEqual(len(workspace["requests"]), 2)
        self.assertEqual(len(workspace["holidays"]), 1)
        approved_request = next(item for item in workspace["requests"] if item["id"] == "txn-req-approved")
        self.assertEqual(approved_request["description"], "Trip")
        self.assertEqual(approved_request["approverNote"], "Approved for travel")
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
                {"id": "2026-us-oct", "name": "October Holiday", "date": "10/15/2026", "teamRegion": "US"},
                {"id": "2026-us-nov", "name": "November Holiday", "date": datetime(2026, 11, 20, 0, 0, 0), "teamRegion": "US"},
                {"id": "2026-us-dec", "name": "December Holiday", "date": "2026-12-31 00:00:00", "teamRegion": "US"},
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


if __name__ == "__main__":
    unittest.main()
