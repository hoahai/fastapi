import unittest
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import patch

from apps.leavesphere.api.v1.helpers import leaveManagement
from apps.leavesphere.api.v1.helpers import myPto
from apps.leavesphere.api.v1.helpers import ptoTransactions
from apps.leavesphere.api.v1.helpers import ptoWorkspaceShared
from apps.leavesphere.api.v1.helpers import config
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
                "hours": 8,
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
                "hours": 4,
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
                "hours": 4,
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
            ptoWorkspaceShared,
            "get_pto_types",
            return_value=[{"code": "VAC", "name": "Vacation", "listingOrder": 1, "usaDefaultHour": 120, "phlDefaultHour": 0}],
        ), patch.object(
            ptoWorkspaceShared,
            "get_pto_actions",
            return_value=[{"code": "LOAD", "name": "Load"}, {"code": "REQUEST", "name": "Request"}],
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
        self.assertEqual(balance["usedHours"], 12.0)
        self.assertEqual(balance["scheduledHours"], 4.0)
        self.assertEqual(balance["remainingHours"], 24.0)

    def test_load_workspace_uses_login_email_mapping_for_current_employee(self):
        request = self._build_request(email="login@example.com")
        employees = [
            {
                "id": "emp-1",
                "firstName": "Hai",
                "lastName": "Truong",
                "email": "employee@example.com",
                "title": "Director",
                "region": "US",
                "active": 1,
            },
        ]

        with patch.object(config, "get_app_scoped_env", return_value="{'login@example.com': 'employee@example.com'}"), patch.object(
            leaveManagement, "get_employees", return_value=employees
        ), patch.object(
            leaveManagement, "get_employee_managers", return_value=[]
        ), patch.object(
            leaveManagement, "get_pto_transactions", return_value=[]
        ), patch.object(
            leaveManagement, "get_holidays", return_value=[]
        ), patch.object(
            leaveManagement,
            "get_last_submission_date",
            return_value=None,
        ), patch.object(
            ptoWorkspaceShared,
            "get_pto_types",
            return_value=[{"code": "VAC", "name": "Vacation", "listingOrder": 1, "usaDefaultHour": 120, "phlDefaultHour": 0}],
        ), patch.object(
            ptoWorkspaceShared,
            "get_pto_actions",
            return_value=[{"code": "LOAD", "name": "Load"}, {"code": "REQUEST", "name": "Request"}],
        ):
            workspace = leaveManagement.load_leave_management_workspace(request=request, year=2026, force_refresh=True)

        self.assertEqual(workspace["currentUserEmail"], "employee@example.com")
        self.assertEqual(workspace["currentUserName"], "Hai Truong")

    def test_my_pto_workspace_exposes_last_submission_date(self):
        request = self._build_request(email="hai@theautoadagency.com")
        employee = {
            "id": "emp-1",
            "firstName": "Hai",
            "lastName": "Truong",
            "email": "hai@theautoadagency.com",
            "title": "Director",
            "region": "US",
            "active": 1,
        }

        with patch.object(myPto, "get_employees_by_email", return_value=[employee]), patch.object(
            leaveManagement, "get_employees", return_value=[employee]
        ), patch.object(
            leaveManagement, "get_employee_managers", return_value=[]
        ), patch.object(
            leaveManagement, "get_pto_transactions", return_value=[]
        ), patch.object(
            leaveManagement, "get_holidays", return_value=[]
        ), patch.object(
            leaveManagement,
            "get_last_submission_date",
            return_value="10-30",
        ), patch.object(
            ptoWorkspaceShared,
            "get_pto_types",
            return_value=[{"code": "VAC", "name": "Vacation", "listingOrder": 1, "usaDefaultHour": 120, "phlDefaultHour": 0}],
        ), patch.object(
            ptoWorkspaceShared,
            "get_pto_actions",
            return_value=[{"code": "LOAD", "name": "Load"}, {"code": "REQUEST", "name": "Request"}],
        ):
            workspace = leaveManagement.load_leave_management_workspace(request=request, year=2026, force_refresh=True)

        self.assertEqual(workspace["lastSubmissionDate"], "10-30")

    def test_create_request_returns_fresh_workspace_payload(self):
        request = self._build_request(email="hai@theautoadagency.com")
        employee = {
            "id": "emp-1",
            "firstName": "Hai",
            "lastName": "Truong",
            "email": "hai@theautoadagency.com",
            "title": "Director",
            "region": "US",
            "active": 1,
        }
        fresh_workspace = {
            "currentUserId": "emp-1",
            "currentUserName": "Hai Truong",
            "currentUserEmail": "hai@theautoadagency.com",
            "managerId": None,
            "currentUserTeamRegion": "US",
            "isManager": False,
            "employees": [employee],
            "ptoTypes": [{"code": "VAC", "type": "vacation", "label": "Vacation", "active": True, "listingOrder": 1}],
            "ptoActions": [{"code": "REQUEST", "name": "Request"}],
            "defaultRequestActionCode": "REQUEST",
            "defaultCancelActionCode": "REQUEST",
            "balances": [
                {
                    "type": "vacation",
                    "code": "VAC",
                    "label": "Vacation",
                    "totalHours": 32.0,
                    "usedHours": 8.0,
                    "scheduledHours": 8.0,
                    "remainingHours": 16.0,
                }
            ],
            "requests": [
                {
                    "id": "pto-1",
                    "employeeId": "emp-1",
                    "managerId": None,
                    "type": "vacation",
                    "ptoTypeCode": "VAC",
                    "startDate": "2026-06-10",
                    "endDate": "2026-06-10",
                    "hours": 8.0,
                    "description": "Family trip",
                    "approverNote": None,
                    "status": "pending",
                    "submittedAt": "2026-06-01",
                    "reviewedAt": None,
                    "reviewerName": None,
                }
            ],
            "holidays": [],
            "directReports": [],
        }

        with patch.object(myPto, "get_employees_by_email", return_value=[employee]), patch.object(
            myPto, "get_employee_managers", return_value=[]
        ), patch.object(
            myPto, "get_pto_transactions", return_value=[]
        ), patch.object(
            myPto, "get_holidays", return_value=[]
        ), patch.object(
            ptoWorkspaceShared,
            "get_pto_types",
            return_value=[{"code": "VAC", "name": "Vacation", "listingOrder": 1, "usaDefaultHour": 120, "phlDefaultHour": 0}],
        ), patch.object(
            ptoWorkspaceShared,
            "get_pto_actions",
            return_value=[{"code": "REQUEST", "name": "Request"}],
        ), patch.object(
            myPto,
            "_apply_my_pto_workspace_mutation_from_cache",
            return_value=None,
        ), patch.object(
            myPto,
            "send_leave_sphere_confirmation_email",
            return_value=True,
        ) as mock_send_confirmation, patch.object(
            myPto,
            "send_leave_sphere_approval_email",
            return_value=True,
        ) as mock_send_approval, patch.object(
            leaveManagement,
            "send_leave_sphere_confirmation_email",
            return_value=True,
        ), patch.object(
            leaveManagement,
            "send_leave_sphere_approval_email",
            return_value=True,
        ), patch.object(
            myPto, "load_my_pto_workspace", return_value=fresh_workspace
        ) as mock_load, patch.object(
            ptoTransactions, "create_request", return_value={"inserted": 1, "status": "Pending"}
        ) as mock_create:
            result = myPto.create_my_pto_request(
                request=request,
                payload={
                    "type": "vacation",
                    "startDate": "2026-06-10",
                    "endDate": "2026-06-10",
                    "hours": 8,
                    "description": "Family trip",
                    "year": 2026,
                    "transactionId": "pto-1",
                },
            )

        self.assertIn("workspacePatch", result)
        self.assertEqual(result["workspacePatch"]["currentUserId"], "emp-1")
        self.assertEqual(result["createdRequestId"], "pto-1")
        self.assertEqual(result["inserted"], 1)
        self.assertEqual(result["status"], "Pending")
        mock_load.assert_called_once_with(request=request, year=2026)
        mock_create.assert_called_once()
        mock_send_confirmation.assert_called_once()
        mock_send_approval.assert_called_once()
        created_item = mock_create.call_args.args[0]
        self.assertEqual(str(created_item["hours"]), "8.00")

    def test_create_leave_management_request_sends_submission_emails(self):
        request = self._build_request(email="hai@theautoadagency.com")
        current_employee = {
            "id": "emp-admin",
            "firstName": "Hai",
            "lastName": "Truong",
            "email": "hai@theautoadagency.com",
            "title": "Director",
            "region": "US",
            "active": 1,
        }
        workspace = {
            "currentUserId": "emp-admin",
            "currentUserName": "Hai Truong",
            "currentUserEmail": "hai@theautoadagency.com",
            "managerId": None,
            "currentUserTeamRegion": "US",
            "isManager": False,
            "employees": [current_employee],
            "ptoTypes": [{"code": "VAC", "type": "vacation", "label": "Vacation", "active": True, "listingOrder": 1}],
            "ptoActions": [{"code": "REQUEST", "name": "Request"}],
            "balances": [],
            "employeeBalances": [],
            "requests": [],
            "holidays": [],
            "directReports": [],
        }

        with patch.object(leaveManagement, "get_employees", return_value=[current_employee]), patch.object(
            leaveManagement, "get_employee_managers", return_value=[{"employeeId": "emp-2", "managerId": "mgr-1"}]
        ), patch.object(
            leaveManagement,
            "load_leave_sphere_pto_workspace_catalogs",
            return_value=SimpleNamespace(
                pto_type_by_code={"VAC": {"code": "VAC", "name": "Vacation"}},
                pto_actions=[{"code": "REQUEST", "name": "Request"}],
                default_request_action_code="REQUEST",
            ),
        ), patch.object(
            leaveManagement, "create_request", return_value={"id": "pto-1", "inserted": 1}
        ) as mock_create, patch.object(
            leaveManagement, "_apply_leave_management_workspace_mutation_from_cache", return_value=workspace
        ), patch.object(
            leaveManagement, "_build_leave_management_workspace_patch", return_value={"requests": [{"id": "pto-1"}]}
        ), patch.object(
            leaveManagement,
            "send_leave_sphere_confirmation_email",
            return_value=True,
        ) as mock_send_confirmation, patch.object(
            leaveManagement,
            "send_leave_sphere_approval_email",
            return_value=True,
        ) as mock_send_approval, patch.object(
            leaveManagement,
            "send_leave_sphere_status_email",
            return_value=True,
        ):
            result = leaveManagement.create_leave_management_request(
                request=request,
                payload={
                    "employeeId": "emp-2",
                    "type": "vacation",
                    "startDate": "2026-06-10",
                    "endDate": "2026-06-10",
                    "hours": 8,
                    "description": "Family trip",
                    "year": 2026,
                },
            )

        self.assertEqual(result["status"], "Pending")
        mock_create.assert_called_once()
        mock_send_confirmation.assert_called_once()
        mock_send_approval.assert_called_once()

    def test_create_leave_management_request_sends_no_email_when_immediate_approval(self):
        request = self._build_request(email="hai@theautoadagency.com")
        current_employee = {
            "id": "emp-admin",
            "firstName": "Hai",
            "lastName": "Truong",
            "email": "hai@theautoadagency.com",
            "title": "Director",
            "region": "US",
            "active": 1,
        }
        workspace = {
            "currentUserId": "emp-admin",
            "currentUserName": "Hai Truong",
            "currentUserEmail": "hai@theautoadagency.com",
            "managerId": None,
            "currentUserTeamRegion": "US",
            "isManager": False,
            "employees": [current_employee],
            "ptoTypes": [{"code": "VAC", "type": "vacation", "label": "Vacation", "active": True, "listingOrder": 1}],
            "ptoActions": [{"code": "REQUEST", "name": "Request"}],
            "balances": [],
            "employeeBalances": [],
            "requests": [],
            "holidays": [],
            "directReports": [],
        }

        with patch.object(leaveManagement, "get_employees", return_value=[current_employee]), patch.object(
            leaveManagement, "get_employee_managers", return_value=[{"employeeId": "emp-2", "managerId": "mgr-1"}]
        ), patch.object(
            leaveManagement,
            "load_leave_sphere_pto_workspace_catalogs",
            return_value=SimpleNamespace(
                pto_type_by_code={"VAC": {"code": "VAC", "name": "Vacation"}},
                pto_actions=[{"code": "REQUEST", "name": "Request"}],
                default_request_action_code="REQUEST",
            ),
        ), patch.object(
            leaveManagement, "_create_immediate_approved_request", return_value=("pto-1", 1)
        ) as mock_create, patch.object(
            leaveManagement, "_apply_leave_management_workspace_mutation_from_cache", return_value=workspace
        ), patch.object(
            leaveManagement, "_build_leave_management_workspace_patch", return_value={"requests": [{"id": "pto-1"}]}
        ), patch.object(
            leaveManagement,
            "send_leave_sphere_confirmation_email",
            return_value=True,
        ) as mock_send_confirmation, patch.object(
            leaveManagement,
            "send_leave_sphere_approval_email",
            return_value=True,
        ) as mock_send_approval, patch.object(
            leaveManagement,
            "send_leave_sphere_status_email",
            return_value=True,
        ) as mock_send_status:
            result = leaveManagement.create_leave_management_request(
                request=request,
                payload={
                    "employeeId": "emp-2",
                    "type": "vacation",
                    "startDate": "2026-06-10",
                    "endDate": "2026-06-10",
                    "hours": 8,
                    "description": "Family trip",
                    "year": 2026,
                    "approveImmediately": True,
                },
            )

        self.assertEqual(result["status"], "Approved")
        mock_create.assert_called_once()
        mock_send_status.assert_not_called()
        mock_send_confirmation.assert_not_called()
        mock_send_approval.assert_not_called()

    def test_create_my_pto_request_blocks_current_year_after_submission_deadline(self):
        request = self._build_request(email="hai@theautoadagency.com")
        employee = {
            "id": "emp-1",
            "firstName": "Hai",
            "lastName": "Truong",
            "email": "hai@theautoadagency.com",
            "title": "Director",
            "region": "US",
            "active": 1,
        }

        with patch.object(myPto, "get_employees_by_email", return_value=[employee]), patch.object(
            myPto, "get_employee_managers", return_value=[]
        ), patch.object(
            myPto, "get_pto_transactions", return_value=[]
        ), patch.object(
            myPto, "get_holidays", return_value=[]
        ), patch.object(
            ptoWorkspaceShared,
            "get_pto_types",
            return_value=[{"code": "VAC", "name": "Vacation", "listingOrder": 1, "usaDefaultHour": 120, "phlDefaultHour": 0}],
        ), patch.object(
            ptoWorkspaceShared,
            "get_pto_actions",
            return_value=[{"code": "REQUEST", "name": "Request"}],
        ), patch.object(
            myPto,
            "get_last_submission_date",
            return_value="10-30",
        ), patch.object(
            myPto,
            "is_submission_cutoff_passed",
            return_value=True,
        ), patch.object(
            ptoTransactions,
            "create_request",
        ) as mock_create:
            with self.assertRaises(ValueError) as exc:
                myPto.create_my_pto_request(
                    request=request,
                    payload={
                        "type": "vacation",
                        "startDate": "2026-11-01",
                        "endDate": "2026-11-01",
                        "hours": 8,
                        "description": "Family trip",
                        "year": 2026,
                    },
                )

        self.assertIn("closed after 10/30", str(exc.exception))
        mock_create.assert_not_called()

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
            ptoWorkspaceShared,
            "get_pto_types",
            return_value=[{"code": "VAC", "name": "Vacation", "listingOrder": 1, "usaDefaultHour": 120, "phlDefaultHour": 0}],
        ), patch.object(
            ptoWorkspaceShared,
            "get_pto_actions",
            return_value=[{"code": "LOAD", "name": "Load"}, {"code": "REQUEST", "name": "Request"}],
        ):
            workspace = leaveManagement.load_leave_management_workspace(request=request, year=2026, force_refresh=True)

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
            "hours": 8,
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
            "hours": 4,
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
            "hours": 4,
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
            "hours": 16,
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
                if start_date_to is not None or end_date_from is not None or start_date_from is not None or end_date_to is not None:
                    return [pending_current_year]
                return [pending_previous_year, pending_current_year]
            if start_date_to is not None or end_date_from is not None or start_date_from is not None or end_date_to is not None:
                return [year_request, cross_year_overlap]
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
            ptoWorkspaceShared,
            "get_pto_types",
            return_value=[{"code": "VAC", "name": "Vacation", "listingOrder": 1, "usaDefaultHour": 120, "phlDefaultHour": 0}],
        ), patch.object(
            ptoWorkspaceShared,
            "get_pto_actions",
            return_value=[{"code": "LOAD", "name": "Load"}, {"code": "REQUEST", "name": "Request"}],
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

    def test_pending_reminder_reminder_groups_requests_by_manager_and_window(self):
        request = SimpleNamespace(base_url="https://workspace.example.com/")
        pending_soon = {
            "id": "txn-pending-soon",
            "employeeId": "emp-1",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": 8,
            "year": 2026,
            "status": "Pending",
            "dateCreated": "2026-06-18T09:00:00",
            "dateUpdated": "2026-06-18T09:00:00",
            "startDate": "2026-06-24",
            "endDate": "2026-06-24",
            "description": "Family trip",
        }
        pending_multi_manager = {
            "id": "txn-pending-managers",
            "employeeId": "emp-2",
            "ptoTypeCode": "SICK",
            "ptoActionCode": "REQUEST",
            "hours": 4,
            "year": 2026,
            "status": "Pending",
            "dateCreated": "2026-06-20T10:00:00",
            "dateUpdated": "2026-06-20T10:00:00",
            "startDate": "2026-06-28",
            "endDate": "2026-06-28",
            "description": "Doctor visit",
        }
        pending_outside_window = {
            "id": "txn-pending-late",
            "employeeId": "emp-1",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": 8,
            "year": 2026,
            "status": "Pending",
            "dateCreated": "2026-06-18T09:00:00",
            "dateUpdated": "2026-06-18T09:00:00",
            "startDate": "2026-07-10",
            "endDate": "2026-07-10",
            "description": "Too far out",
        }
        employees = [
            {
                "id": "emp-1",
                "firstName": "Alex",
                "lastName": "Chen",
                "email": "alex@example.com",
                "pictureUrl": "https://picsum.photos/seed/alex/96/96",
            },
            {
                "id": "emp-2",
                "firstName": "Taylor",
                "lastName": "Morgan",
                "email": "taylor@example.com",
            },
        ]
        managers = [
            {
                "id": "mgr-1",
                "firstName": "Jordan",
                "lastName": "Lee",
                "email": "jordan@example.com",
            },
            {
                "id": "mgr-2",
                "firstName": "Casey",
                "lastName": "Ng",
                "email": "casey@example.com",
            },
        ]

        def _transactions_side_effect(*, transaction_id=None, employee_id=None, employee_ids=None, pto_type_code=None, year=None, status=None, start_date_from=None, start_date_to=None, end_date_from=None, end_date_to=None):
            if transaction_id:
                return []
            if status == "Pending":
                return [pending_soon, pending_multi_manager, pending_outside_window]
            return []

        def _employees_by_ids_side_effect(*, employee_ids):
            requested = list(employee_ids)
            if set(requested) <= {"emp-1", "emp-2"}:
                return [row for row in employees if row["id"] in requested]
            if set(requested) <= {"mgr-1", "mgr-2"}:
                return [row for row in managers if row["id"] in requested]
            return []

        with patch.object(
            leaveManagement,
            "load_leave_sphere_pto_workspace_catalogs",
            return_value=SimpleNamespace(
                pto_type_by_code={
                    "VAC": {"code": "VAC", "name": "Vacation"},
                    "SICK": {"code": "SICK", "name": "Sick Leave"},
                },
                pto_action_by_code={"REQUEST": {"code": "REQUEST", "name": "Request"}},
            ),
        ), patch.object(
            leaveManagement, "get_pto_transactions", side_effect=_transactions_side_effect
        ) as mock_transactions, patch.object(
            leaveManagement, "get_employees_by_ids", side_effect=_employees_by_ids_side_effect
        ) as mock_employees_by_ids, patch.object(
            leaveManagement,
            "get_employee_managers",
            return_value=[
                {"employeeId": "emp-1", "managerId": "mgr-1"},
                {"employeeId": "emp-1", "managerId": "mgr-2"},
                {"employeeId": "emp-2", "managerId": "mgr-2"},
            ],
        ) as mock_employee_managers, patch.object(
            leaveManagement, "send_leave_sphere_reminder_email", return_value=True
        ) as mock_send:
            result = leaveManagement.send_leave_management_pending_approval_reminders(
                request=request,
                year=None,
                coming_days=7,
                today=date(2026, 6, 22),
            )

        self.assertEqual(result["year"], 2026)
        self.assertEqual(result["comingDays"], 7)
        self.assertEqual(result["windowStart"], "2026-01-01")
        self.assertEqual(result["windowEnd"], "2026-06-29")
        self.assertEqual(result["requestsFound"], 2)
        self.assertEqual(result["managersFound"], 2)
        self.assertEqual(result["emailsSent"], 2)
        self.assertEqual(result["emailsFailed"], 0)
        self.assertEqual(result["skippedRequests"], 0)
        self.assertEqual(result["skippedManagerContacts"], 0)

        mock_transactions.assert_called_once_with(
            year=2026,
            status="Pending",
            start_date_from="2026-01-01",
            start_date_to="2026-06-29",
        )
        mock_employee_managers.assert_called_once()
        mock_employees_by_ids.assert_any_call(employee_ids=["emp-1", "emp-2"])
        mock_employees_by_ids.assert_any_call(employee_ids=["mgr-1", "mgr-2"])
        self.assertEqual(mock_send.call_count, 2)

        first_call = mock_send.call_args_list[0].kwargs
        second_call = mock_send.call_args_list[1].kwargs
        self.assertEqual(first_call["manager_email"], "jordan@example.com")
        self.assertEqual(first_call["manager_name"], "Jordan Lee")
        self.assertEqual(len(first_call["pending_requests"]), 1)
        self.assertEqual(first_call["pending_requests"][0]["employeeName"], "Alex Chen")
        self.assertEqual(first_call["pending_requests"][0]["requestUrl"], "https://workspace.example.com/leavesphere/leave-management?year=2026")
        self.assertEqual(second_call["manager_email"], "casey@example.com")
        self.assertEqual(second_call["manager_name"], "Casey Ng")
        self.assertEqual(len(second_call["pending_requests"]), 2)
        self.assertEqual(
            [item["employeeName"] for item in second_call["pending_requests"]],
            ["Alex Chen", "Taylor Morgan"],
        )

    def test_pending_reminder_test_email_override_sends_only_to_override(self):
        request = SimpleNamespace(base_url="https://workspace.example.com/")
        pending_request = {
            "id": "txn-pending-soon",
            "employeeId": "emp-1",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": 8,
            "year": 2026,
            "status": "Pending",
            "dateCreated": "2026-06-18T09:00:00",
            "dateUpdated": "2026-06-18T09:00:00",
            "startDate": "2026-06-24",
            "endDate": "2026-06-24",
            "description": "Family trip",
        }
        employees = [
            {
                "id": "emp-1",
                "firstName": "Alex",
                "lastName": "Chen",
                "email": "alex@example.com",
            },
            {
                "id": "mgr-1",
                "firstName": "Jordan",
                "lastName": "Lee",
                "email": "jordan@example.com",
            },
        ]

        with patch.object(
            leaveManagement,
            "load_leave_sphere_pto_workspace_catalogs",
            return_value=SimpleNamespace(
                pto_type_by_code={"VAC": {"code": "VAC", "name": "Vacation"}},
                pto_action_by_code={"REQUEST": {"code": "REQUEST", "name": "Request"}},
            ),
        ), patch.object(
            leaveManagement,
            "get_pto_transactions",
            return_value=[pending_request],
        ), patch.object(
            leaveManagement,
            "get_employees_by_ids",
            side_effect=lambda *, employee_ids: [row for row in employees if row["id"] in employee_ids],
        ), patch.object(
            leaveManagement,
            "get_employee_managers",
            return_value=[{"employeeId": "emp-1", "managerId": "mgr-1"}],
        ), patch.object(
            leaveManagement,
            "send_leave_sphere_reminder_email",
            return_value=True,
        ) as mock_send:
            result = leaveManagement.send_leave_management_pending_approval_reminders(
                request=request,
                year=2026,
                coming_days=7,
                test_email="hai@theautoadagency.com",
                today=date(2026, 6, 22),
            )

        self.assertEqual(result["emailsSent"], 1)
        call_kwargs = mock_send.call_args.kwargs
        self.assertEqual(call_kwargs["recipient_email"], "hai@theautoadagency.com")
        self.assertEqual(call_kwargs["cc_addresses"], [])

    def test_review_workspace_supports_cancel_and_revert_actions(self):
        request = self._build_request()
        transaction = {
            "id": "txn-1",
            "employeeId": "emp-2",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": 8,
            "year": 2026,
            "status": "Approved",
            "startDate": "2026-07-10",
            "endDate": "2026-07-10",
            "dateCreated": "2026-07-01T00:00:00",
            "dateUpdated": "2026-07-02T00:00:00",
            "description": "Trip",
        }

        with patch.object(leaveManagement, "get_pto_transactions", return_value=[transaction]), patch.object(
            leaveManagement, "_build_workspace", return_value={"workspace": True}
        ), patch.object(
            leaveManagement, "update_pto_transaction", return_value=1
        ) as mock_update:
            cancel_result = leaveManagement.review_leave_management_request(
                request=request,
                payload={"requestId": "txn-1", "action": "cancel", "approverNote": "Cancelled"},
            )

        self.assertEqual(cancel_result["status"], "Canceled")
        self.assertEqual(cancel_result["updated"], 1)
        mock_update.assert_called_once()

        reverted_transaction = {
            **transaction,
            "status": "Canceled",
        }
        with patch.object(leaveManagement, "get_pto_transactions", return_value=[reverted_transaction]), patch.object(
            leaveManagement, "_build_workspace", return_value={"workspace": True}
        ), patch.object(
            leaveManagement, "update_pto_transaction", return_value=1
        ) as mock_update:
            revert_result = leaveManagement.review_leave_management_request(
                request=request,
                payload={"requestId": "txn-1", "action": "revert", "approverNote": "Revert"},
            )

        self.assertEqual(revert_result["status"], "Pending")
        self.assertEqual(revert_result["updated"], 1)
        mock_update.assert_called_once()

    def test_adjust_workspace_stamps_current_admin_as_approver(self):
        request = self._build_request()
        workspace = {
            "currentUserId": "emp-admin",
            "currentUserName": "Admin User",
            "currentUserEmail": "admin@example.com",
            "managerId": None,
            "currentUserTeamRegion": "US",
            "isManager": False,
            "balanceTransactions": [],
            "employees": [],
            "employeeBalances": [],
            "requests": [],
            "holidays": [],
            "ptoTypes": [],
            "ptoActions": [],
        }

        def _patch_workspace(*, workspace, balance_transaction_ids=None, employee_ids=None, **kwargs):
            rows = [row for row in workspace.get("balanceTransactions", []) if isinstance(row, dict)]
            return {"balanceTransactions": [dict(row) for row in rows]}

        def _mutate_workspace(request, year, mutate_workspace):
            mutate_workspace(workspace)
            return workspace

        with patch.object(
            leaveManagement,
            "_resolve_current_employee_record",
            return_value={"id": "emp-admin", "firstName": "Admin", "lastName": "User", "email": "admin@example.com"},
        ), patch.object(
            leaveManagement,
            "load_leave_sphere_pto_workspace_catalogs",
            return_value=SimpleNamespace(
                pto_type_by_code={"VAC": {"code": "VAC"}},
                pto_actions=[{"code": "LOAD"}],
            ),
        ), patch.object(
            leaveManagement,
            "_resolve_pto_type_code",
            return_value="VAC",
        ), patch.object(
            leaveManagement,
            "resolve_action_code",
            return_value="LOAD",
        ), patch.object(
            leaveManagement,
            "create_adjustment",
            return_value={"id": "tx-1", "inserted": 1, "status": "Approved"},
        ) as mock_create, patch.object(
            leaveManagement,
            "_apply_leave_management_workspace_mutation_from_cache",
            side_effect=_mutate_workspace,
        ), patch.object(
            leaveManagement,
            "_build_leave_management_workspace_patch",
            side_effect=_patch_workspace,
        ):
            result = leaveManagement.adjust_leave_management_balance(
                request=request,
                payload={
                    "employeeId": "emp-2",
                    "ptoTypeCode": "VAC",
                    "ptoActionCode": "load_grant",
                    "hours": 8,
                    "year": 2026,
                    "status": "Approved",
                    "description": "Opening balance load",
                    "approverNote": "Opening balance",
                },
            )

        self.assertEqual(result["status"], "Approved")
        self.assertEqual(result["updated"], 1)
        inserted_payload = mock_create.call_args.args[0]
        self.assertEqual(inserted_payload["approverId"], "emp-admin")
        self.assertEqual(inserted_payload["description"], "Opening balance load")
        self.assertEqual(result["workspacePatch"]["balanceTransactions"][0]["approverId"], "emp-admin")
        self.assertEqual(result["workspacePatch"]["balanceTransactions"][0]["description"], "Opening balance load")


if __name__ == "__main__":
    unittest.main()
