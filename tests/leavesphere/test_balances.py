import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from apps.leavesphere.api.v1.helpers import dbQueries
from apps.leavesphere.api.v1.helpers import ptoTransactions
from shared.auth.types import AuthPrincipal, TenantAccessProfile


class LeaveSphereBalancesTests(unittest.TestCase):
    @staticmethod
    def _build_request(*, role: str, permissions: set[str], user_id: str | None) -> SimpleNamespace:
        principal = AuthPrincipal(user_id=user_id or "", email="user@example.com", raw_user={}) if user_id else None
        access = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role=role,
            permissions=frozenset(permissions),
        )
        return SimpleNamespace(state=SimpleNamespace(auth_principal=principal, tenant_access=access))

    def test_balances_query_uses_approved_and_pending_reservation_formula(self):
        with patch.object(dbQueries, "fetch_all", return_value=[]) as mock_fetch:
            dbQueries.get_pto_balances(
                employee_id="emp-1",
                pto_type_code="VAC",
                year=2026,
            )

        query = mock_fetch.call_args[0][0]
        params = mock_fetch.call_args[0][1]
        self.assertIn("CASE WHEN status = 'Approved' AND UPPER(ptoActionCode) LIKE '%REQ%' THEN -ABS(hours)", query)
        self.assertIn("CASE WHEN status = 'Pending' AND UPPER(ptoActionCode) LIKE '%REQ%' THEN ABS(hours) ELSE 0 END", query)
        self.assertIn("approvedBalanceHours", query)
        self.assertIn("pendingRequestHours", query)
        self.assertIn("availableBalanceHours", query)
        self.assertIn("GROUP BY employeeId, ptoTypeCode, year", query)
        self.assertNotIn("status = 'Rejected'", query)
        self.assertNotIn("status = 'Canceled'", query)
        self.assertEqual(params, ("emp-1", "VAC", 2026))

    def test_adjustment_creates_approved_transaction_with_signed_hours(self):
        payload = {
            "employeeId": "emp-1",
            "ptoTypeCode": "vac",
            "ptoActionCode": "load",
            "hours": "-4.5",
            "year": 2026,
            "description": "manual correction",
        }
        with patch.object(ptoTransactions, "get_employees", return_value=[{"id": "emp-1", "active": 1}]), patch.object(
            ptoTransactions, "get_pto_types", return_value=[{"code": "VAC"}]
        ), patch.object(
            ptoTransactions, "get_pto_actions", return_value=[{"code": "LOAD"}]
        ), patch.object(
            ptoTransactions, "insert_pto_transaction", return_value=1
        ) as mock_insert:
            result = ptoTransactions.create_adjustment(payload)

        self.assertEqual(result["status"], "Approved")
        inserted_item = mock_insert.call_args[0][0]
        self.assertEqual(str(inserted_item["hours"]), "-4.50")
        self.assertEqual(inserted_item["status"], "Approved")
        self.assertEqual(inserted_item["ptoTypeCode"], "VAC")
        self.assertEqual(inserted_item["ptoActionCode"], "LOAD")

    def test_adjustment_rejects_zero_hours(self):
        payload = {
            "employeeId": "emp-1",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "LOAD",
            "hours": 0,
            "year": 2026,
        }
        with patch.object(ptoTransactions, "get_employees", return_value=[{"id": "emp-1", "active": 1}]), patch.object(
            ptoTransactions, "get_pto_types", return_value=[{"code": "VAC"}]
        ), patch.object(
            ptoTransactions, "get_pto_actions", return_value=[{"code": "LOAD"}]
        ):
            with self.assertRaises(ValueError) as exc:
                ptoTransactions.create_adjustment(payload)
        self.assertIn("must not be zero", str(exc.exception))

    def test_request_submit_stores_positive_hours_and_pending_status(self):
        payload = {
            "employeeId": "emp-1",
            "ptoTypeCode": "vac",
            "ptoActionCode": "request",
            "hours": "8",
            "year": 2026,
            "startDate": "2026-06-10T00:00:00",
            "endDate": "2026-06-10T23:59:59",
        }
        with patch.object(ptoTransactions, "get_employees", return_value=[{"id": "emp-1", "active": 1}]), patch.object(
            ptoTransactions, "get_pto_types", return_value=[{"code": "VAC"}]
        ), patch.object(
            ptoTransactions, "get_pto_actions", return_value=[{"code": "REQUEST"}]
        ), patch.object(
            ptoTransactions, "create_pto_request_transaction", return_value=1
        ) as mock_create:
            result = ptoTransactions.create_request(payload)

        self.assertEqual(result["status"], "Pending")
        call_kwargs = mock_create.call_args.kwargs
        item = call_kwargs["item"]
        self.assertEqual(str(item["hours"]), "8.00")
        self.assertEqual(item["status"], "Pending")
        self.assertEqual(item["ptoTypeCode"], "VAC")
        self.assertEqual(item["ptoActionCode"], "REQUEST")
        self.assertEqual(str(call_kwargs["requested_hours"]), "8.00")

    def test_request_transaction_insert_uses_matching_placeholders(self):
        class FakeCursor:
            def __init__(self):
                self.executions = []
                self._last_query = ""
                self.rowcount = 0

            def execute(self, query, params=()):
                if query.count("%s") != len(params):
                    raise AssertionError(
                        f"placeholder mismatch: query has {query.count('%s')} placeholders but {len(params)} params"
                    )
                self.executions.append((query, params))
                self._last_query = query
                self.rowcount = 1 if query.lstrip().upper().startswith("INSERT") else 0

            def fetchone(self):
                if "SELECT id FROM" in self._last_query:
                    return ("emp-1",)
                return None

            def fetchall(self):
                if "FROM" in self._last_query and "pto_transactions" in self._last_query:
                    return [("8.00", "Approved")]
                return []

        fake_cursor = FakeCursor()

        def _run_transaction(work):
            return work(fake_cursor)

        item = {
            "id": "pto-1",
            "employeeId": "emp-1",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": Decimal("8.00"),
            "year": 2026,
            "startDate": "2026-06-10",
            "endDate": "2026-06-10",
            "status": "Pending",
            "description": "Family trip",
            "approverNote": None,
            "approverId": None,
            "calendarId": "cal-1",
        }

        with patch.object(dbQueries, "get_db_tables", return_value={"EMPLOYEES": "employees", "PTOTRANSACTIONS": "pto_transactions"}), patch.object(
            dbQueries,
            "run_transaction",
            side_effect=_run_transaction,
        ):
            inserted = dbQueries.create_pto_request_transaction(item=item, requested_hours=Decimal("8.00"))

        self.assertEqual(inserted, 1)
        insert_query, insert_params = fake_cursor.executions[-1]
        self.assertEqual(insert_query.count("%s"), len(insert_params))
        self.assertEqual(len(insert_params), 13)

    def test_request_submit_rejects_zero_or_negative_hours(self):
        for invalid in (0, -1, "-3.5"):
            payload = {
                "employeeId": "emp-1",
                "ptoTypeCode": "VAC",
                "ptoActionCode": "REQUEST",
                "hours": invalid,
                "year": 2026,
            }
            with patch.object(ptoTransactions, "get_employees", return_value=[{"id": "emp-1", "active": 1}]), patch.object(
                ptoTransactions, "get_pto_types", return_value=[{"code": "VAC"}]
            ), patch.object(
                ptoTransactions, "get_pto_actions", return_value=[{"code": "REQUEST"}]
            ):
                with self.assertRaises(ValueError) as exc:
                    ptoTransactions.create_request(payload)
            self.assertIn("greater than zero", str(exc.exception))

    def test_request_submit_blocks_over_request(self):
        payload = {
            "employeeId": "emp-1",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": 9,
            "year": 2026,
        }
        with patch.object(ptoTransactions, "get_employees", return_value=[{"id": "emp-1", "active": 1}]), patch.object(
            ptoTransactions, "get_pto_types", return_value=[{"code": "VAC"}]
        ), patch.object(
            ptoTransactions, "get_pto_actions", return_value=[{"code": "REQUEST"}]
        ), patch.object(
            ptoTransactions,
            "create_pto_request_transaction",
            side_effect=ValueError("Requested hours exceed available balance"),
        ):
            with self.assertRaises(ValueError) as exc:
                ptoTransactions.create_request(payload)
        self.assertIn("exceed available balance", str(exc.exception))

    def test_request_submit_validates_employee_ptotype_ptoaction(self):
        payload = {
            "employeeId": "emp-1",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": 1,
            "year": 2026,
        }
        with patch.object(ptoTransactions, "get_employees", return_value=[]):
            with self.assertRaises(ValueError) as exc:
                ptoTransactions.create_request(payload)
        self.assertIn("employeeId not found", str(exc.exception))

        with patch.object(ptoTransactions, "get_employees", return_value=[{"id": "emp-1", "active": 1}]), patch.object(
            ptoTransactions, "get_pto_types", return_value=[]
        ):
            with self.assertRaises(ValueError) as exc:
                ptoTransactions.create_request(payload)
        self.assertIn("ptoTypeCode not found", str(exc.exception))

        with patch.object(ptoTransactions, "get_employees", return_value=[{"id": "emp-1", "active": 1}]), patch.object(
            ptoTransactions, "get_pto_types", return_value=[{"code": "VAC"}]
        ), patch.object(
            ptoTransactions, "get_pto_actions", return_value=[]
        ):
            with self.assertRaises(ValueError) as exc:
                ptoTransactions.create_request(payload)
        self.assertIn("ptoActionCode not found", str(exc.exception))

    def test_cancel_request_only_allows_pending_transactions(self):
        with patch.object(ptoTransactions, "cancel_pending_pto_transaction", return_value=1):
            result = ptoTransactions.cancel_request(transaction_id="tx-1")
        self.assertEqual(result, {"id": "tx-1", "status": "Canceled", "updated": 1})

        for status in ("Approved", "Rejected", "Canceled"):
            with patch.object(
                ptoTransactions,
                "cancel_pending_pto_transaction",
                side_effect=ValueError("Only Pending PTO transactions can be canceled"),
            ):
                with self.assertRaises(ValueError) as exc:
                    ptoTransactions.cancel_request(transaction_id=f"tx-{status.lower()}")
            self.assertIn("Only Pending", str(exc.exception))

    def test_request_submit_validates_start_end_order(self):
        payload = {
            "employeeId": "emp-1",
            "ptoTypeCode": "VAC",
            "ptoActionCode": "REQUEST",
            "hours": 4,
            "year": 2026,
            "startDate": "2026-06-11T00:00:00",
            "endDate": "2026-06-10T00:00:00",
        }
        with patch.object(ptoTransactions, "get_employees", return_value=[{"id": "emp-1", "active": 1}]), patch.object(
            ptoTransactions, "get_pto_types", return_value=[{"code": "VAC"}]
        ), patch.object(
            ptoTransactions, "get_pto_actions", return_value=[{"code": "REQUEST"}]
        ):
            with self.assertRaises(ValueError) as exc:
                ptoTransactions.create_request(payload)
        self.assertIn("startDate must be on or before endDate", str(exc.exception))

    def test_approve_request_sets_approved_status(self):
        request = self._build_request(
            role="editor",
            permissions={"leavesphere.viewer", "leavesphere.editor"},
            user_id="supabase-manager-1",
        )
        with patch.object(
            ptoTransactions,
            "get_pto_transactions",
            return_value=[{"id": "tx-1", "employeeId": "emp-1", "hours": "8.00", "status": "Pending"}],
        ), patch.object(
            ptoTransactions,
            "get_employees_by_identity_key",
            return_value=[{"id": "mgr-1", "identityKey": "supabase-manager-1", "active": 1}],
        ), patch.object(
            ptoTransactions,
            "get_employee_managers",
            return_value=[{"employeeId": "emp-1", "managerId": "mgr-1"}],
        ), patch.object(
            ptoTransactions,
            "approve_pending_pto_request",
            return_value=1,
        ) as mock_approve:
            result = ptoTransactions.approve_request(request=request, transaction_id="tx-1", approverNote="ok")

        self.assertEqual(result, {"id": "tx-1", "status": "Approved", "updated": 1})
        call = mock_approve.call_args.kwargs
        self.assertEqual(call["approver_id"], "mgr-1")
        self.assertEqual(call["approverNote"], "ok")

    def test_reject_request_sets_rejected_status(self):
        request = self._build_request(
            role="editor",
            permissions={"leavesphere.viewer", "leavesphere.editor"},
            user_id="supabase-manager-1",
        )
        with patch.object(
            ptoTransactions,
            "get_pto_transactions",
            return_value=[{"id": "tx-1", "employeeId": "emp-1", "hours": "8.00", "status": "Pending"}],
        ), patch.object(
            ptoTransactions,
            "get_employees_by_identity_key",
            return_value=[{"id": "mgr-1", "identityKey": "supabase-manager-1", "active": 1}],
        ), patch.object(
            ptoTransactions,
            "get_employee_managers",
            return_value=[{"employeeId": "emp-1", "managerId": "mgr-1"}],
        ), patch.object(
            ptoTransactions,
            "reject_pending_pto_request",
            return_value=1,
        ) as mock_reject:
            result = ptoTransactions.reject_request(request=request, transaction_id="tx-1", approverNote="insufficient coverage")

        self.assertEqual(result, {"id": "tx-1", "status": "Rejected", "updated": 1})
        call = mock_reject.call_args.kwargs
        self.assertEqual(call["approver_id"], "mgr-1")
        self.assertEqual(call["approverNote"], "insufficient coverage")

    def test_non_manager_editor_cannot_approve_or_reject(self):
        request = self._build_request(
            role="editor",
            permissions={"leavesphere.viewer", "leavesphere.editor"},
            user_id="supabase-editor-1",
        )
        with patch.object(
            ptoTransactions,
            "get_pto_transactions",
            return_value=[{"id": "tx-1", "employeeId": "emp-1", "hours": "8.00", "status": "Pending"}],
        ), patch.object(
            ptoTransactions,
            "get_employees_by_identity_key",
            return_value=[{"id": "editor-emp", "identityKey": "supabase-editor-1", "active": 1}],
        ), patch.object(
            ptoTransactions,
            "get_employee_managers",
            return_value=[],
        ):
            with self.assertRaises(ValueError) as exc:
                ptoTransactions.approve_request(request=request, transaction_id="tx-1")
            self.assertIn("direct manager", str(exc.exception))
            with self.assertRaises(ValueError) as exc:
                ptoTransactions.reject_request(request=request, transaction_id="tx-1")
            self.assertIn("direct manager", str(exc.exception))

    def test_admin_can_approve_or_reject_without_employee_mapping(self):
        request = self._build_request(
            role="admin",
            permissions={"leavesphere.viewer", "leavesphere.editor", "leavesphere.admin"},
            user_id="supabase-admin-1",
        )
        with patch.object(
            ptoTransactions,
            "get_pto_transactions",
            return_value=[{"id": "tx-1", "employeeId": "emp-1", "hours": "8.00", "status": "Pending"}],
        ), patch.object(
            ptoTransactions,
            "get_employees_by_identity_key",
            return_value=[],
        ), patch.object(
            ptoTransactions,
            "approve_pending_pto_request",
            return_value=1,
        ) as mock_approve, patch.object(
            ptoTransactions,
            "reject_pending_pto_request",
            return_value=1,
        ) as mock_reject:
            approve_result = ptoTransactions.approve_request(request=request, transaction_id="tx-1")
            reject_result = ptoTransactions.reject_request(request=request, transaction_id="tx-1")

        self.assertEqual(approve_result["status"], "Approved")
        self.assertEqual(reject_result["status"], "Rejected")
        self.assertIsNone(mock_approve.call_args.kwargs["approver_id"])
        self.assertIsNone(mock_reject.call_args.kwargs["approver_id"])

    def test_approve_reject_only_allow_pending_and_debit_transactions(self):
        request = self._build_request(
            role="editor",
            permissions={"leavesphere.viewer", "leavesphere.editor"},
            user_id="supabase-manager-1",
        )
        with patch.object(
            ptoTransactions,
            "get_pto_transactions",
            return_value=[{"id": "tx-1", "employeeId": "emp-1", "hours": "8.00", "status": "Pending"}],
        ), patch.object(
            ptoTransactions,
            "get_employees_by_identity_key",
            return_value=[{"id": "mgr-1", "identityKey": "supabase-manager-1", "active": 1}],
        ), patch.object(
            ptoTransactions,
            "get_employee_managers",
            return_value=[{"employeeId": "emp-1", "managerId": "mgr-1"}],
        ), patch.object(
            ptoTransactions,
            "approve_pending_pto_request",
            side_effect=ValueError("Only Pending PTO transactions can be approved"),
        ), patch.object(
            ptoTransactions,
            "reject_pending_pto_request",
            side_effect=ValueError("Only PTO requests can be rejected"),
        ):
            with self.assertRaises(ValueError) as exc:
                ptoTransactions.approve_request(request=request, transaction_id="tx-1")
            self.assertIn("Only Pending", str(exc.exception))
            with self.assertRaises(ValueError) as exc:
                ptoTransactions.reject_request(request=request, transaction_id="tx-1")
            self.assertIn("PTO requests", str(exc.exception))

    def test_approve_reject_allow_legacy_compat_without_principal_mapping(self):
        request = SimpleNamespace(state=SimpleNamespace(auth_mode="legacy_api_key"))
        with patch.object(
            ptoTransactions,
            "get_pto_transactions",
            return_value=[{"id": "tx-1", "employeeId": "emp-1", "hours": "8.00", "status": "Pending"}],
        ), patch.object(
            ptoTransactions,
            "approve_pending_pto_request",
            return_value=1,
        ) as mock_approve, patch.object(
            ptoTransactions,
            "reject_pending_pto_request",
            return_value=1,
        ) as mock_reject, patch.dict(
            "os.environ",
            {"AUTH_MODE": "compat", "AUTH_ENABLE_LEGACY_API_KEY_FALLBACK": "true"},
            clear=False,
        ):
            approve_result = ptoTransactions.approve_request(request=request, transaction_id="tx-1")
            reject_result = ptoTransactions.reject_request(request=request, transaction_id="tx-1")

        self.assertEqual(approve_result["status"], "Approved")
        self.assertEqual(reject_result["status"], "Rejected")
        self.assertIsNone(mock_approve.call_args.kwargs["approver_id"])
        self.assertIsNone(mock_reject.call_args.kwargs["approver_id"])


if __name__ == "__main__":
    unittest.main()
