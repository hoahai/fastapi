import unittest
from unittest.mock import patch

from apps.fundsphere.api.v1.helpers import directDbQueries as dbq


class FundSphereDirectDbQueryTests(unittest.TestCase):
    def test_create_account_invalidates_account_and_budget_caches(self):
        tables = {
            "ACCOUNTS": "AccountsTbl",
            "ACCOUNTREPS": "AccountRepsTbl",
            "BUDGETCHANGEHISTORIES": "BudgetChangeHistoriesTbl",
            "BUDGETS": "BudgetsTbl",
            "DEPARTMENTS": "DepartmentsTbl",
            "SERVICES": "ServicesTbl",
        }
        invalidation_calls: list[tuple[str, str]] = []

        def _capture_invalidation(*, bucket: str, cache_key_prefix: str):
            invalidation_calls.append((bucket, cache_key_prefix))
            return 1

        with patch.object(dbq, "get_direct_db_tables", return_value=tables), patch.object(
            dbq,
            "_execute_write",
            return_value=1,
        ), patch.object(
            dbq,
            "delete_tenant_shared_cache_values_by_prefix",
            side_effect=_capture_invalidation,
        ):
            result = dbq.create_account({"code": "acme01", "name": "Acme Media"})

        self.assertEqual(result, {"inserted": 1, "code": "ACME01"})
        self.assertIn(("db_reads", "accounts::accountstbl"), invalidation_calls)
        self.assertIn(("db_reads", "budget_data::budgetstbl::"), invalidation_calls)

    def test_create_budget_writes_history_and_returns_id(self):
        tables = {
            "ACCOUNTS": "AccountsTbl",
            "ACCOUNTREPS": "AccountRepsTbl",
            "BUDGETCHANGEHISTORIES": "BudgetChangeHistoriesTbl",
            "BUDGETS": "BudgetsTbl",
            "DEPARTMENTS": "DepartmentsTbl",
            "SERVICES": "ServicesTbl",
        }
        executed: list[tuple[str, tuple[object, ...]]] = []
        invalidation_calls: list[tuple[str, str]] = []

        class _Cursor:
            rowcount = 1

            def execute(self, query, params):
                executed.append((query, params))

        def _run_transaction(callback):
            return callback(_Cursor())

        def _capture_invalidation(*, bucket: str, cache_key_prefix: str):
            invalidation_calls.append((bucket, cache_key_prefix))
            return 1

        with patch.object(dbq, "get_direct_db_tables", return_value=tables), patch.object(
            dbq,
            "_ensure_account_exists",
            return_value=None,
        ), patch.object(
            dbq,
            "_ensure_service_exists",
            return_value=None,
        ), patch.object(
            dbq,
            "run_transaction",
            side_effect=_run_transaction,
        ), patch.object(
            dbq,
            "delete_tenant_shared_cache_values_by_prefix",
            side_effect=_capture_invalidation,
        ):
            result = dbq.create_budget(
                payload={
                    "accountCode": "ACME01",
                    "month": 6,
                    "year": 2026,
                    "serviceId": "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7",
                    "grossAmount": 50000,
                    "commission": 5,
                    "netAdjustment": 0,
                }
            )

        self.assertEqual(result["inserted"], 1)
        self.assertTrue(result["id"])
        self.assertGreaterEqual(len(executed), 2)
        self.assertTrue(any("BudgetChangeHistoriesTbl" in query for query, _ in executed))
        self.assertIn(("db_reads", "budget_data::budgetstbl::"), invalidation_calls)

    def test_create_account_rep_skips_employee_validation_without_employee_table(self):
        tables = {
            "ACCOUNTS": "AccountsTbl",
            "ACCOUNTREPS": "AccountRepsTbl",
            "BUDGETCHANGEHISTORIES": "BudgetChangeHistoriesTbl",
            "BUDGETS": "BudgetsTbl",
            "DEPARTMENTS": "DepartmentsTbl",
            "SERVICES": "ServicesTbl",
        }
        with patch.object(dbq, "get_direct_db_tables", return_value=tables), patch.object(
            dbq,
            "_fetch_one",
            return_value={"code": "ACME01"},
        ), patch.object(
            dbq,
            "_execute_write",
            return_value=1,
        ):
            result = dbq.create_account_rep(
                {"accountCode": "acme01", "employeeId": "13f6b22f-0a86-43b8-946d-cbba67642e8b"}
            )

        self.assertEqual(result["inserted"], 1)
        self.assertTrue(result["id"])

    def test_list_accounts_uses_backend_filters_for_search(self):
        tables = {
            "ACCOUNTS": "AccountsTbl",
            "ACCOUNTREPS": "AccountRepsTbl",
            "BUDGETCHANGEHISTORIES": "BudgetChangeHistoriesTbl",
            "BUDGETS": "BudgetsTbl",
            "DEPARTMENTS": "DepartmentsTbl",
            "EMPLOYEES": "EmployeesTbl",
            "SERVICES": "ServicesTbl",
        }
        captured: list[tuple[str, tuple[object, ...]]] = []

        def _capture_fetch_all(query, params=()):
            captured.append((query, params))
            return [
                {
                    "code": "ACME01",
                    "name": "Acme Media",
                    "logoUrl": None,
                    "conseroId": None,
                    "conseroName": None,
                    "strataName": None,
                    "active": 1,
                    "endDate": None,
                }
            ]

        with patch.object(dbq, "get_direct_db_tables", return_value=tables), patch.object(
            dbq,
            "fetch_all",
            side_effect=_capture_fetch_all,
        ):
            result = dbq.list_accounts(
                code="ACME",
                name="Media",
                ae_name="Alex Chen",
                status="inactive",
            )

        self.assertEqual(result[0]["code"], "ACME01")
        self.assertEqual(len(captured), 1)
        query, params = captured[0]
        self.assertIn("LOWER(a.code) LIKE LOWER(%s)", query)
        self.assertIn("LOWER(a.name) LIKE LOWER(%s)", query)
        self.assertIn("EXISTS (", query)
        self.assertIn("INNER JOIN `EmployeesTbl` e ON e.id = ar.employeeId", query)
        self.assertIn("a.active = 0", query)
        self.assertEqual(params, ("%ACME%", "%Media%", "%Alex Chen%"))

    def test_list_accounts_summary_uses_trimmed_projection(self):
        tables = {
            "ACCOUNTS": "AccountsTbl",
            "ACCOUNTREPS": "AccountRepsTbl",
            "BUDGETCHANGEHISTORIES": "BudgetChangeHistoriesTbl",
            "BUDGETS": "BudgetsTbl",
            "DEPARTMENTS": "DepartmentsTbl",
            "EMPLOYEES": "EmployeesTbl",
            "SERVICES": "ServicesTbl",
        }
        captured: list[tuple[str, tuple[object, ...]]] = []

        def _capture_fetch_all(query, params=()):
            captured.append((query, params))
            return [
                {
                    "code": "ACME01",
                    "name": "Acme Media",
                    "logoUrl": None,
                    "active": 1,
                }
            ]

        with patch.object(dbq, "get_direct_db_tables", return_value=tables), patch.object(
            dbq,
            "fetch_all",
            side_effect=_capture_fetch_all,
        ):
            result = dbq.list_accounts(status="active", summary=True)

        self.assertEqual(result[0]["code"], "ACME01")
        query, _ = captured[0]
        self.assertIn("a.logoUrl, a.active", query)
        self.assertNotIn("conseroId", query)
        self.assertNotIn("endDate", query)

    def test_list_services_uses_backend_filters_for_search(self):
        tables = {
            "ACCOUNTS": "AccountsTbl",
            "ACCOUNTREPS": "AccountRepsTbl",
            "BUDGETCHANGEHISTORIES": "BudgetChangeHistoriesTbl",
            "BUDGETS": "BudgetsTbl",
            "DEPARTMENTS": "DepartmentsTbl",
            "EMPLOYEES": "EmployeesTbl",
            "SERVICES": "ServicesTbl",
        }
        captured: list[tuple[str, tuple[object, ...]]] = []

        def _capture_fetch_all(query, params=()):
            captured.append((query, params))
            return [
                {
                    "id": "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7",
                    "name": "Paid Media",
                    "conseroId": None,
                    "departmentCode": "MKT",
                    "departmentName": "Marketing",
                    "departmentListingOrder": 10,
                    "description": None,
                    "commission": 0,
                    "netAdjustment": 0,
                    "active": 1,
                }
            ]

        with patch.object(dbq, "get_direct_db_tables", return_value=tables), patch.object(
            dbq,
            "fetch_all",
            side_effect=_capture_fetch_all,
        ):
            result = dbq.list_services(
                name="Paid",
                department_code="mkt",
                status="inactive",
            )

        self.assertEqual(result[0]["id"], "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7")
        self.assertEqual(len(captured), 1)
        query, params = captured[0]
        self.assertIn("LOWER(s.name) LIKE LOWER(%s)", query)
        self.assertIn("s.departmentCode = %s", query)
        self.assertIn("s.active = 0", query)
        self.assertEqual(params, ("%Paid%", "MKT"))

    def test_list_services_summary_uses_trimmed_projection(self):
        tables = {
            "ACCOUNTS": "AccountsTbl",
            "ACCOUNTREPS": "AccountRepsTbl",
            "BUDGETCHANGEHISTORIES": "BudgetChangeHistoriesTbl",
            "BUDGETS": "BudgetsTbl",
            "DEPARTMENTS": "DepartmentsTbl",
            "EMPLOYEES": "EmployeesTbl",
            "SERVICES": "ServicesTbl",
        }
        captured: list[tuple[str, tuple[object, ...]]] = []

        def _capture_fetch_all(query, params=()):
            captured.append((query, params))
            return [
                {
                    "id": "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7",
                    "name": "Paid Media",
                    "departmentCode": "MKT",
                    "departmentName": "Marketing",
                    "departmentListingOrder": 10,
                    "description": None,
                    "active": 1,
                }
            ]

        with patch.object(dbq, "get_direct_db_tables", return_value=tables), patch.object(
            dbq,
            "fetch_all",
            side_effect=_capture_fetch_all,
        ):
            result = dbq.list_services(name="Paid", department_code="mkt", summary=True)

        self.assertEqual(result[0]["id"], "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7")
        query, _ = captured[0]
        self.assertIn("s.departmentCode", query)
        self.assertNotIn("commission", query)
        self.assertNotIn("netAdjustment", query)

    def test_list_accounts_requires_employee_table_for_ae_name_search(self):
        tables = {
            "ACCOUNTS": "AccountsTbl",
            "ACCOUNTREPS": "AccountRepsTbl",
            "BUDGETCHANGEHISTORIES": "BudgetChangeHistoriesTbl",
            "BUDGETS": "BudgetsTbl",
            "DEPARTMENTS": "DepartmentsTbl",
            "SERVICES": "ServicesTbl",
        }
        with patch.object(dbq, "get_direct_db_tables", return_value=tables):
            with self.assertRaises(ValueError) as ctx:
                dbq.list_accounts(ae_name="Alex Chen")

        self.assertIn("DB_TABLES.employees is required", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
