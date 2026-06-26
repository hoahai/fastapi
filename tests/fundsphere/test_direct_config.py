import unittest
from unittest.mock import patch

from shared.tenant import TenantConfigValidationError

from apps.fundsphere.api.v1.helpers.directConfig import get_direct_db_tables


class FundSphereDirectConfigTests(unittest.TestCase):
    def test_get_direct_db_tables_resolves_explicit_and_alias_keys(self):
        raw_tables = {
            "accounts": "AccountsTbl",
            "departments": "DepartmentsTbl",
            "services": "ServicesTbl",
            "accountReps": "AccountRepsTbl",
            "budgets": "BudgetsTbl",
            "budgetChangeHistories": "BudgetChangeHistoriesTbl",
            "employees": "EmployeesTbl",
        }
        with patch(
            "apps.fundsphere.api.v1.helpers.directConfig.get_app_scoped_env",
            return_value=str(raw_tables),
        ), patch(
            "apps.fundsphere.api.v1.helpers.directConfig.get_env",
            return_value=None,
        ):
            tables = get_direct_db_tables(require_employees=True)

        self.assertEqual(
            tables,
            {
                "ACCOUNTS": "AccountsTbl",
                "ACCOUNTREPS": "AccountRepsTbl",
                "BUDGETCHANGEHISTORIES": "BudgetChangeHistoriesTbl",
                "BUDGETS": "BudgetsTbl",
                "DEPARTMENTS": "DepartmentsTbl",
                "EMPLOYEES": "EmployeesTbl",
                "SERVICES": "ServicesTbl",
            },
        )

    def test_get_direct_db_tables_rejects_missing_required_keys(self):
        raw_tables = {
            "accounts": "AccountsTbl",
            "departments": "DepartmentsTbl",
            "services": "ServicesTbl",
        }
        with patch(
            "apps.fundsphere.api.v1.helpers.directConfig.get_app_scoped_env",
            return_value=str(raw_tables),
        ), patch(
            "apps.fundsphere.api.v1.helpers.directConfig.get_env",
            return_value=None,
        ):
            with self.assertRaises(TenantConfigValidationError) as ctx:
                get_direct_db_tables()

        self.assertIn("DB_TABLES.ACCOUNTREPS", ctx.exception.missing)
        self.assertIn("DB_TABLES.BUDGETS", ctx.exception.missing)
        self.assertIn("DB_TABLES.BUDGETCHANGEHISTORIES", ctx.exception.missing)


if __name__ == "__main__":
    unittest.main()
