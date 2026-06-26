import unittest

from apps.fundsphere.api.v1.router import router as fundsphere_router


class FundSphereRouteContractTests(unittest.TestCase):
    def test_direct_top_level_routes_are_registered(self):
        actual_routes: set[tuple[str, str]] = set()
        for route in fundsphere_router.routes:
            path = str(getattr(route, "path", "") or "")
            methods = set(getattr(route, "methods", set()) or set())
            for method in methods:
                if method in {"GET", "POST", "PUT", "DELETE"}:
                    actual_routes.add((method, path))

        expected_routes = {
            ("GET", "/v1/accounts"),
            ("POST", "/v1/accounts"),
            ("PUT", "/v1/accounts"),
            ("GET", "/v1/departments"),
            ("POST", "/v1/departments"),
            ("PUT", "/v1/departments"),
            ("GET", "/v1/services"),
            ("POST", "/v1/services"),
            ("PUT", "/v1/services"),
            ("GET", "/v1/accountReps"),
            ("POST", "/v1/accountReps"),
            ("DELETE", "/v1/accountReps"),
            ("GET", "/v1/budgets"),
            ("POST", "/v1/budgets"),
            ("PUT", "/v1/budgets"),
            ("GET", "/v1/budgetChangeHistories"),
        }

        for route_key in expected_routes:
            self.assertIn(route_key, actual_routes)

        self.assertIn(("POST", "/v1/masterBudgetControl/settings/accounts"), actual_routes)
        self.assertIn(("POST", "/v1/masterBudgetControl/settings/services"), actual_routes)
        self.assertIn(("POST", "/v1/masterBudgetControl/budgetData/load"), actual_routes)

    def test_direct_routes_do_not_use_path_parameters(self):
        direct_paths = [
            str(getattr(route, "path", "") or "")
            for route in fundsphere_router.routes
            if any(
                prefix in str(getattr(route, "path", "") or "")
                for prefix in (
                    "/v1/accounts",
                    "/v1/departments",
                    "/v1/services",
                    "/v1/accountReps",
                    "/v1/budgets",
                    "/v1/budgetChangeHistories",
                )
            )
        ]

        self.assertTrue(direct_paths)
        self.assertFalse(any("{" in path for path in direct_paths))

    def test_master_budget_control_surface_remains_intact(self):
        actual_paths = {
            str(getattr(route, "path", "") or "")
            for route in fundsphere_router.routes
            if "/v1/masterBudgetControl" in str(getattr(route, "path", "") or "")
        }

        self.assertIn("/v1/masterBudgetControl/settings/accounts", actual_paths)
        self.assertIn("/v1/masterBudgetControl/settings/services", actual_paths)
        self.assertIn("/v1/masterBudgetControl/budgetData/load", actual_paths)
        self.assertIn("/v1/masterBudgetControl/budgetData/update", actual_paths)
        self.assertIn("/v1/masterBudgetControl/masterBudget/load", actual_paths)
        self.assertIn("/v1/masterBudgetControl/netSpend/load", actual_paths)
        self.assertNotIn("/v1/masterBudgetControl/budgets", actual_paths)
        self.assertFalse(
            any(
                path in {
                    "/v1/masterBudgetControl/accounts",
                    "/v1/masterBudgetControl/departments",
                    "/v1/masterBudgetControl/services",
                    "/v1/masterBudgetControl/accountReps",
                    "/v1/masterBudgetControl/budgets",
                    "/v1/masterBudgetControl/budgetChangeHistories",
                }
                for path in actual_paths
            )
        )


if __name__ == "__main__":
    unittest.main()
