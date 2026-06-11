import unittest

from fastapi import HTTPException
from fastapi.routing import APIRoute
from starlette.requests import Request

from apps.leavesphere.api.main import app as leavesphere_app
from apps.leavesphere.api.v1.router import router as leavesphere_router
from shared.requestValidation import validate_query_params


class LeaveSphereRouteContractTests(unittest.TestCase):
    def test_expected_routes_are_registered(self):
        actual_routes: set[tuple[str, str]] = set()
        for route in leavesphere_router.routes:
            path = str(getattr(route, "path", "") or "")
            methods = set(getattr(route, "methods", set()) or set())
            for method in methods:
                if method in {"GET", "POST", "PUT", "DELETE"}:
                    actual_routes.add((method, path))

        expected_routes = {
            ("GET", "/v1/employees"),
            ("GET", "/v1/employees/{employee_id}"),
            ("POST", "/v1/employees"),
            ("PUT", "/v1/employees/{employee_id}"),
            ("GET", "/v1/employeeManagers"),
            ("GET", "/v1/employeeManagers/{id}"),
            ("POST", "/v1/employeeManagers"),
            ("DELETE", "/v1/employeeManagers/{id}"),
            ("GET", "/v1/ptoTypes"),
            ("GET", "/v1/ptoTypes/{code}"),
            ("POST", "/v1/ptoTypes"),
            ("PUT", "/v1/ptoTypes/{code}"),
            ("GET", "/v1/ptoActions"),
            ("GET", "/v1/ptoActions/{code}"),
            ("POST", "/v1/ptoActions"),
            ("PUT", "/v1/ptoActions/{code}"),
            ("GET", "/v1/ptoTransactions"),
            ("GET", "/v1/ptoTransactions/{transaction_id}"),
            ("GET", "/v1/ptoTransactions/balances"),
            ("POST", "/v1/ptoTransactions/adjustments"),
            ("POST", "/v1/ptoTransactions/requests"),
            ("POST", "/v1/ptoTransactions/{transaction_id}/cancel"),
            ("POST", "/v1/ptoTransactions/{transaction_id}/approve"),
            ("POST", "/v1/ptoTransactions/{transaction_id}/reject"),
            ("GET", "/v1/ui/my-pto/load"),
            ("POST", "/v1/ui/my-pto/requests"),
            ("PUT", "/v1/ui/my-pto/requests"),
            ("DELETE", "/v1/ui/my-pto/requests"),
            ("POST", "/v1/ui/my-pto/review"),
        }
        for route_key in expected_routes:
            self.assertIn(route_key, actual_routes)

        self.assertNotIn(("DELETE", "/v1/employees/{employee_id}"), actual_routes)
        self.assertNotIn(("DELETE", "/v1/ptoTypes/{code}"), actual_routes)
        self.assertNotIn(("DELETE", "/v1/ptoActions/{code}"), actual_routes)

    def test_pto_transaction_route_ordering_is_safe(self):
        route_paths = [
            str(getattr(route, "path", "") or "")
            for route in leavesphere_router.routes
            if "/v1/ptoTransactions" in str(getattr(route, "path", "") or "")
        ]
        self.assertIn("/v1/ptoTransactions/balances", route_paths)
        self.assertIn("/v1/ptoTransactions/adjustments", route_paths)
        self.assertIn("/v1/ptoTransactions/requests", route_paths)
        self.assertIn("/v1/ptoTransactions/{transaction_id}/cancel", route_paths)
        self.assertIn("/v1/ptoTransactions/{transaction_id}/approve", route_paths)
        self.assertIn("/v1/ptoTransactions/{transaction_id}/reject", route_paths)
        self.assertIn("/v1/ptoTransactions/{transaction_id}", route_paths)
        balances_idx = route_paths.index("/v1/ptoTransactions/balances")
        adjustments_idx = route_paths.index("/v1/ptoTransactions/adjustments")
        requests_idx = route_paths.index("/v1/ptoTransactions/requests")
        cancel_idx = route_paths.index("/v1/ptoTransactions/{transaction_id}/cancel")
        approve_idx = route_paths.index("/v1/ptoTransactions/{transaction_id}/approve")
        reject_idx = route_paths.index("/v1/ptoTransactions/{transaction_id}/reject")
        transaction_idx = route_paths.index("/v1/ptoTransactions/{transaction_id}")
        self.assertLess(balances_idx, transaction_idx)
        self.assertLess(adjustments_idx, transaction_idx)
        self.assertLess(requests_idx, transaction_idx)
        self.assertLess(cancel_idx, transaction_idx)
        self.assertLess(approve_idx, transaction_idx)
        self.assertLess(reject_idx, transaction_idx)

    def test_unknown_query_params_are_rejected(self):
        target_route = None
        for route in leavesphere_app.routes:
            if (
                isinstance(route, APIRoute)
                and route.path == "/v1/ptoTypes"
                and "GET" in (route.methods or set())
            ):
                target_route = route
                break

        self.assertIsNotNone(target_route)

        async def _receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        scope = {
            "type": "http",
            "method": "GET",
            "path": "/v1/ptoTypes",
            "headers": [],
            "query_string": b"unexpected=1",
            "route": target_route,
        }
        request = Request(scope, receive=_receive)
        with self.assertRaises(HTTPException) as exc:
            validate_query_params(request)
        self.assertEqual(exc.exception.status_code, 400)
        self.assertIn("Unknown query params", str(exc.exception.detail))


if __name__ == "__main__":
    unittest.main()
