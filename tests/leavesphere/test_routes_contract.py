import unittest

from fastapi import HTTPException
from fastapi.routing import APIRoute
from starlette.requests import Request

from main import app as root_app
from apps.leavesphere.api.main import app as leavesphere_app
from apps.leavesphere.api.v1.router import router as leavesphere_router
from apps.leavesphere.api.v1.endpoints.publicGoogleCalendar import router as public_google_calendar_router
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
            ("POST", "/v1/employees/{employee_id}/activate"),
            ("POST", "/v1/employees/{employee_id}/deactivate"),
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
            ("GET", "/v1/admin/pto/workspace"),
            ("POST", "/v1/admin/pto/email-preview/test-email"),
            ("POST", "/v1/admin/pto/reminders/pending-approvals"),
            ("POST", "/v1/admin/pto/requests"),
            ("PUT", "/v1/admin/pto/requests"),
            ("POST", "/v1/admin/pto/review"),
            ("POST", "/v1/admin/pto/balances/adjust"),
            ("POST", "/v1/admin/pto/balances/duplicate"),
            ("POST", "/v1/admin/pto/setup"),
            ("GET", "/v1/admin/google-calendar/oauth/status"),
            ("GET", "/v1/admin/google-calendar/oauth/url"),
            ("DELETE", "/v1/admin/google-calendar/oauth/connection"),
            ("GET", "/v1/ui/employees/load"),
            ("GET", "/v1/ui/my-pto/load"),
            ("POST", "/v1/ui/my-pto/requests"),
            ("PUT", "/v1/ui/my-pto/requests"),
            ("DELETE", "/v1/ui/my-pto/requests"),
            ("POST", "/v1/ui/my-pto/review"),
        }
        for route_key in expected_routes:
            self.assertIn(route_key, actual_routes)

        self.assertNotIn(("DELETE", "/v1/employees/{employee_id}"), actual_routes)
        self.assertNotIn(("POST", "/v1/employees/{employee_id}/archive"), actual_routes)
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

    def test_admin_pto_route_ordering_is_safe(self):
        route_paths = [
            str(getattr(route, "path", "") or "")
            for route in leavesphere_router.routes
            if "/v1/admin/pto" in str(getattr(route, "path", "") or "")
        ]
        self.assertIn("/v1/admin/pto/workspace", route_paths)
        self.assertIn("/v1/admin/pto/email-preview/test-email", route_paths)
        self.assertIn("/v1/admin/pto/reminders/pending-approvals", route_paths)
        self.assertIn("/v1/admin/pto/requests", route_paths)
        self.assertIn("/v1/admin/pto/review", route_paths)
        self.assertIn("/v1/admin/pto/balances/adjust", route_paths)
        self.assertIn("/v1/admin/pto/balances/duplicate", route_paths)
        self.assertIn("/v1/admin/pto/setup", route_paths)

    def test_google_calendar_public_callback_route_is_registered(self):
        callback_paths = [
            str(getattr(route, "path", "") or "")
            for route in leavesphere_app.routes
            if "/v1/public/google-calendar/oauth" in str(getattr(route, "path", "") or "")
        ]
        self.assertIn("/v1/public/google-calendar/oauth/callback", callback_paths)

    def test_google_calendar_public_path_prefix_is_registered(self):
        public_prefixes = tuple(getattr(leavesphere_app.state, "public_path_prefixes", ()))
        self.assertIn("/api/leavesphere/v1/public/google-calendar/oauth", public_prefixes)

    def test_root_public_path_prefix_includes_google_calendar_callback(self):
        public_prefixes = tuple(getattr(root_app.state, "public_path_prefixes", ()))
        self.assertIn("/api/leavesphere/v1/public/google-calendar/oauth", public_prefixes)

    def test_google_calendar_callback_route_allows_unknown_query_params(self):
        target_route = None
        for route in public_google_calendar_router.routes:
            if isinstance(route, APIRoute) and route.path == "/v1/public/google-calendar/oauth/callback":
                target_route = route
                break

        self.assertIsNotNone(target_route)
        self.assertTrue(
            getattr(target_route.endpoint, "__allow_unknown_query_params__", False),
        )

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
