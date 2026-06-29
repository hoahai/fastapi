import unittest

from fastapi.responses import RedirectResponse

from main import _redirect_to_frontend_canonical_path, _should_redirect_frontend_path


class RootFrontendRouteTests(unittest.TestCase):
    def test_frontend_routes_redirect_to_canonical_slash_route(self):
        for route in [
            "shiftzy/home",
            "tradsphere/home",
            "fundsphere/accounts",
            "leavesphere/leave-management",
            "auth/login",
            "profile",
        ]:
            with self.subTest(route=route):
                self.assertTrue(_should_redirect_frontend_path(route))

    def test_backend_and_asset_paths_are_not_redirected(self):
        for route in ["", "/", "api", "api/shiftzy/v1", "assets/index.js", "fe/assets/index.js"]:
            with self.subTest(route=route):
                self.assertFalse(_should_redirect_frontend_path(route))

    def test_redirect_preserves_query_string(self):
        response = _redirect_to_frontend_canonical_path("shiftzy/home", "tenant=alpha&debug=1")
        self.assertIsInstance(response, RedirectResponse)
        self.assertEqual(response.status_code, 307)
        self.assertEqual(response.headers.get("location"), "/shiftzy/home/?tenant=alpha&debug=1")


if __name__ == "__main__":
    unittest.main()
