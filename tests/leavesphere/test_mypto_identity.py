import unittest
from types import SimpleNamespace
from unittest.mock import patch

from apps.leavesphere.api.v1.helpers import myPto
from apps.leavesphere.api.v1.helpers import dbQueries
from shared.auth.types import AuthPrincipal


class LeaveSphereMyPtoIdentityTests(unittest.TestCase):
    def test_resolve_principal_email_prefers_principal_email(self):
        principal = AuthPrincipal(user_id="user-1", email="Primary@Example.com", raw_user={})
        request = SimpleNamespace(headers={}, state=SimpleNamespace(auth_principal=principal))

        resolved = myPto._resolve_current_principal_email(request)

        self.assertEqual(resolved, "primary@example.com")

    def test_resolve_principal_email_uses_raw_user_metadata(self):
        principal = AuthPrincipal(
            user_id="user-1",
            email=None,
            raw_user={"user_metadata": {"email": "Metadata@Example.com"}},
        )
        request = SimpleNamespace(headers={}, state=SimpleNamespace(auth_principal=principal))

        resolved = myPto._resolve_current_principal_email(request)

        self.assertEqual(resolved, "metadata@example.com")

    def test_resolve_principal_email_uses_header_before_backend_lookup(self):
        principal = AuthPrincipal(user_id="user-1", email=None, raw_user={})
        request = SimpleNamespace(
            headers={"x-user-email": "Lookup@Example.com"},
            state=SimpleNamespace(auth_principal=principal),
        )
        resolved = myPto._resolve_current_principal_email(request)

        self.assertEqual(resolved, "lookup@example.com")

    def test_resolve_principal_email_uses_legacy_header_email(self):
        request = SimpleNamespace(
            headers={"x-user-email": "Legacy.Email@Example.com"},
            state=SimpleNamespace(auth_principal=None),
        )

        resolved = myPto._resolve_current_principal_email(request)

        self.assertEqual(resolved, "legacy.email@example.com")

    def test_resolve_current_employee_prefers_active_match_for_email(self):
        request = SimpleNamespace(
            headers={},
            state=SimpleNamespace(
                auth_principal=AuthPrincipal(user_id="user-1", email="hai@theautoadagency.com", raw_user={})
            ),
        )
        rows = [
            {"id": "emp-old", "active": 0, "email": "hai@theautoadagency.com"},
            {"id": "emp-current", "active": 1, "email": "hai@theautoadagency.com"},
        ]

        with patch.object(myPto, "get_employees_by_email", return_value=rows):
            employee = myPto._resolve_current_employee(request)

        self.assertEqual(employee["id"], "emp-current")

    def test_resolve_current_employee_allows_loads_for_inactive_rows(self):
        request = SimpleNamespace(
            headers={},
            state=SimpleNamespace(
                auth_principal=AuthPrincipal(user_id="user-1", email="hai@theautoadagency.com", raw_user={})
            ),
        )
        rows = [{"id": "emp-inactive", "active": 0, "email": "hai@theautoadagency.com"}]

        with patch.object(myPto, "get_employees_by_email", return_value=rows):
            employee = myPto._resolve_current_employee(request, require_active=False)

        self.assertEqual(employee["id"], "emp-inactive")

    def test_get_employees_by_email_trims_whitespace_in_match(self):
        with patch.object(dbQueries, "fetch_all", return_value=[]) as mock_fetch:
            dbQueries.get_employees_by_email(email=" hai@theautoadagency.com ")

        query = mock_fetch.call_args[0][0]
        params = mock_fetch.call_args[0][1]
        self.assertIn("LOWER(TRIM(email)) = LOWER(TRIM(%s))", query)
        self.assertEqual(params, (" hai@theautoadagency.com ",))


if __name__ == "__main__":
    unittest.main()
