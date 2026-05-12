import unittest
from types import SimpleNamespace
from unittest.mock import patch

from apps.auth.api.v1.endpoints.session import get_session_me, patch_session_me_profile
from shared.auth.types import AuthorizationResult, AuthPrincipal, TenantAccessProfile


def _auth_result() -> AuthorizationResult:
    return AuthorizationResult(
        principal=AuthPrincipal(user_id="user-1", email="user@example.com", raw_user={}),
        access=TenantAccessProfile(
            tenant_id="tenant-1",
            tenant_slug="taaa",
            app_id="app-1",
            app_code="tradsphere",
            role="tradsphere.viewer",
            permissions=frozenset({"tradsphere.viewer"}),
        ),
    )


class SessionEndpointTests(unittest.TestCase):
    def _request(self):
        return SimpleNamespace(
            headers={
                "authorization": "Bearer token-123",
                "x-tenant-id": "taaa",
            },
            state=SimpleNamespace(),
        )

    def test_get_session_me_includes_profile_name_fields(self):
        request = self._request()
        with patch("apps.auth.api.v1.endpoints.session.authorize_bearer_for_tenant_app", return_value=_auth_result()), patch(
            "apps.auth.api.v1.endpoints.session.select_profile_for_user",
            return_value={"user_id": "user-1", "email": "user@example.com", "full_name": "Alex Johnson"},
        ):
            response = get_session_me(request)

        self.assertEqual(response["user"]["fullName"], "Alex Johnson")
        self.assertEqual(response["user"]["firstName"], "Alex")
        self.assertEqual(response["user"]["lastName"], "Johnson")

    def test_patch_session_profile_accepts_first_and_last_name(self):
        request = self._request()
        with patch("apps.auth.api.v1.endpoints.session.authorize_bearer_for_tenant_app", return_value=_auth_result()), patch(
            "apps.auth.api.v1.endpoints.session.upsert_profile_basic_info"
        ) as mock_upsert_profile, patch(
            "apps.auth.api.v1.endpoints.session.select_profile_for_user",
            return_value={"user_id": "user-1", "email": "user@example.com", "full_name": "Updated Name"},
        ):
            response = patch_session_me_profile(
                request=request,
                payload={"profile": {"firstName": "Updated", "lastName": "Name"}},
            )

        mock_upsert_profile.assert_called_once()
        self.assertEqual(mock_upsert_profile.call_args.kwargs["full_name"], "Updated Name")
        self.assertEqual(response["user"]["fullName"], "Updated Name")
        self.assertEqual(response["user"]["firstName"], "Updated")
        self.assertEqual(response["user"]["lastName"], "Name")


if __name__ == "__main__":
    unittest.main()
