import unittest
from types import SimpleNamespace
from unittest.mock import patch

from apps.auth.api.v1.endpoints.session import (
    get_session_assignments,
    get_session_me,
    get_session_validate,
    patch_session_me_profile,
)
from shared.auth.types import AuthorizationResult, AuthPrincipal, TenantAccessProfile


def _auth_result() -> AuthorizationResult:
    return AuthorizationResult(
        principal=AuthPrincipal(user_id="user-1", email="user@example.com", raw_user={}),
        access=TenantAccessProfile(
            tenant_id="tenant-1",
            tenant_slug="taaa",
            app_id="app-1",
            app_code="tradsphere",
            role="viewer",
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
        ), patch(
            "apps.auth.api.v1.endpoints.session._filter_assignments_by_backend_app_config",
            side_effect=lambda assignments: assignments,
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

    def test_get_session_me_super_admin_includes_active_tenant_assignments_when_scoped_assignments_missing(self):
        request = self._request()
        super_admin_result = AuthorizationResult(
            principal=AuthPrincipal(user_id="user-1", email="user@example.com", raw_user={}),
            access=TenantAccessProfile(
                tenant_id="tenant-1",
                tenant_slug="taaa",
                app_id="app-1",
                app_code="tradsphere",
                role="super_admin",
                permissions=frozenset({"workspace.super_admin", "tradsphere.admin", "tradsphere.viewer"}),
            ),
        )
        with patch("apps.auth.api.v1.endpoints.session.authorize_bearer_for_tenant_app", return_value=super_admin_result), patch(
            "apps.auth.api.v1.endpoints.session.select_profile_for_user",
            return_value={"user_id": "user-1", "email": "user@example.com", "full_name": "Alex Johnson"},
        ), patch(
            "apps.auth.api.v1.endpoints.session.list_user_access_assignments",
            return_value=[],
        ), patch(
            "apps.auth.api.v1.endpoints.session._filter_assignments_by_backend_app_config",
            side_effect=lambda assignments: assignments,
        ), patch(
            "apps.auth.api.v1.endpoints.session.is_user_super_admin",
            return_value=True,
        ), patch(
            "apps.auth.api.v1.endpoints.session.list_active_tenants",
            return_value=[
                {"id": "tenant-1", "slug": "taaa", "name": "TAAA"},
                {"id": "tenant-2", "slug": "nucar", "name": "NuCar"},
            ],
        ), patch(
            "apps.auth.api.v1.endpoints.session.list_active_apps",
            return_value=[{"id": "app-1", "code": "tradsphere", "name": "Tradsphere"}],
        ):
            response = get_session_me(request)

        self.assertTrue(response["scope"]["isSuperAdmin"])
        self.assertEqual(len(response["assignments"]), 2)
        self.assertEqual(response["assignments"][0]["tenant"]["slug"], "nucar")
        self.assertEqual(response["assignments"][1]["tenant"]["slug"], "taaa")
        self.assertEqual(response["assignments"][0]["role"], "super_admin")
        self.assertEqual(response["assignments"][0]["app"]["code"], "tradsphere")

    def test_get_session_me_filters_out_backend_disabled_tenant_assignment(self):
        request = self._request()
        assignment = {
            "tenant": {"id": "tenant-1", "slug": "taaa", "name": "TAAA"},
            "app": {"id": "app-1", "code": "tradsphere", "name": "Tradsphere"},
            "role": "viewer",
        }
        with patch("apps.auth.api.v1.endpoints.session.authorize_bearer_for_tenant_app", return_value=_auth_result()), patch(
            "apps.auth.api.v1.endpoints.session.select_profile_for_user",
            return_value={"user_id": "user-1", "email": "user@example.com", "full_name": "Alex Johnson"},
        ), patch(
            "apps.auth.api.v1.endpoints.session.list_user_access_assignments",
            return_value=[assignment],
        ), patch(
            "apps.auth.api.v1.endpoints.session.is_user_super_admin",
            return_value=False,
        ), patch(
            "apps.auth.api.v1.endpoints.session._is_assignment_backend_enabled",
            return_value=False,
        ):
            response = get_session_me(request)

        self.assertEqual(response["assignments"], [])

    def test_get_session_me_allows_active_user_with_no_assignments(self):
        request = self._request()
        with patch("apps.auth.api.v1.endpoints.session.authorize_bearer_for_tenant_app", return_value=_auth_result()), patch(
            "apps.auth.api.v1.endpoints.session.select_profile_for_user",
            return_value={"user_id": "user-1", "email": "user@example.com", "full_name": "Alex Johnson"},
        ), patch(
            "apps.auth.api.v1.endpoints.session.list_user_access_assignments",
            return_value=[],
        ), patch(
            "apps.auth.api.v1.endpoints.session.is_user_super_admin",
            return_value=False,
        ), patch(
            "apps.auth.api.v1.endpoints.session._filter_assignments_by_backend_app_config",
            side_effect=lambda assignments: assignments,
        ):
            response = get_session_me(request)

        self.assertEqual(response["user"]["id"], "user-1")
        self.assertEqual(response["role"], "viewer")
        self.assertEqual(response["assignments"], [])

    def test_get_session_assignments_returns_active_assignments(self):
        request = self._request()
        with patch("apps.auth.api.v1.endpoints.session.authenticate_bearer", return_value=_auth_result().principal), patch(
            "apps.auth.api.v1.endpoints.session.list_user_access_assignments",
            return_value=[
                {
                    "tenant": {"id": "tenant-1", "slug": "taaa", "name": "TAAA"},
                    "app": {"id": "app-1", "code": "tradsphere", "name": "Tradsphere"},
                    "role": "viewer",
                }
            ],
        ), patch(
            "apps.auth.api.v1.endpoints.session.is_user_super_admin",
            return_value=False,
        ), patch(
            "apps.auth.api.v1.endpoints.session._filter_assignments_by_backend_app_config",
            side_effect=lambda assignments: assignments,
        ):
            response = get_session_assignments(request)

        self.assertEqual(len(response["assignments"]), 1)
        self.assertEqual(response["assignments"][0]["tenant"]["slug"], "taaa")

    def test_get_session_validate_returns_access_state_in_one_payload(self):
        request = self._request()
        request.headers["x-app-code"] = "tradsphere"
        assignment = {
            "tenant": {"id": "tenant-1", "slug": "taaa", "name": "TAAA", "status": "active"},
            "app": {"id": "app-1", "code": "tradsphere", "name": "Tradsphere"},
            "role": "viewer",
        }
        with patch("apps.auth.api.v1.endpoints.session.authorize_bearer_for_tenant_app", return_value=_auth_result()), patch(
            "apps.auth.api.v1.endpoints.session.list_user_access_assignments",
            return_value=[assignment],
        ), patch(
            "apps.auth.api.v1.endpoints.session.is_user_super_admin",
            return_value=False,
        ), patch(
            "apps.auth.api.v1.endpoints.session._filter_assignments_by_backend_app_config",
            side_effect=lambda assignments: assignments,
        ), patch(
            "apps.auth.api.v1.endpoints.session.select_profile_for_user",
            return_value={"user_id": "user-1", "email": "user@example.com", "full_name": "Alex Johnson"},
        ):
            response = get_session_validate(request)

        self.assertEqual(response["user"]["id"], "user-1")
        self.assertEqual(response["tenant"]["slug"], "taaa")
        self.assertEqual(response["app"]["code"], "tradsphere")
        self.assertEqual(response["role"], "viewer")
        self.assertEqual(response["permissions"], ["tradsphere.viewer"])
        self.assertEqual(len(response["assignments"]), 1)
        self.assertEqual(response["assignments"][0]["tenant"]["slug"], "taaa")


if __name__ == "__main__":
    unittest.main()
