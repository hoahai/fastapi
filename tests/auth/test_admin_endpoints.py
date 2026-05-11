import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from apps.auth.api.v1.endpoints.admin import (
    create_invitation_admin_route,
    list_roles_route,
    revoke_invitation_admin_route,
    update_user_access_route,
)
from shared.auth.types import AuthorizationResult, AuthPrincipal, TenantAccessProfile


def _admin_result() -> AuthorizationResult:
    return AuthorizationResult(
        principal=AuthPrincipal(user_id="admin-user", email="admin@example.com", raw_user={}),
        access=TenantAccessProfile(
            tenant_id="tenant-1",
            tenant_slug="taaa",
            app_id="app-1",
            app_code="tradsphere",
            role="tradsphere.admin",
            permissions=frozenset({"tradsphere.admin", "tradsphere.viewer"}),
        ),
    )


def _non_admin_result() -> AuthorizationResult:
    return AuthorizationResult(
        principal=AuthPrincipal(user_id="viewer-user", email="viewer@example.com", raw_user={}),
        access=TenantAccessProfile(
            tenant_id="tenant-1",
            tenant_slug="taaa",
            app_id="app-1",
            app_code="tradsphere",
            role="tradsphere.viewer",
            permissions=frozenset({"tradsphere.viewer"}),
        ),
    )


class AdminEndpointTests(unittest.TestCase):
    def _request(self):
        return SimpleNamespace(
            headers={
                "authorization": "Bearer token-123",
                "x-tenant-id": "taaa",
            },
            state=SimpleNamespace(),
        )

    def test_admin_routes_forbid_non_admin(self):
        request = self._request()
        with patch("apps.auth.api.v1.endpoints.admin.authorize_bearer_for_tenant_app", return_value=_non_admin_result()):
            with self.assertRaises(HTTPException) as exc:
                list_roles_route(request)
        self.assertEqual(exc.exception.status_code, 403)

    def test_update_user_invalidates_permission_cache(self):
        request = self._request()
        refreshed = [
            {
                "userId": "target-user",
                "email": "target@example.com",
                "fullName": "Target User",
                "status": "disabled",
                "role": "tradsphere.viewer",
                "createdAt": None,
                "updatedAt": None,
                "roleUpdatedAt": None,
            }
        ]
        with patch("apps.auth.api.v1.endpoints.admin.authorize_bearer_for_tenant_app", return_value=_admin_result()), patch(
            "apps.auth.api.v1.endpoints.admin.set_tenant_user_status"
        ) as mock_set_status, patch(
            "apps.auth.api.v1.endpoints.admin.set_tenant_app_role"
        ) as mock_set_role, patch(
            "apps.auth.api.v1.endpoints.admin.list_tenant_users_with_app_role", return_value=refreshed
        ), patch("apps.auth.api.v1.endpoints.admin.permission_cache.invalidate") as mock_invalidate:
            response = update_user_access_route(
                request=request,
                user_id="target-user",
                payload={"status": "disabled", "role": "tradsphere.viewer"},
            )

        mock_set_status.assert_called_once()
        mock_set_role.assert_called_once()
        mock_invalidate.assert_called_once_with(user_id="target-user")
        self.assertEqual(response["status"], "disabled")
        self.assertTrue(response["permissionCacheInvalidated"])

    def test_admin_invite_create_and_revoke(self):
        request = self._request()

        created_invite = {
            "id": "invite-1",
            "token": "token-abc",
            "email": "new.user@example.com",
            "role": "tradsphere.viewer",
            "status": "pending",
            "expires_at": "2026-05-13T08:00:00+00:00",
        }

        with patch("apps.auth.api.v1.endpoints.admin.authorize_bearer_for_tenant_app", return_value=_admin_result()), patch(
            "apps.auth.api.v1.endpoints.admin.get_active_app", return_value={"id": "app-1", "code": "tradsphere"}
        ), patch("apps.auth.api.v1.endpoints.admin.create_invitation", return_value=created_invite):
            create_response = create_invitation_admin_route(
                request=request,
                payload={
                    "email": "new.user@example.com",
                    "appCode": "tradsphere",
                    "role": "tradsphere.viewer",
                    "expirationHours": 72,
                },
            )

        self.assertEqual(create_response["status"], "pending")
        self.assertIn("/auth/invite/", create_response["inviteUrl"])

        with patch("apps.auth.api.v1.endpoints.admin.authorize_bearer_for_tenant_app", return_value=_admin_result()), patch(
            "apps.auth.api.v1.endpoints.admin.get_invitation_by_id",
            return_value={"id": "invite-1", "tenant_id": "tenant-1"},
        ), patch("apps.auth.api.v1.endpoints.admin.patch_invitation_status") as mock_patch:
            revoke_response = revoke_invitation_admin_route(request=request, invitation_id="invite-1")

        mock_patch.assert_called_once_with(invitation_id="invite-1", status="revoked")
        self.assertEqual(revoke_response, {"status": "revoked", "id": "invite-1"})


if __name__ == "__main__":
    unittest.main()
