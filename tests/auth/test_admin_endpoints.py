import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from apps.auth.api.v1.endpoints.admin import (
    create_invitation_admin_route,
    load_admin_users_page_route,
    list_roles_route,
    revoke_invitation_admin_route,
    update_user_access_v2_route,
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
            role="admin",
            permissions=frozenset({"tradsphere.admin", "tradsphere.viewer"}),
        ),
    )


def _super_admin_result() -> AuthorizationResult:
    return AuthorizationResult(
        principal=AuthPrincipal(user_id="super-user", email="super@example.com", raw_user={}),
        access=TenantAccessProfile(
            tenant_id="tenant-1",
            tenant_slug="taaa",
            app_id="app-1",
            app_code="tradsphere",
            role="super_admin",
            permissions=frozenset({"workspace.super_admin", "tradsphere.admin", "tradsphere.viewer"}),
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
            role="viewer",
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

    def test_admin_users_load_forbids_non_admin(self):
        request = self._request()
        with patch("apps.auth.api.v1.endpoints.admin.authorize_bearer_for_tenant_app", return_value=_non_admin_result()):
            with self.assertRaises(HTTPException) as exc:
                load_admin_users_page_route(request=request)
        self.assertEqual(exc.exception.status_code, 403)

    def test_admin_users_load_returns_scoped_payload(self):
        request = self._request()
        users_payload = [
            {
                "userId": "in-scope-user",
                "email": "in-scope@example.com",
                "fullName": "In Scope",
                "status": "active",
                "role": "viewer",
                "tenantMemberships": [{"tenantId": "tenant-1", "tenantSlug": "taaa", "status": "active"}],
                "appAssignments": [{"tenantId": "tenant-1", "appId": "app-1", "appCode": "tradsphere", "role": "admin"}],
                "isSuperAdmin": False,
            },
            {
                "userId": "out-scope-user",
                "email": "out-scope@example.com",
                "fullName": "Out Scope",
                "status": "active",
                "role": "viewer",
                "tenantMemberships": [{"tenantId": "tenant-2", "tenantSlug": "tbbb", "status": "active"}],
                "appAssignments": [{"tenantId": "tenant-2", "appId": "app-2", "appCode": "fundsphere", "role": "admin"}],
                "isSuperAdmin": False,
            },
        ]
        invitations_rows = [
            {
                "id": "invite-1",
                "email": "pending.in.scope@example.com",
                "tenant_id": "tenant-1",
                "app_id": "app-1",
                "role": "viewer",
                "status": "pending",
                "token": "token-1",
                "created_at": "2026-05-13T08:00:00+00:00",
            },
            {
                "id": "invite-2",
                "email": "pending.out.scope@example.com",
                "tenant_id": "tenant-2",
                "app_id": "app-2",
                "role": "viewer",
                "status": "pending",
                "token": "token-2",
                "created_at": "2026-05-13T07:00:00+00:00",
            },
        ]

        with patch("apps.auth.api.v1.endpoints.admin.authorize_bearer_for_tenant_app", return_value=_admin_result()), patch(
            "apps.auth.api.v1.endpoints.admin.list_admin_assignment_scopes_for_user",
            return_value={("tenant-1", "app-1")},
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_active_tenants",
            return_value=[
                {"id": "tenant-1", "slug": "taaa", "name": "Tenant A", "active": True},
                {"id": "tenant-2", "slug": "tbbb", "name": "Tenant B", "active": True},
            ],
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_active_apps",
            return_value=[
                {"id": "app-1", "code": "tradsphere", "name": "TradSphere", "active": True},
                {"id": "app-2", "code": "fundsphere", "name": "FundSphere", "active": True},
            ],
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_role_keys_from_store",
            return_value=["viewer", "editor", "admin"],
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_users_with_access",
            return_value=users_payload,
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_invitations_scoped",
            return_value=invitations_rows,
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_invitation_assignments",
            side_effect=lambda invitation_id: (
                [{"tenant_id": "tenant-1", "app_id": "app-1", "role": "viewer"}]
                if invitation_id == "invite-1"
                else [{"tenant_id": "tenant-2", "app_id": "app-2", "role": "viewer"}]
            ),
        ):
            payload = load_admin_users_page_route(request=request)

        self.assertEqual(len(payload["tenants"]), 1)
        self.assertEqual(payload["tenants"][0]["id"], "tenant-1")
        self.assertEqual(len(payload["apps"]), 1)
        self.assertEqual(payload["apps"][0]["id"], "app-1")
        self.assertEqual(len(payload["users"]), 1)
        self.assertEqual(payload["users"][0]["userId"], "in-scope-user")
        self.assertEqual(len(payload["invitations"]), 1)
        self.assertEqual(payload["invitations"][0]["id"], "invite-1")
        self.assertTrue(any(item.get("key") == "admin" for item in payload["roles"]))

    def test_role_catalog_includes_super_admin_entry(self):
        request = self._request()
        with patch("apps.auth.api.v1.endpoints.admin.authorize_bearer_for_tenant_app", return_value=_admin_result()), patch(
            "apps.auth.api.v1.endpoints.admin.list_admin_assignment_scopes_for_user",
            return_value={("tenant-1", "app-1")},
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_role_keys_from_store",
            return_value=["viewer", "editor", "admin"],
        ):
            payload = list_roles_route(request)

        role_keys = [item["key"] for item in payload["items"]]
        self.assertIn("super_admin", role_keys)
        self.assertIn("admin", payload["roles"])
        self.assertTrue(payload["superAdminSupported"])

    def test_update_user_access_multi_assignment_invalidates_permission_cache(self):
        request = self._request()
        refreshed = [
            {
                "userId": "target-user",
                "email": "target@example.com",
                "fullName": "Target User",
                "status": "active",
                "role": "admin",
                "tenantMemberships": [],
                "appAssignments": [],
            }
        ]
        with patch("apps.auth.api.v1.endpoints.admin.authorize_bearer_for_tenant_app", return_value=_super_admin_result()), patch(
            "apps.auth.api.v1.endpoints.admin.list_active_apps",
            return_value=[{"id": "app-1", "code": "tradsphere", "name": "TradSphere", "active": True}],
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_role_keys_from_store",
            return_value=["super_admin", "viewer", "editor", "admin"],
        ), patch(
            "apps.auth.api.v1.endpoints.admin.get_user_memberships",
            return_value=[{"tenant_id": "tenant-1", "status": "active"}],
        ), patch(
            "apps.auth.api.v1.endpoints.admin.get_user_app_roles",
            return_value=[{"tenant_id": "tenant-1", "app_id": "app-1", "role": "viewer"}],
        ), patch(
            "apps.auth.api.v1.endpoints.admin.is_user_super_admin",
            return_value=False,
        ), patch(
            "apps.auth.api.v1.endpoints.admin.set_tenant_user_status"
        ) as mock_set_status, patch(
            "apps.auth.api.v1.endpoints.admin.set_tenant_app_role"
        ) as mock_set_role, patch(
            "apps.auth.api.v1.endpoints.admin.remove_tenant_app_role"
        ) as mock_remove_role, patch(
            "apps.auth.api.v1.endpoints.admin.list_users_with_access",
            return_value=refreshed,
        ), patch(
            "apps.auth.api.v1.endpoints.admin.permission_cache.invalidate"
        ) as mock_invalidate:
            response = update_user_access_v2_route(
                request=request,
                user_id="target-user",
                payload={
                    "tenantMemberships": [
                        {"tenantId": "tenant-1", "status": "active"},
                        {"tenantId": "tenant-2", "status": "active"},
                    ],
                    "appAssignments": [
                        {"tenantId": "tenant-1", "appId": "app-1", "role": "admin"},
                        {"tenantId": "tenant-2", "appId": "app-1", "role": "viewer"},
                    ],
                },
            )

        self.assertEqual(mock_set_status.call_count, 1)
        self.assertEqual(mock_set_role.call_count, 2)
        mock_remove_role.assert_not_called()
        mock_invalidate.assert_called_once_with(user_id="target-user")
        self.assertTrue(response["permissionCacheInvalidated"])

    def test_update_user_access_forbids_non_super_cross_tenant_scope(self):
        request = self._request()
        with patch("apps.auth.api.v1.endpoints.admin.authorize_bearer_for_tenant_app", return_value=_admin_result()), patch(
            "apps.auth.api.v1.endpoints.admin.list_admin_assignment_scopes_for_user",
            return_value={("tenant-1", "app-1")},
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_active_apps",
            return_value=[{"id": "app-1", "code": "tradsphere", "name": "TradSphere", "active": True}],
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_role_keys_from_store",
            return_value=["viewer", "editor", "admin"],
        ), patch(
            "apps.auth.api.v1.endpoints.admin.is_user_super_admin",
            return_value=False,
        ):
            with self.assertRaises(HTTPException) as exc:
                update_user_access_v2_route(
                    request=request,
                    user_id="target-user",
                    payload={
                        "tenantMemberships": [{"tenantId": "tenant-2", "status": "active"}],
                        "appAssignments": [{"tenantId": "tenant-2", "appId": "app-1", "role": "viewer"}],
                    },
                )

        self.assertEqual(exc.exception.status_code, 403)

    def test_admin_invite_create_and_revoke(self):
        request = self._request()

        with patch("apps.auth.api.v1.endpoints.admin.authorize_bearer_for_tenant_app", return_value=_super_admin_result()), patch(
            "apps.auth.api.v1.endpoints.admin.list_role_keys_from_store",
            return_value=["super_admin", "viewer", "editor", "admin"],
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_active_tenants",
            return_value=[
                {"id": "tenant-1", "slug": "taaa", "name": "Tenant A", "active": True},
                {"id": "tenant-2", "slug": "tbbb", "name": "Tenant B", "active": True},
            ],
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_active_apps",
            return_value=[{"id": "app-1", "code": "tradsphere", "name": "TradSphere", "active": True}],
        ), patch(
            "apps.auth.api.v1.endpoints.admin.create_invitation",
            return_value={
                "id": "invite-1",
                "token": "token-1",
                "email": "new.user@example.com",
                "role": "viewer",
                "status": "pending",
                "expires_at": "2026-05-13T08:00:00+00:00",
            },
        ) as mock_create:
            with patch("apps.auth.api.v1.endpoints.admin.insert_invitation_assignments") as mock_insert_assignments:
                create_response = create_invitation_admin_route(
                    request=request,
                    payload={
                        "email": "new.user@example.com",
                        "tenantIds": ["tenant-1", "tenant-2"],
                        "appIds": ["app-1"],
                        "role": "viewer",
                        "expirationHours": 72,
                    },
                )

        self.assertEqual(create_response["count"], 2)
        self.assertEqual(mock_create.call_count, 1)
        mock_insert_assignments.assert_called_once()

        with patch("apps.auth.api.v1.endpoints.admin.authorize_bearer_for_tenant_app", return_value=_admin_result()), patch(
            "apps.auth.api.v1.endpoints.admin.list_admin_assignment_scopes_for_user",
            return_value={("tenant-1", "app-1")},
        ), patch(
            "apps.auth.api.v1.endpoints.admin.get_invitation_by_id",
            return_value={"id": "invite-1", "tenant_id": "tenant-1", "app_id": "app-1"},
        ), patch(
            "apps.auth.api.v1.endpoints.admin.list_invitation_assignments",
            return_value=[],
        ), patch("apps.auth.api.v1.endpoints.admin.patch_invitation_status") as mock_patch:
            revoke_response = revoke_invitation_admin_route(request=request, invitation_id="invite-1")

        mock_patch.assert_called_once_with(invitation_id="invite-1", status="revoked")
        self.assertEqual(revoke_response, {"status": "revoked", "id": "invite-1"})


if __name__ == "__main__":
    unittest.main()
