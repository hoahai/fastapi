import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from apps.auth.api.v1.endpoints.invitations import (
    accept_invitation_route,
    create_invitation_route,
    list_scoped_invitation_users_route,
    lookup_active_existing_user_route,
)
from shared.auth.types import AuthorizationResult, AuthPrincipal, TenantAccessProfile


class InvitationEndpointTests(unittest.TestCase):
    def _request(self):
        return SimpleNamespace(
            headers={
                "authorization": "Bearer token-123",
            },
            state=SimpleNamespace(),
        )

    def test_accept_invitation_upserts_profile(self):
        request = self._request()
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=2)).isoformat()
        invitation_row = {
            "id": "invite-1",
            "status": "pending",
            "expires_at": expires_at,
            "tenant_id": "tenant-1",
            "app_id": "app-1",
            "role": "viewer",
            "email": "new.user@example.com",
        }

        with patch(
            "apps.auth.api.v1.endpoints.invitations.authenticate_bearer",
            return_value=AuthPrincipal(user_id="user-1", email="new.user@example.com", raw_user={}),
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.get_invitation_by_token",
            return_value=invitation_row,
        ), patch(
            "apps.auth.api.v1.endpoints.invitations._resolve_tenant_slug",
            return_value="lacs",
        ), patch(
            "apps.auth.api.v1.endpoints.invitations._resolve_app_code",
            return_value="shiftzy",
        ), patch("apps.auth.api.v1.endpoints.invitations.upsert_profile_for_invited_user") as mock_upsert_profile, patch(
            "apps.auth.api.v1.endpoints.invitations.activate_tenant_user"
        ) as mock_activate, patch(
            "apps.auth.api.v1.endpoints.invitations.upsert_tenant_app_role"
        ) as mock_role, patch(
            "apps.auth.api.v1.endpoints.invitations.list_invitation_assignments",
            return_value=[],
        ) as mock_assignments, patch(
            "apps.auth.api.v1.endpoints.invitations.mark_invitation_accepted"
        ) as mock_mark, patch(
            "apps.auth.api.v1.endpoints.invitations.permission_cache.invalidate"
        ) as mock_invalidate:
            response = accept_invitation_route(
                request=request,
                token="token-abc",
                payload={"profile": {"firstName": "Alex", "lastName": "Johnson"}},
            )

        mock_upsert_profile.assert_called_once_with(
            user_id="user-1",
            email="new.user@example.com",
            full_name="Alex Johnson",
        )
        mock_activate.assert_called_once()
        mock_role.assert_called_once()
        mock_assignments.assert_called_once()
        mock_mark.assert_called_once()
        mock_invalidate.assert_called_once_with(user_id="user-1")
        self.assertEqual(response["status"], "accepted")
        self.assertEqual(response["tenantSlug"], "lacs")
        self.assertEqual(response["appCode"], "shiftzy")
        self.assertEqual(response["role"], "viewer")

    def test_tenant_admin_cannot_invite_brand_new_user(self):
        request = SimpleNamespace(
            headers={
                "authorization": "Bearer token-123",
                "x-tenant-id": "taaa",
            },
            state=SimpleNamespace(),
        )
        auth_result = AuthorizationResult(
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
        with patch("apps.auth.api.v1.endpoints.invitations.authorize_bearer_for_tenant_app", return_value=auth_result), patch(
            "apps.auth.api.v1.endpoints.invitations.find_user_id_by_email",
            return_value=None,
        ):
            with self.assertRaises(HTTPException) as exc:
                create_invitation_route(
                    request=request,
                    payload={
                        "email": "brand.new@example.com",
                        "appCode": "tradsphere",
                        "role": "viewer",
                    },
                )
        self.assertEqual(exc.exception.status_code, 403)
        self.assertIn("Only Super Admin", str(exc.exception.detail))

    def test_tenant_admin_can_invite_existing_active_user(self):
        request = SimpleNamespace(
            headers={
                "authorization": "Bearer token-123",
                "x-tenant-id": "taaa",
            },
            state=SimpleNamespace(),
        )
        auth_result = AuthorizationResult(
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
        with patch("apps.auth.api.v1.endpoints.invitations.authorize_bearer_for_tenant_app", return_value=auth_result), patch(
            "apps.auth.api.v1.endpoints.invitations.find_user_id_by_email",
            return_value="user-123",
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.is_auth_user_active",
            return_value=True,
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.get_active_app",
            return_value={"id": "app-1", "code": "tradsphere"},
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.create_invitation",
            return_value={
                "id": "invite-123",
                "token": "token-123",
                "status": "pending",
                "expires_at": "2026-05-15T10:00:00+00:00",
            },
        ):
            response = create_invitation_route(
                request=request,
                payload={
                    "email": "existing@example.com",
                    "appCode": "tradsphere",
                    "role": "viewer",
                },
            )
        self.assertEqual(response["status"], "pending")
        self.assertEqual(response["email"], "existing@example.com")

    def test_super_admin_can_invite_brand_new_user(self):
        request = SimpleNamespace(
            headers={
                "authorization": "Bearer token-123",
                "x-tenant-id": "taaa",
            },
            state=SimpleNamespace(),
        )
        auth_result = AuthorizationResult(
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
        with patch("apps.auth.api.v1.endpoints.invitations.authorize_bearer_for_tenant_app", return_value=auth_result), patch(
            "apps.auth.api.v1.endpoints.invitations.get_active_app",
            return_value={"id": "app-1", "code": "tradsphere"},
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.create_invitation",
            return_value={
                "id": "invite-999",
                "token": "token-999",
                "status": "pending",
                "expires_at": "2026-05-15T10:00:00+00:00",
            },
        ):
            response = create_invitation_route(
                request=request,
                payload={
                    "email": "brand.new@example.com",
                    "appCode": "tradsphere",
                    "role": "viewer",
                },
            )
        self.assertEqual(response["status"], "pending")
        self.assertEqual(response["email"], "brand.new@example.com")

    def test_lookup_active_existing_user_returns_scoped_assignment(self):
        request = SimpleNamespace(
            headers={
                "authorization": "Bearer token-123",
                "x-tenant-id": "taaa",
            },
            state=SimpleNamespace(),
        )
        auth_result = AuthorizationResult(
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
        scoped_rows = [
            {
                "userId": "user-123",
                "email": "existing@example.com",
                "fullName": "Existing User",
                "status": "active",
                "appAssignments": [
                    {
                        "tenantId": "tenant-1",
                        "appId": "app-1",
                        "role": "editor",
                    }
                ],
            }
        ]
        provider = SimpleNamespace(
            select_many=lambda **kwargs: [
                {"user_id": "user-123", "email": "existing@example.com", "full_name": "Existing User"}
            ]
        )
        with patch("apps.auth.api.v1.endpoints.invitations.authorize_bearer_for_tenant_app", return_value=auth_result), patch(
            "apps.auth.api.v1.endpoints.invitations.find_user_id_by_email",
            return_value="user-123",
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.is_auth_user_active",
            return_value=True,
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.list_tenant_users_with_app_role",
            return_value=scoped_rows,
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.get_auth_provider",
            return_value=provider,
        ):
            response = lookup_active_existing_user_route(
                request=request,
                email="existing@example.com",
            )

        self.assertEqual(response["items"], [])

    def test_lookup_active_existing_user_returns_empty_for_unknown_or_inactive_user(self):
        request = SimpleNamespace(
            headers={
                "authorization": "Bearer token-123",
                "x-tenant-id": "taaa",
            },
            state=SimpleNamespace(),
        )
        auth_result = AuthorizationResult(
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
        provider = SimpleNamespace(
            select_many=lambda **kwargs: [
                {"user_id": "user-123", "email": "inactive@example.com", "full_name": "Inactive User"}
            ]
        )
        with patch("apps.auth.api.v1.endpoints.invitations.authorize_bearer_for_tenant_app", return_value=auth_result), patch(
            "apps.auth.api.v1.endpoints.invitations.find_user_id_by_email",
            return_value="user-123",
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.is_auth_user_active",
            return_value=False,
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.list_tenant_users_with_app_role",
            return_value=[],
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.get_auth_provider",
            return_value=provider,
        ):
            response = lookup_active_existing_user_route(
                request=request,
                email="inactive@example.com",
            )
        self.assertEqual(response["items"], [])

    def test_list_scoped_invitation_users_returns_current_scope_assignments(self):
        request = SimpleNamespace(
            headers={
                "authorization": "Bearer token-123",
                "x-tenant-id": "taaa",
            },
            state=SimpleNamespace(),
        )
        auth_result = AuthorizationResult(
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
        scoped_rows = [
            {
                "userId": "user-123",
                "email": "existing@example.com",
                "fullName": "Existing User",
                "status": "active",
                "appAssignments": [
                    {
                        "tenantId": "tenant-1",
                        "appId": "app-1",
                        "role": "viewer",
                    }
                ],
            },
            {
                "userId": "user-456",
                "email": "other@example.com",
                "fullName": "Other User",
                "status": "active",
                "appAssignments": [
                    {
                        "tenantId": "tenant-1",
                        "appId": "app-1",
                        "role": "admin",
                    }
                ],
            },
        ]
        with patch("apps.auth.api.v1.endpoints.invitations.authorize_bearer_for_tenant_app", return_value=auth_result), patch(
            "apps.auth.api.v1.endpoints.invitations.list_tenant_users_with_app_role",
            return_value=scoped_rows,
        ):
            response = list_scoped_invitation_users_route(request=request, query="other@example.com")

        self.assertEqual(len(response["items"]), 1)
        self.assertEqual(response["items"][0]["userId"], "user-456")
        self.assertEqual(response["items"][0]["role"], "admin")

    def test_shiftzy_tenant_admin_can_invite_existing_active_user(self):
        request = SimpleNamespace(
            headers={
                "authorization": "Bearer token-123",
                "x-tenant-id": "taaa",
                "x-app-code": "shiftzy",
            },
            state=SimpleNamespace(),
        )
        auth_result = AuthorizationResult(
            principal=AuthPrincipal(user_id="shiftzy-admin", email="admin@example.com", raw_user={}),
            access=TenantAccessProfile(
                tenant_id="tenant-1",
                tenant_slug="taaa",
                app_id="app-2",
                app_code="shiftzy",
                role="admin",
                permissions=frozenset({"shiftzy.admin", "shiftzy.viewer"}),
            ),
        )
        with patch("apps.auth.api.v1.endpoints.invitations.authorize_bearer_for_tenant_app", return_value=auth_result), patch(
            "apps.auth.api.v1.endpoints.invitations.find_user_id_by_email",
            return_value="user-123",
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.is_auth_user_active",
            return_value=True,
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.get_active_app",
            return_value={"id": "app-2", "code": "shiftzy"},
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.create_invitation",
            return_value={
                "id": "invite-234",
                "token": "token-234",
                "status": "pending",
                "expires_at": "2026-05-15T10:00:00+00:00",
            },
        ):
            response = create_invitation_route(
                request=request,
                payload={
                    "email": "existing.shiftzy@example.com",
                    "appCode": "shiftzy",
                    "role": "editor",
                },
            )

        self.assertEqual(response["status"], "pending")
        self.assertEqual(response["role"], "editor")


if __name__ == "__main__":
    unittest.main()
