import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from apps.auth.api.v1.endpoints.invitations import accept_invitation_route
from shared.auth.types import AuthPrincipal


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
            "role": "tradsphere.viewer",
            "email": "new.user@example.com",
        }

        with patch(
            "apps.auth.api.v1.endpoints.invitations.authenticate_bearer",
            return_value=AuthPrincipal(user_id="user-1", email="new.user@example.com", raw_user={}),
        ), patch(
            "apps.auth.api.v1.endpoints.invitations.get_invitation_by_token",
            return_value=invitation_row,
        ), patch("apps.auth.api.v1.endpoints.invitations.upsert_profile_for_invited_user") as mock_upsert_profile, patch(
            "apps.auth.api.v1.endpoints.invitations.activate_tenant_user"
        ) as mock_activate, patch(
            "apps.auth.api.v1.endpoints.invitations.upsert_tenant_app_role"
        ) as mock_role, patch(
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
        mock_mark.assert_called_once()
        mock_invalidate.assert_called_once_with(user_id="user-1")
        self.assertEqual(response["status"], "accepted")
        self.assertEqual(response["role"], "tradsphere.viewer")


if __name__ == "__main__":
    unittest.main()
