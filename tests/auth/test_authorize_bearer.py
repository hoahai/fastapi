import unittest
from types import SimpleNamespace
from unittest.mock import patch

from fastapi import HTTPException

from shared.auth.dependencies import authorize_bearer_for_tenant_app
from shared.auth.jwt_verify import JwtVerificationError
from shared.auth.permissions_repo import TenantAccessError
from shared.auth.types import AuthPrincipal, TenantAccessProfile


class AuthorizeBearerTests(unittest.TestCase):
    def _request(self, authorization: str, tenant_slug: str = "taaa"):
        return SimpleNamespace(
            headers={
                "authorization": authorization,
                "x-tenant-id": tenant_slug,
            },
            state=SimpleNamespace(),
        )

    def test_valid_bearer_authorizes(self):
        principal = AuthPrincipal(user_id="user-1", email="user@example.com", raw_user={})
        access = TenantAccessProfile(
            tenant_id="tenant-1",
            tenant_slug="taaa",
            app_id="app-1",
            app_code="tradsphere",
            role="viewer",
            permissions=frozenset({"tradsphere.viewer"}),
        )
        request = self._request("Bearer token-123")

        with patch("shared.auth.dependencies.verify_supabase_jwt", return_value=principal), patch(
            "shared.auth.dependencies.get_tenant_access_cached", return_value=access
        ):
            result = authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")

        self.assertEqual(result.principal.user_id, "user-1")
        self.assertEqual(result.access.tenant_slug, "taaa")
        self.assertEqual(request.state.auth_mode, "supabase_jwt")

    def test_wrong_tenant_forbidden(self):
        principal = AuthPrincipal(user_id="user-1", email="user@example.com", raw_user={})
        request = self._request("Bearer token-123", tenant_slug="wrong-tenant")

        with patch("shared.auth.dependencies.verify_supabase_jwt", return_value=principal), patch(
            "shared.auth.dependencies.get_tenant_access_cached",
            side_effect=TenantAccessError("Tenant not found or inactive", code="tenant_not_found"),
        ):
            with self.assertRaises(HTTPException) as exc:
                authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")

        self.assertEqual(exc.exception.status_code, 403)

    def test_auth_service_timeout_returns_503(self):
        request = self._request("Bearer token-123")
        with patch(
            "shared.auth.dependencies.verify_supabase_jwt",
            side_effect=JwtVerificationError("Authentication service timed out", status_code=503),
        ):
            with self.assertRaises(HTTPException) as exc:
                authorize_bearer_for_tenant_app(request=request, app_code="tradsphere")
        self.assertEqual(exc.exception.status_code, 503)


if __name__ == "__main__":
    unittest.main()
