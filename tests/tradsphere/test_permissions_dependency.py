import os
import unittest
from types import SimpleNamespace

from fastapi import HTTPException

from shared.auth.dependencies import enforce_tradsphere_permission
from shared.auth.types import TenantAccessProfile


class TradspherePermissionDependencyTests(unittest.TestCase):
    def setUp(self):
        self._env = dict(os.environ)
        os.environ["AUTH_PROTECT_TRADSPHERE"] = "true"
        os.environ["AUTH_MODE"] = "compat"
        os.environ["AUTH_ENABLE_LEGACY_API_KEY_FALLBACK"] = "true"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._env)

    def test_legacy_mode_allows_bypass(self):
        request = SimpleNamespace(
            method="GET",
            state=SimpleNamespace(auth_mode="legacy_api_key"),
            url=SimpleNamespace(path="/api/tradsphere/v1/contacts"),
        )
        enforce_tradsphere_permission(request)  # should not raise

    def test_requires_access_without_legacy(self):
        request = SimpleNamespace(
            method="GET",
            state=SimpleNamespace(auth_mode="supabase_jwt"),
            url=SimpleNamespace(path="/api/tradsphere/v1/contacts"),
        )
        with self.assertRaises(HTTPException):
            enforce_tradsphere_permission(request)

    def test_editor_required_for_writes(self):
        profile = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="tradsphere",
            role="viewer",
            permissions=frozenset({"tradsphere.viewer"}),
        )
        request = SimpleNamespace(
            method="POST",
            state=SimpleNamespace(auth_mode="supabase_jwt", tenant_access=profile),
            url=SimpleNamespace(path="/api/tradsphere/v1/contacts"),
        )
        with self.assertRaises(HTTPException):
            enforce_tradsphere_permission(request)

    def test_feature_viewer_permission_allows_contacts_get(self):
        profile = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="tradsphere",
            role="viewer",
            permissions=frozenset({"tradsphere.contacts.viewer"}),
        )
        request = SimpleNamespace(
            method="GET",
            state=SimpleNamespace(auth_mode="supabase_jwt", tenant_access=profile),
            url=SimpleNamespace(path="/api/tradsphere/v1/contacts"),
        )
        enforce_tradsphere_permission(request)  # should not raise

    def test_super_admin_bypasses_route_permission_checks(self):
        profile = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="tradsphere",
            role="super_admin",
            permissions=frozenset({"workspace.super_admin"}),
        )
        request = SimpleNamespace(
            method="POST",
            state=SimpleNamespace(auth_mode="supabase_jwt", tenant_access=profile),
            url=SimpleNamespace(path="/api/tradsphere/v1/estNums"),
        )
        enforce_tradsphere_permission(request)  # should not raise


if __name__ == "__main__":
    unittest.main()
