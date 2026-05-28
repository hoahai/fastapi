import os
import unittest
from types import SimpleNamespace

from fastapi import HTTPException

from shared.auth.dependencies import enforce_leavesphere_permission
from shared.auth.types import TenantAccessProfile
from apps.leavesphere.api.v1.permissions import (
    require_leavesphere_admin,
    require_leavesphere_editor,
)


class LeaveSpherePermissionDependencyTests(unittest.TestCase):
    def setUp(self):
        self._env = dict(os.environ)
        os.environ["AUTH_MODE"] = "compat"
        os.environ["AUTH_ENABLE_LEGACY_API_KEY_FALLBACK"] = "true"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._env)

    def test_legacy_mode_allows_bypass(self):
        request = SimpleNamespace(
            method="GET",
            state=SimpleNamespace(auth_mode="legacy_api_key"),
            url=SimpleNamespace(path="/api/leavesphere/v1/employees"),
        )
        enforce_leavesphere_permission(request)

    def test_requires_access_without_legacy(self):
        request = SimpleNamespace(
            method="GET",
            state=SimpleNamespace(auth_mode="supabase_jwt"),
            url=SimpleNamespace(path="/api/leavesphere/v1/employees"),
        )
        with self.assertRaises(HTTPException):
            enforce_leavesphere_permission(request)

    def test_viewer_allows_reads(self):
        profile = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="viewer",
            permissions=frozenset({"leavesphere.viewer"}),
        )
        request = SimpleNamespace(
            method="GET",
            state=SimpleNamespace(auth_mode="supabase_jwt", tenant_access=profile),
            url=SimpleNamespace(path="/api/leavesphere/v1/ptoTransactions"),
        )
        enforce_leavesphere_permission(request)

    def test_editor_required_for_writes(self):
        profile = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="viewer",
            permissions=frozenset({"leavesphere.viewer"}),
        )
        request = SimpleNamespace(
            method="POST",
            state=SimpleNamespace(auth_mode="supabase_jwt", tenant_access=profile),
            url=SimpleNamespace(path="/api/leavesphere/v1/ptoTransactions"),
        )
        with self.assertRaises(HTTPException):
            enforce_leavesphere_permission(request)

    def test_super_admin_bypasses_checks(self):
        profile = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="super_admin",
            permissions=frozenset({"workspace.super_admin"}),
        )
        request = SimpleNamespace(
            method="POST",
            state=SimpleNamespace(auth_mode="supabase_jwt", tenant_access=profile),
            url=SimpleNamespace(path="/api/leavesphere/v1/ptoTransactions"),
        )
        enforce_leavesphere_permission(request)

    def test_admin_permission_dependency_requires_admin_for_jwt(self):
        viewer = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="viewer",
            permissions=frozenset({"leavesphere.viewer"}),
        )
        editor = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="editor",
            permissions=frozenset({"leavesphere.viewer", "leavesphere.editor"}),
        )
        admin = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="admin",
            permissions=frozenset({"leavesphere.viewer", "leavesphere.editor", "leavesphere.admin"}),
        )
        super_admin = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="super_admin",
            permissions=frozenset({"workspace.super_admin"}),
        )

        for blocked in (viewer, editor):
            request = SimpleNamespace(state=SimpleNamespace(auth_mode="supabase_jwt", tenant_access=blocked))
            with self.assertRaises(HTTPException):
                require_leavesphere_admin(request)

        for allowed in (admin, super_admin):
            request = SimpleNamespace(state=SimpleNamespace(auth_mode="supabase_jwt", tenant_access=allowed))
            require_leavesphere_admin(request)

    def test_editor_permission_dependency_requires_editor_for_jwt(self):
        viewer = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="viewer",
            permissions=frozenset({"leavesphere.viewer"}),
        )
        editor = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="editor",
            permissions=frozenset({"leavesphere.viewer", "leavesphere.editor"}),
        )
        admin = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="admin",
            permissions=frozenset({"leavesphere.viewer", "leavesphere.editor", "leavesphere.admin"}),
        )
        super_admin = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="super_admin",
            permissions=frozenset({"workspace.super_admin"}),
        )

        blocked_request = SimpleNamespace(state=SimpleNamespace(auth_mode="supabase_jwt", tenant_access=viewer))
        with self.assertRaises(HTTPException):
            require_leavesphere_editor(blocked_request)

        for allowed in (editor, admin, super_admin):
            request = SimpleNamespace(state=SimpleNamespace(auth_mode="supabase_jwt", tenant_access=allowed))
            require_leavesphere_editor(request)

    def test_admin_permission_dependency_preserves_legacy_compat_bypass(self):
        request = SimpleNamespace(state=SimpleNamespace(auth_mode="legacy_api_key"))
        require_leavesphere_admin(request)

    def test_editor_permission_dependency_preserves_legacy_compat_bypass(self):
        request = SimpleNamespace(state=SimpleNamespace(auth_mode="legacy_api_key"))
        require_leavesphere_editor(request)

    def test_submit_cancel_approve_reject_paths_require_editor_by_route_permission(self):
        viewer = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="viewer",
            permissions=frozenset({"leavesphere.viewer"}),
        )
        editor = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="editor",
            permissions=frozenset({"leavesphere.viewer", "leavesphere.editor"}),
        )
        admin = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="leavesphere",
            role="admin",
            permissions=frozenset({"leavesphere.viewer", "leavesphere.editor", "leavesphere.admin"}),
        )

        blocked_request = SimpleNamespace(
            method="POST",
            state=SimpleNamespace(auth_mode="supabase_jwt", tenant_access=viewer),
            url=SimpleNamespace(path="/api/leavesphere/v1/ptoTransactions/requests"),
        )
        with self.assertRaises(HTTPException):
            enforce_leavesphere_permission(blocked_request)

        for path in (
            "/api/leavesphere/v1/ptoTransactions/requests",
            "/api/leavesphere/v1/ptoTransactions/abc/cancel",
            "/api/leavesphere/v1/ptoTransactions/abc/approve",
            "/api/leavesphere/v1/ptoTransactions/abc/reject",
        ):
            for profile in (editor, admin):
                request = SimpleNamespace(
                    method="POST",
                    state=SimpleNamespace(auth_mode="supabase_jwt", tenant_access=profile),
                    url=SimpleNamespace(path=path),
                )
                enforce_leavesphere_permission(request)


if __name__ == "__main__":
    unittest.main()
