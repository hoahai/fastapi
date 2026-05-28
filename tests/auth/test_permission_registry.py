import unittest

from shared.auth.permission_registry import (
    default_role_permissions_for_app,
    resolve_required_permissions,
)


class PermissionRegistryTests(unittest.TestCase):
    def test_tradsphere_contacts_get_uses_feature_viewer_permissions(self):
        permissions = resolve_required_permissions(
            app_code="tradsphere",
            method="GET",
            path="/api/tradsphere/v1/contacts",
        )
        self.assertIn("tradsphere.contacts.viewer", permissions)

    def test_tradsphere_contacts_post_uses_feature_editor_permissions(self):
        permissions = resolve_required_permissions(
            app_code="tradsphere",
            method="POST",
            path="/api/tradsphere/v1/contacts",
        )
        self.assertIn("tradsphere.contacts.editor", permissions)

    def test_non_tradsphere_defaults_to_app_viewer_for_reads(self):
        permissions = resolve_required_permissions(
            app_code="shiftzy",
            method="GET",
            path="/api/shiftzy/v1/schedules",
        )
        self.assertEqual(permissions, ("shiftzy.viewer",))

    def test_non_tradsphere_defaults_to_app_editor_for_writes(self):
        permissions = resolve_required_permissions(
            app_code="fundsphere",
            method="POST",
            path="/api/fundsphere/v1/masterBudgetControl/settings/accounts",
        )
        self.assertEqual(permissions, ("fundsphere.editor",))

    def test_leavesphere_defaults_to_app_viewer_for_reads(self):
        permissions = resolve_required_permissions(
            app_code="leavesphere",
            method="GET",
            path="/api/leavesphere/v1/ptoTransactions",
        )
        self.assertEqual(permissions, ("leavesphere.viewer",))

    def test_leavesphere_defaults_to_app_editor_for_writes(self):
        permissions = resolve_required_permissions(
            app_code="leavesphere",
            method="POST",
            path="/api/leavesphere/v1/ptoTransactions",
        )
        self.assertEqual(permissions, ("leavesphere.editor",))

    def test_leavesphere_default_role_permissions_are_expected(self):
        viewer = default_role_permissions_for_app(role="viewer", app_code="leavesphere")
        editor = default_role_permissions_for_app(role="editor", app_code="leavesphere")
        admin = default_role_permissions_for_app(role="admin", app_code="leavesphere")
        self.assertEqual(viewer, {"leavesphere.viewer"})
        self.assertEqual(editor, {"leavesphere.viewer", "leavesphere.editor"})
        self.assertEqual(
            admin,
            {"leavesphere.viewer", "leavesphere.editor", "leavesphere.admin"},
        )


if __name__ == "__main__":
    unittest.main()
