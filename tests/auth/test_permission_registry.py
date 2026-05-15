import unittest

from shared.auth.permission_registry import resolve_required_permissions


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


if __name__ == "__main__":
    unittest.main()
