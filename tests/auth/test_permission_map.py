import unittest

from shared.auth.permission_map import expand_permissions_for_role


class PermissionMapTests(unittest.TestCase):
    def test_super_admin_permissions_include_admin_capabilities(self):
        perms = expand_permissions_for_role("super_admin")
        self.assertIn("workspace.super_admin", perms)
        self.assertIn("tradsphere.admin", perms)
        self.assertIn("tradsphere.invites.admin", perms)

    def test_viewer_permissions_include_base(self):
        perms = expand_permissions_for_role("viewer")
        self.assertIn("tradsphere.viewer", perms)
        self.assertIn("tradsphere.contacts.viewer", perms)
        self.assertNotIn("tradsphere.editor", perms)

    def test_editor_permissions_include_feature_editors(self):
        perms = expand_permissions_for_role("editor")
        self.assertIn("tradsphere.editor", perms)
        self.assertIn("tradsphere.contacts.editor", perms)
        self.assertIn("tradsphere.stations.editor", perms)


if __name__ == "__main__":
    unittest.main()
