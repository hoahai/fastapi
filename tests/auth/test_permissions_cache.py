import unittest

from shared.auth.permissions_cache import PermissionCache
from shared.auth.types import TenantAccessProfile


class PermissionCacheTests(unittest.TestCase):
    def test_set_and_get(self):
        cache = PermissionCache()
        profile = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="tradsphere",
            role="viewer",
            permissions=frozenset({"tradsphere.viewer"}),
        )
        cache.set(user_id="u1", tenant_slug="taaa", app_code="tradsphere", value=profile)
        cached = cache.get(user_id="u1", tenant_slug="taaa", app_code="tradsphere")
        self.assertIsNotNone(cached)
        self.assertEqual(cached.role, "viewer")

    def test_invalidate(self):
        cache = PermissionCache()
        profile = TenantAccessProfile(
            tenant_id="tid",
            tenant_slug="taaa",
            app_id="aid",
            app_code="tradsphere",
            role="viewer",
            permissions=frozenset({"tradsphere.viewer"}),
        )
        cache.set(user_id="u1", tenant_slug="taaa", app_code="tradsphere", value=profile)
        removed = cache.invalidate(user_id="u1")
        self.assertEqual(removed, 1)
        self.assertIsNone(cache.get(user_id="u1", tenant_slug="taaa", app_code="tradsphere"))


if __name__ == "__main__":
    unittest.main()
