import unittest
from unittest.mock import patch

from shared.auth.permissions_repo import TenantAccessError, get_tenant_access_cached, resolve_tenant_access
from shared.auth.types import TenantAccessProfile


class _FakeProvider:
    def __init__(self, membership_status: str):
        self.membership_status = membership_status

    def select_single(self, table, filters, select):
        if table == "tenants":
            return {"id": "tenant-1", "slug": "taaa", "active": True}
        if table == "apps":
            return {"id": "app-1", "code": "tradsphere", "active": True}
        if table == "user_global_roles":
            return None
        if table == "tenant_app_roles":
            return {"id": "role-1", "role": "viewer"}
        raise AssertionError(f"Unexpected table lookup: {table}")

    def select_many(self, table, filters, select):
        if table == "tenant_users":
            return [
                {
                    "id": "membership-1",
                    "tenant_id": "tenant-1",
                    "user_id": "user-1",
                    "status": self.membership_status,
                }
            ]
        if table == "role_permissions":
            return [{"permission": "tradsphere.viewer"}]
        raise AssertionError(f"Unexpected table query: {table}")


class PermissionsRepoTests(unittest.TestCase):
    def test_resolve_tenant_access_rejects_disabled_membership(self):
        provider = _FakeProvider(membership_status="disabled")
        with patch("shared.auth.permissions_repo.get_auth_provider", return_value=provider):
            with self.assertRaises(TenantAccessError) as exc:
                resolve_tenant_access(user_id="user-1", tenant_slug="taaa", app_code="tradsphere")
        self.assertEqual(exc.exception.code, "tenant_membership_disabled")

    def test_resolve_tenant_access_rejects_pending_membership(self):
        provider = _FakeProvider(membership_status="pending")
        with patch("shared.auth.permissions_repo.get_auth_provider", return_value=provider):
            with self.assertRaises(TenantAccessError) as exc:
                resolve_tenant_access(user_id="user-1", tenant_slug="taaa", app_code="tradsphere")
        self.assertEqual(exc.exception.code, "tenant_membership_pending")

    def test_cached_access_rechecks_membership_status_for_non_super_admin(self):
        cached_access = TenantAccessProfile(
            tenant_id="tenant-1",
            tenant_slug="taaa",
            app_id="app-1",
            app_code="tradsphere",
            role="viewer",
            permissions=frozenset({"tradsphere.viewer"}),
        )
        with patch("shared.auth.permissions_repo.permission_cache.get", return_value=cached_access), patch(
            "shared.auth.permissions_repo._resolve_active_tenant_user",
            side_effect=TenantAccessError(
                "Your account has been disabled. Contact your workspace administrator.",
                code="tenant_membership_disabled",
            ),
        ), patch("shared.auth.permissions_repo.resolve_tenant_access") as mock_resolve:
            with self.assertRaises(TenantAccessError) as exc:
                get_tenant_access_cached(user_id="user-1", tenant_slug="taaa", app_code="tradsphere")

        mock_resolve.assert_not_called()
        self.assertEqual(exc.exception.code, "tenant_membership_disabled")

    def test_cached_access_skips_membership_recheck_for_super_admin(self):
        cached_access = TenantAccessProfile(
            tenant_id="tenant-1",
            tenant_slug="taaa",
            app_id="app-1",
            app_code="tradsphere",
            role="super_admin",
            permissions=frozenset({"workspace.super_admin", "tradsphere.admin"}),
        )
        with patch("shared.auth.permissions_repo.permission_cache.get", return_value=cached_access), patch(
            "shared.auth.permissions_repo._resolve_active_tenant_user"
        ) as mock_membership_check, patch("shared.auth.permissions_repo.resolve_tenant_access") as mock_resolve:
            resolved = get_tenant_access_cached(user_id="user-1", tenant_slug="taaa", app_code="tradsphere")

        mock_membership_check.assert_not_called()
        mock_resolve.assert_not_called()
        self.assertEqual(resolved.role, "super_admin")

    def test_resolve_tenant_access_rejects_when_any_duplicate_membership_is_disabled(self):
        class _DupProvider(_FakeProvider):
            def select_many(self, table, filters, select):
                if table == "tenant_users":
                    return [
                        {"id": "membership-a", "tenant_id": "tenant-1", "user_id": "user-1", "status": "active"},
                        {"id": "membership-b", "tenant_id": "tenant-1", "user_id": "user-1", "status": "disabled"},
                    ]
                return super().select_many(table, filters, select)

        provider = _DupProvider(membership_status="active")
        with patch("shared.auth.permissions_repo.get_auth_provider", return_value=provider):
            with self.assertRaises(TenantAccessError) as exc:
                resolve_tenant_access(user_id="user-1", tenant_slug="taaa", app_code="tradsphere")
        self.assertEqual(exc.exception.code, "tenant_membership_disabled")

    def test_super_admin_with_disabled_membership_is_rejected(self):
        class _SuperAdminProvider(_FakeProvider):
            def select_single(self, table, filters, select):
                if table == "user_global_roles":
                    return {"id": "global-role-1", "user_id": "user-1", "role": "super_admin", "active": True}
                return super().select_single(table, filters, select)

            def select_many(self, table, filters, select):
                if table == "tenant_users":
                    return [
                        {
                            "id": "membership-1",
                            "tenant_id": "tenant-1",
                            "user_id": "user-1",
                            "status": "disabled",
                        }
                    ]
                return super().select_many(table, filters, select)

        provider = _SuperAdminProvider(membership_status="active")
        with patch("shared.auth.permissions_repo.get_auth_provider", return_value=provider):
            with self.assertRaises(TenantAccessError) as exc:
                resolve_tenant_access(user_id="user-1", tenant_slug="taaa", app_code="tradsphere")
        self.assertEqual(exc.exception.code, "tenant_membership_disabled")

    def test_super_admin_without_membership_is_allowed(self):
        class _SuperAdminNoMembershipProvider(_FakeProvider):
            def select_single(self, table, filters, select):
                if table == "user_global_roles":
                    return {"id": "global-role-1", "user_id": "user-1", "role": "super_admin", "active": True}
                return super().select_single(table, filters, select)

            def select_many(self, table, filters, select):
                if table == "tenant_users":
                    return []
                return super().select_many(table, filters, select)

        provider = _SuperAdminNoMembershipProvider(membership_status="active")
        with patch("shared.auth.permissions_repo.get_auth_provider", return_value=provider):
            access = resolve_tenant_access(user_id="user-1", tenant_slug="taaa", app_code="tradsphere")
        self.assertEqual(access.role, "super_admin")
        self.assertIn("workspace.super_admin", access.permissions)


if __name__ == "__main__":
    unittest.main()
