import unittest
from unittest.mock import patch

from shared.auth.admin_repo import list_tenant_users_with_app_role, list_users_with_access, set_tenant_user_status


class _FakeProvider:
    def __init__(self):
        self.membership_rows = []
        self.role_rows = []
        self.tenant_rows = []
        self.app_rows = []
        self.profiles_by_user_id = {}
        self.profiles_by_id = {}
        self.invitations_by_user_id = {}
        self.patch_calls = []
        self.now_iso_value = "2026-05-15T00:00:00Z"

    def select_many(self, *, table: str, filters: dict[str, str], select: str = "*"):
        if table == "tenant_users":
            return list(self.membership_rows)
        if table == "tenant_app_roles":
            return list(self.role_rows)
        if table == "tenants":
            return list(self.tenant_rows)
        if table == "apps":
            return list(self.app_rows)
        if table == "invitations":
            user_id = str(filters.get("accepted_by_user_id") or "").strip()
            return list(self.invitations_by_user_id.get(user_id, []))
        return []

    def patch_rows(self, *, table: str, filters: dict[str, str], patch: dict[str, object]):
        self.patch_calls.append({"table": table, "filters": filters, "patch": patch})

    def insert_row(self, *, table: str, row: dict[str, object]):
        raise AssertionError("insert_row should not be called in this test")

    def now_iso(self):
        return self.now_iso_value

    def select_single(self, *, table: str, filters: dict[str, str], select: str = "*"):
        if table != "profiles":
            return None
        if "user_id" in filters:
            return self.profiles_by_user_id.get(str(filters["user_id"]))
        if "id" in filters:
            return self.profiles_by_id.get(str(filters["id"]))
        return None


class AdminRepoTests(unittest.TestCase):
    def test_list_users_falls_back_to_profile_id_lookup(self):
        provider = _FakeProvider()
        provider.membership_rows = [
            {"id": "m1", "user_id": "user-1", "status": "active", "created_at": "2026-05-10T00:00:00Z", "updated_at": "2026-05-10T00:00:00Z"}
        ]
        provider.role_rows = [
            {
                "id": "r1",
                "tenant_id": "tenant-1",
                "user_id": "user-1",
                "app_id": "app-1",
                "role": "viewer",
                "created_at": "2026-05-10T00:00:00Z",
                "updated_at": "2026-05-10T00:00:00Z",
            }
        ]
        provider.profiles_by_id["user-1"] = {
            "id": "user-1",
            "email": "id-lookup@example.com",
            "full_name": "ID Lookup",
            "created_at": "2026-05-10T00:00:00Z",
            "updated_at": "2026-05-10T00:00:00Z",
        }

        with patch("shared.auth.admin_repo.get_auth_provider", return_value=provider):
            rows = list_tenant_users_with_app_role(tenant_id="tenant-1", app_id="app-1")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["userId"], "user-1")
        self.assertEqual(rows[0]["email"], "id-lookup@example.com")

    def test_list_users_falls_back_to_accepted_invitation_email(self):
        provider = _FakeProvider()
        provider.membership_rows = [
            {"id": "m2", "user_id": "user-2", "status": "active", "created_at": "2026-05-11T00:00:00Z", "updated_at": "2026-05-11T00:00:00Z"}
        ]
        provider.role_rows = [
            {
                "id": "r2",
                "tenant_id": "tenant-1",
                "user_id": "user-2",
                "app_id": "app-1",
                "role": "viewer",
                "created_at": "2026-05-11T00:00:00Z",
                "updated_at": "2026-05-11T00:00:00Z",
            }
        ]
        provider.invitations_by_user_id["user-2"] = [
            {"email": "older@example.com", "accepted_at": "2026-05-10T00:00:00Z", "created_at": "2026-05-10T00:00:00Z"},
            {"email": "newer@example.com", "accepted_at": "2026-05-11T00:00:00Z", "created_at": "2026-05-11T00:00:00Z"},
        ]

        with patch("shared.auth.admin_repo.get_auth_provider", return_value=provider):
            rows = list_tenant_users_with_app_role(tenant_id="tenant-1", app_id="app-1")

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["userId"], "user-2")
        self.assertEqual(rows[0]["email"], "newer@example.com")

    def test_list_users_with_access_excludes_assignments_for_disabled_membership(self):
        provider = _FakeProvider()
        provider.tenant_rows = [{"id": "tenant-1", "slug": "taaa", "name": "Tenant AAA", "active": True}]
        provider.app_rows = [{"id": "app-1", "code": "tradsphere", "name": "TradSphere", "active": True}]
        provider.membership_rows = [
            {
                "id": "m3",
                "tenant_id": "tenant-1",
                "user_id": "user-3",
                "status": "disabled",
                "created_at": "2026-05-12T00:00:00Z",
                "updated_at": "2026-05-12T00:00:00Z",
            }
        ]
        provider.role_rows = [
            {
                "id": "r3",
                "tenant_id": "tenant-1",
                "user_id": "user-3",
                "app_id": "app-1",
                "role": "viewer",
                "created_at": "2026-05-12T00:00:00Z",
                "updated_at": "2026-05-12T00:00:00Z",
            }
        ]
        provider.profiles_by_id["user-3"] = {
            "id": "user-3",
            "email": "disabled@example.com",
            "full_name": "Disabled User",
            "created_at": "2026-05-12T00:00:00Z",
            "updated_at": "2026-05-12T00:00:00Z",
        }

        with patch("shared.auth.admin_repo.get_auth_provider", return_value=provider):
            rows = list_users_with_access(
                scope_tenant_ids={"tenant-1"},
                scope_app_ids={"app-1"},
                primary_tenant_id="tenant-1",
                primary_app_id="app-1",
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["userId"], "user-3")
        self.assertEqual(rows[0]["status"], "disabled")
        self.assertEqual(rows[0]["appAssignments"], [])

    def test_set_tenant_user_status_updates_all_matching_membership_rows(self):
        provider = _FakeProvider()
        provider.membership_rows = [
            {"id": "m1", "tenant_id": "tenant-1", "user_id": "user-1", "status": "active"},
            {"id": "m2", "tenant_id": "tenant-1", "user_id": "user-1", "status": "pending"},
        ]

        with patch("shared.auth.admin_repo.get_auth_provider", return_value=provider):
            set_tenant_user_status(tenant_id="tenant-1", user_id="user-1", status="disabled")

        self.assertEqual(len(provider.patch_calls), 1)
        patch_call = provider.patch_calls[0]
        self.assertEqual(patch_call["table"], "tenant_users")
        self.assertEqual(patch_call["filters"], {"tenant_id": "tenant-1", "user_id": "user-1"})
        self.assertEqual(patch_call["patch"]["status"], "disabled")


if __name__ == "__main__":
    unittest.main()
