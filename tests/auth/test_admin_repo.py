import unittest
from unittest.mock import patch

from shared.auth.admin_repo import list_tenant_users_with_app_role


class _FakeProvider:
    def __init__(self):
        self.membership_rows = []
        self.role_rows = []
        self.profiles_by_user_id = {}
        self.profiles_by_id = {}
        self.invitations_by_user_id = {}

    def select_many(self, *, table: str, filters: dict[str, str], select: str = "*"):
        if table == "tenant_users":
            return list(self.membership_rows)
        if table == "tenant_app_roles":
            return list(self.role_rows)
        if table == "invitations":
            user_id = str(filters.get("accepted_by_user_id") or "").strip()
            return list(self.invitations_by_user_id.get(user_id, []))
        return []

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
                "role": "tradsphere.viewer",
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
                "role": "tradsphere.viewer",
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


if __name__ == "__main__":
    unittest.main()
