import unittest

from shared.auth.supabase_client import SupabaseRestClient


class SupabaseClientHeaderTests(unittest.TestCase):
    def setUp(self):
        self.client = SupabaseRestClient()

    def test_sb_secret_service_key_omits_authorization_when_no_bearer(self):
        self.client.service_role_key = "sb_secret_example_key"
        headers = self.client._build_headers(
            bearer_token=None,
            use_service_role=True,
        )
        self.assertEqual(headers.get("apikey"), "sb_secret_example_key")
        self.assertNotIn("Authorization", headers)

    def test_legacy_jwt_service_key_keeps_authorization_compat(self):
        self.client.service_role_key = "aaa.bbb.ccc"
        headers = self.client._build_headers(
            bearer_token=None,
            use_service_role=True,
        )
        self.assertEqual(headers.get("apikey"), "aaa.bbb.ccc")
        self.assertEqual(headers.get("Authorization"), "Bearer aaa.bbb.ccc")

    def test_bearer_token_is_used_for_user_jwt(self):
        self.client.anon_key = "sb_publishable_example_key"
        headers = self.client._build_headers(
            bearer_token="user.jwt.token",
            use_service_role=False,
        )
        self.assertEqual(headers.get("apikey"), "sb_publishable_example_key")
        self.assertEqual(headers.get("Authorization"), "Bearer user.jwt.token")


if __name__ == "__main__":
    unittest.main()

