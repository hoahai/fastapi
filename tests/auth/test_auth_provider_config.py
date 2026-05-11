import os
import unittest

from shared.auth.config import get_auth_provider_name


class AuthProviderConfigTests(unittest.TestCase):
    def setUp(self):
        self._env = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._env)

    def test_defaults_to_supabase(self):
        os.environ.pop("AUTH_PROVIDER", None)
        self.assertEqual(get_auth_provider_name(), "supabase")

    def test_unknown_provider_falls_back_to_supabase(self):
        os.environ["AUTH_PROVIDER"] = "custom-provider"
        self.assertEqual(get_auth_provider_name(), "supabase")


if __name__ == "__main__":
    unittest.main()
