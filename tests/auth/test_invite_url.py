import unittest

from shared.auth.invite_url import build_invite_url


class InviteUrlTests(unittest.TestCase):
    def test_build_invite_url_without_base(self):
        self.assertEqual(build_invite_url(token="abc123", base_url=""), "/auth/invite/abc123")

    def test_build_invite_url_with_host_base(self):
        self.assertEqual(
            build_invite_url(token="abc123", base_url="https://workspace.example.com"),
            "https://workspace.example.com/auth/invite/abc123",
        )

    def test_build_invite_url_with_auth_invite_base(self):
        self.assertEqual(
            build_invite_url(token="abc123", base_url="https://workspace.example.com/auth/invite"),
            "https://workspace.example.com/auth/invite/abc123",
        )

    def test_build_invite_url_with_auth_invite_base_trailing_slash(self):
        self.assertEqual(
            build_invite_url(token="abc123", base_url="https://workspace.example.com/auth/invite/"),
            "https://workspace.example.com/auth/invite/abc123",
        )


if __name__ == "__main__":
    unittest.main()
