import unittest

from shared.auth.quick_approval_url import build_quick_approval_url


class QuickApprovalUrlTests(unittest.TestCase):
    def test_build_quick_approval_url_without_base(self) -> None:
        self.assertEqual(
            build_quick_approval_url(token="abc123", base_url=""),
            "/leavesphere/quick-approval/abc123",
        )

    def test_build_quick_approval_url_with_host_base(self) -> None:
        self.assertEqual(
            build_quick_approval_url(token="abc123", base_url="https://workspace.example.com"),
            "https://workspace.example.com/leavesphere/quick-approval/abc123",
        )

    def test_build_quick_approval_url_with_path_base(self) -> None:
        self.assertEqual(
            build_quick_approval_url(
                token="abc123",
                base_url="https://workspace.example.com/leavesphere/quick-approval/",
            ),
            "https://workspace.example.com/leavesphere/quick-approval/abc123",
        )


if __name__ == "__main__":
    unittest.main()
