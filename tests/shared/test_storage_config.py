import unittest
from unittest.mock import patch

from shared.storage.config import get_cloudinary_config


class CloudinaryStorageConfigTests(unittest.TestCase):
    def test_prefers_flat_app_scoped_cloudinary_keys(self):
        with patch(
            "shared.storage.config.get_app_scoped_env",
            side_effect=lambda app_name, key: {
                "CLOUDINARY_CLOUD_NAME": "flat-cloud",
                "CLOUDINARY_API_KEY": "flat-key",
                "CLOUDINARY_API_SECRET": "flat-secret",
            }.get(key),
        ), patch("shared.storage.config.get_env", return_value=None):
            config = get_cloudinary_config(app_name="LeaveSphere")

        self.assertEqual(config.cloud_name, "flat-cloud")
        self.assertEqual(config.api_key, "flat-key")
        self.assertEqual(config.api_secret, "flat-secret")

    def test_falls_back_to_nested_app_scoped_cloudinary_object(self):
        with patch(
            "shared.storage.config.get_app_scoped_env",
            side_effect=lambda app_name, key: {
                "CLOUDINARY": "{'cloud_name': 'nested-cloud', 'api_key': 'nested-key', 'api_secret': 'nested-secret'}",
            }.get(key),
        ), patch("shared.storage.config.get_env", return_value=None):
            config = get_cloudinary_config(app_name="LeaveSphere")

        self.assertEqual(config.cloud_name, "nested-cloud")
        self.assertEqual(config.api_key, "nested-key")
        self.assertEqual(config.api_secret, "nested-secret")

    def test_falls_back_to_global_nested_cloudinary_object(self):
        with patch(
            "shared.storage.config.get_app_scoped_env",
            return_value=None,
        ), patch(
            "shared.storage.config.get_env",
            side_effect=lambda key: (
                '{"cloud_name": "global-cloud", "api_key": "global-key", "api_secret": "global-secret"}'
                if key == "CLOUDINARY"
                else None
            ),
        ):
            config = get_cloudinary_config(app_name="TradSphere")

        self.assertEqual(config.cloud_name, "global-cloud")
        self.assertEqual(config.api_key, "global-key")
        self.assertEqual(config.api_secret, "global-secret")


if __name__ == "__main__":
    unittest.main()
