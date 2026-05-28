import unittest
from unittest.mock import patch

from shared.tenant import TenantConfigValidationError
from apps.leavesphere.api.v1.helpers import config


class LeaveSphereConfigTests(unittest.TestCase):
    def setUp(self):
        config._VALIDATED_TENANTS.clear()

    def test_validate_tenant_config_requires_leavesphere_section(self):
        with patch.object(config, "get_tenant_id", return_value="taaa"), patch.object(
            config, "_has_leavesphere_config", return_value=False
        ):
            with self.assertRaises(TenantConfigValidationError) as exc:
                config.validate_tenant_config()

        self.assertIn("leavesphere", exc.exception.missing)

    def test_validate_tenant_config_accepts_present_config(self):
        with patch.object(config, "get_tenant_id", return_value="taaa"), patch.object(
            config, "_has_leavesphere_config", return_value=True
        ), patch.object(config, "get_db_tables", return_value=dict(config._DEFAULT_DB_TABLES)):
            config.validate_tenant_config()


if __name__ == "__main__":
    unittest.main()
