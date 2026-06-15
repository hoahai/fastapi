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

    def test_validate_tenant_config_accepts_optional_holidays_table_mapping(self):
        with patch.object(config, "get_tenant_id", return_value="taaa"), patch.object(
            config, "_has_leavesphere_config", return_value=True
        ), patch.object(
            config,
            "get_db_tables",
            return_value={**dict(config._DEFAULT_DB_TABLES), "HOLIDAYS": "Holidays"},
        ):
            config.validate_tenant_config()

    def test_validate_tenant_config_accepts_employee_email_map(self):
        with patch.object(config, "get_tenant_id", return_value="taaa"), patch.object(
            config, "_has_leavesphere_config", return_value=True
        ), patch.object(config, "get_db_tables", return_value=dict(config._DEFAULT_DB_TABLES)), patch.object(
            config,
            "get_employee_email_map",
            return_value={"login@example.com": "employee@example.com"},
        ):
            config.validate_tenant_config()

    def test_get_employee_email_map_parses_and_normalizes_aliases(self):
        with patch.object(
            config,
            "get_app_scoped_env",
            return_value="{'Login@Example.com': 'Employee@Example.com'}",
        ):
            self.assertEqual(
                config.get_employee_email_map(),
                {"login@example.com": "employee@example.com"},
            )

    def test_validate_tenant_config_rejects_invalid_employee_email_map(self):
        with patch.object(config, "get_tenant_id", return_value="taaa"), patch.object(
            config, "_has_leavesphere_config", return_value=True
        ), patch.object(config, "get_db_tables", return_value=dict(config._DEFAULT_DB_TABLES)), patch.object(
            config,
            "get_app_scoped_env",
            return_value="{'Login@Example.com': 'not-an-email'}",
        ):
            with self.assertRaises(TenantConfigValidationError) as exc:
                config.validate_tenant_config()

        self.assertIn("leavesphere.EMPLOYEE_EMAIL_MAP.Login@Example.com", exc.exception.invalid)


if __name__ == "__main__":
    unittest.main()
