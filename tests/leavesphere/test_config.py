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

    def test_validate_tenant_config_accepts_last_submission_date(self):
        with patch.object(config, "get_tenant_id", return_value="taaa"), patch.object(
            config, "_has_leavesphere_config", return_value=True
        ), patch.object(config, "get_db_tables", return_value=dict(config._DEFAULT_DB_TABLES)), patch.object(
            config,
            "get_employee_email_map",
            return_value={},
        ), patch.object(
            config,
            "get_last_submission_date",
            return_value="10-30",
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

    def test_get_reminder_cc_emails_parses_and_deduplicates(self):
        with patch.object(
            config,
            "get_app_scoped_env",
            return_value="['HR@example.com', 'ops@example.com', 'hr@example.com']",
        ):
            self.assertEqual(
                config.get_reminder_cc_emails(),
                ["hr@example.com", "ops@example.com"],
            )

    def test_get_action_cc_emails_parses_and_deduplicates(self):
        with patch.object(
            config,
            "get_app_scoped_env",
            return_value="['HR@example.com', 'ops@example.com', 'hr@example.com']",
        ):
            self.assertEqual(
                config.get_action_cc_emails(),
                ["hr@example.com", "ops@example.com"],
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

    def test_get_last_submission_date_normalizes_alias_format(self):
        with patch.object(config, "get_app_scoped_env", return_value="10/30"):
            self.assertEqual(config.get_last_submission_date(), "10-30")

    def test_is_submission_cutoff_passed_uses_current_year_only(self):
        with patch.object(config, "get_last_submission_date", return_value="10-30"):
            self.assertTrue(config.is_submission_cutoff_passed(year=2026, today=config.date(2026, 11, 1)))
            self.assertFalse(config.is_submission_cutoff_passed(year=2027, today=config.date(2026, 11, 1)))


if __name__ == "__main__":
    unittest.main()
