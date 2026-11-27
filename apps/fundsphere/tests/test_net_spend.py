from __future__ import annotations

from decimal import Decimal
import unittest

from apps.fundsphere.api.v1.helpers.netSpend import (
    build_net_spend_rows,
    build_rollover_rows,
    parse_service_catalog,
    parse_service_grouping_config,
)


class NetSpendTests(unittest.TestCase):
    def setUp(self):
        self.catalog = parse_service_catalog(
            [
                ["sem", "Search Engine Marketing", "1. Google", "12"],
                ["smm", "Social Media Management", "3. Internal", "43"],
                ["pkg", "Advanced Organic Social Media", "1. Package", "3"],
            ]
        )

    def test_backend_calculates_net_value_and_ignores_empty_accounts(self):
        rows = build_net_spend_rows(
            [
                {
                    "accountCode": "AUC",
                    "accountName": "Auto City Credit",
                    "serviceId": "sem",
                    "serviceName": "Search Engine Marketing",
                    "grossAmount": "11503.00",
                    "commission": "32",
                    "netAdjustment": "0",
                },
                {
                    "accountCode": "",
                    "accountName": "",
                    "serviceId": "sem",
                    "grossAmount": "100",
                    "commission": "0",
                },
            ],
            service_catalog=self.catalog,
            month=11,
            year=2026,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["Value"], Decimal("7822.04"))

    def test_combined_rows_include_pivot_fields(self):
        rows = build_net_spend_rows(
            [
                {
                    "accountCode": "AUC",
                    "accountName": "Auto City Credit",
                    "serviceId": "sem",
                    "grossAmount": "11503",
                    "commission": "32",
                    "netAdjustment": "0",
                },
                {
                    "accountCode": "AUC",
                    "accountName": "Auto City Credit",
                    "serviceId": "smm",
                    "subService": "Advanced Website Management",
                    "grossAmount": "0",
                    "commission": "0",
                },
                {
                    "accountCode": "AUC",
                    "accountName": "Auto City Credit",
                    "serviceId": "pkg",
                    "subService": "Content Development",
                    "grossAmount": "0",
                    "commission": "0",
                },
            ],
            service_catalog=self.catalog,
            month=11,
            year=2026,
        )

        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["serviceGroup"], "1. Package")
        self.assertEqual(rows[0]["groupOrder"], 3)
        self.assertEqual(rows[0]["Value"], Decimal("0"))
        self.assertEqual(rows[1]["serviceGroup"], "1. Google")
        self.assertEqual(rows[1]["Value"], Decimal("7822.04"))

    def test_tenant_service_grouping_config_maps_group_and_order(self):
        catalog = parse_service_grouping_config(
            [
                {
                    "group": "1. Google",
                    "services": [
                        {
                            "id": "sem",
                            "name": "Search Engine Marketing",
                        }
                    ],
                }
            ]
        )

        self.assertEqual(
            catalog,
            [
                {
                    "serviceId": "sem",
                    "serviceName": "Search Engine Marketing",
                    "serviceGroup": "1. Google",
                    "groupOrder": 1,
                }
            ],
        )

    def test_rollovers_are_filtered_and_mapped_to_report_rows(self):
        rows = build_rollover_rows(
            [
                {
                    "Account Name": "Royal Nissan",
                    "Month": "11",
                    "Year": "2026",
                    "Vendor": "Google Ads",
                    "Roll Amount to Month": "$723.13",
                    "Rollable": "1",
                    "Note": "",
                    "accountCode": "RN",
                },
                {
                    "Account Name": "Royal Nissan",
                    "Month": "11",
                    "Year": "2026",
                    "Vendor": "Google Ads",
                    "Roll Amount to Month": "$20.00",
                    "Rollable": "0",
                    "accountCode": "RN",
                },
            ],
            vendor_mappings=[
                {
                    "vendor": "Google Ads",
                    "serviceId": "google-roll-over",
                    "serviceName": "Google Ads Rollover",
                    "departmentName": "Digital Media",
                    "budgetId": "roll-id-digital",
                    "commission": "0.0001",
                    "netAdjustment": 0,
                }
            ],
            month=11,
            year=2026,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["accountCode"], "RN")
        self.assertEqual(rows[0]["grossAmount"], Decimal("723.13"))


if __name__ == "__main__":
    unittest.main()
