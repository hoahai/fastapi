from __future__ import annotations

import unittest
from unittest.mock import patch

from apps.spendsphere.api.v1.helpers.dataTransform import generate_update_payloads
from apps.spendsphere.api.v1.helpers.email import (
    _format_issue_title,
    _normalize_budget_issue,
)
from apps.spendsphere.api.v1.helpers.ggAd import (
    get_demand_gen_minimum_budget,
    validate_updates,
)


class GoogleAdsMutationRegressionTests(unittest.TestCase):
    def test_demand_gen_minimum_budget_uses_compatible_gaql_resources(self):
        class _Currency:
            currency_code = "USD"

        class _DemandGenInfo:
            minimum_budget_per_day_micros = 5_000_000

        class _CurrencyConstant:
            demand_gen_info = _DemandGenInfo()

        class _CurrencyRow:
            customer = _Currency()

        class _MinimumRow:
            currency_constant = _CurrencyConstant()

        class _GoogleAdsService:
            def __init__(self):
                self.queries = []

            def search(self, *, customer_id, query):
                self.queries.append((customer_id, query))
                if "FROM customer" in query:
                    return [_CurrencyRow()]
                return [_MinimumRow()]

        class _Client:
            def __init__(self):
                self.service = _GoogleAdsService()

            def get_service(self, name):
                self.service_name = name
                return self.service

        client = _Client()
        with patch(
            "apps.spendsphere.api.v1.helpers.ggAd.get_client",
            return_value=client,
        ):
            minimum = get_demand_gen_minimum_budget("7127968448")

        self.assertEqual(minimum, 5)
        self.assertEqual(len(client.service.queries), 2)
        self.assertIn("FROM customer", client.service.queries[0][1])
        self.assertIn("FROM currency_constant", client.service.queries[1][1])
        self.assertIn("currency_constant.code = 'USD'", client.service.queries[1][1])

    def test_non_positive_demand_gen_budget_only_generates_pause_status(self):
        row = {
            "ggAccountId": "1234567890",
            "budgetId": "15423514704",
            "accountCode": "TAAA",
            "allocation": 100,
            "dailyBudget": 0,
            "budgetAmount": 25,
            "isActive": True,
            "totalCost": 0,
            "campaigns": [
                {
                    "campaignId": "987654321",
                    "campaignName": "Demand Gen Campaign",
                    "status": "ENABLED",
                    "channelType": "DEMAND_GEN",
                }
            ],
        }

        with patch(
            "apps.spendsphere.api.v1.helpers.dataTransform.get_google_ads_inactive_prefixes",
            return_value=("zzz.",),
        ):
            budget_payloads, campaign_payloads = generate_update_payloads([row])

        self.assertEqual(budget_payloads, [])
        self.assertEqual(len(campaign_payloads), 1)
        self.assertEqual(campaign_payloads[0]["updates"][0]["newStatus"], "PAUSED")

    def test_non_positive_non_demand_gen_budget_keeps_legacy_fallback(self):
        with patch(
            "apps.spendsphere.api.v1.helpers.dataTransform.get_google_ads_inactive_prefixes",
            return_value=("zzz.",),
        ):
            budget_payloads, campaign_payloads = generate_update_payloads(
                [
                    {
                        "ggAccountId": "1234567890",
                        "budgetId": "15423514704",
                        "accountCode": "TAAA",
                        "allocation": 100,
                        "dailyBudget": 0,
                        "budgetAmount": 25,
                        "isActive": True,
                        "totalCost": 0,
                        "campaigns": [
                            {
                                "campaignId": "987654321",
                                "campaignName": "Search Campaign",
                                "status": "ENABLED",
                                "channelType": "SEARCH",
                            }
                        ],
                    }
                ]
            )

        self.assertEqual(budget_payloads[0]["updates"][0]["newAmount"], 0.01)
        self.assertEqual(campaign_payloads[0]["updates"][0]["newStatus"], "PAUSED")

    def test_demand_gen_budget_below_google_minimum_is_skipped_and_paused(self):
        row = {
            "ggAccountId": "7127968448",
            "budgetId": "15423514704",
            "accountCode": "NMT",
            "allocation": 59.575,
            "dailyBudget": 0.07,
            "budgetAmount": 19.35,
            "demandGenMinimumBudget": 5.0,
            "isActive": True,
            "totalCost": 186.19,
            "campaigns": [
                {
                    "campaignId": "23637174717",
                    "campaignName": "NMT_VID_English Offers",
                    "status": "ENABLED",
                    "channelType": "DEMAND_GEN",
                }
            ],
        }

        with patch(
            "apps.spendsphere.api.v1.helpers.dataTransform.get_google_ads_inactive_prefixes",
            return_value=("zzz.",),
        ):
            budget_payloads, campaign_payloads = generate_update_payloads([row])

        self.assertEqual(budget_payloads, [])
        self.assertEqual(campaign_payloads[0]["updates"][0]["newStatus"], "PAUSED")

    def test_zzz_campaign_skips_status_but_keeps_budget_mutation(self):
        row = {
            "ggAccountId": "7127968448",
            "budgetId": "15423514704",
            "accountCode": "NMT",
            "allocation": 59.575,
            "dailyBudget": 10.0,
            "budgetAmount": 5.0,
            "isActive": True,
            "totalCost": 0,
            "campaigns": [
                {
                    "campaignId": "23637174717",
                    "campaignName": "zzz.NMT_VID_English Offers",
                    "status": "ENABLED",
                    "channelType": "SEARCH",
                }
            ],
        }

        with patch(
            "apps.spendsphere.api.v1.helpers.dataTransform.get_google_ads_inactive_prefixes",
            return_value=("zzz.",),
        ):
            budget_payloads, campaign_payloads = generate_update_payloads([row])

        self.assertEqual(budget_payloads[0]["updates"][0]["newAmount"], 10.0)
        self.assertEqual(campaign_payloads, [])

    def test_direct_budget_validation_keeps_non_positive_legacy_behavior(self):
        valid, invalid = validate_updates(
            customer_id="1234567890",
            updates=[
                {
                    "budgetId": "15423514704",
                    "currentAmount": 25,
                    "newAmount": 0,
                    "accountCode": "TAAA",
                    "campaignNames": ["Video Campaign"],
                }
            ],
            mode="budget",
        )

        self.assertEqual(len(valid), 1)
        self.assertEqual(invalid, [])

    def test_budget_failure_alert_uses_campaign_names(self):
        issue = _normalize_budget_issue(
            {
                "budgetId": "15423514704",
                "campaignNames": ["Video Campaign"],
                "accountCode": "TAAA",
                "errorCode": "campaign_budget_error.BUDGET_BELOW_PER_DAY_MINIMUM",
                "error": "Budget amount is below the required minimum.",
            },
            "TAAA",
            operation="update_budgets",
        )

        title = _format_issue_title(issue)

        self.assertIn("Video Campaign", title)
        self.assertNotIn("Unknown Campaign", title)


if __name__ == "__main__":
    unittest.main()
