from __future__ import annotations

import unittest
from unittest.mock import patch

from apps.spendsphere.api.v1.helpers.dataTransform import generate_update_payloads
from apps.spendsphere.api.v1.helpers.email import (
    _format_issue_title,
    _normalize_budget_issue,
)
from apps.spendsphere.api.v1.helpers.ggAd import validate_updates


class GoogleAdsMutationRegressionTests(unittest.TestCase):
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
