import unittest
from unittest.mock import patch

from fastapi import HTTPException

from apps.tradsphere.api.v1.endpoints.core.ui import main as ui_main


class UiAccountCreationLookupRouteTests(unittest.TestCase):
    def test_lookup_route_returns_lookup_payload(self):
        lookup_payload = {
            "accountCode": "TAAA",
            "accountName": "Alpha Motors",
            "logoUrl": "https://cdn.example.com/logos/taaa.png",
            "existsInTradSphere": False,
            "tradSphereAccountCode": None,
            "billingType": None,
            "market": None,
            "note": None,
            "active": 1,
        }

        with patch.object(ui_main, "get_account_creation_lookup", return_value=lookup_payload) as mock_lookup:
            result = ui_main.get_ui_account_creation_lookup_route(account_code="TAAA")

        self.assertEqual(result, lookup_payload)
        mock_lookup.assert_called_once_with("TAAA")

    def test_lookup_route_rejects_unknown_master_accounts(self):
        with patch.object(ui_main, "get_account_creation_lookup", return_value=None):
            with self.assertRaises(HTTPException) as context:
                ui_main.get_ui_account_creation_lookup_route(account_code="TAAA")

        self.assertEqual(context.exception.status_code, 400)
        self.assertIn("Unknown master accountCode values", str(context.exception.detail))


if __name__ == "__main__":
    unittest.main()
