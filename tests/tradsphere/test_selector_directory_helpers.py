import unittest
from unittest.mock import patch

from apps.tradsphere.api.v1.helpers import accounts as accounts_helper
from apps.tradsphere.api.v1.helpers import contacts as contacts_helper


class AccountsDirectoryHelperTests(unittest.TestCase):
    def test_list_accounts_directory_maps_lightweight_fields(self):
        with patch.object(
            accounts_helper,
            "get_accounts_directory",
            return_value=[
                {
                    "accountCode": "taaa",
                    "accountName": "Alpha Motors",
                    "billingType": "Calendar",
                    "active": 1,
                    "market": "ignored",
                }
            ],
        ) as mock_get:
            rows = accounts_helper.list_accounts_directory(active=False)

        self.assertEqual(
            rows,
            [
                {
                    "accountCode": "TAAA",
                    "accountName": "Alpha Motors",
                    "billingType": "Calendar",
                    "active": 1,
                }
            ],
        )
        self.assertEqual(mock_get.call_args.kwargs["active_only"], False)


class ContactsSelectorHelperTests(unittest.TestCase):
    def test_list_contacts_selector_data_maps_contact_fields(self):
        with patch.object(
            contacts_helper,
            "get_contacts_selector",
            return_value=[
                {
                    "contactId": 12,
                    "firstName": "Mina",
                    "lastName": "Tran",
                    "contactEmail": "rep@kabc.com",
                    "office": "213-555-0100",
                    "cell": "213-555-0101",
                    "company": "KABC",
                    "jobTitle": "Sales Rep",
                    "note": "",
                    "active": 1,
                }
            ],
        ) as mock_get:
            rows = contacts_helper.list_contacts_selector_data(active=True)

        self.assertEqual(
            rows,
            [
                {
                    "contactId": 12,
                    "firstName": "Mina",
                    "lastName": "Tran",
                    "contactName": "Mina Tran",
                    "contactEmail": "rep@kabc.com",
                    "office": "213-555-0100",
                    "cell": "213-555-0101",
                    "company": "KABC",
                    "jobTitle": "Sales Rep",
                    "note": "",
                    "active": 1,
                }
            ],
        )
        self.assertEqual(mock_get.call_args.kwargs["active_only"], True)

    def test_list_contacts_selector_data_skips_invalid_contact_id_rows(self):
        with patch.object(
            contacts_helper,
            "get_contacts_selector",
            return_value=[
                {"contactId": "bad"},
                {"contactId": 9, "firstName": "Ari", "lastName": "Nguyen", "contactEmail": "ari@example.com"},
            ],
        ):
            rows = contacts_helper.list_contacts_selector_data(active=True)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["contactId"], 9)
        self.assertEqual(rows[0]["contactName"], "Ari Nguyen")


if __name__ == "__main__":
    unittest.main()
