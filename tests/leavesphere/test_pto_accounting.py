import unittest
from decimal import Decimal

from apps.leavesphere.api.v1.helpers.ptoAccounting import (
    is_load_action_code,
    is_request_action_code,
    request_action_sql,
    resolve_action_code,
    signed_pto_hours,
)


class LeaveSpherePtoAccountingTests(unittest.TestCase):
    def test_resolve_action_code_prefers_matching_tokens(self):
        catalog = [
            {"code": "LOAD", "name": "Load"},
            {"code": "REQUEST", "name": "Request"},
            {"code": "ADJUST", "name": "Adjustment"},
        ]

        self.assertEqual(resolve_action_code(catalog, include_tokens=("request",)), "REQUEST")
        self.assertEqual(resolve_action_code(catalog, include_tokens=("load", "grant")), "LOAD")
        self.assertEqual(resolve_action_code(catalog, include_tokens=("adjust",)), "ADJUST")

    def test_sign_helpers_treat_load_as_positive_and_request_as_negative(self):
        self.assertTrue(is_load_action_code("LOAD"))
        self.assertTrue(is_request_action_code("REQUEST"))
        self.assertEqual(signed_pto_hours("LOAD", Decimal("8.00")), Decimal("8.00"))
        self.assertEqual(signed_pto_hours("REQUEST", Decimal("8.00")), Decimal("-8.00"))
        self.assertEqual(signed_pto_hours("ADJUST", Decimal("-4.00")), Decimal("-4.00"))

    def test_request_action_sql_targets_request_rows(self):
        self.assertEqual(request_action_sql(), "UPPER(ptoActionCode) LIKE '%REQ%'")


if __name__ == "__main__":
    unittest.main()
