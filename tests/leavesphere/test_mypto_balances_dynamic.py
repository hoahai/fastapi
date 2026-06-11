import unittest

from apps.leavesphere.api.v1.helpers import myPto


class LeaveSphereMyPtoDynamicBalanceTests(unittest.TestCase):
    def test_build_balance_rows_is_transaction_driven_and_type_dynamic(self):
        rows = [
            {"ptoTypeCode": "PTO", "ptoActionCode": "LOAD", "hours": 8, "status": "Approved"},
            {"ptoTypeCode": "PTO", "ptoActionCode": "REQ", "hours": -2, "status": "Approved"},
            {"ptoTypeCode": "PTO", "ptoActionCode": "REQ", "hours": -3, "status": "Pending"},
            {"ptoTypeCode": "SICK", "ptoActionCode": "LOAD", "hours": 4, "status": "Approved"},
            {"ptoTypeCode": "SICK", "ptoActionCode": "REQ", "hours": -1, "status": "Rejected"},
        ]
        pto_type_by_code = {
            "PTO": {"code": "PTO", "label": "Paid Time Off", "listingOrder": 2},
            "SICK": {"code": "SICK", "label": "Sick", "listingOrder": 1},
        }

        balances = myPto._build_balance_rows(rows=rows, pto_type_by_code=pto_type_by_code)

        self.assertEqual([row["code"] for row in balances], ["SICK", "PTO"])
        sick = balances[0]
        pto = balances[1]

        self.assertEqual(sick["label"], "Sick")
        self.assertEqual(sick["totalHours"], 4.0)
        self.assertEqual(sick["usedHours"], 0.0)
        self.assertEqual(sick["scheduledHours"], 0.0)
        self.assertEqual(sick["remainingHours"], 4.0)

        self.assertEqual(pto["label"], "Paid Time Off")
        self.assertEqual(pto["totalHours"], 8.0)
        self.assertEqual(pto["usedHours"], 2.0)
        self.assertEqual(pto["scheduledHours"], 3.0)
        self.assertEqual(pto["remainingHours"], 3.0)
        self.assertNotIn("Floating Holiday", {row["label"] for row in balances})


if __name__ == "__main__":
    unittest.main()
