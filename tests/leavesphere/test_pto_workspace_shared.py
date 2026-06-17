import unittest

from apps.leavesphere.api.v1.helpers.ptoWorkspaceShared import (
    build_pto_employee_map,
    build_pto_request_rows,
)


class LeaveSpherePtoWorkspaceSharedTests(unittest.TestCase):
    def test_build_pto_employee_map_indexes_by_normalized_id(self):
        employees = [
            {"id": " emp-1 ", "firstName": "Hai"},
            {"employeeId": "emp-2", "firstName": "Lee"},
            {"id": "   "},
        ]

        employee_map = build_pto_employee_map(employees)

        self.assertEqual(sorted(employee_map.keys()), ["emp-1"])
        self.assertEqual(employee_map["emp-1"]["firstName"], "Hai")

    def test_build_pto_request_rows_shapes_common_request_payload(self):
        rows = [
            {
                "id": "txn-2",
                "employeeId": "emp-2",
                "ptoTypeCode": "VAC",
                "hours": 4,
                "status": "Pending",
                "dateCreated": "2026-06-10T00:00:00",
                "dateUpdated": "2026-06-10T00:00:00",
                "description": "Later request",
            },
            {
                "id": "txn-1",
                "employeeId": "emp-2",
                "ptoTypeCode": "VAC",
                "hours": 8,
                "status": "Approved",
                "dateCreated": "2026-06-01T00:00:00",
                "dateUpdated": "2026-06-02T00:00:00",
                "approverId": "emp-1",
                "approverNote": "Approved",
                "description": "Earlier request",
            },
        ]
        employee_map = {
            "emp-1": {"id": "emp-1", "firstName": "Hai", "lastName": "Truong"},
            "emp-2": {"id": "emp-2", "firstName": "Lee", "lastName": "Chen"},
        }
        pto_type_by_code = {
            "VAC": {"code": "VAC", "type": "vacation", "label": "Vacation"},
        }

        requests = build_pto_request_rows(
            rows=rows,
            employee_map=employee_map,
            pto_type_by_code=pto_type_by_code,
            current_employee_id="emp-1",
            manager_id_by_employee_id={"emp-2": "emp-1"},
            employee_full_name_fn=lambda employee: " ".join(
                part for part in [employee.get("firstName"), employee.get("lastName")] if part
            )
            if employee
            else "",
        )

        self.assertEqual([item["id"] for item in requests], ["txn-2", "txn-1"])
        self.assertEqual(requests[0]["managerId"], "emp-1")
        self.assertEqual(requests[0]["type"], "vacation")
        self.assertEqual(requests[0]["hours"], 4.0)
        self.assertEqual(requests[0]["reviewedAt"], None)
        self.assertEqual(requests[1]["reviewerName"], "Hai Truong")
        self.assertEqual(requests[1]["status"], "approved")


if __name__ == "__main__":
    unittest.main()
