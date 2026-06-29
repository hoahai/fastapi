import unittest
from unittest.mock import call, patch

from apps.leavesphere.api.v1.helpers import employees as employee_helpers


class LeaveSphereEmployeeManagementBackendTests(unittest.TestCase):
    def test_load_employee_management_workspace_uses_cache_after_first_build(self):
        rows = [
            {
                "id": "emp-1",
                "firstName": "Alex",
                "lastName": "Chen",
                "email": "alex@example.com",
                "active": 1,
            },
            {
                "id": "emp-2",
                "firstName": "Jamie",
                "lastName": "Lee",
                "email": "jamie@example.com",
                "active": 0,
            },
        ]

        with patch.object(employee_helpers, "read_leave_sphere_read_cache", side_effect=[None, {"cached": True}]) as mock_read, patch.object(
            employee_helpers,
            "write_leave_sphere_read_cache",
        ) as mock_write, patch.object(employee_helpers, "list_employees", return_value=rows) as mock_list:
            first = employee_helpers.load_employee_management_workspace()
            second = employee_helpers.load_employee_management_workspace()

        self.assertEqual(first["pageCode"], "employee-management")
        self.assertEqual(first["summary"]["totalEmployees"], 2)
        self.assertEqual(first["summary"]["activeEmployees"], 1)
        self.assertEqual(first["summary"]["inactiveEmployees"], 1)
        self.assertFalse(first["capabilities"]["canArchive"])
        self.assertEqual(second, {"cached": True})
        self.assertEqual(mock_list.call_count, 1)
        self.assertEqual(mock_read.call_count, 2)
        mock_write.assert_called_once_with(namespace="employee-management:workspace", value=first)

    def test_load_employee_management_workspace_force_refresh_bypasses_page_cache(self):
        rows = [
            {
                "id": "emp-1",
                "firstName": "Alex",
                "lastName": "Chen",
                "email": "alex@example.com",
                "active": 1,
            }
        ]

        with patch.object(employee_helpers, "read_leave_sphere_read_cache") as mock_read, patch.object(
            employee_helpers,
            "get_employees",
            return_value=rows,
        ) as mock_get, patch.object(employee_helpers, "write_leave_sphere_read_cache") as mock_write:
            workspace = employee_helpers.load_employee_management_workspace(force_refresh=True)

        self.assertEqual(workspace["summary"]["totalEmployees"], 1)
        self.assertEqual(workspace["summary"]["activeEmployees"], 1)
        self.assertEqual(workspace["summary"]["inactiveEmployees"], 0)
        mock_read.assert_not_called()
        mock_get.assert_called_once_with(summary=False)
        mock_write.assert_has_calls(
            [
                call(namespace="employees:list", value=rows),
                call(namespace="employee-management:workspace", value=workspace),
            ]
        )

    def test_load_employee_management_workspace_summary_uses_summary_cache(self):
        rows = [
            {
                "id": "emp-1",
                "identityKey": "identity-1",
                "firstName": "Alex",
                "lastName": "Chen",
                "email": "alex@example.com",
                "pictureUrl": None,
                "region": "US",
                "title": "AE",
                "isAE": True,
                "active": 1,
            }
        ]

        with patch.object(employee_helpers, "read_leave_sphere_read_cache", side_effect=[None, None]) as mock_read, patch.object(
            employee_helpers,
            "list_employees",
            return_value=rows,
        ) as mock_list, patch.object(employee_helpers, "write_leave_sphere_read_cache") as mock_write:
            workspace = employee_helpers.load_employee_management_workspace(summary=True)

        self.assertEqual(workspace["employees"], rows)
        self.assertEqual(mock_read.call_count, 1)
        mock_list.assert_called_once_with(summary=True)
        mock_write.assert_called_once_with(namespace="employee-management:workspace:summary", value=workspace)

    def test_activate_and_deactivate_employee_toggle_active_flag(self):
        for helper_name, expected_active in (
            ("activate_employee", 1),
            ("deactivate_employee", 0),
        ):
            updated_employee = {
                "id": "emp-1",
                "firstName": "Alex",
                "lastName": "Chen",
                "email": "alex@example.com",
                "active": expected_active,
            }
            with patch.object(employee_helpers, "modify_employee", return_value={"updated": 1}) as mock_modify, patch.object(
                employee_helpers,
                "get_employees",
                return_value=[updated_employee],
            ) as mock_get:
                result = getattr(employee_helpers, helper_name)("emp-1")

            self.assertEqual(result["updated"], 1)
            self.assertEqual(result["employee"], updated_employee)
            mock_modify.assert_called_once_with(employee_id="emp-1", payload={"active": expected_active})
            mock_get.assert_called_once_with(employee_id="emp-1")


if __name__ == "__main__":
    unittest.main()
