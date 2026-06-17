import unittest
from types import SimpleNamespace
from unittest.mock import patch

from apps.leavesphere.api.v1.helpers import employees as employee_helpers
from apps.leavesphere.api.v1.endpoints import ptoTypes
from apps.leavesphere.api.v1.helpers import readCache
from apps.leavesphere.api.v1.helpers import workspaceCache
from apps.leavesphere.api.v1.helpers import ptoWorkspaceShared


class LeaveSphereWorkspaceCacheInvalidationTests(unittest.TestCase):
    def test_invalidate_leave_sphere_workspace_caches_clears_both_pages_and_catalogs(self):
        with patch.object(workspaceCache, "clear_leave_sphere_workspace_cache_by_page", side_effect=[2, 3]) as mock_clear, patch.object(
            ptoWorkspaceShared,
            "clear_leave_sphere_pto_workspace_catalog_cache",
        ) as mock_clear_catalogs, patch.object(readCache, "clear_leave_sphere_read_cache", return_value=4) as mock_clear_read:
            removed = workspaceCache.invalidate_leave_sphere_workspace_caches(include_catalogs=True)

        self.assertEqual(removed, 9)
        self.assertEqual(mock_clear.call_args_list[0].kwargs, {"page_code": "admin-pto"})
        self.assertEqual(mock_clear.call_args_list[1].kwargs, {"page_code": "my-pto"})
        mock_clear_catalogs.assert_called_once_with()
        mock_clear_read.assert_called_once_with()

    def test_list_employees_uses_read_cache(self):
        cached_rows = [
            {
                "id": "emp-1",
                "identityKey": "identity-1",
                "firstName": "Hai",
                "lastName": "Truong",
                "email": "hai@example.com",
                "active": 1,
            }
        ]

        with patch.object(employee_helpers, "read_leave_sphere_read_cache", side_effect=[None, cached_rows]) as mock_read, patch.object(
            employee_helpers,
            "write_leave_sphere_read_cache",
        ) as mock_write, patch.object(employee_helpers, "get_employees", return_value=cached_rows) as mock_get:
            first = employee_helpers.list_employees()
            second = employee_helpers.list_employees()

        self.assertEqual(first, cached_rows)
        self.assertEqual(second, cached_rows)
        self.assertEqual(mock_get.call_count, 1)
        self.assertEqual(mock_read.call_count, 2)
        mock_write.assert_called_once_with(namespace="employees:list", value=cached_rows)

    def test_create_pto_type_route_invalidates_workspace_caches(self):
        request = SimpleNamespace()
        payload = SimpleNamespace(
            model_dump=lambda: {
                "code": "VAC",
                "name": "Vacation",
                "rolloverable": True,
                "payoutable": True,
                "usaDefaultHour": 80,
                "phlDefaultHour": 80,
            }
        )

        with patch.object(ptoTypes, "create_pto_type", return_value={"code": "VAC", "inserted": 1}) as mock_create, patch.object(
            ptoTypes,
            "invalidate_leave_sphere_workspace_caches",
        ) as mock_invalidate:
            result = ptoTypes.create_pto_type_route(request=request, payload=payload, _=None)

        self.assertEqual(result, {"code": "VAC", "inserted": 1})
        mock_create.assert_called_once()
        mock_invalidate.assert_called_once_with(include_catalogs=True)


if __name__ == "__main__":
    unittest.main()
