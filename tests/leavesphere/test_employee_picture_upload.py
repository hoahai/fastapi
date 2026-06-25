import importlib
import sys
import types
import unittest
from unittest.mock import patch

from shared.storage.types import StoredAsset


class EmployeePictureUploadTests(unittest.TestCase):
    def test_upload_employee_picture_uses_employee_name_for_storage_key_and_filename(self):
        db_queries_module = types.ModuleType("apps.leavesphere.api.v1.helpers.dbQueries")
        db_queries_module.get_employees = lambda *args, **kwargs: []
        db_queries_module.insert_employee = lambda *args, **kwargs: 1
        db_queries_module.update_employee = lambda *args, **kwargs: 1
        read_cache_module = types.ModuleType("apps.leavesphere.api.v1.helpers.readCache")
        read_cache_module.read_leave_sphere_read_cache = lambda *args, **kwargs: None
        read_cache_module.write_leave_sphere_read_cache = lambda *args, **kwargs: None
        with patch.dict(
            sys.modules,
            {
                "apps.leavesphere.api.v1.helpers.dbQueries": db_queries_module,
                "apps.leavesphere.api.v1.helpers.readCache": read_cache_module,
            },
        ):
            sys.modules.pop("apps.leavesphere.api.v1.helpers.employees", None)
            employees = importlib.import_module("apps.leavesphere.api.v1.helpers.employees")

        stored_asset = StoredAsset(
            storage_provider="cloudinary",
            provider_asset_id="asset-1",
            provider_public_id="Alex Chen-123e4567-e89b-12d3-a456-426614174000",
            provider_resource_type="image",
            access_url="https://res.cloudinary.com/demo/image/upload/v1/leavesphere/tenants/taaa/employee-picture/profile/Alex%20Chen-123e4567-e89b-12d3-a456-426614174000.png",
            original_filename="Alex Chen-123e4567-e89b-12d3-a456-426614174000.png",
            mime_type="image/png",
            file_size=123,
            provider_metadata={"format": "png"},
        )

        with patch.object(employees, "get_tenant_id", return_value="taaa"), patch.object(
            employees,
            "validate_attachment_payload",
            return_value=("profile-picture.png", "image/png", 123),
        ), patch.object(
            employees,
            "uuid4",
            return_value="123e4567-e89b-12d3-a456-426614174000",
        ), patch.object(
            employees,
            "upload_file",
            return_value=stored_asset,
        ) as upload_file:
            result = employees.upload_employee_picture(
                first_name="Alex",
                last_name="Chen",
                filename="profile-picture.png",
                mime_type="image/png",
                file_bytes=b"png",
                uploaded_by="alex@example.com",
            )

        payload = upload_file.call_args.kwargs["payload"]
        self.assertEqual(payload.storage_key, "Alex Chen-123e4567-e89b-12d3-a456-426614174000")
        self.assertEqual(payload.original_filename, "Alex Chen-123e4567-e89b-12d3-a456-426614174000.png")
        self.assertEqual(result["pictureUrl"], stored_asset.access_url)
        self.assertEqual(result["fileName"], stored_asset.original_filename)


if __name__ == "__main__":
    unittest.main()
