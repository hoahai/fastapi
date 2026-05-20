import unittest
from unittest.mock import Mock, patch

from shared.storage.config import CloudinaryStorageConfig
from shared.storage.errors import StorageDeleteError
from shared.storage.providers.cloudinary import CloudinaryStorageProvider
from shared.storage.types import DeleteAssetInput, StorageUploadInput


class CloudinaryStorageProviderTests(unittest.TestCase):
    def _build_provider(self, *, upload_result=None, destroy_result=None):
        cloudinary_module = Mock()
        uploader_module = Mock()
        uploader_module.upload.return_value = upload_result or {}
        uploader_module.destroy.return_value = destroy_result or {"result": "ok"}

        with patch(
            "shared.storage.providers.cloudinary.importlib.import_module",
            side_effect=[cloudinary_module, uploader_module],
        ):
            provider = CloudinaryStorageProvider(
                config=CloudinaryStorageConfig(
                    cloud_name="demo-cloud",
                    api_key="demo-key",
                    api_secret="demo-secret",
                )
            )
        return provider, cloudinary_module, uploader_module

    def test_upload_uses_sdk_and_returns_provider_metadata(self):
        provider, cloudinary_module, uploader_module = self._build_provider(
            upload_result={
                "secure_url": "https://res.cloudinary.com/demo/image/upload/v1/demo/file.png",
                "asset_id": "asset-1",
                "public_id": "tradsphere/tenants/taaa/invoice-checklist-note/5/file",
                "resource_type": "image",
                "format": "png",
                "version": 123,
                "bytes": 11,
                "width": 100,
                "height": 50,
            }
        )
        payload = StorageUploadInput(
            app_name="TradSphere",
            tenant_slug="taaa",
            owner_entity_type="invoice_checklist_note",
            owner_entity_id="5",
            uploaded_by="user@demo.com",
            file_bytes=b"hello world",
            original_filename="file.png",
            mime_type="image/png",
        )

        stored = provider.upload(payload)

        cloudinary_module.config.assert_called_once()
        kwargs = uploader_module.upload.call_args.kwargs
        self.assertEqual(kwargs["resource_type"], "image")
        self.assertEqual(kwargs["folder"], "tradsphere/tenants/taaa/invoice-checklist-note")
        self.assertEqual(stored.provider_public_id, "tradsphere/tenants/taaa/invoice-checklist-note/5/file")
        self.assertEqual(stored.access_url, "https://res.cloudinary.com/demo/image/upload/v1/demo/file.png")
        self.assertEqual(stored.provider_metadata["format"], "png")
        self.assertEqual(stored.provider_metadata["resource_type"], "image")

    def test_upload_uses_explicit_storage_key_as_public_id(self):
        provider, _, uploader_module = self._build_provider(
            upload_result={
                "secure_url": "https://res.cloudinary.com/demo/raw/upload/v1/tradsphere/tenants/taaa/invoice-checklist-note/5_2001_kabc_2.pdf",
                "asset_id": "asset-2",
                "public_id": "tradsphere/tenants/taaa/invoice-checklist-note/5_2001_kabc_2",
                "resource_type": "raw",
                "format": "pdf",
                "bytes": 20,
            }
        )
        payload = StorageUploadInput(
            app_name="TradSphere",
            tenant_slug="taaa",
            owner_entity_type="invoice_checklist_note",
            owner_entity_id="5",
            uploaded_by=None,
            file_bytes=b"pdf-bytes",
            original_filename="invoice.pdf",
            mime_type="application/pdf",
            storage_key="5_2001_kabc_2",
        )

        provider.upload(payload)

        kwargs = uploader_module.upload.call_args.kwargs
        self.assertEqual(kwargs["folder"], "tradsphere/tenants/taaa/invoice-checklist-note")
        self.assertEqual(kwargs["public_id"], "5_2001_kabc_2")
        self.assertEqual(kwargs["use_filename"], False)
        self.assertEqual(kwargs["unique_filename"], False)

    def test_delete_allows_not_found_and_uses_mime_fallback(self):
        provider, _, uploader_module = self._build_provider(
            destroy_result={"result": "not found"}
        )
        provider.delete(
            DeleteAssetInput(
                provider_resource_type=None,
                provider_public_id="tradsphere/tenants/taaa/invoice-checklist-note/5/file",
                mime_type="application/pdf",
            )
        )

        kwargs = uploader_module.destroy.call_args.kwargs
        self.assertEqual(kwargs["resource_type"], "raw")
        self.assertEqual(kwargs["invalidate"], True)

    def test_delete_raises_when_destroy_result_is_unexpected(self):
        provider, _, _ = self._build_provider(destroy_result={"result": "error"})
        with self.assertRaises(StorageDeleteError):
            provider.delete(
                DeleteAssetInput(
                    provider_resource_type="image",
                    provider_public_id="tradsphere/tenants/taaa/invoice-checklist-note/5/file",
                    mime_type="image/png",
                )
            )


if __name__ == "__main__":
    unittest.main()
