from __future__ import annotations

import importlib
import io
from pathlib import Path

from shared.storage.config import CloudinaryStorageConfig
from shared.storage.errors import StorageConfigError, StorageDeleteError, StorageUploadError
from shared.storage.types import DeleteAssetInput, StoredAsset, StorageUploadInput


class CloudinaryStorageProvider:
    name = "cloudinary"

    def __init__(self, *, config: CloudinaryStorageConfig) -> None:
        self._config = config
        if not self._config.cloud_name or not self._config.api_key or not self._config.api_secret:
            raise StorageConfigError("Cloudinary storage is not configured")
        self._cloudinary_module, self._uploader_module = _load_cloudinary_sdk()
        self._cloudinary_module.config(
            cloud_name=self._config.cloud_name,
            api_key=self._config.api_key,
            api_secret=self._config.api_secret,
            secure=True,
        )

    def upload(self, payload: StorageUploadInput) -> StoredAsset:
        filename = Path(payload.original_filename).name
        resource_type = _resolve_resource_type(
            provider_resource_type=None,
            mime_type=payload.mime_type,
        )
        folder = self._build_folder(
            app_name=payload.app_name,
            tenant_slug=payload.tenant_slug,
            owner_entity_type=payload.owner_entity_type,
            owner_entity_id=payload.owner_entity_id,
        )
        try:
            upload_kwargs: dict[str, object] = {
                "resource_type": resource_type,
                "folder": folder,
                "use_filename": not bool(payload.storage_key),
                "unique_filename": not bool(payload.storage_key),
                "overwrite": False,
                "filename_override": filename,
            }
            storage_key = str(payload.storage_key or "").strip()
            if storage_key:
                upload_kwargs["public_id"] = storage_key
            parsed = self._uploader_module.upload(
                io.BytesIO(payload.file_bytes),
                **upload_kwargs,
            )
        except Exception as exc:  # pragma: no cover - sdk/runtime safety
            raise StorageUploadError("File upload failed") from exc
        parsed = parsed if isinstance(parsed, dict) else {}

        access_url = str(parsed.get("secure_url") or "").strip()
        if not access_url:
            raise StorageUploadError("File upload failed")
        file_size = int(parsed.get("bytes") or len(payload.file_bytes))
        resolved_resource_type = str(parsed.get("resource_type") or resource_type).strip() or resource_type
        provider_metadata = {
            "asset_id": parsed.get("asset_id"),
            "public_id": parsed.get("public_id"),
            "resource_type": resolved_resource_type,
            "format": parsed.get("format"),
            "version": parsed.get("version"),
            "bytes": file_size,
            "width": parsed.get("width"),
            "height": parsed.get("height"),
        }

        return StoredAsset(
            storage_provider=self.name,
            provider_asset_id=str(parsed.get("asset_id") or "").strip() or None,
            provider_public_id=str(parsed.get("public_id") or "").strip() or None,
            provider_resource_type=resolved_resource_type,
            access_url=access_url,
            original_filename=filename,
            mime_type=payload.mime_type,
            file_size=file_size,
            provider_metadata=provider_metadata,
        )

    def delete(self, payload: DeleteAssetInput) -> None:
        public_id = str(payload.provider_public_id or "").strip()
        if not public_id:
            return

        resource_type = _resolve_resource_type(
            provider_resource_type=payload.provider_resource_type,
            mime_type=payload.mime_type,
        )
        try:
            parsed = self._uploader_module.destroy(
                public_id,
                resource_type=resource_type,
                invalidate=True,
            )
        except Exception as exc:  # pragma: no cover - sdk/runtime safety
            raise StorageDeleteError("File delete failed") from exc
        parsed = parsed if isinstance(parsed, dict) else {}

        result = str(parsed.get("result") or "").strip().lower()
        if result not in {"ok", "not found"}:
            raise StorageDeleteError("File delete failed")

    @staticmethod
    def _build_folder(*, app_name: str, tenant_slug: str, owner_entity_type: str, owner_entity_id: str) -> str:
        app = str(app_name or "").strip().lower()
        tenant = str(tenant_slug or "").strip().lower()
        owner_type = str(owner_entity_type or "").strip().lower().replace("_", "-")
        owner_id = str(owner_entity_id or "").strip().lower()
        if owner_type == "invoice-checklist-note":
            return f"{app}/tenants/{tenant}/{owner_type}"
        return f"{app}/tenants/{tenant}/{owner_type}/{owner_id}"


def _load_cloudinary_sdk() -> tuple[object, object]:
    try:
        cloudinary_module = importlib.import_module("cloudinary")
        uploader_module = importlib.import_module("cloudinary.uploader")
    except Exception as exc:  # pragma: no cover - import/runtime safety
        raise StorageConfigError("Cloudinary storage SDK is not installed") from exc
    return cloudinary_module, uploader_module


def _resolve_resource_type(*, provider_resource_type: str | None, mime_type: str | None) -> str:
    resolved = str(provider_resource_type or "").strip().lower()
    if resolved in {"image", "raw", "video"}:
        return resolved
    normalized_mime_type = str(mime_type or "").strip().lower()
    if normalized_mime_type.startswith("image/"):
        return "image"
    return "raw"
