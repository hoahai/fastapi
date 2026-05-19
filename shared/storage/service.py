from __future__ import annotations

from shared.storage.base import StorageProvider
from shared.storage.config import get_cloudinary_config, get_storage_provider_name
from shared.storage.errors import StorageConfigError
from shared.storage.providers.cloudinary import CloudinaryStorageProvider
from shared.storage.types import DeleteAssetInput, StoredAsset, StorageUploadInput


def _resolve_provider(*, app_name: str) -> StorageProvider:
    provider_name = get_storage_provider_name(app_name=app_name)
    if provider_name == "cloudinary":
        return CloudinaryStorageProvider(config=get_cloudinary_config(app_name=app_name))
    raise StorageConfigError(f"Unsupported storage provider: {provider_name}")


def upload_file(*, payload: StorageUploadInput) -> StoredAsset:
    provider = _resolve_provider(app_name=payload.app_name)
    return provider.upload(payload)


def delete_file(
    *,
    app_name: str,
    payload: DeleteAssetInput,
    storage_provider: str | None,
) -> None:
    target_provider = str(storage_provider or "").strip().lower() or get_storage_provider_name(app_name=app_name)
    if target_provider == "cloudinary":
        provider = CloudinaryStorageProvider(config=get_cloudinary_config(app_name=app_name))
        provider.delete(payload)
        return
    raise StorageConfigError(f"Unsupported storage provider: {target_provider}")
