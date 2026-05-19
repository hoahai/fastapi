from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StoredAsset:
    storage_provider: str
    provider_asset_id: str | None
    provider_public_id: str | None
    provider_resource_type: str | None
    access_url: str
    original_filename: str
    mime_type: str
    file_size: int


@dataclass(frozen=True)
class StorageUploadInput:
    app_name: str
    tenant_slug: str
    owner_entity_type: str
    owner_entity_id: str
    uploaded_by: str | None
    file_bytes: bytes
    original_filename: str
    mime_type: str


@dataclass(frozen=True)
class DeleteAssetInput:
    provider_resource_type: str | None
    provider_public_id: str | None
