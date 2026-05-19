from __future__ import annotations

from typing import Protocol

from shared.storage.types import DeleteAssetInput, StoredAsset, StorageUploadInput


class StorageProvider(Protocol):
    name: str

    def upload(self, payload: StorageUploadInput) -> StoredAsset:
        ...

    def delete(self, payload: DeleteAssetInput) -> None:
        ...
