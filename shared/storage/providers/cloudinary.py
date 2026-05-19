from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.parse
import urllib.request
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

    def upload(self, payload: StorageUploadInput) -> StoredAsset:
        filename = Path(payload.original_filename).name
        resource_type = "image" if payload.mime_type.startswith("image/") else "raw"
        timestamp = int(time.time())
        folder = self._build_folder(
            app_name=payload.app_name,
            tenant_slug=payload.tenant_slug,
            owner_entity_type=payload.owner_entity_type,
            owner_entity_id=payload.owner_entity_id,
        )
        signature = _sign_payload(
            {
                "folder": folder,
                "timestamp": str(timestamp),
            },
            api_secret=self._config.api_secret,
        )

        body, content_type = _encode_multipart_form_data(
            fields={
                "api_key": self._config.api_key,
                "timestamp": str(timestamp),
                "signature": signature,
                "folder": folder,
            },
            files={
                "file": (filename, payload.file_bytes, payload.mime_type),
            },
        )

        request = urllib.request.Request(
            url=self._upload_url(resource_type=resource_type),
            method="POST",
            headers={"Content-Type": content_type},
            data=body,
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            raise StorageUploadError("File upload failed") from exc
        except Exception as exc:  # pragma: no cover - network/runtime safety
            raise StorageUploadError("File upload failed") from exc

        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise StorageUploadError("File upload failed") from exc

        access_url = str(parsed.get("secure_url") or "").strip()
        if not access_url:
            raise StorageUploadError("File upload failed")

        return StoredAsset(
            storage_provider=self.name,
            provider_asset_id=str(parsed.get("asset_id") or "").strip() or None,
            provider_public_id=str(parsed.get("public_id") or "").strip() or None,
            provider_resource_type=str(parsed.get("resource_type") or resource_type).strip() or resource_type,
            access_url=access_url,
            original_filename=filename,
            mime_type=payload.mime_type,
            file_size=int(parsed.get("bytes") or len(payload.file_bytes)),
        )

    def delete(self, payload: DeleteAssetInput) -> None:
        public_id = str(payload.provider_public_id or "").strip()
        if not public_id:
            return

        resource_type = str(payload.provider_resource_type or "").strip().lower() or "raw"
        if resource_type not in {"image", "raw", "video"}:
            resource_type = "raw"

        timestamp = int(time.time())
        signature = _sign_payload(
            {
                "public_id": public_id,
                "timestamp": str(timestamp),
            },
            api_secret=self._config.api_secret,
        )

        body = urllib.parse.urlencode(
            {
                "public_id": public_id,
                "api_key": self._config.api_key,
                "timestamp": str(timestamp),
                "signature": signature,
                "invalidate": "true",
            }
        ).encode("utf-8")

        request = urllib.request.Request(
            url=self._destroy_url(resource_type=resource_type),
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=body,
        )

        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            raise StorageDeleteError("File delete failed") from exc
        except Exception as exc:  # pragma: no cover - network/runtime safety
            raise StorageDeleteError("File delete failed") from exc

        try:
            parsed = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            raise StorageDeleteError("File delete failed") from exc

        result = str(parsed.get("result") or "").strip().lower()
        if result not in {"ok", "not found"}:
            raise StorageDeleteError("File delete failed")

    def _upload_url(self, *, resource_type: str) -> str:
        return f"https://api.cloudinary.com/v1_1/{self._config.cloud_name}/{resource_type}/upload"

    def _destroy_url(self, *, resource_type: str) -> str:
        return f"https://api.cloudinary.com/v1_1/{self._config.cloud_name}/{resource_type}/destroy"

    @staticmethod
    def _build_folder(*, app_name: str, tenant_slug: str, owner_entity_type: str, owner_entity_id: str) -> str:
        app = str(app_name or "").strip().lower()
        tenant = str(tenant_slug or "").strip().lower()
        owner_type = str(owner_entity_type or "").strip().lower().replace("_", "-")
        owner_id = str(owner_entity_id or "").strip().lower()
        return f"{app}/tenants/{tenant}/{owner_type}/{owner_id}"


def _sign_payload(payload: dict[str, str], *, api_secret: str) -> str:
    message = "&".join(
        f"{key}={payload[key]}" for key in sorted(payload.keys()) if str(payload[key]).strip() != ""
    )
    return hashlib.sha1(f"{message}{api_secret}".encode("utf-8")).hexdigest()


def _encode_multipart_form_data(
    *,
    fields: dict[str, str],
    files: dict[str, tuple[str, bytes, str]],
) -> tuple[bytes, str]:
    boundary = f"----codexboundary{int(time.time() * 1000)}"
    lines: list[bytes] = []

    for key, value in fields.items():
        lines.append(f"--{boundary}".encode("utf-8"))
        lines.append(f'Content-Disposition: form-data; name="{key}"'.encode("utf-8"))
        lines.append(b"")
        lines.append(str(value).encode("utf-8"))

    for key, (filename, content, mime_type) in files.items():
        lines.append(f"--{boundary}".encode("utf-8"))
        lines.append(
            f'Content-Disposition: form-data; name="{key}"; filename="{filename}"'.encode("utf-8")
        )
        lines.append(f"Content-Type: {mime_type}".encode("utf-8"))
        lines.append(b"")
        lines.append(content)

    lines.append(f"--{boundary}--".encode("utf-8"))
    lines.append(b"")

    body = b"\r\n".join(lines)
    return body, f"multipart/form-data; boundary={boundary}"
