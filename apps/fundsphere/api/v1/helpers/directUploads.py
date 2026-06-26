from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from shared.storage import (
    StorageUploadInput,
    StorageValidationError,
    upload_file,
    validate_attachment_payload,
    is_image_mime,
)
from shared.tenant import get_tenant_id


def _normalize_name_part(value: object, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    text = " ".join(text.split())
    text = "".join(char for char in text if char.isalnum() or char in {" ", ".", "_", "-"})
    text = " ".join(text.split())
    if not text:
        raise ValueError(f"{field} must contain at least one letter or number")
    return text


def _build_account_logo_storage_names(*, account_code: str, original_filename: str) -> tuple[str, str]:
    normalized_account_code = _normalize_name_part(account_code, field="accountCode")
    storage_key = f"{normalized_account_code.lower()}-logo-{uuid4()}"
    extension = Path(str(original_filename or "").strip()).suffix.strip().lower()
    if extension:
        return storage_key, f"{storage_key}{extension}"
    return storage_key, storage_key


def upload_account_logo(
    *,
    account_code: str,
    filename: str,
    mime_type: str,
    file_bytes: bytes,
    uploaded_by: str | None = None,
) -> dict[str, object]:
    tenant_slug = str(get_tenant_id() or "").strip().lower()
    if not tenant_slug:
        raise ValueError("Missing tenant context")

    normalized_name, normalized_mime, _ = validate_attachment_payload(
        file_bytes=file_bytes,
        filename=filename,
        mime_type=mime_type,
    )
    if not is_image_mime(normalized_mime):
        raise StorageValidationError("Only PNG, JPG, JPEG, and WEBP files are allowed")

    storage_key, stored_file_name = _build_account_logo_storage_names(
        account_code=account_code,
        original_filename=normalized_name,
    )

    stored_asset = upload_file(
        payload=StorageUploadInput(
            app_name="FundSphere",
            tenant_slug=tenant_slug,
            owner_entity_type="account_logo",
            owner_entity_id=_normalize_name_part(account_code, field="accountCode"),
            uploaded_by=str(uploaded_by or "").strip() or None,
            file_bytes=file_bytes,
            original_filename=stored_file_name,
            mime_type=normalized_mime,
            storage_key=storage_key,
        )
    )
    return {
        "logoUrl": stored_asset.access_url,
        "fileName": stored_asset.original_filename,
        "mimeType": stored_asset.mime_type,
        "storageProvider": stored_asset.storage_provider,
    }
