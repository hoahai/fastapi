from __future__ import annotations

import os

from shared.storage.errors import StorageValidationError

_ALLOWED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
_ALLOWED_DOC_EXTENSIONS = {".pdf"}
_ALLOWED_EXTENSIONS = _ALLOWED_IMAGE_EXTENSIONS | _ALLOWED_DOC_EXTENSIONS

_BLOCKED_EXTENSIONS = {
    ".exe",
    ".dll",
    ".bat",
    ".cmd",
    ".sh",
    ".zip",
    ".rar",
    ".7z",
    ".apk",
    ".msi",
    ".com",
    ".scr",
    ".ps1",
}

_ALLOWED_MIME_TYPES = {
    "image/png",
    "image/jpeg",
    "image/webp",
    "application/pdf",
}

_IMAGE_MAX_BYTES = 10 * 1024 * 1024
_DOCUMENT_MAX_BYTES = 20 * 1024 * 1024
_NOTE_ATTACHMENT_MAX_COUNT = 5


def get_note_attachment_limit() -> int:
    return _NOTE_ATTACHMENT_MAX_COUNT


def is_image_mime(mime_type: str) -> bool:
    return str(mime_type or "").strip().lower().startswith("image/")


def normalize_filename(filename: str) -> str:
    name = os.path.basename(str(filename or "").strip())
    if not name:
        raise StorageValidationError("File name is required")
    if len(name) > 255:
        raise StorageValidationError("File name must be <= 255 characters")
    return name


def normalize_mime_type(mime_type: str, filename: str) -> str:
    normalized = str(mime_type or "").split(";", 1)[0].strip().lower()
    _, ext = os.path.splitext(filename)
    ext = ext.lower()

    if ext in {".jpg", ".jpeg"} and not normalized:
        normalized = "image/jpeg"
    elif ext == ".png" and not normalized:
        normalized = "image/png"
    elif ext == ".webp" and not normalized:
        normalized = "image/webp"
    elif ext == ".pdf" and not normalized:
        normalized = "application/pdf"

    if normalized == "image/jpg":
        normalized = "image/jpeg"

    if normalized not in _ALLOWED_MIME_TYPES:
        raise StorageValidationError("Unsupported file type. Only PNG, JPG, JPEG, WEBP, and PDF are allowed.")

    return normalized


def validate_attachment_payload(*, file_bytes: bytes, filename: str, mime_type: str) -> tuple[str, str, int]:
    normalized_name = normalize_filename(filename)
    _, ext = os.path.splitext(normalized_name)
    ext = ext.lower()

    if ext in _BLOCKED_EXTENSIONS:
        raise StorageValidationError("Blocked file type")

    if ext not in _ALLOWED_EXTENSIONS:
        raise StorageValidationError("Unsupported file extension. Only .png, .jpg, .jpeg, .webp, and .pdf are allowed.")

    normalized_mime = normalize_mime_type(mime_type, normalized_name)

    size = len(file_bytes or b"")
    if size <= 0:
        raise StorageValidationError("Uploaded file is empty")

    max_bytes = _IMAGE_MAX_BYTES if normalized_mime.startswith("image/") else _DOCUMENT_MAX_BYTES
    if size > max_bytes:
        raise StorageValidationError(f"File is too large. Maximum allowed size is {max_bytes // (1024 * 1024)} MB.")

    _validate_magic(file_bytes=file_bytes, mime_type=normalized_mime)
    return normalized_name, normalized_mime, size


def _validate_magic(*, file_bytes: bytes, mime_type: str) -> None:
    if mime_type == "image/png":
        if not file_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
            raise StorageValidationError("Invalid PNG file")
        return

    if mime_type == "image/jpeg":
        if len(file_bytes) < 3 or not file_bytes.startswith(b"\xff\xd8\xff"):
            raise StorageValidationError("Invalid JPEG file")
        return

    if mime_type == "image/webp":
        if len(file_bytes) < 12 or not (file_bytes.startswith(b"RIFF") and file_bytes[8:12] == b"WEBP"):
            raise StorageValidationError("Invalid WEBP file")
        return

    if mime_type == "application/pdf":
        if not file_bytes.startswith(b"%PDF"):
            raise StorageValidationError("Invalid PDF file")
        return
