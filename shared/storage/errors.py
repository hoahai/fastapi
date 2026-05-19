from __future__ import annotations


class StorageError(RuntimeError):
    """Base storage error with safe user-facing messages."""


class StorageConfigError(StorageError):
    pass


class StorageValidationError(StorageError):
    pass


class StorageUploadError(StorageError):
    pass


class StorageDeleteError(StorageError):
    pass
