from __future__ import annotations

from dataclasses import dataclass

from shared.tenant import get_app_scoped_env, get_env


@dataclass(frozen=True)
class CloudinaryStorageConfig:
    cloud_name: str
    api_key: str
    api_secret: str


def _get_config_value(*, app_name: str, key: str) -> str | None:
    return (
        get_app_scoped_env(app_name, key)
        or get_env(str(key).strip().upper())
    )


def get_storage_provider_name(*, app_name: str) -> str:
    value = _get_config_value(app_name=app_name, key="STORAGE_PROVIDER")
    return str(value or "cloudinary").strip().lower() or "cloudinary"


def get_cloudinary_config(*, app_name: str) -> CloudinaryStorageConfig:
    cloud_name = str(_get_config_value(app_name=app_name, key="CLOUDINARY_CLOUD_NAME") or "").strip()
    api_key = str(_get_config_value(app_name=app_name, key="CLOUDINARY_API_KEY") or "").strip()
    api_secret = str(_get_config_value(app_name=app_name, key="CLOUDINARY_API_SECRET") or "").strip()

    return CloudinaryStorageConfig(
        cloud_name=cloud_name,
        api_key=api_key,
        api_secret=api_secret,
    )
