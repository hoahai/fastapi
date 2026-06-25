from __future__ import annotations

import ast
import json
from dataclasses import dataclass

from shared.tenant import get_app_scoped_env, get_env


@dataclass(frozen=True)
class CloudinaryStorageConfig:
    cloud_name: str
    api_key: str
    api_secret: str


_CLOUDINARY_KEY_ALIASES = {
    "cloud_name": "cloud_name",
    "cloudname": "cloud_name",
    "api_key": "api_key",
    "apikey": "api_key",
    "api_secret": "api_secret",
    "apisecret": "api_secret",
}


def _get_config_value(*, app_name: str, key: str) -> str | None:
    return (
        get_app_scoped_env(app_name, key)
        or get_env(str(key).strip().upper())
    )


def _parse_config_object(raw: str | None) -> dict[str, object]:
    text = str(raw or "").strip()
    if not text:
        return {}

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            return {}

    if not isinstance(parsed, dict):
        return {}
    return parsed


def _get_cloudinary_object_value(*, app_name: str, key: str) -> str | None:
    config_object = _parse_config_object(
        get_app_scoped_env(app_name, "CLOUDINARY") or get_env("CLOUDINARY")
    )
    if not config_object:
        return None

    normalized_key = _CLOUDINARY_KEY_ALIASES.get(str(key or "").strip().lower())
    if not normalized_key:
        return None

    value = config_object.get(normalized_key)
    if value is None:
        for raw_key, raw_value in config_object.items():
            candidate_key = _CLOUDINARY_KEY_ALIASES.get(
                str(raw_key or "").strip().lower().replace("-", "").replace(" ", "")
            )
            if candidate_key == normalized_key:
                value = raw_value
                break

    text = str(value or "").strip()
    return text or None


def get_storage_provider_name(*, app_name: str) -> str:
    value = _get_config_value(app_name=app_name, key="STORAGE_PROVIDER")
    return str(value or "cloudinary").strip().lower() or "cloudinary"


def get_cloudinary_config(*, app_name: str) -> CloudinaryStorageConfig:
    cloud_name = str(
        _get_config_value(app_name=app_name, key="CLOUDINARY_CLOUD_NAME")
        or _get_cloudinary_object_value(app_name=app_name, key="cloud_name")
        or ""
    ).strip()
    api_key = str(
        _get_config_value(app_name=app_name, key="CLOUDINARY_API_KEY")
        or _get_cloudinary_object_value(app_name=app_name, key="api_key")
        or ""
    ).strip()
    api_secret = str(
        _get_config_value(app_name=app_name, key="CLOUDINARY_API_SECRET")
        or _get_cloudinary_object_value(app_name=app_name, key="api_secret")
        or ""
    ).strip()

    return CloudinaryStorageConfig(
        cloud_name=cloud_name,
        api_key=api_key,
        api_secret=api_secret,
    )
