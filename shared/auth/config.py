from __future__ import annotations

import os


AUTH_MODE_COMPAT = "compat"
AUTH_MODE_JWT_ONLY = "jwt_only"


SUPABASE_ENV_KEYS = (
    "SUPABASE_URL",
    "SUPABASE_ANON_KEY",
    "SUPABASE_SERVICE_ROLE_KEY",
)


def _as_bool(value: object, default: bool) -> bool:
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _as_int(value: object, default: int) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        parsed = default
    return parsed


def get_auth_mode() -> str:
    raw = str(os.getenv("AUTH_MODE", AUTH_MODE_COMPAT)).strip().lower()
    if raw not in {AUTH_MODE_COMPAT, AUTH_MODE_JWT_ONLY}:
        return AUTH_MODE_COMPAT
    return raw


def should_protect_tradsphere() -> bool:
    return _as_bool(os.getenv("AUTH_PROTECT_TRADSPHERE"), False)


def is_legacy_api_key_fallback_enabled() -> bool:
    return _as_bool(os.getenv("AUTH_ENABLE_LEGACY_API_KEY_FALLBACK"), True)


def get_permission_cache_ttl_seconds() -> int:
    ttl = _as_int(os.getenv("AUTH_PERMISSION_CACHE_TTL_SECONDS"), 120)
    return max(ttl, 0)


def is_debug_endpoint_enabled() -> bool:
    return _as_bool(os.getenv("AUTH_ENABLE_DEBUG_ENDPOINTS"), False)


def get_supabase_url() -> str:
    return str(os.getenv("SUPABASE_URL", "")).strip().rstrip("/")


def get_supabase_anon_key() -> str:
    return str(os.getenv("SUPABASE_ANON_KEY", "")).strip()


def get_supabase_service_role_key() -> str:
    return str(os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")).strip()


def get_invite_base_url() -> str:
    return str(os.getenv("AUTH_INVITE_BASE_URL", "")).strip()


def get_invite_ttl_hours() -> int:
    hours = _as_int(os.getenv("AUTH_INVITE_TTL_HOURS"), 72)
    return max(hours, 1)


def get_required_supabase_env_keys() -> tuple[str, ...]:
    return SUPABASE_ENV_KEYS


def is_supabase_configured() -> bool:
    return bool(get_supabase_url() and get_supabase_anon_key() and get_supabase_service_role_key())
