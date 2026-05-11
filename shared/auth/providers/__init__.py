from __future__ import annotations

from functools import lru_cache

from shared.auth.config import get_auth_provider_name

from .base import AuthDataProvider
from .supabase import SupabaseAuthProvider


@lru_cache(maxsize=1)
def get_auth_provider() -> AuthDataProvider:
    provider_name = get_auth_provider_name()
    if provider_name == "supabase":
        return SupabaseAuthProvider()
    raise RuntimeError(f"Unsupported auth provider: {provider_name}")
