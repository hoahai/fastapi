from __future__ import annotations

from shared.auth.providers import get_auth_provider
from shared.auth.supabase_client import SupabaseAuthError, SupabaseClientError
from shared.auth.types import AuthPrincipal


class JwtVerificationError(RuntimeError):
    def __init__(self, message: str, *, status_code: int = 401) -> None:
        super().__init__(message)
        self.status_code = status_code


def verify_supabase_jwt(access_token: str) -> AuthPrincipal:
    token = str(access_token or "").strip()
    if not token:
        raise JwtVerificationError("Missing bearer token")

    provider = get_auth_provider()
    try:
        user = provider.verify_access_token(token)
    except SupabaseAuthError as exc:
        detail = str(exc).lower()
        if "timed out" in detail:
            raise JwtVerificationError("Authentication service timed out", status_code=503) from exc
        raise JwtVerificationError("Invalid Supabase JWT") from exc
    except SupabaseClientError as exc:
        raise JwtVerificationError("Authentication service unavailable", status_code=503) from exc

    user_id = str(user.get("id") or "").strip()
    if not user_id:
        raise JwtVerificationError("Supabase JWT user id is missing")

    email = str(user.get("email") or "").strip() or None
    return AuthPrincipal(user_id=user_id, email=email, raw_user=user)
