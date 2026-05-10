from __future__ import annotations

from shared.auth.supabase_client import SupabaseAuthError, supabase_client
from shared.auth.types import AuthPrincipal


class JwtVerificationError(RuntimeError):
    pass


def verify_supabase_jwt(access_token: str) -> AuthPrincipal:
    token = str(access_token or "").strip()
    if not token:
        raise JwtVerificationError("Missing bearer token")

    try:
        user = supabase_client.get_user_from_token(token)
    except SupabaseAuthError as exc:
        raise JwtVerificationError("Invalid Supabase JWT") from exc

    user_id = str(user.get("id") or "").strip()
    if not user_id:
        raise JwtVerificationError("Supabase JWT user id is missing")

    email = str(user.get("email") or "").strip() or None
    return AuthPrincipal(user_id=user_id, email=email, raw_user=user)
