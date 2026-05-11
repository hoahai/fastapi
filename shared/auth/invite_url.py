from __future__ import annotations


def build_invite_url(token: str, base_url: str) -> str:
    """
    Build canonical invite URL from configured base URL + invitation token.

    The base URL can be either:
    - host root (for example: https://workspace.example.com)
    - invite path root (for example: https://workspace.example.com/auth/invite)
    """
    normalized_token = str(token or "").strip()
    normalized_base = str(base_url or "").strip().rstrip("/")

    if not normalized_base:
        return f"/auth/invite/{normalized_token}"

    base_lower = normalized_base.lower()
    if base_lower.endswith("/auth/invite"):
        return f"{normalized_base}/{normalized_token}"

    return f"{normalized_base}/auth/invite/{normalized_token}"
