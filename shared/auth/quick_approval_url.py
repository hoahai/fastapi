from __future__ import annotations


def build_quick_approval_url(token: str, base_url: str) -> str:
    """
    Build canonical quick-approval URL from configured base URL + approval token.

    The base URL can be either:
    - host root (for example: https://workspace.example.com)
    - quick-approval path root (for example: https://workspace.example.com/leavesphere/quick-approval)
    """
    normalized_token = str(token or "").strip()
    normalized_base = str(base_url or "").strip().rstrip("/")

    if not normalized_base:
        return f"/leavesphere/quick-approval/{normalized_token}"

    base_lower = normalized_base.lower()
    if base_lower.endswith("/leavesphere/quick-approval"):
        return f"{normalized_base}/{normalized_token}"

    return f"{normalized_base}/leavesphere/quick-approval/{normalized_token}"

