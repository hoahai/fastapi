from __future__ import annotations

from datetime import datetime, timezone


def is_active_identity_ban_time(value: object) -> bool:
    normalized = str(value or "").strip()
    if not normalized:
        return False
    lowered = normalized.lower()
    if lowered in {"none", "null", "false", "0"}:
        return False
    try:
        banned_until = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        # If value exists but cannot be parsed, treat it as active ban marker.
        return True

    if banned_until.tzinfo is None:
        banned_until = banned_until.replace(tzinfo=timezone.utc)
    return banned_until > datetime.now(timezone.utc)


def is_auth_user_disabled(raw_user: dict[str, object] | None) -> bool:
    payload = raw_user if isinstance(raw_user, dict) else {}
    banned_until_value = payload.get("banned_until")
    if is_active_identity_ban_time(banned_until_value):
        return True

    app_metadata = payload.get("app_metadata")
    app_meta = app_metadata if isinstance(app_metadata, dict) else {}
    identity_status = str(app_meta.get("status") or app_meta.get("user_status") or "").strip().lower()
    return identity_status in {"disabled", "inactive", "terminated", "banned"}

