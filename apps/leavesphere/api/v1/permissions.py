from __future__ import annotations

from fastapi import Request

from shared.auth.config import get_auth_mode, is_legacy_api_key_fallback_enabled
from shared.auth.dependencies import require_permission

LEAVESPHERE_VIEWER_PERMISSION = "leavesphere.viewer"
LEAVESPHERE_EDITOR_PERMISSION = "leavesphere.editor"
LEAVESPHERE_ADMIN_PERMISSION = "leavesphere.admin"


def require_leavesphere_editor(request: Request) -> None:
    auth_mode = str(getattr(request.state, "auth_mode", "")).strip().lower()
    if (
        auth_mode == "legacy_api_key"
        and get_auth_mode() == "compat"
        and is_legacy_api_key_fallback_enabled()
    ):
        return
    require_permission(request, LEAVESPHERE_EDITOR_PERMISSION)


def require_leavesphere_admin(request: Request) -> None:
    auth_mode = str(getattr(request.state, "auth_mode", "")).strip().lower()
    if (
        auth_mode == "legacy_api_key"
        and get_auth_mode() == "compat"
        and is_legacy_api_key_fallback_enabled()
    ):
        return
    require_permission(request, LEAVESPHERE_ADMIN_PERMISSION)
