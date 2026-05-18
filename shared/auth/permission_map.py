from __future__ import annotations

from shared.auth.permission_registry import APP_CODES, default_role_permissions_for_app
from shared.auth.roles import (
    ROLE_ADMIN,
    ROLE_SUPER_ADMIN,
    normalize_role_key,
)


def expand_permissions_for_role(
    role: str,
    role_permissions: list[str] | None = None,
    *,
    app_code: str | None = None,
) -> set[str]:
    normalized = normalize_role_key(role)
    base: set[str] = set()

    if normalized == ROLE_SUPER_ADMIN:
        base.add("workspace.super_admin")
        # Keep broad app-level permissions for UX compatibility, while backend
        # authorization relies on `workspace.super_admin` as a global bypass.
        for known_app_code in APP_CODES:
            base.update(default_role_permissions_for_app(role=ROLE_ADMIN, app_code=known_app_code))
    else:
        effective_app_code = str(app_code or "").strip().lower() or "tradsphere"
        base.update(default_role_permissions_for_app(role=normalized, app_code=effective_app_code))

    for permission in role_permissions or []:
        normalized_permission = str(permission or "").strip()
        if normalized_permission:
            base.add(normalized_permission)
    return base
