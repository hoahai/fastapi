from __future__ import annotations

from shared.auth.permission_registry import APP_CODES, default_role_permissions_for_app
from shared.auth.roles import (
    ROLE_ADMIN,
    ROLE_EDITOR,
    ROLE_SUPER_ADMIN,
    ROLE_VIEWER,
    normalize_role_key,
)

def _tradsphere_feature_permissions(*, role: str) -> set[str]:
    normalized_role = normalize_role_key(role)
    permissions: set[str] = {"tradsphere.viewer"}
    if normalized_role in {ROLE_EDITOR, ROLE_ADMIN}:
        permissions.add("tradsphere.editor")
    if normalized_role == ROLE_ADMIN:
        permissions.add("tradsphere.admin")
        permissions.add("tradsphere.invites.admin")

    permissions.add("tradsphere.contacts.viewer")
    permissions.add("tradsphere.stations.viewer")
    permissions.add("tradsphere.estnums.viewer")
    permissions.add("tradsphere.schedules.viewer")

    if normalized_role in {ROLE_EDITOR, ROLE_ADMIN}:
        permissions.add("tradsphere.contacts.editor")
        permissions.add("tradsphere.stations.editor")
        permissions.add("tradsphere.estnums.editor")

    return permissions


def _role_permissions_for_app(*, role: str, app_code: str) -> set[str]:
    normalized_app = str(app_code or "").strip().lower()
    if normalized_app == "tradsphere":
        return _tradsphere_feature_permissions(role=role)
    return default_role_permissions_for_app(role=role, app_code=normalized_app)


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
        base.update(_tradsphere_feature_permissions(role=ROLE_ADMIN))
    else:
        effective_app_code = str(app_code or "").strip().lower() or "tradsphere"
        base.update(_role_permissions_for_app(role=normalized, app_code=effective_app_code))

    for permission in role_permissions or []:
        normalized_permission = str(permission or "").strip()
        if normalized_permission:
            base.add(normalized_permission)
    return base
