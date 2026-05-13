from __future__ import annotations

from shared.auth.roles import (
    ROLE_ADMIN,
    ROLE_EDITOR,
    ROLE_SUPER_ADMIN,
    ROLE_VIEWER,
    normalize_role_key,
)

ROLE_TO_PERMISSIONS: dict[str, set[str]] = {
    ROLE_SUPER_ADMIN: {
        "workspace.super_admin",
        "tradsphere.viewer",
        "tradsphere.editor",
        "tradsphere.admin",
        "tradsphere.invites.admin",
        "tradsphere.contacts.viewer",
        "tradsphere.contacts.editor",
        "tradsphere.stations.viewer",
        "tradsphere.stations.editor",
        "tradsphere.estnums.viewer",
        "tradsphere.estnums.editor",
        "tradsphere.schedules.viewer",
    },
    ROLE_ADMIN: {
        "tradsphere.viewer",
        "tradsphere.editor",
        "tradsphere.admin",
        "tradsphere.invites.admin",
        "tradsphere.contacts.viewer",
        "tradsphere.contacts.editor",
        "tradsphere.stations.viewer",
        "tradsphere.stations.editor",
        "tradsphere.estnums.viewer",
        "tradsphere.estnums.editor",
        "tradsphere.schedules.viewer",
    },
    ROLE_VIEWER: {
        "tradsphere.viewer",
        "tradsphere.contacts.viewer",
        "tradsphere.stations.viewer",
        "tradsphere.estnums.viewer",
        "tradsphere.schedules.viewer",
    },
    ROLE_EDITOR: {
        "tradsphere.viewer",
        "tradsphere.editor",
        "tradsphere.contacts.viewer",
        "tradsphere.contacts.editor",
        "tradsphere.stations.viewer",
        "tradsphere.stations.editor",
        "tradsphere.estnums.viewer",
        "tradsphere.estnums.editor",
        "tradsphere.schedules.viewer",
    },
}


def expand_permissions_for_role(role: str, role_permissions: list[str] | None = None) -> set[str]:
    normalized = normalize_role_key(role)
    base = set(ROLE_TO_PERMISSIONS.get(normalized, set()))
    for permission in role_permissions or []:
        normalized = str(permission or "").strip()
        if normalized:
            base.add(normalized)
    return base
