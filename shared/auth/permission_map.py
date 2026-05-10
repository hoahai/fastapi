from __future__ import annotations

ROLE_TO_PERMISSIONS: dict[str, set[str]] = {
    "tradsphere.viewer": {
        "tradsphere.viewer",
        "tradsphere.contacts.viewer",
        "tradsphere.stations.viewer",
        "tradsphere.estnums.viewer",
        "tradsphere.schedules.viewer",
    },
    "tradsphere.editor": {
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
    "tradsphere.admin": {
        "tradsphere.viewer",
        "tradsphere.editor",
        "tradsphere.admin",
        "tradsphere.contacts.viewer",
        "tradsphere.contacts.editor",
        "tradsphere.stations.viewer",
        "tradsphere.stations.editor",
        "tradsphere.estnums.viewer",
        "tradsphere.estnums.editor",
        "tradsphere.schedules.viewer",
        "tradsphere.invites.admin",
    },
}


def expand_permissions_for_role(role: str, role_permissions: list[str] | None = None) -> set[str]:
    base = set(ROLE_TO_PERMISSIONS.get(str(role or "").strip(), set()))
    for permission in role_permissions or []:
        normalized = str(permission or "").strip()
        if normalized:
            base.add(normalized)
    return base
