from __future__ import annotations

from dataclasses import dataclass

ROLE_SUPER_ADMIN = "workspace.super_admin"
ROLE_ADMIN = "tradsphere.admin"
ROLE_EDITOR = "tradsphere.editor"
ROLE_VIEWER = "tradsphere.viewer"

ROLE_LABELS: dict[str, str] = {
    ROLE_SUPER_ADMIN: "Super Admin",
    ROLE_ADMIN: "Admin",
    ROLE_EDITOR: "Editor",
    ROLE_VIEWER: "Viewer",
}

ROLE_ORDER = [
    ROLE_SUPER_ADMIN,
    ROLE_ADMIN,
    ROLE_EDITOR,
    ROLE_VIEWER,
]

ADMIN_MANAGE_PERMISSION_KEYS = {
    "workspace.super_admin",
    "workspace.admin",
    "app.admin",
    "tradsphere.admin",
}


@dataclass(frozen=True)
class RoleDefinition:
    key: str
    label: str
    assignable: bool
    requires_schema_change: bool = False


def role_label(role_key: str) -> str:
    normalized = str(role_key or "").strip()
    if not normalized:
        return "Other/Unknown"
    return ROLE_LABELS.get(normalized, normalized)


def role_group_key(role_key: str) -> str:
    normalized = str(role_key or "").strip()
    if normalized in ROLE_LABELS:
        return normalized
    return "other"

