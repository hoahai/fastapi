from __future__ import annotations

from dataclasses import dataclass

ROLE_SUPER_ADMIN = "super_admin"
ROLE_ADMIN = "admin"
ROLE_EDITOR = "editor"
ROLE_VIEWER = "viewer"

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
    "tradsphere.admin",
}

LEGACY_ROLE_ALIASES: dict[str, str] = {
    "workspace.super_admin": ROLE_SUPER_ADMIN,
    "tradsphere.admin": ROLE_ADMIN,
    "tradsphere.editor": ROLE_EDITOR,
    "tradsphere.viewer": ROLE_VIEWER,
    "user": ROLE_VIEWER,
}


@dataclass(frozen=True)
class RoleDefinition:
    key: str
    label: str
    assignable: bool
    requires_schema_change: bool = False


def role_label(role_key: str) -> str:
    normalized = normalize_role_key(role_key)
    if not normalized:
        return "Other/Unknown"
    return ROLE_LABELS.get(normalized, normalized)


def role_group_key(role_key: str) -> str:
    normalized = normalize_role_key(role_key)
    if normalized in ROLE_LABELS:
        return normalized
    return "other"


def normalize_role_key(role_key: str | None) -> str:
    normalized = str(role_key or "").strip()
    if not normalized:
        return ""
    return LEGACY_ROLE_ALIASES.get(normalized, normalized)


def is_super_admin_role(role_key: str | None) -> bool:
    return normalize_role_key(role_key) == ROLE_SUPER_ADMIN
