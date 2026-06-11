from __future__ import annotations

from dataclasses import dataclass

READ_METHODS = {"GET", "HEAD", "OPTIONS"}
APP_CODES = ("tradsphere", "spendsphere", "fundsphere", "shiftzy", "leavesphere", "opssphere")

# Explicit route exemptions used by permission guards.
PUBLIC_PATHS_EXACT = {
    "/",
    "/ping",
}
PUBLIC_PATH_PREFIXES = (
    "/auth/",
    "/assets/",
    "/fe/assets/",
    "/public/opssphere/advWebsiteReport/reports/cta",
)


@dataclass(frozen=True)
class PermissionRouteRule:
    app_code: str
    path_fragments: tuple[str, ...]
    read_permissions: tuple[str, ...]
    write_permissions: tuple[str, ...]


def is_public_or_exempt_path(path: str) -> bool:
    normalized = str(path or "").strip()
    if not normalized:
        return False
    if normalized in PUBLIC_PATHS_EXACT:
        return True
    return any(normalized.startswith(prefix) for prefix in PUBLIC_PATH_PREFIXES)


def is_read_method(method: str) -> bool:
    return str(method or "").upper() in READ_METHODS


def default_role_permissions_for_app(*, role: str, app_code: str) -> set[str]:
    normalized_role = str(role or "").strip().lower()
    normalized_app_code = str(app_code or "").strip().lower()
    if not normalized_role or not normalized_app_code:
        return set()

    permissions: set[str] = {f"{normalized_app_code}.viewer"}
    if normalized_role in {"editor", "admin"}:
        permissions.add(f"{normalized_app_code}.editor")
    if normalized_role == "admin":
        permissions.add(f"{normalized_app_code}.admin")
    return permissions


TRADSPHERE_ROUTE_RULES: tuple[PermissionRouteRule, ...] = (
    PermissionRouteRule(
        app_code="tradsphere",
        path_fragments=("/contacts/stationscontacts", "/stationscontacts"),
        read_permissions=("tradsphere.contacts.viewer", "tradsphere.viewer"),
        write_permissions=("tradsphere.contacts.editor", "tradsphere.editor"),
    ),
    PermissionRouteRule(
        app_code="tradsphere",
        path_fragments=("/contacts",),
        read_permissions=("tradsphere.contacts.viewer", "tradsphere.viewer"),
        write_permissions=("tradsphere.contacts.editor", "tradsphere.editor"),
    ),
    PermissionRouteRule(
        app_code="tradsphere",
        path_fragments=("/stations/deliverymethods", "/deliverymethods"),
        read_permissions=("tradsphere.stations.viewer", "tradsphere.viewer"),
        write_permissions=("tradsphere.stations.editor", "tradsphere.editor"),
    ),
    PermissionRouteRule(
        app_code="tradsphere",
        path_fragments=("/stations",),
        read_permissions=("tradsphere.stations.viewer", "tradsphere.viewer"),
        write_permissions=("tradsphere.stations.editor", "tradsphere.editor"),
    ),
    PermissionRouteRule(
        app_code="tradsphere",
        path_fragments=("/estnums",),
        read_permissions=("tradsphere.estnums.viewer", "tradsphere.viewer"),
        write_permissions=("tradsphere.estnums.editor", "tradsphere.editor"),
    ),
    PermissionRouteRule(
        app_code="tradsphere",
        path_fragments=("/schedules",),
        read_permissions=("tradsphere.schedules.viewer", "tradsphere.viewer"),
        write_permissions=("tradsphere.editor",),
    ),
    PermissionRouteRule(
        app_code="tradsphere",
        path_fragments=("/broadcastcalendar",),
        read_permissions=("tradsphere.viewer",),
        write_permissions=("tradsphere.editor",),
    ),
    PermissionRouteRule(
        app_code="tradsphere",
        path_fragments=("/accounts", "/ui/"),
        read_permissions=("tradsphere.viewer",),
        write_permissions=("tradsphere.editor",),
    ),
)

LEAVESPHERE_ROUTE_RULES: tuple[PermissionRouteRule, ...] = (
    PermissionRouteRule(
        app_code="leavesphere",
        path_fragments=("/ui/my-pto/requests",),
        read_permissions=("leavesphere.viewer",),
        write_permissions=("leavesphere.viewer",),
    ),
    PermissionRouteRule(
        app_code="leavesphere",
        path_fragments=("/ui/my-pto/review",),
        read_permissions=("leavesphere.viewer",),
        write_permissions=("leavesphere.editor",),
    ),
)


def resolve_required_permissions(*, app_code: str, method: str, path: str) -> tuple[str, ...]:
    normalized_app_code = str(app_code or "").strip().lower()
    normalized_path = str(path or "").strip().lower()

    if not normalized_app_code:
        return tuple()

    if is_public_or_exempt_path(normalized_path):
        return tuple()

    if normalized_app_code == "tradsphere":
        for rule in TRADSPHERE_ROUTE_RULES:
            if any(fragment in normalized_path for fragment in rule.path_fragments):
                return rule.read_permissions if is_read_method(method) else rule.write_permissions

    if normalized_app_code == "leavesphere":
        for rule in LEAVESPHERE_ROUTE_RULES:
            if any(fragment in normalized_path for fragment in rule.path_fragments):
                return rule.read_permissions if is_read_method(method) else rule.write_permissions

    return (
        (f"{normalized_app_code}.viewer",)
        if is_read_method(method)
        else (f"{normalized_app_code}.editor",)
    )
