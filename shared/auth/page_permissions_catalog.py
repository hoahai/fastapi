from __future__ import annotations

from collections.abc import Iterable

PageDefinition = dict[str, str]

_APP_PAGE_CATALOG: dict[str, list[PageDefinition]] = {
    "fundsphere": [
        {"key": "fundsphere_accounts", "label": "Accounts", "route": "/fundsphere/accounts"},
        {"key": "fundsphere_budgets", "label": "Budgets", "route": "/fundsphere/budgets"},
        {"key": "fundsphere_services", "label": "Services", "route": "/fundsphere/services"},
    ],
    "leavesphere": [
        {"key": "leavesphere_home", "label": "My PTO / Manager PTO", "route": "/leavesphere/home"},
        {"key": "leavesphere_leave_management", "label": "Leave Management", "route": "/leavesphere/leave-management"},
        {"key": "leavesphere_employees", "label": "Employee Management", "route": "/leavesphere/employees"},
    ],
    "tradsphere": [
        {"key": "tradsphere_home", "label": "Accounts", "route": "/tradsphere/home"},
        {"key": "tradsphere_estnums", "label": "Estimate Numbers", "route": "/tradsphere/estnums"},
        {"key": "tradsphere_contacts", "label": "Contacts", "route": "/tradsphere/contacts"},
        {"key": "tradsphere_stations", "label": "Stations", "route": "/tradsphere/stations"},
        {"key": "tradsphere_traffic", "label": "Traffic", "route": "/tradsphere/traffic"},
        {"key": "tradsphere_invoice_checklists", "label": "Invoice Checklists", "route": "/tradsphere/invoice-checklists"},
    ],
    "shiftzy": [
        {"key": "shiftzy_home", "label": "Schedules", "route": "/shiftzy/home"},
        {"key": "shiftzy_employees", "label": "Employees", "route": "/shiftzy/employees"},
    ],
}

_LEGACY_PAGE_KEY_ALIASES: dict[str, str] = {
    "leavesphere_admin_pto": "leavesphere_leave_management",
}


def normalize_app_code(value: object) -> str:
    normalized = str(value or "").strip().lower()
    return normalized


def normalize_page_key(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    return _LEGACY_PAGE_KEY_ALIASES.get(normalized, normalized)


def list_page_catalog_for_app(*, app_code: str) -> list[PageDefinition]:
    normalized_app_code = normalize_app_code(app_code)
    pages = _APP_PAGE_CATALOG.get(normalized_app_code, [])
    return [{**page} for page in pages]


def list_page_keys_for_app(*, app_code: str) -> set[str]:
    return {
        str(page.get("key") or "").strip()
        for page in list_page_catalog_for_app(app_code=app_code)
        if str(page.get("key") or "").strip()
    }


def normalize_page_keys(*, app_code: str, page_keys: Iterable[object]) -> list[str]:
    allowed_keys = list_page_keys_for_app(app_code=app_code)
    normalized_keys: set[str] = set()
    for value in page_keys:
        key = normalize_page_key(value)
        if key and key in allowed_keys:
            normalized_keys.add(key)
    return sorted(normalized_keys)


def _normalize_path(path: object) -> str:
    value = str(path or "").strip().lower()
    if not value:
        return ""
    if "?" in value:
        value = value.split("?", 1)[0]
    return value.rstrip("/") or "/"


_APP_API_PAGE_KEY_RULES: dict[str, tuple[tuple[tuple[str, ...], tuple[str, ...]], ...]] = {
    "fundsphere": (
        (
            (
                "/accounts",
                "/accountreps",
                "/budgets",
                "/departments",
                "/services",
                "/budgetchangehistories",
                "/masterbudgetcontrol",
                "/masterbudgetcontrol/budgetmatrix",
            ),
            ("fundsphere_accounts", "fundsphere_budgets", "fundsphere_services"),
        ),
    ),
    "leavesphere": (
        (("/ui/my-pto", "/my-pto"), ("leavesphere_home",)),
        (
            ("/ui/employees", "/employees/picture/upload", "/employeemanagers", "/employees"),
            ("leavesphere_employees", "leavesphere_leave_management"),
        ),
        (
            ("/admin/pto", "/ptotypes", "/ptoactions", "/admin/google-calendar"),
            ("leavesphere_leave_management",),
        ),
    ),
    "tradsphere": (
        # Shared account selector/load helpers used by multiple pages.
        (
            ("/ui/main/selections",),
            (
                "tradsphere_home",
                "tradsphere_estnums",
                "tradsphere_contacts",
                "tradsphere_stations",
                "tradsphere_traffic",
                "tradsphere_invoice_checklists",
            ),
        ),
        (("/ui/accounts/load", "/ui/main/load", "/accounts", "/schedules", "/broadcastcalendar"), ("tradsphere_home",)),
        (("/estnums",), ("tradsphere_estnums",)),
        (("/contacts/station-codes", "/contacts/selector"), ("tradsphere_stations", "tradsphere_contacts")),
        (("/contacts/stationscontacts", "/contacts"), ("tradsphere_contacts",)),
        (("/stations/deliverymethods", "/stations"), ("tradsphere_stations",)),
        (("/traffic",), ("tradsphere_traffic",)),
        (
            (
                "/ui/invoice-checklists/load",
                "/invoice-checklists",
                "/invoice-checklist-stations",
                "/invoice-checklist-notes",
                "/invoice-note-attachments",
            ),
            ("tradsphere_invoice_checklists",),
        ),
    ),
    "shiftzy": (
        (("/home",), ("shiftzy_home",)),
        (("/employees",), ("shiftzy_employees",)),
    ),
}


def resolve_page_keys_for_api_path(*, app_code: str, path: object) -> set[str]:
    normalized_app_code = normalize_app_code(app_code)
    normalized_path = _normalize_path(path)
    if not normalized_app_code or not normalized_path:
        return set()

    rules = _APP_API_PAGE_KEY_RULES.get(normalized_app_code, tuple())
    matched: set[str] = set()
    for fragments, page_keys in rules:
        if any(fragment in normalized_path for fragment in fragments):
            matched.update(page_keys)
    return matched
