from __future__ import annotations

from collections.abc import Iterable

PageDefinition = dict[str, str]

_APP_PAGE_CATALOG: dict[str, list[PageDefinition]] = {
    "tradsphere": [
        {"key": "tradsphere_home", "label": "Accounts", "route": "/tradsphere/home"},
        {"key": "tradsphere_estnums", "label": "Estimate Numbers", "route": "/tradsphere/estnums"},
        {"key": "tradsphere_contacts", "label": "Contacts", "route": "/tradsphere/contacts"},
        {"key": "tradsphere_stations", "label": "Stations", "route": "/tradsphere/stations"},
        {"key": "tradsphere_invoice_checklists", "label": "Invoice Checklists", "route": "/tradsphere/invoice-checklists"},
        {"key": "tradsphere_admin", "label": "Admin", "route": "/tradsphere/admin"},
    ],
    "shiftzy": [
        {"key": "shiftzy_home", "label": "Schedules", "route": "/shiftzy/home"},
        {"key": "shiftzy_employees", "label": "Employees", "route": "/shiftzy/employees"},
        {"key": "shiftzy_admin", "label": "Admin", "route": "/shiftzy/admin"},
    ],
}


def normalize_app_code(value: object) -> str:
    normalized = str(value or "").strip().lower()
    return normalized


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
        key = str(value or "").strip().lower()
        if key and key in allowed_keys:
            normalized_keys.add(key)
    return sorted(normalized_keys)
