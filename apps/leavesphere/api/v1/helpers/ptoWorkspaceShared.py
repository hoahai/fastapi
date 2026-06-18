from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from functools import lru_cache
import re
from typing import Callable

from apps.leavesphere.api.v1.helpers.dbQueries import get_pto_actions, get_pto_types
from apps.leavesphere.api.v1.helpers.ptoAccounting import (
    REQUEST_ACTION_TOKENS,
    REQUEST_CANCEL_ACTION_TOKENS,
    resolve_action_code,
)


@dataclass(frozen=True)
class LeaveSpherePtoWorkspaceCatalogs:
    pto_types: list[dict]
    pto_type_by_code: dict[str, dict]
    pto_actions: list[dict]
    pto_action_by_code: dict[str, dict]
    default_request_action_code: str
    default_cancel_action_code: str


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def _normalize_lookup_key(value: object | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", _normalize_text(value).lower())


def _to_date_string(value: object | None) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = _normalize_text(value)
    if not text:
        return ""
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return text[:10]


def _split_ui_type_tokens(*values: object | None) -> str | None:
    text = " ".join(_normalize_text(value).lower() for value in values if _normalize_text(value))
    if not text:
        return None
    for token, ui_type in (
        ("vac", "vacation"),
        ("sick", "sick"),
        ("personal", "personal"),
        ("float", "floating"),
    ):
        if token in text:
            return ui_type
    return None


@lru_cache(maxsize=1)
def build_pto_type_catalog() -> tuple[list[dict], dict[str, dict]]:
    rows = get_pto_types()
    catalog: list[dict] = []
    by_code: dict[str, dict] = {}
    for row in rows:
        code = _normalize_text(row.get("code")).upper()
        if not code:
            continue
        label = _normalize_text(row.get("name")) or code
        ui_type = _split_ui_type_tokens(code, label) or code.lower()
        item = {
            "code": code,
            "type": ui_type,
            "label": label,
            "active": True,
            "listingOrder": int(row.get("listingOrder") or 0),
            "rolloverable": bool(int(row.get("rolloverable") or 0)),
            "payoutable": bool(int(row.get("payoutable") or 0)),
            "usaDefaultHour": int(row.get("usaDefaultHour") or 0),
            "phlDefaultHour": int(row.get("phlDefaultHour") or 0),
        }
        catalog.append(item)
        by_code[code] = item

    catalog.sort(key=lambda item: (item["listingOrder"], item["label"].lower(), item["code"]))
    if not catalog:
        for ui_type in ("vacation", "sick", "personal", "floating"):
            item = {
                "code": ui_type.upper(),
                "type": ui_type,
                "label": ui_type.title(),
                "active": True,
                "listingOrder": 0,
                "rolloverable": False,
                "payoutable": False,
                "usaDefaultHour": 0,
                "phlDefaultHour": 0,
            }
            catalog.append(item)
            by_code[item["code"]] = item
    return catalog, by_code


@lru_cache(maxsize=1)
def build_pto_action_catalog() -> tuple[list[dict], dict[str, dict]]:
    rows = get_pto_actions()
    catalog: list[dict] = []
    by_code: dict[str, dict] = {}
    for row in rows:
        code = _normalize_text(row.get("code")).upper()
        if not code:
            continue
        item = {
            "code": code,
            "name": _normalize_text(row.get("name")) or code,
            "color": _normalize_text(row.get("color")) or None,
        }
        catalog.append(item)
        by_code[code] = item
    catalog.sort(key=lambda item: (item["code"], item["name"].lower()))
    return catalog, by_code


def build_pto_employee_map(employees: list[dict]) -> dict[str, dict]:
    return {
        _normalize_text(employee.get("id")): employee
        for employee in employees
        if _normalize_text(employee.get("id"))
    }


def resolve_pto_type_code_from_catalog(*, value: object | None, pto_type_by_code: dict[str, dict]) -> str:
    normalized = _normalize_lookup_key(value)
    if not normalized:
        return ""

    for code, item in pto_type_by_code.items():
        candidates = (
            _normalize_lookup_key(code),
            _normalize_lookup_key(item.get("type")),
            _normalize_lookup_key(item.get("label")),
        )
        if normalized in candidates:
            return code

    return _normalize_text(value).upper()


def load_leave_sphere_pto_workspace_catalogs() -> LeaveSpherePtoWorkspaceCatalogs:
    pto_types, pto_type_by_code = build_pto_type_catalog()
    pto_actions, pto_action_by_code = build_pto_action_catalog()
    return LeaveSpherePtoWorkspaceCatalogs(
        pto_types=pto_types,
        pto_type_by_code=pto_type_by_code,
        pto_actions=pto_actions,
        pto_action_by_code=pto_action_by_code,
        default_request_action_code=resolve_action_code(
            pto_actions,
            include_tokens=REQUEST_ACTION_TOKENS,
        ),
        default_cancel_action_code=resolve_action_code(
            pto_actions,
            include_tokens=REQUEST_CANCEL_ACTION_TOKENS,
            fallback_index=0,
        ),
    )


def clear_leave_sphere_pto_workspace_catalog_cache() -> None:
    build_pto_type_catalog.cache_clear()
    build_pto_action_catalog.cache_clear()


def build_leave_sphere_workspace_common_payload(
    catalogs: LeaveSpherePtoWorkspaceCatalogs,
    *,
    last_submission_date: str | None = None,
) -> dict:
    return {
        "ptoTypes": catalogs.pto_types,
        "ptoActions": catalogs.pto_actions,
        "defaultRequestActionCode": catalogs.default_request_action_code,
        "defaultCancelActionCode": catalogs.default_cancel_action_code,
        "lastSubmissionDate": last_submission_date,
    }


def build_pto_request_rows(
    *,
    rows: list[dict],
    employee_map: dict[str, dict],
    pto_type_by_code: dict[str, dict],
    current_employee_id: str,
    manager_id_by_employee_id: dict[str, str | None],
    employee_full_name_fn: Callable[[dict | None], str],
) -> list[dict]:
    requests: list[dict] = []
    for row in rows:
        employee_id = _normalize_text(row.get("employeeId"))
        employee = employee_map.get(employee_id)
        if employee is None:
            continue
        pto_type_code = _normalize_text(row.get("ptoTypeCode")).upper()
        pto_type = pto_type_by_code.get(pto_type_code)
        ui_type = _normalize_text(pto_type.get("type")) if pto_type else pto_type_code.lower()
        if not ui_type:
            continue

        status = _normalize_text(row.get("status")).lower()
        hours = abs(Decimal(str(row.get("hours") or 0))).quantize(Decimal("0.01"))
        approver_id = _normalize_text(row.get("approverId"))
        approver = employee_map.get(approver_id) if approver_id else None
        submitted_at = _to_date_string(row.get("dateCreated"))
        reviewed_at = _to_date_string(row.get("dateUpdated")) if status != "pending" else ""
        description = _normalize_text(row.get("description"))
        requests.append(
            {
                "id": _normalize_text(row.get("id")),
                "employeeId": employee_id,
                "managerId": manager_id_by_employee_id.get(employee_id) or current_employee_id,
                "type": ui_type,
                "ptoTypeCode": pto_type_code,
                "startDate": _to_date_string(row.get("startDate")),
                "endDate": _to_date_string(row.get("endDate")),
                "hours": float(hours),
                "description": description,
                "status": status,
                "submittedAt": submitted_at,
                "reviewedAt": reviewed_at or None,
                "reviewerName": employee_full_name_fn(approver) if approver else None,
                "approverNote": _normalize_text(row.get("approverNote")) or None,
            }
        )

    requests.sort(key=lambda item: (item["submittedAt"] or "", item["id"]), reverse=True)
    return requests
