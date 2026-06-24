from __future__ import annotations

from uuid import uuid4

from apps.leavesphere.api.v1.helpers.dbQueries import (
    get_employees,
    insert_employee,
    update_employee,
)
from apps.leavesphere.api.v1.helpers.readCache import read_leave_sphere_read_cache, write_leave_sphere_read_cache

_VALID_REGIONS = {"US", "MEXICO", "PHILIPPINES"}
_EMPLOYEE_MANAGEMENT_WORKSPACE_NAMESPACE = "employee-management:workspace"
_EMPLOYEE_LIST_NAMESPACE = "employees:list"


def _normalize_required_text(value: object, *, field: str, max_length: int = 255) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    if len(text) > max_length:
        raise ValueError(f"{field} must be <= {max_length} characters")
    return text


def _normalize_optional_text(value: object, *, field: str, max_length: int) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) > max_length:
        raise ValueError(f"{field} must be <= {max_length} characters")
    return text


def _normalize_bool(value: object | None, *, field: str, default: int | None = None) -> int:
    if value is None:
        if default is None:
            raise ValueError(f"{field} is required")
        return int(default)
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if bool(value) else 0
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return 1
    if text in {"0", "false", "no", "n", "off"}:
        return 0
    raise ValueError(f"{field} must be boolean-like")


def _normalize_region(value: object | None, *, required: bool) -> str | None:
    if value is None:
        return "US" if required else None
    text = str(value).strip()
    if not text:
        return "US" if required else None
    normalized = text.upper()
    if normalized not in _VALID_REGIONS:
        raise ValueError("region must be one of US, Mexico, Philippines")
    if normalized == "MEXICO":
        return "Mexico"
    if normalized == "PHILIPPINES":
        return "Philippines"
    return "US"


def _normalize_duplicate_error(exc: Exception) -> str | None:
    detail = str(exc)
    lower = detail.lower()
    if "duplicate entry" not in lower:
        return None
    if "for key 'identitykey'" in lower:
        return "identityKey already exists"
    if "for key 'email'" in lower:
        return "email already exists"
    if "for key 'phone'" in lower:
        return "phone already exists"
    return "Duplicate employee value violates unique constraints"


def list_employees() -> list[dict]:
    cached = read_leave_sphere_read_cache(namespace=_EMPLOYEE_LIST_NAMESPACE)
    if isinstance(cached, list):
        return cached
    rows = get_employees()
    write_leave_sphere_read_cache(namespace=_EMPLOYEE_LIST_NAMESPACE, value=rows)
    return rows


def get_employee(employee_id: str) -> dict | None:
    normalized_employee_id = str(employee_id or "").strip()
    cached = read_leave_sphere_read_cache(namespace="employees:item", params={"employee_id": normalized_employee_id})
    if isinstance(cached, dict):
        return cached
    rows = get_employees(employee_id=normalized_employee_id)
    item = rows[0] if rows else None
    if item is not None:
        write_leave_sphere_read_cache(
            namespace="employees:item",
            params={"employee_id": normalized_employee_id},
            value=item,
        )
    return item


def create_employee(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    item = {
        "id": str(payload.get("id") or "").strip() or str(uuid4()),
        "identityKey": _normalize_required_text(payload.get("identityKey"), field="identityKey", max_length=36),
        "firstName": _normalize_required_text(payload.get("firstName"), field="firstName"),
        "lastName": _normalize_required_text(payload.get("lastName"), field="lastName"),
        "email": _normalize_required_text(payload.get("email"), field="email"),
        "phone": _normalize_optional_text(payload.get("phone"), field="phone", max_length=20),
        "dob": payload.get("dob"),
        "pictureUrl": _normalize_optional_text(payload.get("pictureUrl"), field="pictureUrl", max_length=2048),
        "region": _normalize_region(payload.get("region"), required=True),
        "startDate": payload.get("startDate"),
        "title": _normalize_optional_text(payload.get("title"), field="title", max_length=255),
        "isAE": _normalize_bool(payload.get("isAE"), field="isAE", default=0),
        "active": _normalize_bool(payload.get("active"), field="active", default=1),
    }

    try:
        inserted = insert_employee(item)
    except Exception as exc:
        friendly = _normalize_duplicate_error(exc)
        if friendly:
            raise ValueError(friendly) from exc
        raise

    return {"id": item["id"], "inserted": inserted}


def modify_employee(*, employee_id: str, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    updates: dict[str, object] = {}
    if "identityKey" in payload:
        updates["identityKey"] = _normalize_required_text(payload.get("identityKey"), field="identityKey", max_length=36)
    if "firstName" in payload:
        updates["firstName"] = _normalize_required_text(payload.get("firstName"), field="firstName")
    if "lastName" in payload:
        updates["lastName"] = _normalize_required_text(payload.get("lastName"), field="lastName")
    if "email" in payload:
        updates["email"] = _normalize_required_text(payload.get("email"), field="email")
    if "phone" in payload:
        updates["phone"] = _normalize_optional_text(payload.get("phone"), field="phone", max_length=20)
    if "dob" in payload:
        updates["dob"] = payload.get("dob")
    if "pictureUrl" in payload:
        updates["pictureUrl"] = _normalize_optional_text(payload.get("pictureUrl"), field="pictureUrl", max_length=2048)
    if "region" in payload:
        updates["region"] = _normalize_region(payload.get("region"), required=False)
    if "startDate" in payload:
        updates["startDate"] = payload.get("startDate")
    if "title" in payload:
        updates["title"] = _normalize_optional_text(payload.get("title"), field="title", max_length=255)
    if "isAE" in payload:
        updates["isAE"] = _normalize_bool(payload.get("isAE"), field="isAE")
    if "active" in payload:
        updates["active"] = _normalize_bool(payload.get("active"), field="active")

    if not updates:
        raise ValueError("At least one updatable field is required")
    if get_employee(employee_id) is None:
        raise ValueError("Employee not found")

    try:
        updated = update_employee(employee_id=employee_id, updates=updates)
    except Exception as exc:
        friendly = _normalize_duplicate_error(exc)
        if friendly:
            raise ValueError(friendly) from exc
        raise
    return {"updated": updated}


def _build_employee_management_workspace(employees: list[dict]) -> dict:
    employee_rows = [row for row in employees if isinstance(row, dict)]
    active_count = sum(1 for row in employee_rows if int(row.get("active") or 0) == 1)
    total_count = len(employee_rows)
    inactive_count = total_count - active_count
    return {
        "pageCode": "employee-management",
        "pageTitle": "Employee Management",
        "summary": {
            "totalEmployees": total_count,
            "activeEmployees": active_count,
            "inactiveEmployees": inactive_count,
        },
        "capabilities": {
            "canCreate": True,
            "canUpdate": True,
            "canActivate": True,
            "canDeactivate": True,
            "canArchive": False,
        },
        "employees": employee_rows,
    }


def _read_employee_rows(*, force_refresh: bool) -> list[dict]:
    if force_refresh:
        rows = get_employees()
        write_leave_sphere_read_cache(namespace=_EMPLOYEE_LIST_NAMESPACE, value=rows)
        return rows
    return list_employees()


def load_employee_management_workspace(*, force_refresh: bool = False) -> dict:
    if not force_refresh:
        cached = read_leave_sphere_read_cache(namespace=_EMPLOYEE_MANAGEMENT_WORKSPACE_NAMESPACE)
        if isinstance(cached, dict):
            return cached

    employees = _read_employee_rows(force_refresh=force_refresh)
    workspace = _build_employee_management_workspace(employees)
    write_leave_sphere_read_cache(namespace=_EMPLOYEE_MANAGEMENT_WORKSPACE_NAMESPACE, value=workspace)
    return workspace


def _set_employee_active(*, employee_id: str, active: bool) -> dict:
    normalized_employee_id = str(employee_id or "").strip()
    if not normalized_employee_id:
        raise ValueError("Employee not found")

    try:
        result = modify_employee(employee_id=normalized_employee_id, payload={"active": 1 if active else 0})
    except ValueError:
        raise

    rows = get_employees(employee_id=normalized_employee_id)
    employee = rows[0] if rows else None
    if employee is None:
        raise ValueError("Employee not found")
    updated = result.get("updated") if isinstance(result, dict) else result
    return {"updated": int(updated or 0), "employee": employee}


def activate_employee(employee_id: str) -> dict:
    return _set_employee_active(employee_id=employee_id, active=True)


def deactivate_employee(employee_id: str) -> dict:
    return _set_employee_active(employee_id=employee_id, active=False)
