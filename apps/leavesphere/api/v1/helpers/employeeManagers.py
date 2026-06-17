from __future__ import annotations

from uuid import uuid4

from apps.leavesphere.api.v1.helpers.dbQueries import (
    delete_employee_manager,
    get_employee_managers,
    get_employees,
    insert_employee_manager,
)
from apps.leavesphere.api.v1.helpers.readCache import read_leave_sphere_read_cache, write_leave_sphere_read_cache


def list_employee_managers(
    *,
    employee_id: str | None = None,
    manager_id: str | None = None,
) -> list[dict]:
    normalized_employee_id = str(employee_id or "").strip() or None
    normalized_manager_id = str(manager_id or "").strip() or None
    cache_params = {
        "employee_id": normalized_employee_id,
        "manager_id": normalized_manager_id,
    }
    cached = read_leave_sphere_read_cache(namespace="employee-managers:list", params=cache_params)
    if isinstance(cached, list):
        return cached
    rows = get_employee_managers(employee_id=normalized_employee_id, manager_id=normalized_manager_id)
    write_leave_sphere_read_cache(namespace="employee-managers:list", params=cache_params, value=rows)
    return rows


def get_employee_manager(mapping_id: str) -> dict | None:
    normalized_mapping_id = str(mapping_id or "").strip()
    cached = read_leave_sphere_read_cache(namespace="employee-managers:item", params={"mapping_id": normalized_mapping_id})
    if isinstance(cached, dict):
        return cached
    rows = get_employee_managers(mapping_id=normalized_mapping_id)
    item = rows[0] if rows else None
    if item is not None:
        write_leave_sphere_read_cache(
            namespace="employee-managers:item",
            params={"mapping_id": normalized_mapping_id},
            value=item,
        )
    return item


def _require_active_employee(employee_id: str, *, field: str) -> None:
    rows = get_employees(employee_id=employee_id)
    if not rows:
        raise ValueError(f"{field} not found")
    active = int(rows[0].get("active") or 0)
    if active != 1:
        raise ValueError(f"{field} must be active")


def _normalize_duplicate_error(exc: Exception) -> str | None:
    detail = str(exc).lower()
    if "duplicate entry" in detail and "employeeid_managerid" in detail:
        return "Employee manager relationship already exists"
    return None


def create_employee_manager(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    employee_id = str(payload.get("employeeId") or "").strip()
    manager_id = str(payload.get("managerId") or "").strip()
    if not employee_id:
        raise ValueError("employeeId is required")
    if not manager_id:
        raise ValueError("managerId is required")
    if employee_id == manager_id:
        raise ValueError("employeeId cannot be the same as managerId")

    _require_active_employee(employee_id, field="employeeId")
    _require_active_employee(manager_id, field="managerId")

    row = {
        "id": str(payload.get("id") or "").strip() or str(uuid4()),
        "employeeId": employee_id,
        "managerId": manager_id,
    }
    try:
        inserted = insert_employee_manager(item=row)
    except Exception as exc:
        friendly = _normalize_duplicate_error(exc)
        if friendly:
            raise ValueError(friendly) from exc
        raise
    return {"id": row["id"], "inserted": inserted}


def remove_employee_manager(mapping_id: str) -> dict:
    if get_employee_manager(mapping_id) is None:
        raise ValueError("Employee manager mapping not found")
    deleted = delete_employee_manager(mapping_id=mapping_id)
    return {"deleted": deleted}
