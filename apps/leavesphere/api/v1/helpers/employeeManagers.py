from __future__ import annotations

from uuid import uuid4

from apps.leavesphere.api.v1.helpers.dbQueries import (
    delete_employee_manager,
    get_employee_managers,
    get_employees,
    insert_employee_manager,
)


def list_employee_managers(
    *,
    employee_id: str | None = None,
    manager_id: str | None = None,
) -> list[dict]:
    return get_employee_managers(employee_id=employee_id, manager_id=manager_id)


def get_employee_manager(mapping_id: str) -> dict | None:
    rows = get_employee_managers(mapping_id=mapping_id)
    return rows[0] if rows else None


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
