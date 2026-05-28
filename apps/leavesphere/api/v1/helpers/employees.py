from __future__ import annotations

from uuid import uuid4

from apps.leavesphere.api.v1.helpers.dbQueries import (
    get_employees,
    insert_employee,
    update_employee,
)

_VALID_REGIONS = {"US", "MEXICO", "PHILIPPINES"}


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
    return get_employees()


def get_employee(employee_id: str) -> dict | None:
    rows = get_employees(employee_id=employee_id)
    return rows[0] if rows else None


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
