from __future__ import annotations

from apps.leavesphere.api.v1.helpers.dbQueries import (
    get_pto_types,
    insert_pto_type,
    update_pto_type,
)
from apps.leavesphere.api.v1.helpers.readCache import read_leave_sphere_read_cache, write_leave_sphere_read_cache


def _normalize_code(value: object) -> str:
    code = str(value or "").strip().upper()
    if not code:
        raise ValueError("code is required")
    if len(code) > 10:
        raise ValueError("code must be <= 10 characters")
    return code


def _normalize_name(value: object) -> str:
    name = str(value or "").strip()
    if not name:
        raise ValueError("name is required")
    if len(name) > 255:
        raise ValueError("name must be <= 255 characters")
    return name


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


def _normalize_uint(value: object | None, *, field: str, default: int | None = None, allow_none: bool = False) -> int | None:
    if value is None:
        if allow_none:
            return None
        if default is None:
            raise ValueError(f"{field} is required")
        return int(default)
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if parsed < 0:
        raise ValueError(f"{field} must be >= 0")
    if parsed > 255:
        raise ValueError(f"{field} must be <= 255")
    return parsed


def _normalize_duplicate_error(exc: Exception) -> str | None:
    detail = str(exc).lower()
    if "duplicate entry" in detail and "for key 'primary'" in detail:
        return "PTO type code already exists"
    return None


def list_pto_types() -> list[dict]:
    cached = read_leave_sphere_read_cache(namespace="pto-types:list")
    if isinstance(cached, list):
        return cached
    rows = get_pto_types()
    write_leave_sphere_read_cache(namespace="pto-types:list", value=rows)
    return rows


def get_pto_type(code: str) -> dict | None:
    normalized_code = _normalize_code(code)
    cached = read_leave_sphere_read_cache(namespace="pto-types:item", params={"code": normalized_code})
    if isinstance(cached, dict):
        return cached
    rows = get_pto_types(code=normalized_code)
    item = rows[0] if rows else None
    if item is not None:
        write_leave_sphere_read_cache(namespace="pto-types:item", params={"code": normalized_code}, value=item)
    return item


def create_pto_type(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    item = {
        "code": _normalize_code(payload.get("code")),
        "name": _normalize_name(payload.get("name")),
        "rolloverable": _normalize_bool(payload.get("rolloverable"), field="rolloverable", default=0),
        "payoutable": _normalize_bool(payload.get("payoutable"), field="payoutable", default=0),
        "listingOrder": _normalize_uint(payload.get("listingOrder"), field="listingOrder", allow_none=True),
        "usaDefaultHour": _normalize_uint(payload.get("usaDefaultHour"), field="usaDefaultHour", default=0),
        "phlDefaultHour": _normalize_uint(payload.get("phlDefaultHour"), field="phlDefaultHour", default=0),
    }
    try:
        inserted = insert_pto_type(item)
    except Exception as exc:
        friendly = _normalize_duplicate_error(exc)
        if friendly:
            raise ValueError(friendly) from exc
        raise
    return {"code": item["code"], "inserted": inserted}


def modify_pto_type(*, code: str, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    normalized_code = _normalize_code(code)
    if get_pto_type(normalized_code) is None:
        raise ValueError("PTO type not found")

    updates: dict[str, object] = {}
    if "name" in payload:
        updates["name"] = _normalize_name(payload.get("name"))
    if "rolloverable" in payload:
        updates["rolloverable"] = _normalize_bool(payload.get("rolloverable"), field="rolloverable")
    if "payoutable" in payload:
        updates["payoutable"] = _normalize_bool(payload.get("payoutable"), field="payoutable")
    if "listingOrder" in payload:
        updates["listingOrder"] = _normalize_uint(payload.get("listingOrder"), field="listingOrder", allow_none=True)
    if "usaDefaultHour" in payload:
        updates["usaDefaultHour"] = _normalize_uint(payload.get("usaDefaultHour"), field="usaDefaultHour")
    if "phlDefaultHour" in payload:
        updates["phlDefaultHour"] = _normalize_uint(payload.get("phlDefaultHour"), field="phlDefaultHour")
    if not updates:
        raise ValueError("At least one updatable field is required")

    updated = update_pto_type(code=normalized_code, updates=updates)
    return {"updated": updated}
