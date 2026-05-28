from __future__ import annotations

from apps.leavesphere.api.v1.helpers.dbQueries import (
    get_pto_actions,
    insert_pto_action,
    update_pto_action,
)


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


def _normalize_color(value: object) -> str | None:
    color = str(value or "").strip()
    if not color:
        return None
    if len(color) > 255:
        raise ValueError("color must be <= 255 characters")
    return color


def _normalize_duplicate_error(exc: Exception) -> str | None:
    detail = str(exc).lower()
    if "duplicate entry" in detail and "for key 'primary'" in detail:
        return "PTO action code already exists"
    return None


def list_pto_actions() -> list[dict]:
    return get_pto_actions()


def get_pto_action(code: str) -> dict | None:
    rows = get_pto_actions(code=code)
    return rows[0] if rows else None


def create_pto_action(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    item = {
        "code": _normalize_code(payload.get("code")),
        "name": _normalize_name(payload.get("name")),
        "color": _normalize_color(payload.get("color")),
    }
    try:
        inserted = insert_pto_action(item)
    except Exception as exc:
        friendly = _normalize_duplicate_error(exc)
        if friendly:
            raise ValueError(friendly) from exc
        raise
    return {"code": item["code"], "inserted": inserted}


def modify_pto_action(*, code: str, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    normalized_code = _normalize_code(code)
    if get_pto_action(normalized_code) is None:
        raise ValueError("PTO action not found")

    updates: dict[str, object] = {}
    if "name" in payload:
        updates["name"] = _normalize_name(payload.get("name"))
    if "color" in payload:
        updates["color"] = _normalize_color(payload.get("color"))
    if not updates:
        raise ValueError("At least one updatable field is required")

    updated = update_pto_action(code=normalized_code, updates=updates)
    return {"updated": updated}
