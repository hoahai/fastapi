from __future__ import annotations

import ast
import json
from collections.abc import Iterable

from fastapi import HTTPException, Request
from shared.tenant import get_env


def _to_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    return None


def _parse_feature_flags(raw: str) -> dict[str, object]:
    value = (raw or "").strip()
    if not value:
        return {}

    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(value)
        except (ValueError, SyntaxError) as exc:
            raise HTTPException(status_code=400, detail="Invalid FEATURE_FLAGS format") from exc

    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="Invalid FEATURE_FLAGS format")

    return {str(k).strip().lower(): v for k, v in parsed.items() if str(k).strip()}


def require_feature(feature_name: str):
    normalized_feature = str(feature_name).strip().lower()

    def _dependency() -> None:
        feature_flags_raw = str(get_env("FEATURE_FLAGS", "") or "")
        flags = _parse_feature_flags(feature_flags_raw)
        enabled = _to_bool(flags.get(normalized_feature))
        if enabled is not True:
            raise HTTPException(
                status_code=403,
                detail=f"Tenant is not allowed to access feature '{normalized_feature}'",
            )

    return _dependency


def require_allowed_tenants(allowed_tenants: Iterable[str]):
    allowed = {
        str(tenant).strip().lower()
        for tenant in allowed_tenants
        if str(tenant).strip()
    }

    def _dependency(request: Request) -> None:
        tenant_id = str(getattr(request.state, "tenant_id", "") or "").strip().lower()
        if not tenant_id:
            raise HTTPException(status_code=400, detail="Missing tenant context")
        if tenant_id not in allowed:
            raise HTTPException(
                status_code=403,
                detail=f"Tenant '{tenant_id}' is not allowed for this route",
            )

    return _dependency
