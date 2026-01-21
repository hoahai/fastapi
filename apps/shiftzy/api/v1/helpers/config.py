from __future__ import annotations

import ast
from datetime import date
import json
import threading

from shared.tenant import (
    TenantConfigValidationError,
    get_env,
    get_tenant_id,
)

APP_NAME = "Shiftzy"


_VALIDATED_TENANTS: set[str] = set()
_VALIDATION_LOCK = threading.Lock()


# ============================================================
# ENUMS
# ============================================================


def _parse_list(key: str) -> list[str]:
    raw = get_env(key)
    if raw is None or str(raw).strip() == "":
        raise TenantConfigValidationError(app_name=APP_NAME, missing=[key])

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(raw)
        except (ValueError, SyntaxError) as exc:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                invalid=[key],
            ) from exc

    if not isinstance(parsed, list):
        raise TenantConfigValidationError(app_name=APP_NAME, invalid=[key])

    return [str(item) for item in parsed]


def get_position_areas() -> list[str]:
    return _parse_list("POSITION_AREAS_ENUM")


def get_schedule_sections() -> list[str]:
    return _parse_list("SCHEDULE_SECTIONS_ENUM")


# ============================================================
# VALIDATION
# ============================================================


def validate_tenant_config(tenant_id: str | None = None) -> None:
    tenant_id = tenant_id or get_tenant_id()
    if not tenant_id:
        raise TenantConfigValidationError(app_name=APP_NAME, missing=["tenant_id"])

    with _VALIDATION_LOCK:
        if tenant_id in _VALIDATED_TENANTS:
            return

        missing: list[str] = []
        invalid: list[str] = []

        def _check_required(key: str) -> str | None:
            raw = get_env(key)
            if raw is None or str(raw).strip() == "":
                missing.append(key)
                return None
            return str(raw).strip()

        def _check_int(key: str) -> int | None:
            raw = _check_required(key)
            if raw is None:
                return None
            try:
                return int(raw)
            except ValueError:
                invalid.append(key)
                return None

        def _check_start_date() -> None:
            raw = _check_required("START_DATE")
            if raw is None:
                return
            try:
                parsed = date.fromisoformat(raw)
            except ValueError:
                invalid.append("START_DATE")
                return
            if parsed.weekday() != 0:
                invalid.append("START_DATE")

        def _check_list(key: str) -> None:
            raw = _check_required(key)
            if raw is None:
                return
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                try:
                    parsed = ast.literal_eval(raw)
                except (ValueError, SyntaxError):
                    invalid.append(key)
                    return
            if not isinstance(parsed, list):
                invalid.append(key)

        _check_int("START_WEEK_NO")
        _check_start_date()

        before = _check_int("WEEK_BEFORE")
        after = _check_int("WEEK_AFTER")
        if before is not None and before < 0:
            invalid.append("WEEK_BEFORE")
        if after is not None and after < 0:
            invalid.append("WEEK_AFTER")

        _check_list("POSITION_AREAS_ENUM")
        _check_list("SCHEDULE_SECTIONS_ENUM")

        if missing or invalid:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                missing=missing,
                invalid=invalid,
            )

        _VALIDATED_TENANTS.add(tenant_id)
