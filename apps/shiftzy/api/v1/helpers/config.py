from __future__ import annotations

from datetime import date
import threading

from shared.tenant import (
    TenantConfigValidationError,
    get_env,
    get_tenant_id,
)

APP_NAME = "Shiftzy"


_VALIDATED_TENANTS: set[str] = set()
_VALIDATION_LOCK = threading.Lock()


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

        _check_int("START_WEEK_NO")
        _check_start_date()

        before = _check_int("WEEK_BEFORE")
        after = _check_int("WEEK_AFTER")
        if before is not None and before < 0:
            invalid.append("WEEK_BEFORE")
        if after is not None and after < 0:
            invalid.append("WEEK_AFTER")

        if missing or invalid:
            raise TenantConfigValidationError(
                app_name=APP_NAME,
                missing=missing,
                invalid=invalid,
            )

        _VALIDATED_TENANTS.add(tenant_id)
