from __future__ import annotations

import threading

from shared.tenant import TenantConfigValidationError, get_tenant_id


# ============================================================
# CONSTANTS
# ============================================================

APP_NAME = "FundSphere"

_VALIDATED_TENANTS: set[str] = set()
_VALIDATION_LOCK = threading.Lock()


# ============================================================
# VALIDATION
# ============================================================


def validate_tenant_config(tenant_id: str | None = None) -> None:
    """
    Validate tenant context for FundSphere.

    This v1 scaffold only enforces that a tenant id exists.
    """
    resolved_tenant_id = tenant_id or get_tenant_id()
    if not resolved_tenant_id:
        raise TenantConfigValidationError(app_name=APP_NAME, missing=["tenant_id"])

    with _VALIDATION_LOCK:
        if resolved_tenant_id in _VALIDATED_TENANTS:
            return
        _VALIDATED_TENANTS.add(resolved_tenant_id)
