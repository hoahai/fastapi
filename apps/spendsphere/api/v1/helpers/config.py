import ast
import json
import threading

from shared.tenant import get_env, get_tenant_id, TenantConfigError


def _require_env_value(key: str) -> str:
    raw = get_env(key)
    if raw is None or str(raw).strip() == "":
        raise TenantConfigError(f"Missing tenant config: {key}")
    return str(raw).strip()


def _parse_env_value(key: str, expected_type):
    raw = _require_env_value(key)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(raw)
        except (ValueError, SyntaxError) as exc:
            raise TenantConfigError(f"Invalid tenant config: {key}") from exc

    if not isinstance(parsed, expected_type):
        raise TenantConfigError(f"Invalid tenant config: {key}")
    return parsed


def get_service_budgets() -> list[str]:
    value = _parse_env_value("SERVICE_BUDGETS", list)
    return [str(v) for v in value]


def get_service_mapping() -> dict:
    return _parse_env_value("SERVICE_MAPPING", dict)


def get_adtypes() -> dict:
    return _parse_env_value("ADTYPES", dict)


def get_spendsphere_sheets() -> dict[str, dict[str, str]]:
    spreadsheet_id = _require_env_value("SPENDSPHERE_SPREADSHEET_ID")
    rollovers_sheet_name = _require_env_value("SPENDSPHERE_ROLLOVERS_SHEET_NAME")
    active_sheet_name = _require_env_value("SPENDSPHERE_ACTIVEPERIOD_SHEET_NAME")

    return {
        "rollovers": {
            "spreadsheet_id": spreadsheet_id,
            "range_name": rollovers_sheet_name,
        },
        "active_period": {
            "spreadsheet_id": spreadsheet_id,
            "range_name": active_sheet_name,
        },
    }


_VALIDATED_TENANTS: set[str] = set()
_VALIDATION_LOCK = threading.Lock()


def validate_tenant_config(tenant_id: str | None = None) -> None:
    """
    Ensure all required tenant config keys exist and are valid for v1.
    Cached per tenant to avoid re-validating on every request.
    """
    tenant_id = tenant_id or get_tenant_id()
    if not tenant_id:
        raise TenantConfigError("Missing tenant config: tenant_id")

    with _VALIDATION_LOCK:
        if tenant_id in _VALIDATED_TENANTS:
            return

        get_service_budgets()
        get_service_mapping()
        get_adtypes()
        get_spendsphere_sheets()
        _VALIDATED_TENANTS.add(tenant_id)
