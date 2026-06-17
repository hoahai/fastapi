from __future__ import annotations

from decimal import Decimal, InvalidOperation

REQUEST_ACTION_TOKENS = ("req", "request")
REQUEST_CANCEL_ACTION_TOKENS = ("request", "cancel")
LOAD_ACTION_TOKENS = ("load", "grant", "accrual", "carry")
ADJUSTMENT_ACTION_TOKENS = ("adjust", "manual", "correction", "fix")


def _normalize_text(value: object | None) -> str:
    return str(value or "").strip()


def build_action_haystack(*values: object | None) -> str:
    parts = [_normalize_text(value).lower() for value in values if _normalize_text(value)]
    return " ".join(parts)


def action_matches_tokens(*values: object | None, tokens: tuple[str, ...]) -> bool:
    haystack = build_action_haystack(*values)
    return any(token in haystack for token in tokens)


def resolve_action_code(
    action_catalog: list[dict],
    *,
    include_tokens: tuple[str, ...],
    fallback_index: int = 0,
) -> str:
    for row in action_catalog:
        if action_matches_tokens(row.get("code"), row.get("name"), tokens=include_tokens):
            return _normalize_text(row.get("code")).upper()
    if 0 <= fallback_index < len(action_catalog):
        return _normalize_text(action_catalog[fallback_index].get("code")).upper()
    return ""


def resolve_action_kind(action_code: object | None, action_name: object | None = None) -> str | None:
    if action_matches_tokens(action_code, action_name, tokens=REQUEST_ACTION_TOKENS):
        return "request"
    if action_matches_tokens(action_code, action_name, tokens=LOAD_ACTION_TOKENS):
        return "load"
    return None


def is_request_action_code(action_code: object | None, action_name: object | None = None) -> bool:
    return resolve_action_kind(action_code, action_name) == "request"


def is_load_action_code(action_code: object | None, action_name: object | None = None) -> bool:
    return resolve_action_kind(action_code, action_name) == "load"


def _resolve_action_name(action_by_code: dict[str, dict] | None, action_code: object | None) -> str:
    if not action_by_code:
        return ""
    normalized_code = _normalize_text(action_code).upper()
    if not normalized_code:
        return ""
    action = action_by_code.get(normalized_code)
    if not isinstance(action, dict):
        return ""
    return _normalize_text(action.get("name"))


def is_request_action_row(row: dict, action_by_code: dict[str, dict] | None = None) -> bool:
    action_code = _normalize_text(row.get("ptoActionCode")).upper()
    action_name = _resolve_action_name(action_by_code, action_code)
    return is_request_action_code(action_code, action_name)


def is_load_action_row(row: dict, action_by_code: dict[str, dict] | None = None) -> bool:
    action_code = _normalize_text(row.get("ptoActionCode")).upper()
    action_name = _resolve_action_name(action_by_code, action_code)
    return is_load_action_code(action_code, action_name)


def request_action_sql(column: str = "ptoActionCode") -> str:
    normalized_column = _normalize_text(column) or "ptoActionCode"
    return f"UPPER({normalized_column}) LIKE '%REQ%'"


def signed_pto_hours(
    action_code: object | None,
    hours: object | None,
    action_name: object | None = None,
) -> Decimal:
    try:
        amount = Decimal(str(hours or 0)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("hours must be a decimal number") from exc

    kind = resolve_action_kind(action_code, action_name)
    if kind == "request":
        return (amount.copy_abs() * Decimal("-1")).quantize(Decimal("0.01"))
    if kind == "load":
        return amount.copy_abs().quantize(Decimal("0.01"))
    return amount.quantize(Decimal("0.01"))
