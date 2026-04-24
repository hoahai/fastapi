from __future__ import annotations

from apps.tradsphere.api.v1.helpers.accountValidation import (
    ensure_master_account_codes_exist,
    ensure_tradsphere_account_codes_exist,
    find_existing_tradsphere_account_codes,
    invalidate_validation_cache,
    require_account_code,
)
from apps.tradsphere.api.v1.helpers.dbQueries import (
    get_accounts,
    insert_accounts,
    update_accounts,
)

_BILLING_TYPE_VALUES = {"BROADCAST": "Broadcast", "CALENDAR": "Calendar"}


def _ensure_list(payload: list[dict] | dict) -> list[dict]:
    if isinstance(payload, dict):
        return [payload]
    if not isinstance(payload, list):
        raise ValueError("Payload must be an object or an array of objects")
    return payload


def _normalize_billing_type(value: object | None, *, required: bool) -> str | None:
    if value is None:
        if required:
            return "Calendar"
        return None
    normalized = str(value).strip()
    if not normalized:
        if required:
            return "Calendar"
        return None
    mapped = _BILLING_TYPE_VALUES.get(normalized.upper())
    if not mapped:
        raise ValueError("billingType must be either 'Broadcast' or 'Calendar'")
    return mapped


def _normalize_optional_text(
    value: object,
    *,
    field: str,
    max_length: int | None = None,
) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if max_length is not None and len(text) > max_length:
        raise ValueError(f"{field} must be <= {max_length} characters")
    return text


def _find_duplicate_account_codes(values: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
            continue
        seen.add(value)
    return sorted(duplicates)


def list_accounts(
    *,
    account_codes: list[str] | None = None,
    active: bool = True,
) -> list[dict]:
    return get_accounts(account_codes=account_codes or [], active_only=bool(active))


def create_accounts(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    normalized_rows: list[dict] = []
    requested_codes: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each accounts item must be an object")
        account_code = require_account_code(row.get("accountCode"))
        normalized_rows.append(
            {
                "accountCode": account_code,
                "billingType": _normalize_billing_type(
                    row.get("billingType"),
                    required=True,
                ),
                "market": _normalize_optional_text(
                    row.get("market"),
                    field="market",
                    max_length=255,
                ),
                "note": _normalize_optional_text(
                    row.get("note"),
                    field="note",
                    max_length=2048,
                ),
            }
        )
        requested_codes.append(account_code)

    duplicate_codes_in_payload = _find_duplicate_account_codes(requested_codes)
    if duplicate_codes_in_payload:
        raise ValueError(
            "Duplicate accountCode values in payload: "
            + ", ".join(duplicate_codes_in_payload)
        )

    ensure_master_account_codes_exist(requested_codes)
    existing_codes = find_existing_tradsphere_account_codes(requested_codes)
    if existing_codes:
        raise ValueError(
            "accountCode values already exist in TradSphere accounts: "
            + ", ".join(existing_codes)
        )

    try:
        inserted = insert_accounts(normalized_rows)
    except Exception as exc:
        detail = str(exc).lower()
        if "duplicate entry" in detail:
            raise ValueError(
                "accountCode values already exist in TradSphere accounts"
            ) from exc
        raise

    invalidate_validation_cache()
    return {"inserted": inserted}


def modify_accounts(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    normalized_rows: list[dict] = []
    target_codes: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each accounts item must be an object")
        account_code = require_account_code(row.get("accountCode"))
        target_codes.append(account_code)
        item: dict[str, object] = {"accountCode": account_code}

        if "billingType" in row:
            item["billingType"] = _normalize_billing_type(
                row.get("billingType"),
                required=False,
            )
        if "market" in row:
            item["market"] = _normalize_optional_text(
                row.get("market"),
                field="market",
                max_length=255,
            )
        if "note" in row:
            item["note"] = _normalize_optional_text(
                row.get("note"),
                field="note",
                max_length=2048,
            )

        if len(item) == 1:
            raise ValueError(
                f"No updatable fields provided for accountCode '{account_code}'"
            )
        normalized_rows.append(item)

    ensure_tradsphere_account_codes_exist(target_codes)
    updated = update_accounts(normalized_rows)
    invalidate_validation_cache()
    return {"updated": updated}
