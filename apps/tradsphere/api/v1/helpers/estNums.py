from __future__ import annotations

from datetime import date

from apps.tradsphere.api.v1.helpers.accountValidation import (
    ensure_account_codes_exist,
    invalidate_validation_cache,
)
from apps.tradsphere.api.v1.helpers.config import get_media_types
from apps.tradsphere.api.v1.helpers.dbQueries import (
    get_est_nums,
    insert_est_nums,
    update_est_nums,
)


def _ensure_list(payload: list[dict] | dict) -> list[dict]:
    if isinstance(payload, dict):
        return [payload]
    if not isinstance(payload, list):
        raise ValueError("Payload must be an object or an array of objects")
    return payload


def _ensure_media_type(value: object, *, field: str = "mediaType") -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise ValueError(f"{field} is required")
    allowed = set(get_media_types())
    if normalized not in allowed:
        raise ValueError(
            f"Invalid {field}: {normalized}. Allowed values: {', '.join(sorted(allowed))}"
        )
    return normalized


def _ensure_est_num(value: object, *, field: str = "estNum") -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an unsigned integer") from exc
    if parsed < 0 or parsed > 4294967295:
        raise ValueError(f"{field} must be an unsigned integer")
    return parsed


def _ensure_date(value: object, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO date YYYY-MM-DD") from exc


def _ensure_optional_date(value: object, *, field: str) -> str | None:
    if value is None:
        raise ValueError(f"{field} cannot be empty")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field} cannot be empty")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO date YYYY-MM-DD") from exc


def _validate_flight_range(
    *,
    flight_start: str | None,
    flight_end: str | None,
) -> None:
    if flight_start is None or flight_end is None:
        return
    start_value = date.fromisoformat(flight_start)
    end_value = date.fromisoformat(flight_end)
    if start_value > end_value:
        raise ValueError("flightStart must be on or before flightEnd")


def _ensure_buyer(value: object, *, required: bool) -> str | None:
    if value is None:
        if required:
            raise ValueError("buyer is required")
        raise ValueError("buyer cannot be empty")
    text = str(value).strip()
    if not text:
        if required:
            raise ValueError("buyer is required")
        raise ValueError("buyer cannot be empty")
    if len(text) > 36:
        raise ValueError("buyer must be <= 36 characters")
    return text


def _ensure_note(value: object) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) > 2048:
        raise ValueError("note must be <= 2048 characters")
    return text


def list_est_nums_data(
    *,
    est_nums: list[int] | None = None,
    account_codes: list[str] | None = None,
    media_types: list[str] | None = None,
) -> list[dict]:
    return get_est_nums(
        est_nums=est_nums or [],
        account_codes=account_codes or [],
        media_types=media_types or [],
    )


def create_est_nums_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    normalized_rows: list[dict] = []
    account_codes: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each estNums item must be an object")
        est_num = _ensure_est_num(row.get("estNum"))
        account_code = str(row.get("accountCode") or "").strip().upper()
        if not account_code:
            raise ValueError("accountCode is required")
        flight_start = _ensure_date(row.get("flightStart"), field="flightStart")
        flight_end = _ensure_date(row.get("flightEnd"), field="flightEnd")
        _validate_flight_range(flight_start=flight_start, flight_end=flight_end)
        media_type = _ensure_media_type(row.get("mediaType"))
        buyer = _ensure_buyer(row.get("buyer"), required=True)
        normalized_rows.append(
            {
                "estNum": est_num,
                "accountCode": account_code,
                "flightStart": flight_start,
                "flightEnd": flight_end,
                "mediaType": media_type,
                "buyer": buyer,
                "note": _ensure_note(row.get("note")),
            }
        )
        account_codes.append(account_code)

    ensure_account_codes_exist(account_codes)
    inserted = insert_est_nums(normalized_rows)
    if inserted > 0:
        invalidate_validation_cache()
    return {"inserted": inserted}


def modify_est_nums_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    normalized_rows: list[dict] = []
    account_codes_to_validate: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each estNums item must be an object")
        est_num = _ensure_est_num(row.get("estNum"))

        item: dict[str, object] = {"estNum": est_num}
        if "accountCode" in row:
            account_code = str(row.get("accountCode") or "").strip().upper()
            if not account_code:
                raise ValueError("accountCode cannot be empty")
            item["accountCode"] = account_code
            account_codes_to_validate.append(account_code)

        if "mediaType" in row:
            item["mediaType"] = _ensure_media_type(row.get("mediaType"))

        if "flightStart" in row:
            item["flightStart"] = _ensure_optional_date(
                row.get("flightStart"),
                field="flightStart",
            )

        if "flightEnd" in row:
            item["flightEnd"] = _ensure_optional_date(
                row.get("flightEnd"),
                field="flightEnd",
            )

        if "buyer" in row:
            item["buyer"] = _ensure_buyer(row.get("buyer"), required=False)

        if "note" in row:
            item["note"] = _ensure_note(row.get("note"))

        _validate_flight_range(
            flight_start=item.get("flightStart")
            if isinstance(item.get("flightStart"), str)
            else None,
            flight_end=item.get("flightEnd")
            if isinstance(item.get("flightEnd"), str)
            else None,
        )

        if len(item) == 1:
            raise ValueError(f"No updatable fields provided for estNum '{est_num}'")
        normalized_rows.append(item)

    if account_codes_to_validate:
        ensure_account_codes_exist(account_codes_to_validate)

    updated = update_est_nums(normalized_rows)
    if updated > 0:
        invalidate_validation_cache()
    return {"updated": updated}
