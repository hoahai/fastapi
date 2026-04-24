from __future__ import annotations

from datetime import date, datetime

from apps.tradsphere.api.v1.helpers.accountValidation import (
    ensure_tradsphere_account_codes_exist,
    invalidate_validation_cache,
)
from apps.tradsphere.api.v1.helpers.broadcastCalendar import (
    get_broadcast_weeks_in_range,
)
from apps.tradsphere.api.v1.helpers.config import get_media_types
from apps.tradsphere.api.v1.helpers.dbQueries import (
    get_est_nums,
    get_scheduled_est_nums,
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


def _find_duplicate_est_nums(values: list[int]) -> list[int]:
    seen: set[int] = set()
    duplicates: set[int] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
            continue
        seen.add(value)
    return sorted(duplicates)


def _find_existing_est_nums(est_nums: list[int]) -> list[int]:
    if not est_nums:
        return []
    existing_rows = get_est_nums(est_nums=est_nums, account_codes=[])
    existing: set[int] = set()
    for row in existing_rows:
        est_num_value = row.get("estNum")
        if est_num_value is None:
            continue
        existing.add(int(est_num_value))
    return sorted(existing)


def _ensure_optional_year(value: object | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("year must be an integer") from exc
    if parsed < 1901 or parsed > 2155:
        raise ValueError("year must be between 1901 and 2155")
    return parsed


def _ensure_optional_month(value: object | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("month must be an integer") from exc
    if parsed < 1 or parsed > 12:
        raise ValueError("month must be between 1 and 12")
    return parsed


def _ensure_optional_quarter(value: object | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("quarter must be an integer") from exc
    if parsed < 1 or parsed > 4:
        raise ValueError("quarter must be between 1 and 4")
    return parsed


def _validate_month_quarter_consistency(
    *,
    month: int | None,
    quarter: int | None,
) -> None:
    if month is None or quarter is None:
        return

    expected_quarter = ((month - 1) // 3) + 1
    if expected_quarter != quarter:
        raise ValueError(
            f"month {month} does not belong to quarter {quarter}"
        )


def _coerce_db_date(value: object, *, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO date YYYY-MM-DD") from exc


def _row_matches_broadcast_period(
    *,
    flight_start: date,
    flight_end: date,
    year: int | None,
    month: int | None,
    quarter: int | None,
) -> bool:
    weeks = get_broadcast_weeks_in_range(flight_start, flight_end)
    for week in weeks:
        week_end = week["weekEnd"]
        broadcast_year = week_end.year
        broadcast_month = week_end.month
        broadcast_quarter = ((broadcast_month - 1) // 3) + 1
        if year is not None and broadcast_year != year:
            continue
        if month is not None and broadcast_month != month:
            continue
        if quarter is not None and broadcast_quarter != quarter:
            continue
        return True
    return False


def _build_broadcast_months_years_for_range(
    *,
    flight_start: date,
    flight_end: date,
) -> tuple[list[int], list[int]]:
    weeks = get_broadcast_weeks_in_range(flight_start, flight_end)
    months_seen: set[int] = set()
    years_seen: set[int] = set()
    months: list[int] = []
    years: list[int] = []
    for week in weeks:
        week_end = week["weekEnd"]
        broadcast_month = int(week_end.month)
        broadcast_year = int(week_end.year)
        if broadcast_month not in months_seen:
            months_seen.add(broadcast_month)
            months.append(broadcast_month)
        if broadcast_year not in years_seen:
            years_seen.add(broadcast_year)
            years.append(broadcast_year)
    return months, years


def list_est_nums_data(
    *,
    est_nums: list[int] | None = None,
    account_codes: list[str] | None = None,
    year: int | None = None,
    month: int | None = None,
    quarter: int | None = None,
) -> list[dict]:
    validated_year = _ensure_optional_year(year)
    validated_month = _ensure_optional_month(month)
    validated_quarter = _ensure_optional_quarter(quarter)
    _validate_month_quarter_consistency(
        month=validated_month,
        quarter=validated_quarter,
    )

    rows = get_est_nums(
        est_nums=est_nums or [],
        account_codes=account_codes or [],
    )
    final_rows = rows
    if not (
        validated_year is None
        and validated_month is None
        and validated_quarter is None
    ):
        filtered_rows: list[dict] = []
        broadcast_match_cache: dict[tuple[date, date], bool] = {}
        for row in rows:
            flight_start = _coerce_db_date(row.get("flightStart"), field="flightStart")
            flight_end = _coerce_db_date(row.get("flightEnd"), field="flightEnd")
            if flight_start > flight_end:
                continue
            cache_key = (flight_start, flight_end)
            matches = broadcast_match_cache.get(cache_key)
            if matches is None:
                matches = _row_matches_broadcast_period(
                    flight_start=flight_start,
                    flight_end=flight_end,
                    year=validated_year,
                    month=validated_month,
                    quarter=validated_quarter,
                )
                broadcast_match_cache[cache_key] = matches
            if matches:
                filtered_rows.append(row)
        final_rows = filtered_rows

    est_num_values: list[int] = []
    for row in final_rows:
        raw_value = row.get("estNum")
        if raw_value is None:
            continue
        try:
            est_num_values.append(int(raw_value))
        except (TypeError, ValueError):
            continue

    scheduled_est_nums = get_scheduled_est_nums(est_num_values)
    broadcast_period_cache: dict[tuple[date, date], tuple[list[int], list[int]]] = {}
    for row in final_rows:
        raw_value = row.get("estNum")
        has_schedule = False
        if raw_value is not None:
            try:
                has_schedule = int(raw_value) in scheduled_est_nums
            except (TypeError, ValueError):
                has_schedule = False
        row["hasSchedule"] = has_schedule

        months: list[int] = []
        years: list[int] = []
        try:
            flight_start = _coerce_db_date(row.get("flightStart"), field="flightStart")
            flight_end = _coerce_db_date(row.get("flightEnd"), field="flightEnd")
            if flight_start <= flight_end:
                cache_key = (flight_start, flight_end)
                cached = broadcast_period_cache.get(cache_key)
                if cached is None:
                    cached = _build_broadcast_months_years_for_range(
                        flight_start=flight_start,
                        flight_end=flight_end,
                    )
                    broadcast_period_cache[cache_key] = cached
                months, years = cached
        except ValueError:
            months, years = [], []

        row["broadcastMonths"] = months
        row["broadcastYears"] = years

    return final_rows


def create_est_nums_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    normalized_rows: list[dict] = []
    account_codes: list[str] = []
    requested_est_nums: list[int] = []
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
        requested_est_nums.append(est_num)

    duplicate_est_nums_in_payload = _find_duplicate_est_nums(requested_est_nums)
    if duplicate_est_nums_in_payload:
        raise ValueError(
            "Duplicate estNum values in payload: "
            + ", ".join(map(str, duplicate_est_nums_in_payload))
        )

    ensure_tradsphere_account_codes_exist(account_codes)
    existing_est_nums = _find_existing_est_nums(requested_est_nums)
    if existing_est_nums:
        raise ValueError(
            "estNum values already exist in TradSphere estNums: "
            + ", ".join(map(str, existing_est_nums))
        )

    try:
        inserted = insert_est_nums(normalized_rows)
    except Exception as exc:
        detail = str(exc).lower()
        if "duplicate entry" in detail:
            raise ValueError(
                "estNum values already exist in TradSphere estNums"
            ) from exc
        raise

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
        ensure_tradsphere_account_codes_exist(account_codes_to_validate)

    updated = update_est_nums(normalized_rows)
    if updated > 0:
        invalidate_validation_cache()
    return {"updated": updated}
