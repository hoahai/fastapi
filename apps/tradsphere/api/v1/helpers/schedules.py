from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
import hashlib
import uuid

from apps.tradsphere.api.v1.helpers.accountValidation import (
    ensure_est_nums_exist,
    ensure_schedule_ids_exist,
    ensure_station_codes_exist,
    invalidate_validation_cache,
)
from apps.tradsphere.api.v1.helpers.broadcastCalendar import (
    get_broadcast_calendar_info,
    get_broadcast_weeks_in_range,
)
from apps.tradsphere.api.v1.helpers.clientBillingCode import (
    parse_client_billing_code,
)
from apps.tradsphere.api.v1.helpers.config import get_media_types
from apps.tradsphere.api.v1.helpers.dbQueries import (
    get_est_nums,
    get_schedules_by_match_keys,
    get_schedule_weeks,
    get_schedules,
    get_station_media_types,
    insert_schedule_weeks,
    insert_schedules,
    update_schedule_weeks,
    update_schedules,
)
from shared.tenantDataCache import (
    delete_tenant_shared_cache_values_by_prefix,
    get_tenant_shared_cache_value,
    set_tenant_shared_cache_value,
)

_MAX_SCHEDULE_WEEK_FIELDS = 5
_PDF_SCHEDULE_CACHE_BUCKET = "db_reads"
_PDF_SCHEDULE_CACHE_PREFIX = "tradsphere_pdf::schedules_data::"
_PDF_SCHEDULE_CACHE_TTL_SECONDS = 60 * 60 * 24 * 90
_PDF_SCHEDULE_CACHE_SCHEMA = "v1"
_SCHEDULE_CREATE_REQUIRED_FIELDS: tuple[str, ...] = (
    "scheduleId",
    "lineNum",
    "estNum",
    "billingCode",
    "mediaType",
    "stationCode",
    "broadcastMonth",
    "broadcastYear",
    "startDate",
    "endDate",
    "length",
    "runtime",
    "days",
    "daypart",
)


def _build_pdf_schedule_cache_key(*, est_num: int) -> str:
    return f"{_PDF_SCHEDULE_CACHE_PREFIX}{_PDF_SCHEDULE_CACHE_SCHEMA}::estnum::{int(est_num)}"


def _parse_sort_date(value: object) -> date:
    text = str(value or "").strip()
    if not text:
        return date.max
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return date.max


def _to_sort_decimal(value: object) -> Decimal:
    text = str(value or "").strip()
    if not text:
        return Decimal("0")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _to_sort_est_num(value: object) -> int:
    text = str(value or "").strip()
    if not text:
        return 2_147_483_647
    try:
        return int(text)
    except (TypeError, ValueError):
        return 2_147_483_647


def _vendor_sort_value(row: dict, station_names: dict[str, str]) -> str:
    station_code = str(row.get("stationCode") or "").strip().upper()
    station_name = str(station_names.get(station_code) or "").strip()
    media_type = str(row.get("mediaType") or "").strip().upper()
    if media_type in {"CA", "CABLE"}:
        if station_code:
            return f"CABLE ({station_code})"
        return "CABLE"
    if station_name and station_code:
        return f"{station_name} ({station_code})".upper()
    if station_name:
        return station_name.upper()
    return station_code.upper()


def _sort_pdf_schedules_for_report(
    schedules: list[dict],
    station_names: dict[str, str],
) -> list[dict]:
    rows = [row for row in schedules if isinstance(row, dict)]

    def _key(row: dict) -> tuple[object, ...]:
        return (
            _to_sort_est_num(row.get("estNum")),
            _vendor_sort_value(row, station_names),
            str(row.get("programName") or "").strip().upper(),
            _parse_sort_date(row.get("startDate")),
            _parse_sort_date(row.get("endDate")),
            -_to_sort_decimal(row.get("rateGross")),
            -_to_sort_decimal(row.get("rtg")),
        )

    return sorted(rows, key=_key)


def _map_station_media_types_for_schedules(schedules: list[dict]) -> dict[str, str]:
    station_codes = [
        str(row.get("stationCode") or "").strip().upper()
        for row in schedules
        if isinstance(row, dict) and str(row.get("stationCode") or "").strip()
    ]
    station_codes = list(dict.fromkeys(station_codes))
    if not station_codes:
        return {}
    return get_station_media_types(codes=station_codes)


def _apply_station_media_types_to_schedules(
    schedules: list[dict],
    station_media_types: dict[str, str],
) -> list[dict]:
    normalized_media_types = {
        str(code or "").strip().upper(): str(media_type or "").strip().upper()
        for code, media_type in (station_media_types or {}).items()
        if str(code or "").strip() and str(media_type or "").strip()
    }
    adjusted: list[dict] = []
    for row in schedules:
        if not isinstance(row, dict):
            continue
        next_row = dict(row)
        station_code = str(next_row.get("stationCode") or "").strip().upper()
        station_media_type = normalized_media_types.get(station_code)
        if station_media_type:
            next_row["mediaType"] = station_media_type
        adjusted.append(next_row)
    return adjusted


def invalidate_pdf_schedule_cache() -> int:
    return delete_tenant_shared_cache_values_by_prefix(
        bucket=_PDF_SCHEDULE_CACHE_BUCKET,
        cache_key_prefix=_PDF_SCHEDULE_CACHE_PREFIX,
    )


def _lookup_est_num_note(*, est_num: int) -> str:
    rows = get_est_nums(est_nums=[int(est_num)])
    for row in rows:
        row_est_num = _to_sort_est_num(row.get("estNum"))
        if row_est_num != int(est_num):
            continue
        return str(row.get("note") or "").strip()
    return ""


def get_pdf_schedule_data(*, est_num: int) -> dict[str, object]:
    cache_key = _build_pdf_schedule_cache_key(est_num=est_num)
    cached_value, cache_hit = get_tenant_shared_cache_value(
        bucket=_PDF_SCHEDULE_CACHE_BUCKET,
        cache_key=cache_key,
        ttl_seconds=_PDF_SCHEDULE_CACHE_TTL_SECONDS,
    )
    if cache_hit and isinstance(cached_value, dict):
        schedules = cached_value.get("schedules")
        schedule_weeks = cached_value.get("scheduleWeeks")
        station_names = cached_value.get("stationNames")
        est_num_note = str(cached_value.get("estNumNote") or "").strip()
        if (
            isinstance(schedules, list)
            and isinstance(schedule_weeks, list)
            and isinstance(station_names, dict)
        ):
            normalized_station_names = {
                str(key or "").strip().upper(): str(value or "").strip()
                for key, value in station_names.items()
                if str(key or "").strip()
            }
            station_media_types = _map_station_media_types_for_schedules(schedules)
            schedules_for_report = _apply_station_media_types_to_schedules(
                schedules,
                station_media_types,
            )
            sorted_schedules = _sort_pdf_schedules_for_report(
                schedules_for_report,
                normalized_station_names,
            )
            if not est_num_note:
                est_num_note = _lookup_est_num_note(est_num=int(est_num))
            return {
                "schedules": sorted_schedules,
                "scheduleWeeks": schedule_weeks,
                "stationNames": normalized_station_names,
                "estNumNote": est_num_note,
            }

    schedules = list_schedules_data(est_nums=[int(est_num)])
    schedule_ids = [
        str(row.get("id") or "").strip()
        for row in schedules
        if isinstance(row, dict) and str(row.get("id") or "").strip()
    ]
    station_codes = [
        str(row.get("stationCode") or "").strip().upper()
        for row in schedules
        if isinstance(row, dict) and str(row.get("stationCode") or "").strip()
    ]
    schedule_weeks: list[dict] = []
    if schedule_ids:
        schedule_weeks = list_schedule_weeks_data(schedule_ids=schedule_ids)

    # Local import to avoid helper-module circular dependency at import time.
    from apps.tradsphere.api.v1.helpers.stations import map_station_names_by_codes

    station_names = map_station_names_by_codes(station_codes)
    station_media_types = _map_station_media_types_for_schedules(schedules)
    schedules_for_report = _apply_station_media_types_to_schedules(
        schedules,
        station_media_types,
    )
    est_num_note = _lookup_est_num_note(est_num=int(est_num))
    sorted_schedules = _sort_pdf_schedules_for_report(
        schedules_for_report,
        station_names,
    )
    payload: dict[str, object] = {
        "schedules": sorted_schedules,
        "scheduleWeeks": schedule_weeks,
        "stationNames": station_names,
        "estNumNote": est_num_note,
    }
    set_tenant_shared_cache_value(
        bucket=_PDF_SCHEDULE_CACHE_BUCKET,
        cache_key=cache_key,
        value=payload,
    )
    return payload


def _ensure_list(payload: list[dict] | dict) -> list[dict]:
    if isinstance(payload, dict):
        return [payload]
    if not isinstance(payload, list):
        raise ValueError("Payload must be an object or an array of objects")
    return payload


def _ensure_required_text(
    value: object,
    *,
    field: str,
    max_length: int,
) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    if len(text) > max_length:
        raise ValueError(f"{field} must be <= {max_length} characters")
    return text


def _ensure_optional_text(
    value: object,
    *,
    field: str,
    max_length: int,
) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) > max_length:
        raise ValueError(f"{field} must be <= {max_length} characters")
    return text


def _ensure_uuid_text(value: object, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    try:
        return str(uuid.UUID(text))
    except ValueError as exc:
        raise ValueError(f"{field} must be a valid UUID") from exc


def _ensure_unsigned_int(
    value: object,
    *,
    field: str,
    required: bool,
    minimum: int = 0,
    maximum: int | None = None,
) -> int | None:
    if value is None:
        if required:
            raise ValueError(f"{field} is required")
        return None
    text = str(value).strip()
    if not text:
        if required:
            raise ValueError(f"{field} is required")
        raise ValueError(f"{field} cannot be empty")
    try:
        parsed = int(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an unsigned integer") from exc
    if parsed < minimum:
        raise ValueError(f"{field} must be >= {minimum}")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"{field} must be <= {maximum}")
    return parsed


def _ensure_decimal(
    value: object,
    *,
    field: str,
    required: bool,
    default: Decimal | None = None,
    allow_negative: bool = False,
) -> str | None:
    if value is None:
        if default is not None:
            return str(default)
        if required:
            raise ValueError(f"{field} is required")
        return None

    text = str(value).strip()
    if not text:
        if default is not None:
            return str(default)
        if required:
            raise ValueError(f"{field} is required")
        raise ValueError(f"{field} cannot be empty")

    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc
    if not allow_negative and parsed < 0:
        raise ValueError(f"{field} must be >= 0")
    return str(parsed)


def _ensure_required_date(value: object, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO date YYYY-MM-DD") from exc


def _ensure_optional_date(value: object, *, field: str) -> str:
    if value is None:
        raise ValueError(f"{field} cannot be null")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field} cannot be empty")
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO date YYYY-MM-DD") from exc


def _validate_date_range(
    *,
    start_date: str,
    end_date: str,
    start_field: str,
    end_field: str,
) -> None:
    if date.fromisoformat(start_date) > date.fromisoformat(end_date):
        raise ValueError(f"{start_field} must be on or before {end_field}")


def _validate_date_within_broadcast_period(
    *,
    date_value: str,
    date_field: str,
    broadcast_month: int,
    broadcast_year: int,
) -> None:
    info = get_broadcast_calendar_info(date_value)
    date_broadcast_month = int(info["broadcastMonth"])
    date_broadcast_year = int(info["broadcastYear"])
    if (
        date_broadcast_month != broadcast_month
        or date_broadcast_year != broadcast_year
    ):
        raise ValueError(
            f"{date_field} must be within broadcastMonth/broadcastYear "
            f"{broadcast_month}/{broadcast_year} "
            f"(resolved to {date_broadcast_month}/{date_broadcast_year})"
        )


def _validate_schedule_dates_within_broadcast_period(
    *,
    start_date: str,
    end_date: str,
    broadcast_month: int,
    broadcast_year: int,
) -> None:
    _validate_date_within_broadcast_period(
        date_value=start_date,
        date_field="startDate",
        broadcast_month=broadcast_month,
        broadcast_year=broadcast_year,
    )
    _validate_date_within_broadcast_period(
        date_value=end_date,
        date_field="endDate",
        broadcast_month=broadcast_month,
        broadcast_year=broadcast_year,
    )


def _ensure_media_type(value: object, *, field: str = "mediaType") -> str:
    media_type = str(value or "").strip().upper()
    if not media_type:
        raise ValueError(f"{field} is required")
    allowed = set(get_media_types())
    if media_type not in allowed:
        raise ValueError(
            f"Invalid {field}: {media_type}. Allowed values: {', '.join(sorted(allowed))}"
        )
    return media_type


def _ensure_year(value: object, *, field: str) -> int:
    year = _ensure_unsigned_int(
        value,
        field=field,
        required=True,
        minimum=1901,
        maximum=2155,
    )
    assert year is not None
    return year


def _compute_match_key(
    *,
    schedule_id: str,
    line_num: int,
    est_num: int,
    start_date: str,
    end_date: str,
) -> str:
    raw = f"{schedule_id}|{line_num}|{est_num}|{start_date}|{end_date}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _resolve_row_label(row: dict, *, index: int) -> str:
    source_line = row.get("_sourceLine")
    if source_line is not None:
        try:
            parsed_line = int(source_line)
            if parsed_line > 0:
                return f"line {parsed_line}"
        except (TypeError, ValueError):
            pass
    return f"item {index}"


def _format_row_labels(labels: list[str]) -> str:
    seen: set[str] = set()
    ordered: list[str] = []
    for label in labels:
        normalized = str(label or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ", ".join(ordered)


def _validate_required_create_fields(rows: list[dict]) -> None:
    missing_by_field: dict[str, list[str]] = {}

    for index, row in enumerate(rows, start=1):
        row_label = _resolve_row_label(row, index=index)
        for field_name in _SCHEDULE_CREATE_REQUIRED_FIELDS:
            value = row.get(field_name)
            if value is None:
                missing_by_field.setdefault(field_name, []).append(row_label)
                continue
            if isinstance(value, str) and not value.strip():
                missing_by_field.setdefault(field_name, []).append(row_label)

    if not missing_by_field:
        return

    details: list[str] = []
    for field_name in _SCHEDULE_CREATE_REQUIRED_FIELDS:
        labels = missing_by_field.get(field_name)
        if not labels:
            continue
        details.append(f"{field_name} (rows: {_format_row_labels(labels)})")

    raise ValueError("Missing required fields: " + "; ".join(details))


def _extract_unknown_values(detail: str, *, prefix: str) -> list[str]:
    text = str(detail or "").strip()
    if not text.startswith(prefix):
        return []
    raw_values = text[len(prefix) :].strip()
    if not raw_values:
        return []
    return [value.strip() for value in raw_values.split(",") if value.strip()]


def _parse_optional_week_spot(
    value: object,
    *,
    field: str,
) -> int:
    if value is None:
        return 0
    text = str(value).strip()
    if not text:
        return 0
    parsed = _ensure_unsigned_int(
        text,
        field=field,
        required=True,
        minimum=0,
        maximum=4294967295,
    )
    assert parsed is not None
    return parsed


def _format_week_field_list(count: int) -> str:
    if count <= 0:
        return ""
    return ", ".join(f"w{week_index}" for week_index in range(1, count + 1))


def _extract_week_spots_for_create(
    row: dict,
    *,
    start_date: str,
    end_date: str,
) -> list[int]:
    provided_indexes: list[int] = []
    values_by_index: dict[int, int] = {}

    for week_index in range(1, _MAX_SCHEDULE_WEEK_FIELDS + 1):
        lower_key = f"w{week_index}"
        upper_key = f"W{week_index}"
        if lower_key in row or upper_key in row:
            provided_indexes.append(week_index)
            raw_value = row.get(lower_key, row.get(upper_key))
            values_by_index[week_index] = _parse_optional_week_spot(
                raw_value,
                field=lower_key,
            )

    if not provided_indexes:
        return []

    allowed_weeks_count = len(get_broadcast_weeks_in_range(start_date, end_date))
    if allowed_weeks_count > _MAX_SCHEDULE_WEEK_FIELDS:
        raise ValueError(
            "startDate/endDate covers "
            f"{allowed_weeks_count} broadcast week(s), but only "
            f"w1..w{_MAX_SCHEDULE_WEEK_FIELDS} are supported"
        )

    provided_set = set(provided_indexes)
    expected_indexes = list(range(1, allowed_weeks_count + 1))
    missing_indexes = [
        week_index for week_index in expected_indexes if week_index not in provided_set
    ]
    out_of_range_indexes = [
        week_index
        for week_index in sorted(provided_set)
        if week_index > allowed_weeks_count
    ]
    if missing_indexes or out_of_range_indexes:
        expected_fields = _format_week_field_list(allowed_weeks_count)
        details: list[str] = []
        if missing_indexes:
            details.append(
                "missing: "
                + ", ".join(f"w{week_index}" for week_index in missing_indexes)
            )
        if out_of_range_indexes:
            details.append(
                "out of range: "
                + ", ".join(f"w{week_index}" for week_index in out_of_range_indexes)
            )
        detail_suffix = f" ({'; '.join(details)})" if details else ""
        raise ValueError(
            "w fields must be consecutive and complete for this date range: "
            f"requires {allowed_weeks_count} week field(s): {expected_fields}{detail_suffix}"
        )

    normalized_values: list[int] = []
    for week_index in expected_indexes:
        normalized_values.append(values_by_index.get(week_index, 0))
    return normalized_values


def _build_week_rows_for_schedule(
    *,
    schedule_row_id: str,
    start_date: str,
    end_date: str,
    week_spots: list[int],
) -> list[dict]:
    if not week_spots:
        return []

    start_date_obj = date.fromisoformat(start_date)
    end_date_obj = date.fromisoformat(end_date)
    weeks_in_range = get_broadcast_weeks_in_range(start_date_obj, end_date_obj)

    out: list[dict] = []
    for week_index, spots in enumerate(week_spots):
        if week_index >= len(weeks_in_range):
            break
        full_week = weeks_in_range[week_index]
        week_start = max(full_week["weekStart"], start_date_obj)
        week_end = min(full_week["weekEnd"], end_date_obj)
        if week_start > week_end:
            continue
        out.append(
            {
                "scheduleId": schedule_row_id,
                "weekStart": week_start.isoformat(),
                "weekEnd": week_end.isoformat(),
                "spots": int(spots),
            }
        )
    return out


def _compute_total_spot_from_week_spots(week_spots: list[int]) -> int:
    return int(sum(int(value) for value in week_spots))


def _compute_total_gross_value(*, total_spot: int, rate_gross: str) -> str:
    return str(Decimal(total_spot) * Decimal(rate_gross))


def list_schedules_data(
    *,
    ids: list[str] | None = None,
    schedule_ids: list[str] | None = None,
    est_nums: list[int] | None = None,
    billing_codes: list[str] | None = None,
    media_types: list[str] | None = None,
    station_codes: list[str] | None = None,
    broadcast_month: int | None = None,
    broadcast_year: int | None = None,
    start_date_from: str | None = None,
    start_date_to: str | None = None,
    end_date_from: str | None = None,
    end_date_to: str | None = None,
) -> list[dict]:
    normalized_media_types: list[str] = []
    for media_type in media_types or []:
        normalized_media_types.append(_ensure_media_type(media_type))
    normalized_media_types = list(dict.fromkeys(normalized_media_types))

    normalized_station_codes = [
        str(value or "").strip().upper()
        for value in (station_codes or [])
        if str(value or "").strip()
    ]
    normalized_station_codes = list(dict.fromkeys(normalized_station_codes))

    validated_broadcast_month = _ensure_unsigned_int(
        broadcast_month,
        field="broadcastMonth",
        required=False,
        minimum=1,
        maximum=12,
    )
    validated_broadcast_year = (
        _ensure_unsigned_int(
            broadcast_year,
            field="broadcastYear",
            required=False,
            minimum=1901,
            maximum=2155,
        )
        if broadcast_year is not None
        else None
    )

    if start_date_from and start_date_to:
        _validate_date_range(
            start_date=start_date_from,
            end_date=start_date_to,
            start_field="startDateFrom",
            end_field="startDateTo",
        )
    if end_date_from and end_date_to:
        _validate_date_range(
            start_date=end_date_from,
            end_date=end_date_to,
            start_field="endDateFrom",
            end_field="endDateTo",
        )

    return get_schedules(
        ids=ids or [],
        schedule_ids=schedule_ids or [],
        est_nums=est_nums or [],
        billing_codes=billing_codes or [],
        media_types=normalized_media_types,
        station_codes=normalized_station_codes,
        broadcast_month=validated_broadcast_month,
        broadcast_year=validated_broadcast_year,
        start_date_from=start_date_from,
        start_date_to=start_date_to,
        end_date_from=end_date_from,
        end_date_to=end_date_to,
    )


def create_schedules_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    if not rows:
        return {
            "inserted": 0,
            "scheduleWeeksUpserted": 0,
            "insertedNew": 0,
            "updatedExisting": 0,
            "dedupedRows": 0,
        }

    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise ValueError(f"item {index}: Each schedules item must be an object")
    _validate_required_create_fields(rows)

    normalized_rows: list[dict] = []
    est_nums_to_validate: list[int] = []
    est_num_labels: dict[int, str] = {}
    station_codes_to_validate: list[str] = []
    station_code_labels: dict[str, str] = {}
    for index, row in enumerate(rows, start=1):
        row_label = _resolve_row_label(row, index=index)
        try:
            schedule_row_id = row.get("id")
            if schedule_row_id is None or str(schedule_row_id).strip() == "":
                schedule_row_id_text = str(uuid.uuid4())
            else:
                schedule_row_id_text = _ensure_uuid_text(schedule_row_id, field="id")

            est_num = _ensure_unsigned_int(
                row.get("estNum"),
                field="estNum",
                required=True,
                minimum=0,
                maximum=4294967295,
            )
            assert est_num is not None

            station_code = _ensure_required_text(
                row.get("stationCode"),
                field="stationCode",
                max_length=10,
            ).upper()

            start_date = _ensure_required_date(row.get("startDate"), field="startDate")
            end_date = _ensure_required_date(row.get("endDate"), field="endDate")
            _validate_date_range(
                start_date=start_date,
                end_date=end_date,
                start_field="startDate",
                end_field="endDate",
            )
            schedule_business_id = _ensure_required_text(
                row.get("scheduleId"),
                field="scheduleId",
                max_length=20,
            )
            line_num = _ensure_unsigned_int(
                row.get("lineNum"),
                field="lineNum",
                required=True,
                minimum=0,
                maximum=9999,
            )
            assert line_num is not None
            week_spots = _extract_week_spots_for_create(
                row,
                start_date=start_date,
                end_date=end_date,
            )

            billing_code = parse_client_billing_code(
                _ensure_required_text(
                    row.get("billingCode"),
                    field="billingCode",
                    max_length=20,
                ),
                field="billingCode",
            )["normalized"]

            normalized_row = {
                "id": schedule_row_id_text,
                "scheduleId": schedule_business_id,
                "lineNum": line_num,
                "estNum": est_num,
                "billingCode": billing_code,
                "mediaType": _ensure_media_type(row.get("mediaType")),
                "stationCode": station_code,
                "broadcastMonth": _ensure_unsigned_int(
                    row.get("broadcastMonth"),
                    field="broadcastMonth",
                    required=True,
                    minimum=1,
                    maximum=12,
                ),
                "broadcastYear": _ensure_year(
                    row.get("broadcastYear"),
                    field="broadcastYear",
                ),
                "startDate": start_date,
                "endDate": end_date,
                "totalSpot": _ensure_unsigned_int(
                    row.get("totalSpot"),
                    field="totalSpot",
                    required=False,
                    minimum=0,
                    maximum=4294967295,
                )
                if row.get("totalSpot") is not None
                else 0,
                "totalGross": _ensure_decimal(
                    row.get("totalGross"),
                    field="totalGross",
                    required=False,
                    default=Decimal("0"),
                ),
                "rateGross": _ensure_decimal(
                    row.get("rateGross"),
                    field="rateGross",
                    required=False,
                    default=Decimal("0"),
                ),
                "length": _ensure_unsigned_int(
                    row.get("length"),
                    field="length",
                    required=True,
                    minimum=0,
                    maximum=255,
                ),
                "runtime": _ensure_required_text(
                    row.get("runtime"),
                    field="runtime",
                    max_length=50,
                ),
                "programName": _ensure_optional_text(
                    row.get("programName"),
                    field="programName",
                    max_length=255,
                ),
                "days": _ensure_required_text(
                    row.get("days"),
                    field="days",
                    max_length=20,
                ),
                "daypart": _ensure_required_text(
                    row.get("daypart"),
                    field="daypart",
                    max_length=10,
                ),
                "rtg": _ensure_decimal(
                    row.get("rtg"),
                    field="rtg",
                    required=False,
                    default=None,
                ),
                "_weekSpots": week_spots,
                "_rowLabel": row_label,
            }
            broadcast_month = int(normalized_row["broadcastMonth"])
            broadcast_year = int(normalized_row["broadcastYear"])
            _validate_schedule_dates_within_broadcast_period(
                start_date=start_date,
                end_date=end_date,
                broadcast_month=broadcast_month,
                broadcast_year=broadcast_year,
            )
            total_spot = _compute_total_spot_from_week_spots(week_spots)
            normalized_row["totalSpot"] = total_spot
            normalized_row["totalGross"] = _compute_total_gross_value(
                total_spot=total_spot,
                rate_gross=str(normalized_row["rateGross"]),
            )
            normalized_row["matchKey"] = _compute_match_key(
                schedule_id=schedule_business_id,
                line_num=line_num,
                est_num=est_num,
                start_date=start_date,
                end_date=end_date,
            )
        except ValueError as exc:
            raise ValueError(f"{row_label}: {exc}") from exc

        normalized_rows.append(normalized_row)
        est_nums_to_validate.append(est_num)
        station_codes_to_validate.append(station_code)
        est_num_labels.setdefault(est_num, row_label)
        station_code_labels.setdefault(station_code, row_label)

    try:
        ensure_est_nums_exist(est_nums_to_validate)
    except ValueError as exc:
        missing_values = _extract_unknown_values(
            str(exc),
            prefix="Unknown estNum values:",
        )
        if missing_values:
            for missing in missing_values:
                try:
                    missing_int = int(missing)
                except ValueError:
                    continue
                row_label = est_num_labels.get(missing_int)
                if row_label:
                    raise ValueError(f"{row_label}: Unknown estNum '{missing_int}'") from exc
        raise

    try:
        ensure_station_codes_exist(station_codes_to_validate)
    except ValueError as exc:
        missing_values = _extract_unknown_values(
            str(exc),
            prefix="Unknown stationCode values:",
        )
        if missing_values:
            for missing in missing_values:
                key = str(missing).strip().upper()
                row_label = station_code_labels.get(key)
                if row_label:
                    raise ValueError(f"{row_label}: Unknown stationCode '{key}'") from exc
        raise

    existing_schedule_rows = get_schedules_by_match_keys(
        [str(item.get("matchKey") or "") for item in normalized_rows]
    )
    existing_id_by_match_key = {
        str(item.get("matchKey") or "").strip(): str(item.get("id") or "").strip()
        for item in existing_schedule_rows
        if str(item.get("matchKey") or "").strip() and str(item.get("id") or "").strip()
    }
    canonical_id_by_match_key = dict(existing_id_by_match_key)

    deduped_rows_by_match_key: dict[str, dict] = {}
    for normalized_row in normalized_rows:
        match_key = str(normalized_row.get("matchKey") or "").strip()
        if not match_key:
            raise ValueError("matchKey is required")
        canonical_id = canonical_id_by_match_key.get(match_key)
        if not canonical_id:
            canonical_id = str(normalized_row.get("id") or "").strip()
            canonical_id_by_match_key[match_key] = canonical_id

        deduped_row = dict(normalized_row)
        deduped_row["id"] = canonical_id
        deduped_rows_by_match_key[match_key] = deduped_row

    schedules_payload: list[dict] = []
    schedule_weeks_payload: list[dict] = []
    for deduped_row in deduped_rows_by_match_key.values():
        schedule_row_id = str(deduped_row["id"])
        week_spots = list(deduped_row.get("_weekSpots") or [])
        schedules_payload.append(
            {
                key: value
                for key, value in deduped_row.items()
                if not str(key).startswith("_")
            }
        )
        schedule_weeks_payload.extend(
            _build_week_rows_for_schedule(
                schedule_row_id=schedule_row_id,
                start_date=str(deduped_row["startDate"]),
                end_date=str(deduped_row["endDate"]),
                week_spots=week_spots,
            )
        )

    deduped_match_keys = set(deduped_rows_by_match_key.keys())
    existing_match_keys = set(existing_id_by_match_key.keys())
    updated_existing = sum(
        1 for match_key in deduped_match_keys if match_key in existing_match_keys
    )
    inserted_new = len(deduped_match_keys) - updated_existing
    has_new_schedule_rows = inserted_new > 0

    inserted = insert_schedules(schedules_payload)
    if schedules_payload:
        invalidate_pdf_schedule_cache()
    weeks_upserted = 0
    if inserted > 0:
        invalidate_validation_cache()
    if schedule_weeks_payload:
        if has_new_schedule_rows and inserted <= 0:
            invalidate_validation_cache()
        weeks_result = create_schedule_weeks_data(schedule_weeks_payload)
        weeks_upserted = int(weeks_result.get("inserted") or 0)
    return {
        "inserted": inserted,
        "scheduleWeeksUpserted": weeks_upserted,
        "insertedNew": int(inserted_new),
        "updatedExisting": int(updated_existing),
        "dedupedRows": int(len(deduped_rows_by_match_key)),
    }


def modify_schedules_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    if not rows:
        return {"updated": 0}

    normalized_rows: list[dict] = []
    schedule_ids_to_validate: list[str] = []
    schedule_id_labels: dict[str, str] = {}
    station_codes_to_validate: list[str] = []
    station_code_labels: dict[str, str] = {}
    immutable_match_key_fields = ("scheduleId", "lineNum", "estNum", "startDate", "endDate")

    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise ValueError(f"item {index}: Each schedules item must be an object")

        row_label = _resolve_row_label(row, index=index)

        provided_immutable_fields = [
            field for field in immutable_match_key_fields if field in row
        ]
        if provided_immutable_fields:
            raise ValueError(
                f"{row_label}: Cannot update matchKey source fields on schedules PUT: "
                + ", ".join(provided_immutable_fields)
            )
        if "matchKey" in row:
            raise ValueError(f"{row_label}: matchKey cannot be updated")

        try:
            schedule_row_id = _ensure_uuid_text(row.get("id"), field="id")
            schedule_ids_to_validate.append(schedule_row_id)
            schedule_id_labels.setdefault(schedule_row_id, row_label)
            item: dict[str, object] = {"id": schedule_row_id}

            if "billingCode" in row:
                item["billingCode"] = parse_client_billing_code(
                    _ensure_required_text(
                        row.get("billingCode"),
                        field="billingCode",
                        max_length=20,
                    ),
                    field="billingCode",
                )["normalized"]
            if "mediaType" in row:
                item["mediaType"] = _ensure_media_type(row.get("mediaType"))
            if "stationCode" in row:
                station_code = _ensure_required_text(
                    row.get("stationCode"),
                    field="stationCode",
                    max_length=10,
                ).upper()
                item["stationCode"] = station_code
                station_codes_to_validate.append(station_code)
                station_code_labels.setdefault(station_code, row_label)
            if "broadcastMonth" in row:
                item["broadcastMonth"] = _ensure_unsigned_int(
                    row.get("broadcastMonth"),
                    field="broadcastMonth",
                    required=True,
                    minimum=1,
                    maximum=12,
                )
            if "broadcastYear" in row:
                item["broadcastYear"] = _ensure_year(
                    row.get("broadcastYear"),
                    field="broadcastYear",
                )
            if "totalSpot" in row:
                item["totalSpot"] = _ensure_unsigned_int(
                    row.get("totalSpot"),
                    field="totalSpot",
                    required=True,
                    minimum=0,
                    maximum=4294967295,
                )
            if "totalGross" in row:
                item["totalGross"] = _ensure_decimal(
                    row.get("totalGross"),
                    field="totalGross",
                    required=True,
                )
            if "rateGross" in row:
                item["rateGross"] = _ensure_decimal(
                    row.get("rateGross"),
                    field="rateGross",
                    required=True,
                )
            if "length" in row:
                item["length"] = _ensure_unsigned_int(
                    row.get("length"),
                    field="length",
                    required=True,
                    minimum=0,
                    maximum=255,
                )
            if "runtime" in row:
                item["runtime"] = _ensure_required_text(
                    row.get("runtime"),
                    field="runtime",
                    max_length=50,
                )
            if "programName" in row:
                item["programName"] = _ensure_optional_text(
                    row.get("programName"),
                    field="programName",
                    max_length=255,
                )
            if "days" in row:
                item["days"] = _ensure_required_text(
                    row.get("days"),
                    field="days",
                    max_length=20,
                )
            if "daypart" in row:
                item["daypart"] = _ensure_required_text(
                    row.get("daypart"),
                    field="daypart",
                    max_length=10,
                )
            if "rtg" in row:
                item["rtg"] = _ensure_decimal(
                    row.get("rtg"),
                    field="rtg",
                    required=False,
                    default=None,
                )

            if len(item) == 1:
                raise ValueError(
                    f"No updatable fields provided for schedule id '{schedule_row_id}'"
                )
        except ValueError as exc:
            raise ValueError(f"{row_label}: {exc}") from exc

        normalized_rows.append(item)

    try:
        ensure_schedule_ids_exist(schedule_ids_to_validate)
    except ValueError as exc:
        missing_values = _extract_unknown_values(
            str(exc),
            prefix="Unknown schedule id values:",
        )
        if missing_values:
            for missing in missing_values:
                row_label = schedule_id_labels.get(str(missing).strip())
                if row_label:
                    raise ValueError(
                        f"{row_label}: Unknown schedule id '{str(missing).strip()}'"
                    ) from exc
        raise

    if station_codes_to_validate:
        try:
            ensure_station_codes_exist(station_codes_to_validate)
        except ValueError as exc:
            missing_values = _extract_unknown_values(
                str(exc),
                prefix="Unknown stationCode values:",
            )
            if missing_values:
                for missing in missing_values:
                    key = str(missing).strip().upper()
                    row_label = station_code_labels.get(key)
                    if row_label:
                        raise ValueError(
                            f"{row_label}: Unknown stationCode '{key}'"
                        ) from exc
            raise

    existing_rows = get_schedules(ids=schedule_ids_to_validate)
    existing_by_id = {
        str(row.get("id") or "").strip(): row
        for row in existing_rows
        if str(row.get("id") or "").strip()
    }
    schedule_week_rows = get_schedule_weeks(schedule_ids=schedule_ids_to_validate)
    total_spot_by_schedule_id: dict[str, int] = {}
    for week_row in schedule_week_rows:
        schedule_row_id = str(week_row.get("scheduleId") or "").strip()
        if not schedule_row_id:
            continue
        spots = _ensure_unsigned_int(
            week_row.get("spots"),
            field="spots",
            required=False,
            minimum=0,
            maximum=4294967295,
        )
        total_spot_by_schedule_id[schedule_row_id] = (
            int(total_spot_by_schedule_id.get(schedule_row_id, 0)) + int(spots or 0)
        )

    for item in normalized_rows:
        schedule_row_id = str(item.get("id") or "").strip()
        row_label = schedule_id_labels.get(schedule_row_id, f"id '{schedule_row_id}'")
        existing = existing_by_id.get(schedule_row_id)
        if not isinstance(existing, dict):
            raise ValueError(f"{row_label}: Unknown schedule id '{schedule_row_id}'")

        try:
            effective_broadcast_month = _ensure_unsigned_int(
                item.get("broadcastMonth", existing.get("broadcastMonth")),
                field="broadcastMonth",
                required=True,
                minimum=1,
                maximum=12,
            )
            assert effective_broadcast_month is not None
            effective_broadcast_year = _ensure_year(
                item.get("broadcastYear", existing.get("broadcastYear")),
                field="broadcastYear",
            )
            effective_start_date = _ensure_required_date(
                existing.get("startDate"),
                field="startDate",
            )
            effective_end_date = _ensure_required_date(
                existing.get("endDate"),
                field="endDate",
            )
            _validate_schedule_dates_within_broadcast_period(
                start_date=effective_start_date,
                end_date=effective_end_date,
                broadcast_month=effective_broadcast_month,
                broadcast_year=effective_broadcast_year,
            )

            effective_rate_gross = _ensure_decimal(
                item.get("rateGross", existing.get("rateGross")),
                field="rateGross",
                required=True,
            )
            assert effective_rate_gross is not None
            total_spot = int(total_spot_by_schedule_id.get(schedule_row_id, 0))
            item["totalSpot"] = total_spot
            item["totalGross"] = _compute_total_gross_value(
                total_spot=total_spot,
                rate_gross=effective_rate_gross,
            )
        except ValueError as exc:
            raise ValueError(f"{row_label}: {exc}") from exc

    updated = update_schedules(normalized_rows)
    if normalized_rows:
        invalidate_pdf_schedule_cache()
    if updated > 0:
        invalidate_validation_cache()
    return {"updated": updated}


def list_schedule_weeks_data(
    *,
    ids: list[int] | None = None,
    schedule_ids: list[str] | None = None,
    week_start_from: str | None = None,
    week_start_to: str | None = None,
    week_end_from: str | None = None,
    week_end_to: str | None = None,
) -> list[dict]:
    if week_start_from and week_start_to:
        _validate_date_range(
            start_date=week_start_from,
            end_date=week_start_to,
            start_field="weekStartFrom",
            end_field="weekStartTo",
        )
    if week_end_from and week_end_to:
        _validate_date_range(
            start_date=week_end_from,
            end_date=week_end_to,
            start_field="weekEndFrom",
            end_field="weekEndTo",
        )

    return get_schedule_weeks(
        ids=ids or [],
        schedule_ids=schedule_ids or [],
        week_start_from=week_start_from,
        week_start_to=week_start_to,
        week_end_from=week_end_from,
        week_end_to=week_end_to,
    )


def create_schedule_weeks_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    if not rows:
        return {"inserted": 0}

    normalized_rows: list[dict] = []
    schedule_ids_to_validate: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each schedules weeks item must be an object")
        schedule_row_id = _ensure_uuid_text(row.get("scheduleId"), field="scheduleId")
        week_start = _ensure_required_date(row.get("weekStart"), field="weekStart")
        week_end = _ensure_required_date(row.get("weekEnd"), field="weekEnd")
        _validate_date_range(
            start_date=week_start,
            end_date=week_end,
            start_field="weekStart",
            end_field="weekEnd",
        )
        spots = _ensure_unsigned_int(
            row.get("spots"),
            field="spots",
            required=False,
            minimum=0,
            maximum=4294967295,
        )
        normalized_rows.append(
            {
                "scheduleId": schedule_row_id,
                "weekStart": week_start,
                "weekEnd": week_end,
                "spots": spots if spots is not None else 0,
            }
        )
        schedule_ids_to_validate.append(schedule_row_id)

    ensure_schedule_ids_exist(schedule_ids_to_validate)
    inserted = insert_schedule_weeks(normalized_rows)
    if inserted > 0:
        invalidate_pdf_schedule_cache()
    return {"inserted": inserted}


def modify_schedule_weeks_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    if not rows:
        return {"updated": 0}

    normalized_rows: list[dict] = []
    week_ids_to_check: list[int] = []
    schedule_ids_to_validate: list[str] = []

    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each schedules weeks item must be an object")
        week_row_id = _ensure_unsigned_int(
            row.get("id"),
            field="id",
            required=True,
            minimum=0,
            maximum=4294967295,
        )
        assert week_row_id is not None
        week_ids_to_check.append(week_row_id)

        item: dict[str, object] = {"id": week_row_id}
        if "scheduleId" in row:
            schedule_row_id = _ensure_uuid_text(row.get("scheduleId"), field="scheduleId")
            item["scheduleId"] = schedule_row_id
            schedule_ids_to_validate.append(schedule_row_id)
        if "weekStart" in row:
            item["weekStart"] = _ensure_optional_date(row.get("weekStart"), field="weekStart")
        if "weekEnd" in row:
            item["weekEnd"] = _ensure_optional_date(row.get("weekEnd"), field="weekEnd")
        if "spots" in row:
            item["spots"] = _ensure_unsigned_int(
                row.get("spots"),
                field="spots",
                required=True,
                minimum=0,
                maximum=4294967295,
            )

        if len(item) == 1:
            raise ValueError(
                f"No updatable fields provided for schedules weeks id '{week_row_id}'"
            )
        normalized_rows.append(item)

    if schedule_ids_to_validate:
        ensure_schedule_ids_exist(schedule_ids_to_validate)

    existing_rows = get_schedule_weeks(ids=week_ids_to_check)
    existing_by_id = {
        int(row.get("id")): row
        for row in existing_rows
        if row.get("id") is not None
    }
    for item in normalized_rows:
        week_row_id = int(item["id"])
        existing = existing_by_id.get(week_row_id)
        if not isinstance(existing, dict):
            raise ValueError(f"schedules weeks id '{week_row_id}' not found")
        effective_week_start = str(item.get("weekStart", existing.get("weekStart")) or "")
        effective_week_end = str(item.get("weekEnd", existing.get("weekEnd")) or "")
        _validate_date_range(
            start_date=effective_week_start,
            end_date=effective_week_end,
            start_field="weekStart",
            end_field="weekEnd",
        )

    updated = update_schedule_weeks(normalized_rows)
    if updated > 0:
        invalidate_pdf_schedule_cache()
    return {"updated": updated}
