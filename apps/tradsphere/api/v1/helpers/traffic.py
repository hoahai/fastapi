from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from html import escape as html_escape
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import re
import uuid

import mysql.connector

from apps.tradsphere.api.v1.helpers.accountValidation import (
    ensure_station_codes_exist,
    ensure_tradsphere_account_codes_exist,
    require_account_code,
)
from apps.tradsphere.api.v1.helpers.broadcastCalendar import get_calendar_month_bucket
from apps.tradsphere.api.v1.helpers.config import get_smtp_settings
from apps.tradsphere.api.v1.helpers.dbQueries import (
    delete_traffic_flight,
    delete_traffic_station,
    get_traffic_email_row,
    get_traffic_flight_row,
    get_traffic_row,
    list_schedule_station_candidates_for_account_range,
    get_traffic_station_row,
    insert_traffic,
    insert_traffic_flight,
    insert_traffic_station,
    list_traffic_by_account,
    list_traffic_flights,
    list_traffic_station_attachments,
    list_traffic_stations,
    save_traffic_bulk_changes,
    update_traffic,
    update_traffic_flight,
    update_traffic_station,
    upsert_traffic_email,
)
from apps.tradsphere.api.v1.helpers.stations import list_stations_data
from apps.tradsphere.api.v1.helpers.stations import map_station_names_by_codes
from shared.smtp import SmtpSendError, SmtpSettings, send_smtp_email
from shared.normalization import normalize_optional_note_text as _normalize_optional_note_text
from shared.tenant import get_tenant_id
from shared.utils import run_parallel


_TRAFFIC_STATUSES = {"draft", "ready", "sent", "confirmed", "archived"}
_FLIGHT_MEDIA_VALUES = {"TV", "RA", "CA", "OD", "NP", "CINE"}
_FLIGHT_LANGUAGE_VALUES = {"English", "Spanish"}
_DELIVERY_STATUS_VALUES = {
    "",
    "not_started",
    "needs_manual_upload",
    "ready_to_email",
    "sent",
    "skipped",
    "issue",
}
_CONFIRMED_STATUS_VALUES = {"", "pending", "confirmed", "issue", "not_required"}
_EMAIL_SENT_STATUS_VALUES = {"draft", "ready", "sent", "failed"}
_TEST_EMAIL_NOTICE_TEXT = "TEST EMAIL - This is a test copy of a Tradsphere Traffic email."
_TEST_EMAIL_SUBJECT_PREFIX = "[Test]"

_ROTATION_TARGET = Decimal("100.00")
_ROTATION_MIN = Decimal("0.00")
_ROTATION_MAX = Decimal("100.00")
_ROTATION_QUANT = Decimal("0.01")


class ConflictError(ValueError):
    pass


class NotFoundError(ValueError):
    pass


class InvalidReferenceError(ValueError):
    pass


class SafeDatabaseError(RuntimeError):
    pass


class EmailSendError(RuntimeError):
    pass


def _extract_db_error_message(exc: Exception) -> str:
    err_no = int(getattr(exc, "errno", 0) or 0)
    raw_message = str(exc).lower()
    if err_no == 1062:
        return "Duplicate record"
    if err_no in {1451, 1452}:
        return "Related parent/child record is missing or constrained"
    if err_no in {3819, 1265, 1366}:
        return "Invalid enum or constrained value"
    if "check constraint" in raw_message:
        return "Constraint check failed"
    return "Database operation failed"


def _map_db_exception(exc: Exception) -> Exception:
    if not isinstance(exc, mysql.connector.Error):
        return SafeDatabaseError("Database operation failed")
    err_no = int(getattr(exc, "errno", 0) or 0)
    friendly = _extract_db_error_message(exc)
    if err_no == 1062:
        return ConflictError(friendly)
    if err_no in {1451, 1452}:
        return InvalidReferenceError(friendly)
    if err_no in {3819, 1265, 1366}:
        return ValueError(friendly)
    return SafeDatabaseError(friendly)


def _safe_db_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        mapped = _map_db_exception(exc)
        if isinstance(mapped, (ConflictError, InvalidReferenceError, ValueError)):
            raise mapped from exc
        raise SafeDatabaseError("Database operation failed") from exc


def _to_iso_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    text = str(value).strip()
    return text or None


def _parse_json_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _require_uuid4(value: object, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    try:
        parsed = uuid.UUID(text)
    except ValueError as exc:
        raise ValueError(f"{field} must be a valid UUID") from exc
    if parsed.version != 4:
        raise ValueError(f"{field} must be a UUID v4")
    return str(parsed)


def _normalize_required_text(value: object, *, field: str, max_length: int) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    if len(text) > max_length:
        raise ValueError(f"{field} must be <= {max_length} characters")
    return text


def _normalize_optional_text(value: object, *, field: str, max_length: int) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) > max_length:
        raise ValueError(f"{field} must be <= {max_length} characters")
    return text


def _normalize_enum(
    value: object,
    *,
    field: str,
    allowed_values: set[str],
    default_value: str | None = None,
    allow_blank: bool = False,
) -> str:
    text = str(value or "").strip()
    if not text:
        if allow_blank:
            return ""
        if default_value is None:
            raise ValueError(f"{field} is required")
        return default_value
    lowered = text.lower()
    if lowered not in allowed_values:
        allowed = ", ".join(sorted(allowed_values))
        raise ValueError(f"{field} must be one of: {allowed}")
    return lowered


def _normalize_flight_medium(value: object) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return "TV"
    if text not in _FLIGHT_MEDIA_VALUES:
        allowed = ", ".join(sorted(_FLIGHT_MEDIA_VALUES))
        raise ValueError(f"medium must be one of: {allowed}")
    return text


def _normalize_flight_language(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return "English"
    lowered = text.lower()
    if lowered == "english":
        return "English"
    if lowered == "spanish":
        return "Spanish"
    allowed = ", ".join(sorted(_FLIGHT_LANGUAGE_VALUES))
    raise ValueError(f"language must be one of: {allowed}")


def _coerce_flight_language_for_output(value: object) -> str:
    try:
        return _normalize_flight_language(value)
    except ValueError:
        return "English"


def _normalize_rotation(value: object) -> Decimal:
    text = str(value if value is not None else "").strip()
    if not text:
        return Decimal("100.00")
    try:
        rotation = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("rotation must be a valid decimal number") from exc
    if not rotation.is_finite():
        raise ValueError("rotation must be finite")
    if rotation < _ROTATION_MIN or rotation > _ROTATION_MAX:
        raise ValueError("rotation must be between 0 and 100")
    return rotation.quantize(_ROTATION_QUANT, rounding=ROUND_HALF_UP)


def _normalize_unsigned_int(
    value: object,
    *,
    field: str,
    default: int,
    max_value: int,
) -> int:
    if value is None or str(value).strip() == "":
        return int(default)
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if parsed < 0:
        raise ValueError(f"{field} must be >= 0")
    if parsed > int(max_value):
        raise ValueError(f"{field} must be <= {int(max_value)}")
    return parsed


def _normalize_date(value: object, *, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field} must be ISO date YYYY-MM-DD") from exc
    return parsed.isoformat()


def _normalize_contacts_snapshot(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("contactsSnapshot must be valid JSON") from exc
    if not isinstance(parsed, (dict, list)):
        raise ValueError("contactsSnapshot must be a JSON object or array")
    return json.dumps(parsed, ensure_ascii=False)


def _normalize_email_list(value: object, *, field: str, required: bool) -> list[str]:
    if value is None:
        if required:
            raise ValueError(f"{field} is required")
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field} must be an array")

    normalized: list[str] = []
    seen: set[str] = set()
    for index, item in enumerate(value):
        text = str(item or "").strip()
        if not text:
            continue
        if "@" not in text:
            raise ValueError(f"{field}[{index}] must be an email-like value")
        email_value = text.lower()
        if email_value in seen:
            continue
        seen.add(email_value)
        normalized.append(email_value)

    if required and not normalized:
        raise ValueError(f"{field} is required")
    return normalized


def _html_to_plain_text(value: object) -> str:
    text = str(value or "")
    if not text.strip():
        return ""
    output = re.sub(r"(?i)<\s*br\s*/?>", "\n", text)
    output = re.sub(r"(?i)</\s*p\s*>", "\n", output)
    output = re.sub(r"(?is)<style[\s\S]*?</style>", " ", output)
    output = re.sub(r"(?is)<script[\s\S]*?</script>", " ", output)
    output = re.sub(r"(?is)<[^>]+>", " ", output)
    output = (
        output.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
    )
    output = re.sub(r"[ \t]+\n", "\n", output)
    output = re.sub(r"\n{3,}", "\n\n", output)
    output = re.sub(r"[ \t]{2,}", " ", output)
    return output.strip()


def _looks_like_html(value: object) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return bool(re.search(r"</?[a-z][\s\S]*>", text, re.IGNORECASE))


def _build_test_email_notice_text() -> str:
    return _TEST_EMAIL_NOTICE_TEXT


def _build_test_email_notice_html() -> str:
    notice_text = html_escape(_TEST_EMAIL_NOTICE_TEXT)
    return (
        '<div style="margin:0 0 18px;padding:12px 14px;border:1px solid #f59e0b;'
        'border-radius:10px;background:#fffbeb;color:#92400e;font-size:13px;'
        'line-height:1.5;font-weight:600;">'
        f"{notice_text}"
        "</div>"
    )


def _build_test_email_subject(subject: str) -> str:
    subject_text = str(subject or "").strip()
    if not subject_text:
        return _TEST_EMAIL_SUBJECT_PREFIX
    if subject_text.lower().startswith(_TEST_EMAIL_SUBJECT_PREFIX.lower()):
        return subject_text
    return f"{_TEST_EMAIL_SUBJECT_PREFIX} {subject_text}"


def _inject_test_email_notice_html(body_text: str) -> str:
    raw_body = str(body_text or "")
    notice_html = _build_test_email_notice_html()
    if not raw_body.strip():
        return notice_html
    if not _looks_like_html(raw_body):
        escaped_text = html_escape(raw_body)
        return (
            "<!DOCTYPE html><html><body>"
            f"{notice_html}"
            f'<pre style="margin:0;white-space:pre-wrap;font-family:inherit;">{escaped_text}</pre>'
            "</body></html>"
        )

    match = re.search(r"(?is)<body[^>]*>", raw_body)
    if match is None:
        return notice_html + raw_body
    insert_at = match.end()
    return raw_body[:insert_at] + notice_html + raw_body[insert_at:]


def _safe_decimal(value: object) -> Decimal:
    text = str(value if value is not None else "0").strip() or "0"
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _build_rotation_summary(*, total_rotation: Decimal) -> dict[str, object]:
    normalized_total = total_rotation.quantize(_ROTATION_QUANT, rounding=ROUND_HALF_UP)
    warning = normalized_total != _ROTATION_TARGET
    message = None
    if warning:
        message = (
            "Total rotation is "
            f"{normalized_total:.2f}" + "%" + "; expected 100.00%"
        )
    warnings: list[dict[str, str]] = []
    if message:
        warnings.append(
            {
                "code": "ROTATION_TOTAL_NOT_100",
                "message": message,
            }
        )
    return {
        "totalRotation": float(normalized_total),
        "rotationWarning": warning,
        "rotationWarningMessage": message,
        "warnings": warnings,
    }


def _to_non_negative_int(value: object, *, default: int = 0) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return int(default)
    return parsed


def _to_decimal(value: object, *, default: Decimal = Decimal("0")) -> Decimal:
    text = str(value if value is not None else "").strip()
    if not text:
        return default
    try:
        parsed = Decimal(text)
    except (InvalidOperation, ValueError):
        return default
    if not parsed.is_finite():
        return default
    return parsed


def _format_station_candidate_month_label(*, year: int, month: int) -> str:
    month_names = [
        "",
        "JAN",
        "FEB",
        "MAR",
        "APR",
        "MAY",
        "JUN",
        "JUL",
        "AUG",
        "SEP",
        "OCT",
        "NOV",
        "DEC",
    ]
    if year < 1000 or month < 1 or month > 12:
        return ""
    return f"{month_names[month]}'{str(year)[-2:]}"


def _get_station_candidate_month_key(*, start_date: str) -> tuple[int, int] | None:
    text = str(start_date or "").strip()
    if not text:
        return None
    try:
        parsed = date.fromisoformat(text[:10])
    except ValueError:
        return None
    return get_calendar_month_bucket(parsed)


def _build_station_candidate_summary(rows: list[dict]) -> dict[str, object]:
    month_keys: set[tuple[int, int]] = set()
    row_map: dict[tuple[int, str], dict[str, object]] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue

        est_num = _to_non_negative_int(row.get("estNum"), default=-1)
        station_code = str(row.get("stationCode") or "").strip().upper()
        if est_num < 0 or not station_code:
            continue

        month_key = _get_station_candidate_month_key(start_date=row.get("startDate"))
        if month_key is None:
            continue

        broadcast_year, broadcast_month = month_key
        month_keys.add((broadcast_year, broadcast_month))
        row_key = (est_num, station_code)
        row_entry = row_map.get(row_key)
        if row_entry is None:
            row_entry = {
                "estNum": est_num,
                "stationCode": station_code,
                "stationName": str(row.get("stationName") or "").strip() or None,
                "_cells": {},
            }
            row_map[row_key] = row_entry
        elif not row_entry.get("stationName"):
            station_name = str(row.get("stationName") or "").strip() or None
            if station_name:
                row_entry["stationName"] = station_name

        cells = row_entry["_cells"]
        if not isinstance(cells, dict):
            cells = {}
            row_entry["_cells"] = cells
        cell = cells.get(month_key)
        if cell is None:
            cell = {
                "monthKey": f"{broadcast_year:04d}-{broadcast_month:02d}",
                "year": broadcast_year,
                "month": broadcast_month,
                "label": _format_station_candidate_month_label(
                    year=broadcast_year,
                    month=broadcast_month,
                ),
                "hasSchedule": False,
                "hasSpot": False,
                "scheduleCount": 0,
                "totalSpot": 0,
                "totalGross": Decimal("0"),
            }
            cells[month_key] = cell

        cell["hasSchedule"] = True
        cell["scheduleCount"] = max(0, _to_non_negative_int(cell.get("scheduleCount"), default=0)) + 1
        total_spot = max(0, _to_non_negative_int(row.get("totalSpot"), default=0))
        cell["totalSpot"] = max(0, _to_non_negative_int(cell.get("totalSpot"), default=0)) + total_spot
        cell["totalGross"] = _to_decimal(cell.get("totalGross"), default=Decimal("0")) + _to_decimal(
            row.get("totalGross"),
            default=Decimal("0"),
        )
        if total_spot > 0:
            cell["hasSpot"] = True

    months = [
        {
            "monthKey": f"{year:04d}-{month:02d}",
            "year": year,
            "month": month,
            "label": _format_station_candidate_month_label(year=year, month=month),
        }
        for year, month in sorted(month_keys)
    ]
    month_key_order = [(month["year"], month["month"]) for month in months]

    rows_out: list[dict[str, object]] = []
    for row_key in sorted(row_map.keys()):
        row_entry = row_map[row_key]
        cells = row_entry.get("_cells")
        cells_by_key = cells if isinstance(cells, dict) else {}
        ordered_cells: list[dict[str, object]] = []
        for month_key in month_key_order:
            cell = cells_by_key.get(month_key)
            if isinstance(cell, dict):
                ordered_cells.append(
                    {
                        "monthKey": str(cell.get("monthKey") or ""),
                        "year": _to_non_negative_int(cell.get("year"), default=0),
                        "month": _to_non_negative_int(cell.get("month"), default=0),
                        "label": str(cell.get("label") or ""),
                        "hasSchedule": bool(cell.get("hasSchedule")),
                        "hasSpot": bool(cell.get("hasSpot")),
                        "scheduleCount": _to_non_negative_int(cell.get("scheduleCount"), default=0),
                        "totalSpot": _to_non_negative_int(cell.get("totalSpot"), default=0),
                        "totalGrossText": f"${_to_decimal(cell.get('totalGross'), default=Decimal('0')).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):.2f}",
                    }
                )
                continue

            year, month = month_key
            ordered_cells.append(
                {
                    "monthKey": f"{year:04d}-{month:02d}",
                    "year": year,
                    "month": month,
                    "label": _format_station_candidate_month_label(year=year, month=month),
                    "hasSchedule": False,
                    "hasSpot": False,
                    "scheduleCount": 0,
                    "totalSpot": 0,
                    "totalGrossText": "$0.00",
                }
            )

        rows_out.append(
            {
                "estNum": int(row_entry.get("estNum") or 0),
                "stationCode": str(row_entry.get("stationCode") or "").strip().upper(),
                "stationName": row_entry.get("stationName"),
                "monthCells": ordered_cells,
            }
        )

    return {
        "months": months,
        "rows": rows_out,
    }


def _build_traffic_list_item_from_detail(detail: dict) -> dict:
    traffic_row = detail.get("traffic") if isinstance(detail, dict) else {}
    flights = detail.get("flights") if isinstance(detail, dict) else []
    stations = detail.get("stations") if isinstance(detail, dict) else []
    email = detail.get("email") if isinstance(detail, dict) else None
    summary = detail.get("summary") if isinstance(detail, dict) else {}
    traffic_serialized = _serialize_traffic_row(traffic_row if isinstance(traffic_row, dict) else {})

    station_codes = [
        str(station.get("stationCode") or "").strip().upper()
        for station in stations
        if isinstance(station, dict) and str(station.get("stationCode") or "").strip()
    ]
    station_name_by_code = map_station_names_by_codes(station_codes)
    search_stations = _merge_search_tokens(
        station_codes,
        [
            str(station_name_by_code.get(code, "") or "").strip().upper()
            for code in station_codes
            if str(station_name_by_code.get(code, "") or "").strip()
        ],
    )
    search_emails = _extract_email_search_values(
        (email or {}).get("toEmails") if isinstance(email, dict) else None,
        (email or {}).get("ccEmails") if isinstance(email, dict) else None,
        (email or {}).get("bccEmails") if isinstance(email, dict) else None,
    )
    search_iscis: list[str] = []
    seen_iscis: set[str] = set()
    for flight in flights:
        if not isinstance(flight, dict):
            continue
        isci = str(flight.get("isci") or "").strip().upper()
        if not isci or isci in seen_iscis:
            continue
        seen_iscis.add(isci)
        search_iscis.append(isci)

    return {
        "id": traffic_serialized.get("id", ""),
        "accountCode": traffic_serialized.get("accountCode", ""),
        "campaign": traffic_serialized.get("campaign", ""),
        "searchCampaign": str(traffic_serialized.get("campaign") or "").lower(),
        "status": traffic_serialized.get("status", "draft"),
        "note": traffic_serialized.get("note"),
        "dateCreated": traffic_serialized.get("dateCreated"),
        "dateUpdated": traffic_serialized.get("dateUpdated"),
        "flightCount": int(summary.get("flightCount") or 0),
        "stationCount": int(summary.get("stationCount") or 0),
        "emailSentStatus": email.get("sentStatus") if isinstance(email, dict) else None,
        "emailSentAt": _to_iso_text(email.get("sentAt")) if isinstance(email, dict) else None,
        "searchIscis": search_iscis,
        "searchStations": search_stations,
        "searchEmails": search_emails,
        "summary": {
            "totalRotation": float(_safe_decimal(summary.get("totalRotation")).quantize(_ROTATION_QUANT, rounding=ROUND_HALF_UP)),
            "rotationWarning": bool(summary.get("rotationWarning")),
            "rotationWarningMessage": summary.get("rotationWarningMessage"),
            "warnings": summary.get("warnings") if isinstance(summary.get("warnings"), list) else [],
        },
    }


def _serialize_traffic_row(row: dict) -> dict:
    return {
        "id": str(row.get("id") or "").strip(),
        "accountCode": str(row.get("accountCode") or "").strip().upper(),
        "campaign": row.get("campaign"),
        "status": row.get("status"),
        "note": row.get("note"),
        "dateCreated": _to_iso_text(row.get("dateCreated")),
        "dateUpdated": _to_iso_text(row.get("dateUpdated")),
    }


def _serialize_flight_row(row: dict) -> dict:
    rotation_decimal = _safe_decimal(row.get("rotation")).quantize(
        _ROTATION_QUANT,
        rounding=ROUND_HALF_UP,
    )
    return {
        "id": int(row.get("id")),
        "trafficId": str(row.get("trafficId") or "").strip(),
        "flightStart": _to_iso_text(row.get("flightStart")),
        "flightEnd": _to_iso_text(row.get("flightEnd")),
        "medium": row.get("medium"),
        "language": _coerce_flight_language_for_output(row.get("language")),
        "length": int(row.get("length") or 0),
        "isci": row.get("isci"),
        "rotation": float(rotation_decimal),
        "fileUrl": row.get("fileUrl"),
        "scriptUrl": row.get("scriptUrl"),
        "note": row.get("note"),
        "dateCreated": _to_iso_text(row.get("dateCreated")),
        "dateUpdated": _to_iso_text(row.get("dateUpdated")),
    }


def _serialize_attachment_row(row: dict) -> dict:
    station_id = row.get("stationId")
    if station_id is None:
        owner_entity_id = str(row.get("ownerEntityId") or "").strip()
        if owner_entity_id.isdigit():
            station_id = int(owner_entity_id)
    parsed_provider_metadata = _parse_json_value(row.get("providerMetadata"))
    return {
        "id": int(row.get("attachmentId")),
        "stationId": int(station_id) if station_id is not None else None,
        "appCode": row.get("appCode"),
        "ownerEntityType": row.get("ownerEntityType"),
        "ownerEntityId": row.get("ownerEntityId"),
        "accessUrl": row.get("accessUrl"),
        "originalFileName": row.get("originalFileName"),
        "mimeType": row.get("mimeType"),
        "fileSize": row.get("fileSize"),
        "storageProvider": row.get("storageProvider"),
        "storageKey": row.get("storageKey"),
        "providerMetadata": parsed_provider_metadata,
        "uploadedBy": row.get("uploadedBy"),
        "tenantSlug": row.get("tenantSlug"),
        "dateCreated": _to_iso_text(row.get("dateCreated")),
        "dateUpdated": _to_iso_text(row.get("dateUpdated")),
    }


def _serialize_station_row(row: dict, *, attachments: list[dict]) -> dict:
    return {
        "id": int(row.get("id")),
        "trafficId": str(row.get("trafficId") or "").strip(),
        "stationCode": str(row.get("stationCode") or "").strip().upper(),
        "contactsSnapshot": _parse_json_value(row.get("contactsSnapshot")),
        "deliveryMethod": row.get("deliveryMethod"),
        "deliveryStatus": row.get("deliveryStatus"),
        "confirmedStatus": row.get("confirmedStatus"),
        "note": row.get("note"),
        "attachments": attachments,
        "dateCreated": _to_iso_text(row.get("dateCreated")),
        "dateUpdated": _to_iso_text(row.get("dateUpdated")),
    }


def _serialize_email_row(row: dict | None) -> dict | None:
    if row is None:
        return None
    return {
        "id": int(row.get("id")),
        "trafficId": str(row.get("trafficId") or "").strip(),
        "toEmails": _parse_json_value(row.get("toEmails")) or [],
        "ccEmails": _parse_json_value(row.get("ccEmails")) or [],
        "bccEmails": _parse_json_value(row.get("bccEmails")) or [],
        "subject": row.get("subject"),
        "body": row.get("body"),
        "sentStatus": row.get("sentStatus"),
        "sentAt": _to_iso_text(row.get("sentAt")),
        "sentByUserId": row.get("sentByUserId"),
        "smtpMessageId": row.get("smtpMessageId"),
        "lastSendAttemptAt": _to_iso_text(row.get("lastSendAttemptAt")),
        "lastSendError": row.get("lastSendError"),
        "dateCreated": _to_iso_text(row.get("dateCreated")),
        "dateUpdated": _to_iso_text(row.get("dateUpdated")),
    }


def _split_search_concat_tokens(raw_value: object) -> list[str]:
    normalized = str(raw_value or "").strip()
    if not normalized:
        return []
    seen: set[str] = set()
    output: list[str] = []
    for token in normalized.split("||"):
        text = str(token or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        output.append(text)
    return output


def _merge_search_tokens(*groups: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for group in groups:
        for token in group:
            text = str(token or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            output.append(text)
    return output


def _extract_email_search_values(*raw_values: object) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for raw in raw_values:
        if raw is None:
            continue
        values: list[object] = []
        if isinstance(raw, (list, tuple)):
            values.extend(list(raw))
        else:
            text = str(raw).strip()
            if not text:
                continue
            if text.startswith("["):
                try:
                    parsed = json.loads(text)
                except Exception:
                    parsed = []
                if isinstance(parsed, list):
                    values.extend(parsed)
                else:
                    values.append(parsed)
            else:
                values.append(text)

        for value in values:
            email = str(value or "").strip().lower()
            if not email or email in seen:
                continue
            seen.add(email)
            output.append(email)
    return output


def _ensure_traffic_exists(traffic_id: str) -> dict:
    traffic_row = _safe_db_call(get_traffic_row, traffic_id=traffic_id)
    if not traffic_row:
        raise NotFoundError("Traffic record not found")
    return traffic_row


def _ensure_not_archived(traffic_row: dict, *, action: str) -> None:
    status = str(traffic_row.get("status") or "").strip().lower()
    if status == "archived":
        raise ValueError(f"Archived traffic cannot be modified ({action})")


def _load_traffic_flights_safe(traffic_id: str) -> list[dict]:
    return _safe_db_call(list_traffic_flights, traffic_id=traffic_id)


def _load_traffic_stations_safe(traffic_id: str) -> list[dict]:
    return _safe_db_call(list_traffic_stations, traffic_id=traffic_id)


def _load_traffic_email_safe(traffic_id: str) -> dict | None:
    return _safe_db_call(get_traffic_email_row, traffic_id=traffic_id)


def _build_detail_payload(*, traffic_row: dict) -> dict:
    traffic_id = str(traffic_row.get("id") or "").strip()
    flights_rows, stations_rows, email_row = run_parallel(
        tasks=[
            (_load_traffic_flights_safe, (traffic_id,)),
            (_load_traffic_stations_safe, (traffic_id,)),
            (_load_traffic_email_safe, (traffic_id,)),
        ],
        api_name="tradsphere.traffic.detail",
    )

    serialized_flights = [_serialize_flight_row(row) for row in flights_rows]

    station_ids = [int(row.get("id")) for row in stations_rows if row.get("id") is not None]
    tenant_slug = get_tenant_id()
    attachment_rows = _safe_db_call(
        list_traffic_station_attachments,
        station_ids=station_ids,
        tenant_slug=tenant_slug,
    )
    attachments_by_station: dict[int, list[dict]] = defaultdict(list)
    for row in attachment_rows:
        serialized_attachment = _serialize_attachment_row(row)
        station_id = serialized_attachment.get("stationId")
        if station_id is None:
            continue
        attachments_by_station[int(station_id)].append(serialized_attachment)

    serialized_stations = [
        _serialize_station_row(
            row,
            attachments=attachments_by_station.get(int(row.get("id")), []),
        )
        for row in stations_rows
    ]

    total_rotation = sum(
        (_safe_decimal(item.get("rotation")) for item in serialized_flights),
        Decimal("0.00"),
    )
    rotation_summary = _build_rotation_summary(total_rotation=total_rotation)

    return {
        "traffic": _serialize_traffic_row(traffic_row),
        "flights": serialized_flights,
        "stations": serialized_stations,
        "email": _serialize_email_row(email_row),
        "summary": {
            **rotation_summary,
            "flightCount": len(serialized_flights),
            "stationCount": len(serialized_stations),
        },
    }


def list_traffic_for_account_data(
    *,
    account_code: str,
    include_archived: bool = False,
) -> list[dict]:
    normalized_code = require_account_code(account_code, field="code")
    ensure_tradsphere_account_codes_exist([normalized_code])
    rows = _safe_db_call(
        list_traffic_by_account,
        account_code=normalized_code,
        include_archived=bool(include_archived),
    )

    out: list[dict] = []
    for row in rows:
        total_rotation = _safe_decimal(row.get("totalRotation")).quantize(
            _ROTATION_QUANT,
            rounding=ROUND_HALF_UP,
        )
        summary = _build_rotation_summary(total_rotation=total_rotation)
        search_station_codes = _split_search_concat_tokens(row.get("stationCodeSearch"))
        search_station_names = _split_search_concat_tokens(row.get("stationNameSearch"))
        out.append(
            {
                **_serialize_traffic_row(row),
                "flightCount": int(row.get("flightCount") or 0),
                "stationCount": int(row.get("stationCount") or 0),
                "emailSentStatus": row.get("emailSentStatus"),
                "emailSentAt": _to_iso_text(row.get("emailSentAt")),
                "searchIscis": _split_search_concat_tokens(row.get("isciSearch")),
                "searchStations": _merge_search_tokens(search_station_codes, search_station_names),
                "searchEmails": _extract_email_search_values(
                    row.get("searchToEmails"),
                    row.get("searchCcEmails"),
                    row.get("searchBccEmails"),
                ),
                "summary": summary,
            }
        )
    return out


def get_traffic_detail_data(*, traffic_id: str) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    traffic_row = _ensure_traffic_exists(normalized_traffic_id)
    return _build_detail_payload(traffic_row=traffic_row)


def list_station_candidates_for_flight_range_data(
    *,
    account_code: str,
    flight_start: str,
    flight_end: str,
    est_nums: list[int] | None = None,
    languages: list[str] | None = None,
    media_types: list[str] | None = None,
) -> dict:
    normalized_account_code = require_account_code(account_code, field="accountCode")
    ensure_tradsphere_account_codes_exist([normalized_account_code])
    normalized_flight_start = _normalize_date(flight_start, field="flightStart")
    normalized_flight_end = _normalize_date(flight_end, field="flightEnd")
    normalized_est_nums = sorted({int(item) for item in (est_nums or [])})
    normalized_languages = sorted(
        {
            _normalize_flight_language(item)
            for item in (languages or [])
            if str(item or "").strip()
        }
    )
    normalized_media_types = sorted(
        {
            _normalize_flight_medium(item)
            for item in (media_types or [])
            if str(item or "").strip()
        }
    )
    for est_num in normalized_est_nums:
        if est_num < 0:
            raise ValueError("estNums must contain unsigned integers")
    if date.fromisoformat(normalized_flight_start) > date.fromisoformat(normalized_flight_end):
        raise ValueError("flightStart must be on or before flightEnd")

    rows = _safe_db_call(
        list_schedule_station_candidates_for_account_range,
        account_code=normalized_account_code,
        flight_start=normalized_flight_start,
        flight_end=normalized_flight_end,
        est_nums=normalized_est_nums,
        languages=normalized_languages,
        media_types=normalized_media_types,
    )
    summary = _build_station_candidate_summary(rows)

    station_codes: list[str] = []
    stations: list[dict] = []
    seen_codes: set[str] = set()
    est_num_meta_by_value: dict[int, dict[str, object]] = {}
    for row in rows:
        est_num_raw = row.get("estNum")
        try:
            est_num = int(est_num_raw)
        except (TypeError, ValueError):
            est_num = None
        if est_num is not None and est_num >= 0 and est_num not in est_num_meta_by_value:
            est_num_note_raw = str(row.get("estNumNote") or "").strip()
            est_num_medium_raw = str(row.get("estNumMedium") or "").strip().upper()
            est_num_meta_by_value[est_num] = {
                "estNum": est_num,
                "note": est_num_note_raw or None,
                "medium": est_num_medium_raw or None,
                "stationCodes": set(),
            }

        station_code = str(row.get("stationCode") or "").strip().upper()
        if not station_code or station_code in seen_codes:
            if est_num is not None and est_num in est_num_meta_by_value:
                station_codes_set = est_num_meta_by_value[est_num]["stationCodes"]
                if isinstance(station_codes_set, set) and station_code:
                    station_codes_set.add(station_code)
            continue
        seen_codes.add(station_code)
        station_codes.append(station_code)
        if est_num is not None and est_num in est_num_meta_by_value:
            station_codes_set = est_num_meta_by_value[est_num]["stationCodes"]
            if isinstance(station_codes_set, set):
                station_codes_set.add(station_code)
        stations.append(
            {
                "stationCode": station_code,
                "stationName": str(row.get("stationName") or "").strip() or None,
            }
        )

    station_detail_by_code: dict[str, dict[str, object]] = {}
    if station_codes:
        station_rows = list_stations_data(
            codes=station_codes,
            delivery_method_detail=False,
            contact_detail=True,
            include_contacts=True,
        )
        for station_row in station_rows:
            station_code = str(station_row.get("code") or "").strip().upper()
            if not station_code:
                continue

            delivery_method_name: str | None = None
            delivery_method = station_row.get("deliveryMethod")
            if isinstance(delivery_method, dict):
                delivery_method_name = str(delivery_method.get("name") or "").strip() or None
            elif delivery_method is not None:
                delivery_method_name = str(delivery_method).strip() or None

            contacts_snapshot = station_row.get("contacts")
            station_detail_by_code[station_code] = {
                "deliveryMethod": delivery_method_name,
                "contactsSnapshot": contacts_snapshot if isinstance(contacts_snapshot, dict) else None,
            }

    for station in stations:
        station_code = str(station.get("stationCode") or "").strip().upper()
        detail = station_detail_by_code.get(station_code, {})
        station["deliveryMethod"] = detail.get("deliveryMethod")
        station["contactsSnapshot"] = detail.get("contactsSnapshot")

    est_num_candidates: list[dict] = []
    for est_num in sorted(est_num_meta_by_value.keys()):
        meta = est_num_meta_by_value[est_num]
        station_codes_set = meta.get("stationCodes")
        station_count = len(station_codes_set) if isinstance(station_codes_set, set) else 0
        est_num_candidates.append(
            {
                "estNum": est_num,
                "note": meta.get("note"),
                "medium": meta.get("medium"),
                "stationCount": station_count,
            }
        )

    return {
        "accountCode": normalized_account_code,
        "flightStart": normalized_flight_start,
        "flightEnd": normalized_flight_end,
        "estNums": est_num_candidates,
        "stations": stations,
        "summary": {
            "candidateCount": len(stations),
            "estNumCount": len(est_num_candidates),
            "months": summary["months"],
            "rows": summary["rows"],
        },
    }


def create_traffic_data(*, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    account_code = require_account_code(payload.get("accountCode"), field="accountCode")
    ensure_tradsphere_account_codes_exist([account_code])

    campaign = _normalize_required_text(payload.get("campaign"), field="campaign", max_length=255)
    status = _normalize_enum(
        payload.get("status"),
        field="status",
        allowed_values=_TRAFFIC_STATUSES,
        default_value="draft",
    )
    note = _normalize_optional_note_text(payload.get("note"))

    traffic_id = str(uuid.uuid4())
    _safe_db_call(
        insert_traffic,
        {
            "id": traffic_id,
            "accountCode": account_code,
            "campaign": campaign,
            "status": status,
            "note": note,
        },
    )
    traffic_row = _ensure_traffic_exists(traffic_id)
    return _serialize_traffic_row(traffic_row)


def update_traffic_data(*, traffic_id: str, payload: dict) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    traffic_row = _ensure_traffic_exists(normalized_traffic_id)
    _ensure_not_archived(traffic_row, action="update")

    fields: dict[str, object] = {}
    if "campaign" in payload:
        fields["campaign"] = _normalize_required_text(
            payload.get("campaign"),
            field="campaign",
            max_length=255,
        )
    if "status" in payload:
        fields["status"] = _normalize_enum(
            payload.get("status"),
            field="status",
            allowed_values=_TRAFFIC_STATUSES,
        )
    if "note" in payload:
        fields["note"] = _normalize_optional_note_text(payload.get("note"))

    if not fields:
        raise ValueError("At least one updatable field is required: campaign, status, note")

    _safe_db_call(update_traffic, traffic_id=normalized_traffic_id, fields=fields)
    updated_row = _ensure_traffic_exists(normalized_traffic_id)
    return _serialize_traffic_row(updated_row)


def mark_traffic_ready_data(*, traffic_id: str) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    traffic_row = _ensure_traffic_exists(normalized_traffic_id)
    _ensure_not_archived(traffic_row, action="ready")
    _safe_db_call(
        update_traffic,
        traffic_id=normalized_traffic_id,
        fields={"status": "ready"},
    )
    detail = _build_detail_payload(traffic_row=_ensure_traffic_exists(normalized_traffic_id))
    return {
        "id": normalized_traffic_id,
        "status": "ready",
        "summary": detail.get("summary"),
    }


def archive_traffic_data(*, traffic_id: str) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    _ = _ensure_traffic_exists(normalized_traffic_id)
    _safe_db_call(
        update_traffic,
        traffic_id=normalized_traffic_id,
        fields={"status": "archived"},
    )
    return {
        "id": normalized_traffic_id,
        "status": "archived",
    }


def create_traffic_flight_data(*, traffic_id: str, payload: dict) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    traffic_row = _ensure_traffic_exists(normalized_traffic_id)
    _ensure_not_archived(traffic_row, action="create flight")

    flight_start = _normalize_date(payload.get("flightStart"), field="flightStart")
    flight_end = _normalize_date(payload.get("flightEnd"), field="flightEnd")
    if date.fromisoformat(flight_start) > date.fromisoformat(flight_end):
        raise ValueError("flightStart must be on or before flightEnd")

    medium = _normalize_flight_medium(payload.get("medium"))
    language = _normalize_flight_language(payload.get("language"))
    length = _normalize_unsigned_int(
        payload.get("length"),
        field="length",
        default=60,
        max_value=65535,
    )
    isci = _normalize_optional_text(payload.get("isci"), field="isci", max_length=128)
    rotation = _normalize_rotation(payload.get("rotation"))
    file_url = _normalize_optional_text(payload.get("fileUrl"), field="fileUrl", max_length=2048)
    script_url = _normalize_optional_text(payload.get("scriptUrl"), field="scriptUrl", max_length=2048)
    note = _normalize_optional_note_text(payload.get("note"))

    created_flight_id = _safe_db_call(
        insert_traffic_flight,
        {
            "trafficId": normalized_traffic_id,
            "flightStart": flight_start,
            "flightEnd": flight_end,
            "medium": medium,
            "language": language,
            "length": length,
            "isci": isci,
            "rotation": str(rotation),
            "fileUrl": file_url,
            "scriptUrl": script_url,
            "note": note,
        },
    )
    if int(created_flight_id or 0) <= 0:
        raise SafeDatabaseError("Failed to create flight")

    created_row = _safe_db_call(
        get_traffic_flight_row,
        traffic_id=normalized_traffic_id,
        flight_id=int(created_flight_id),
    )
    if not created_row:
        raise SafeDatabaseError("Failed to load created flight")
    return _serialize_flight_row(created_row)


def update_traffic_flight_data(*, traffic_id: str, flight_id: int, payload: dict) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    if int(flight_id) < 0:
        raise ValueError("flightId must be an unsigned integer")
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    traffic_row = _ensure_traffic_exists(normalized_traffic_id)
    _ensure_not_archived(traffic_row, action="update flight")

    existing_row = _safe_db_call(
        get_traffic_flight_row,
        traffic_id=normalized_traffic_id,
        flight_id=int(flight_id),
    )
    if not existing_row:
        raise NotFoundError("Flight record not found for this traffic")

    fields: dict[str, object] = {}
    if "flightStart" in payload:
        fields["flightStart"] = _normalize_date(payload.get("flightStart"), field="flightStart")
    if "flightEnd" in payload:
        fields["flightEnd"] = _normalize_date(payload.get("flightEnd"), field="flightEnd")

    resolved_start = fields.get("flightStart") or _to_iso_text(existing_row.get("flightStart"))
    resolved_end = fields.get("flightEnd") or _to_iso_text(existing_row.get("flightEnd"))
    if resolved_start and resolved_end and date.fromisoformat(str(resolved_start)) > date.fromisoformat(str(resolved_end)):
        raise ValueError("flightStart must be on or before flightEnd")

    if "medium" in payload:
        fields["medium"] = _normalize_flight_medium(payload.get("medium"))
    if "language" in payload:
        fields["language"] = _normalize_flight_language(payload.get("language"))
    if "length" in payload:
        fields["length"] = _normalize_unsigned_int(
            payload.get("length"),
            field="length",
            default=60,
            max_value=65535,
        )
    if "isci" in payload:
        fields["isci"] = _normalize_optional_text(payload.get("isci"), field="isci", max_length=128)
    if "rotation" in payload:
        fields["rotation"] = str(_normalize_rotation(payload.get("rotation")))
    if "fileUrl" in payload:
        fields["fileUrl"] = _normalize_optional_text(payload.get("fileUrl"), field="fileUrl", max_length=2048)
    if "scriptUrl" in payload:
        fields["scriptUrl"] = _normalize_optional_text(payload.get("scriptUrl"), field="scriptUrl", max_length=2048)
    if "note" in payload:
        fields["note"] = _normalize_optional_note_text(payload.get("note"))

    if not fields:
        raise ValueError(
            "At least one updatable field is required: flightStart, flightEnd, medium, language, length, isci, rotation, fileUrl, scriptUrl, note"
        )

    updated = _safe_db_call(
        update_traffic_flight,
        traffic_id=normalized_traffic_id,
        flight_id=int(flight_id),
        fields=fields,
    )
    if int(updated or 0) <= 0:
        raise NotFoundError("Flight record not found for this traffic")

    updated_row = _safe_db_call(
        get_traffic_flight_row,
        traffic_id=normalized_traffic_id,
        flight_id=int(flight_id),
    )
    if not updated_row:
        raise NotFoundError("Flight record not found for this traffic")
    return _serialize_flight_row(updated_row)


def delete_traffic_flight_data(*, traffic_id: str, flight_id: int) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    if int(flight_id) < 0:
        raise ValueError("flightId must be an unsigned integer")

    traffic_row = _ensure_traffic_exists(normalized_traffic_id)
    _ensure_not_archived(traffic_row, action="delete flight")

    existing_row = _safe_db_call(
        get_traffic_flight_row,
        traffic_id=normalized_traffic_id,
        flight_id=int(flight_id),
    )
    if not existing_row:
        raise NotFoundError("Flight record not found for this traffic")

    deleted = _safe_db_call(
        delete_traffic_flight,
        traffic_id=normalized_traffic_id,
        flight_id=int(flight_id),
    )
    if int(deleted or 0) <= 0:
        raise NotFoundError("Flight record not found for this traffic")
    return {"deleted": 1, "id": int(flight_id)}


def create_traffic_station_data(*, traffic_id: str, payload: dict) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    traffic_row = _ensure_traffic_exists(normalized_traffic_id)
    _ensure_not_archived(traffic_row, action="create station")

    station_code = require_account_code(payload.get("stationCode"), field="stationCode")
    ensure_station_codes_exist([station_code])

    contacts_snapshot = _normalize_contacts_snapshot(payload.get("contactsSnapshot"))
    delivery_method = _normalize_optional_text(payload.get("deliveryMethod"), field="deliveryMethod", max_length=128)
    delivery_status = _normalize_enum(
        payload.get("deliveryStatus"),
        field="deliveryStatus",
        allowed_values=_DELIVERY_STATUS_VALUES,
        default_value="",
        allow_blank=True,
    )
    confirmed_status = _normalize_enum(
        payload.get("confirmedStatus"),
        field="confirmedStatus",
        allowed_values=_CONFIRMED_STATUS_VALUES,
        default_value="",
        allow_blank=True,
    )
    note = _normalize_optional_note_text(payload.get("note"))

    created_station_id = _safe_db_call(
        insert_traffic_station,
        {
            "trafficId": normalized_traffic_id,
            "stationCode": station_code,
            "contactsSnapshot": contacts_snapshot,
            "deliveryMethod": delivery_method,
            "deliveryStatus": delivery_status,
            "confirmedStatus": confirmed_status,
            "note": note,
        },
    )
    if int(created_station_id or 0) <= 0:
        raise SafeDatabaseError("Failed to create station")

    created_row = _safe_db_call(
        get_traffic_station_row,
        traffic_id=normalized_traffic_id,
        station_id=int(created_station_id),
    )
    if not created_row:
        raise SafeDatabaseError("Failed to load created station")
    return _serialize_station_row(created_row, attachments=[])


def update_traffic_station_data(*, traffic_id: str, station_id: int, payload: dict) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    if int(station_id) < 0:
        raise ValueError("stationId must be an unsigned integer")
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    traffic_row = _ensure_traffic_exists(normalized_traffic_id)
    _ensure_not_archived(traffic_row, action="update station")

    existing_row = _safe_db_call(
        get_traffic_station_row,
        traffic_id=normalized_traffic_id,
        station_id=int(station_id),
    )
    if not existing_row:
        raise NotFoundError("Station record not found for this traffic")

    fields: dict[str, object] = {}
    if "stationCode" in payload:
        station_code = require_account_code(payload.get("stationCode"), field="stationCode")
        ensure_station_codes_exist([station_code])
        fields["stationCode"] = station_code
    if "contactsSnapshot" in payload:
        fields["contactsSnapshot"] = _normalize_contacts_snapshot(payload.get("contactsSnapshot"))
    if "deliveryMethod" in payload:
        fields["deliveryMethod"] = _normalize_optional_text(
            payload.get("deliveryMethod"),
            field="deliveryMethod",
            max_length=128,
        )
    if "deliveryStatus" in payload:
        fields["deliveryStatus"] = _normalize_enum(
            payload.get("deliveryStatus"),
            field="deliveryStatus",
            allowed_values=_DELIVERY_STATUS_VALUES,
            default_value="",
            allow_blank=True,
        )
    if "confirmedStatus" in payload:
        fields["confirmedStatus"] = _normalize_enum(
            payload.get("confirmedStatus"),
            field="confirmedStatus",
            allowed_values=_CONFIRMED_STATUS_VALUES,
            default_value="",
            allow_blank=True,
        )
    if "note" in payload:
        fields["note"] = _normalize_optional_note_text(payload.get("note"))

    if not fields:
        raise ValueError(
            "At least one updatable field is required: stationCode, contactsSnapshot, deliveryMethod, deliveryStatus, confirmedStatus, note"
        )

    updated = _safe_db_call(
        update_traffic_station,
        traffic_id=normalized_traffic_id,
        station_id=int(station_id),
        fields=fields,
    )
    if int(updated or 0) <= 0:
        raise NotFoundError("Station record not found for this traffic")

    updated_row = _safe_db_call(
        get_traffic_station_row,
        traffic_id=normalized_traffic_id,
        station_id=int(station_id),
    )
    if not updated_row:
        raise NotFoundError("Station record not found for this traffic")
    return _serialize_station_row(updated_row, attachments=[])


def delete_traffic_station_data(*, traffic_id: str, station_id: int) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    if int(station_id) < 0:
        raise ValueError("stationId must be an unsigned integer")

    traffic_row = _ensure_traffic_exists(normalized_traffic_id)
    _ensure_not_archived(traffic_row, action="delete station")

    existing_row = _safe_db_call(
        get_traffic_station_row,
        traffic_id=normalized_traffic_id,
        station_id=int(station_id),
    )
    if not existing_row:
        raise NotFoundError("Station record not found for this traffic")

    deleted = _safe_db_call(
        delete_traffic_station,
        traffic_id=normalized_traffic_id,
        station_id=int(station_id),
    )
    if int(deleted or 0) <= 0:
        raise NotFoundError("Station record not found for this traffic")
    return {"deleted": 1, "id": int(station_id)}


def upsert_traffic_email_data(*, traffic_id: str, payload: dict, sent_by_user_id: str | None = None) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    traffic_row = _ensure_traffic_exists(normalized_traffic_id)
    _ensure_not_archived(traffic_row, action="update email")

    sent_status = _normalize_enum(
        payload.get("sentStatus"),
        field="sentStatus",
        allowed_values=_EMAIL_SENT_STATUS_VALUES,
        default_value="draft",
    )
    require_ready_fields = sent_status == "ready"

    to_emails = _normalize_email_list(
        payload.get("toEmails"),
        field="toEmails",
        required=require_ready_fields,
    )
    cc_emails = _normalize_email_list(
        payload.get("ccEmails") or [],
        field="ccEmails",
        required=False,
    )
    bcc_emails = _normalize_email_list(
        payload.get("bccEmails") or [],
        field="bccEmails",
        required=False,
    )

    subject = _normalize_optional_text(payload.get("subject"), field="subject", max_length=500)
    body_text = str(payload.get("body") or "").strip()

    if require_ready_fields:
        if not subject:
            raise ValueError("subject is required when sentStatus is ready")
        if not body_text:
            raise ValueError("body is required when sentStatus is ready")

    if not subject:
        subject = "(draft)"
    if not body_text:
        body_text = ""

    sent_at_value = payload.get("sentAt")
    if sent_at_value is not None and str(sent_at_value).strip() != "":
        sent_at_text = str(sent_at_value).strip().replace("Z", "+00:00")
        try:
            _ = datetime.fromisoformat(sent_at_text)
        except ValueError as exc:
            raise ValueError("sentAt must be ISO datetime") from exc
    else:
        sent_at_text = None

    last_send_attempt_value = payload.get("lastSendAttemptAt")
    if last_send_attempt_value is not None and str(last_send_attempt_value).strip() != "":
        last_send_attempt_text = str(last_send_attempt_value).strip().replace("Z", "+00:00")
        try:
            _ = datetime.fromisoformat(last_send_attempt_text)
        except ValueError as exc:
            raise ValueError("lastSendAttemptAt must be ISO datetime") from exc
    else:
        last_send_attempt_text = None

    sent_by_value = _normalize_optional_text(
        payload.get("sentByUserId") or sent_by_user_id,
        field="sentByUserId",
        max_length=128,
    )
    smtp_message_id = _normalize_optional_text(payload.get("smtpMessageId"), field="smtpMessageId", max_length=500)
    last_send_error = _normalize_optional_text(payload.get("lastSendError"), field="lastSendError", max_length=8192)

    _safe_db_call(
        upsert_traffic_email,
        {
            "trafficId": normalized_traffic_id,
            "toEmails": json.dumps(to_emails),
            "ccEmails": json.dumps(cc_emails) if cc_emails else None,
            "bccEmails": json.dumps(bcc_emails) if bcc_emails else None,
            "subject": subject,
            "body": body_text,
            "sentStatus": sent_status,
            "sentAt": sent_at_text,
            "sentByUserId": sent_by_value,
            "smtpMessageId": smtp_message_id,
            "lastSendAttemptAt": last_send_attempt_text,
            "lastSendError": last_send_error,
        },
    )

    row = _safe_db_call(get_traffic_email_row, traffic_id=normalized_traffic_id)
    return _serialize_email_row(row)


def send_traffic_email_data(*, traffic_id: str, payload: dict, sent_by_user_id: str | None = None) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    traffic_row = _ensure_traffic_exists(normalized_traffic_id)
    _ensure_not_archived(traffic_row, action="send email")

    to_emails = _normalize_email_list(
        payload.get("toEmails"),
        field="toEmails",
        required=True,
    )
    cc_emails = _normalize_email_list(
        payload.get("ccEmails") or [],
        field="ccEmails",
        required=False,
    )
    bcc_emails = _normalize_email_list(
        payload.get("bccEmails") or [],
        field="bccEmails",
        required=False,
    )
    subject = _normalize_required_text(payload.get("subject"), field="subject", max_length=500)
    body_text = str(payload.get("body") or "").strip()
    if not body_text:
        raise ValueError("body is required")

    try:
        smtp_config = get_smtp_settings(required=True)
    except Exception as exc:
        raise ValueError(str(exc)) from exc
    if not smtp_config:
        raise ValueError("TradSphere SMTP config is required")

    smtp_settings = SmtpSettings(
        host=str(smtp_config["host"]),
        port=int(smtp_config["port"]),
        username=(
            str(smtp_config.get("username") or "").strip()
            or None
        ),
        password=(
            str(smtp_config.get("password") or "").strip()
            or None
        ),
        from_email=str(smtp_config["from_email"]),
        from_name=(
            str(smtp_config.get("from_name") or "").strip()
            or None
        ),
        reply_to=(
            str(smtp_config.get("reply_to") or "").strip()
            or None
        ),
        use_tls=bool(smtp_config.get("use_tls")),
        use_ssl=bool(smtp_config.get("use_ssl")),
        timeout_seconds=float(smtp_config.get("timeout_seconds") or 20.0),
    )

    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    sent_by_value = _normalize_optional_text(
        payload.get("sentByUserId") or sent_by_user_id,
        field="sentByUserId",
        max_length=128,
    )

    try:
        smtp_result = send_smtp_email(
            settings=smtp_settings,
            to_addresses=to_emails,
            cc_addresses=cc_emails,
            bcc_addresses=bcc_emails,
            subject=subject,
            text_body=_html_to_plain_text(body_text) or body_text,
            html_body=body_text if _looks_like_html(body_text) else None,
        )
    except (SmtpSendError, ValueError) as exc:
        _safe_db_call(
            upsert_traffic_email,
            {
                "trafficId": normalized_traffic_id,
                "toEmails": json.dumps(to_emails),
                "ccEmails": json.dumps(cc_emails) if cc_emails else None,
                "bccEmails": json.dumps(bcc_emails) if bcc_emails else None,
                "subject": subject,
                "body": body_text,
                "sentStatus": "failed",
                "sentAt": None,
                "sentByUserId": sent_by_value,
                "smtpMessageId": None,
                "lastSendAttemptAt": now_iso,
                "lastSendError": str(exc)[:8192],
            },
        )
        raise EmailSendError(str(exc) or "SMTP send failed") from exc

    smtp_message_id = _normalize_optional_text(
        smtp_result.get("message_id"),
        field="smtpMessageId",
        max_length=500,
    )
    _safe_db_call(
        upsert_traffic_email,
        {
            "trafficId": normalized_traffic_id,
            "toEmails": json.dumps(to_emails),
            "ccEmails": json.dumps(cc_emails) if cc_emails else None,
            "bccEmails": json.dumps(bcc_emails) if bcc_emails else None,
            "subject": subject,
            "body": body_text,
            "sentStatus": "sent",
            "sentAt": now_iso,
            "sentByUserId": sent_by_value,
            "smtpMessageId": smtp_message_id,
            "lastSendAttemptAt": now_iso,
            "lastSendError": None,
        },
    )
    row = _safe_db_call(get_traffic_email_row, traffic_id=normalized_traffic_id)
    if bool(payload.get("markSentAfterSend")):
        _safe_db_call(
            update_traffic,
            traffic_id=normalized_traffic_id,
            fields={"status": "sent"},
        )
    detail = get_traffic_detail_data(traffic_id=normalized_traffic_id)
    return {
        "trafficId": normalized_traffic_id,
        "email": _serialize_email_row(row),
        "detail": detail,
    }


def send_traffic_email_test_data(*, traffic_id: str, payload: dict) -> dict:
    normalized_traffic_id = _require_uuid4(traffic_id, field="trafficId")
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    traffic_row = _ensure_traffic_exists(normalized_traffic_id)
    _ensure_not_archived(traffic_row, action="send test email")

    to_email = _normalize_email_list(
        [payload.get("toEmail")],
        field="toEmail",
        required=True,
    )[0]
    subject = _build_test_email_subject(
        _normalize_required_text(payload.get("subject"), field="subject", max_length=500)
    )
    body_text = str(payload.get("body") or "").strip()
    if not body_text:
        raise ValueError("body is required")

    try:
        smtp_config = get_smtp_settings(required=True)
    except Exception as exc:
        raise ValueError(str(exc)) from exc
    if not smtp_config:
        raise ValueError("TradSphere SMTP config is required")

    smtp_settings = SmtpSettings(
        host=str(smtp_config["host"]),
        port=int(smtp_config["port"]),
        username=(
            str(smtp_config.get("username") or "").strip()
            or None
        ),
        password=(
            str(smtp_config.get("password") or "").strip()
            or None
        ),
        from_email=str(smtp_config["from_email"]),
        from_name=(
            str(smtp_config.get("from_name") or "").strip()
            or None
        ),
        reply_to=(
            str(smtp_config.get("reply_to") or "").strip()
            or None
        ),
        use_tls=bool(smtp_config.get("use_tls")),
        use_ssl=bool(smtp_config.get("use_ssl")),
        timeout_seconds=float(smtp_config.get("timeout_seconds") or 20.0),
    )

    test_notice_text = _build_test_email_notice_text()
    html_body = _inject_test_email_notice_html(body_text)
    text_body = f"{test_notice_text}\n\n{_html_to_plain_text(body_text) or body_text}"

    try:
        smtp_result = send_smtp_email(
            settings=smtp_settings,
            to_addresses=[to_email],
            subject=subject,
            text_body=text_body,
            html_body=html_body,
        )
    except (SmtpSendError, ValueError) as exc:
        raise EmailSendError(str(exc) or "SMTP send failed") from exc

    smtp_message_id = _normalize_optional_text(
        smtp_result.get("message_id"),
        field="smtpMessageId",
        max_length=500,
    )
    return {
        "trafficId": normalized_traffic_id,
        "testEmail": {
            "toEmail": to_email,
            "smtpMessageId": smtp_message_id,
        },
    }


def bulk_save_traffic_data(*, payload: dict, sent_by_user_id: str | None = None) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    traffic_payload = payload.get("traffic")
    if not isinstance(traffic_payload, dict):
        raise ValueError("traffic is required")

    traffic_id_raw = str(payload.get("trafficId") or "").strip()
    update_traffic = bool(payload.get("updateTraffic"))

    def _ensure_dict_items(value: object, *, field: str) -> list[dict]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError(f"{field} must be an array")
        out: list[dict] = []
        for index, item in enumerate(value):
            if not isinstance(item, dict):
                raise ValueError(f"{field}[{index}] must be an object")
            out.append(item)
        return out

    def _ensure_int_items(value: object, *, field: str) -> list[int]:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError(f"{field} must be an array")
        out: list[int] = []
        for index, item in enumerate(value):
            try:
                parsed = int(item)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{field}[{index}] must be an integer") from exc
            if parsed < 0:
                raise ValueError(f"{field}[{index}] must be an unsigned integer")
            out.append(parsed)
        return out

    flight_creates = _ensure_dict_items(payload.get("flightCreates"), field="flightCreates")
    flight_updates = _ensure_dict_items(payload.get("flightUpdates"), field="flightUpdates")
    flight_deletes = _ensure_int_items(payload.get("flightDeletes"), field="flightDeletes")
    station_creates = _ensure_dict_items(payload.get("stationCreates"), field="stationCreates")
    station_updates = _ensure_dict_items(payload.get("stationUpdates"), field="stationUpdates")
    station_deletes = _ensure_int_items(payload.get("stationDeletes"), field="stationDeletes")

    traffic_create_item: dict[str, object] | None = None
    traffic_fields: dict[str, object] = {}
    if traffic_id_raw:
        normalized_traffic_id = _require_uuid4(traffic_id_raw, field="trafficId")
        traffic_row = _ensure_traffic_exists(normalized_traffic_id)
        _ensure_not_archived(traffic_row, action="bulk save")
        if update_traffic:
            traffic_fields = {
                "campaign": _normalize_required_text(
                    traffic_payload.get("campaign"),
                    field="campaign",
                    max_length=255,
                ),
                "status": _normalize_enum(
                    traffic_payload.get("status"),
                    field="status",
                    allowed_values=_TRAFFIC_STATUSES,
                ),
                "note": _normalize_optional_note_text(traffic_payload.get("note")),
            }
    else:
        account_code = require_account_code(traffic_payload.get("accountCode"), field="accountCode")
        ensure_tradsphere_account_codes_exist([account_code])
        traffic_create_item = {
            "accountCode": account_code,
            "campaign": _normalize_required_text(
                traffic_payload.get("campaign"),
                field="campaign",
                max_length=255,
            ),
            "status": _normalize_enum(
                traffic_payload.get("status"),
                field="status",
                allowed_values=_TRAFFIC_STATUSES,
                default_value="draft",
            ),
            "note": _normalize_optional_note_text(traffic_payload.get("note")),
        }
        normalized_traffic_id = str(uuid.uuid4())

    existing_flights_rows, existing_stations_rows = run_parallel(
        tasks=[
            (_load_traffic_flights_safe, (normalized_traffic_id,)),
            (_load_traffic_stations_safe, (normalized_traffic_id,)),
        ],
        api_name="tradsphere.traffic.bulk_save.prefetch",
    )
    existing_flights_by_id = {
        int(row.get("id")): row
        for row in existing_flights_rows
        if row.get("id") is not None
    }
    existing_stations_by_id = {
        int(row.get("id")): row
        for row in existing_stations_rows
        if row.get("id") is not None
    }

    normalized_flight_deletes = sorted({int(item) for item in flight_deletes if int(item) > 0}, reverse=True)
    normalized_station_deletes = sorted({int(item) for item in station_deletes if int(item) > 0}, reverse=True)
    for flight_id in normalized_flight_deletes:
        if flight_id not in existing_flights_by_id:
            raise NotFoundError("Flight record not found for this traffic")
    for station_id in normalized_station_deletes:
        if station_id not in existing_stations_by_id:
            raise NotFoundError("Station record not found for this traffic")

    normalized_flight_creates: list[dict] = []
    for item in flight_creates:
        flight_start = _normalize_date(item.get("flightStart"), field="flightStart")
        flight_end = _normalize_date(item.get("flightEnd"), field="flightEnd")
        if date.fromisoformat(flight_start) > date.fromisoformat(flight_end):
            raise ValueError("flightStart must be on or before flightEnd")
        normalized_flight_creates.append(
            {
                "flightStart": flight_start,
                "flightEnd": flight_end,
                "medium": _normalize_flight_medium(item.get("medium")),
                "language": _normalize_flight_language(item.get("language")),
                "length": _normalize_unsigned_int(
                    item.get("length"),
                    field="length",
                    default=60,
                    max_value=65535,
                ),
                "isci": _normalize_optional_text(item.get("isci"), field="isci", max_length=128),
                "rotation": str(_normalize_rotation(item.get("rotation"))),
                "fileUrl": _normalize_optional_text(item.get("fileUrl"), field="fileUrl", max_length=2048),
                "scriptUrl": _normalize_optional_text(item.get("scriptUrl"), field="scriptUrl", max_length=2048),
                "note": _normalize_optional_note_text(item.get("note")),
            }
        )

    normalized_flight_updates: list[dict] = []
    for item in flight_updates:
        flight_id_value = item.get("id")
        if flight_id_value is None:
            raise ValueError("flightUpdates[].id is required")
        try:
            flight_id = int(flight_id_value)
        except (TypeError, ValueError) as exc:
            raise ValueError("flightUpdates[].id must be an unsigned integer") from exc
        if flight_id < 0:
            raise ValueError("flightUpdates[].id must be an unsigned integer")
        existing_row = existing_flights_by_id.get(flight_id)
        if existing_row is None:
            raise NotFoundError("Flight record not found for this traffic")

        fields: dict[str, object] = {}
        if "flightStart" in item:
            fields["flightStart"] = _normalize_date(item.get("flightStart"), field="flightStart")
        if "flightEnd" in item:
            fields["flightEnd"] = _normalize_date(item.get("flightEnd"), field="flightEnd")

        resolved_start = fields.get("flightStart") or _to_iso_text(existing_row.get("flightStart"))
        resolved_end = fields.get("flightEnd") or _to_iso_text(existing_row.get("flightEnd"))
        if resolved_start and resolved_end and date.fromisoformat(str(resolved_start)) > date.fromisoformat(str(resolved_end)):
            raise ValueError("flightStart must be on or before flightEnd")

        if "medium" in item:
            fields["medium"] = _normalize_flight_medium(item.get("medium"))
        if "language" in item:
            fields["language"] = _normalize_flight_language(item.get("language"))
        if "length" in item:
            fields["length"] = _normalize_unsigned_int(
                item.get("length"),
                field="length",
                default=60,
                max_value=65535,
            )
        if "isci" in item:
            fields["isci"] = _normalize_optional_text(item.get("isci"), field="isci", max_length=128)
        if "rotation" in item:
            fields["rotation"] = str(_normalize_rotation(item.get("rotation")))
        if "fileUrl" in item:
            fields["fileUrl"] = _normalize_optional_text(item.get("fileUrl"), field="fileUrl", max_length=2048)
        if "scriptUrl" in item:
            fields["scriptUrl"] = _normalize_optional_text(item.get("scriptUrl"), field="scriptUrl", max_length=2048)
        if "note" in item:
            fields["note"] = _normalize_optional_note_text(item.get("note"))
        if not fields:
            continue
        normalized_flight_updates.append(
            {
                "id": flight_id,
                "fields": fields,
            }
        )

    station_codes_to_validate: list[str] = []
    for item in station_creates:
        station_codes_to_validate.append(
            require_account_code(item.get("stationCode"), field="stationCode")
        )
    for item in station_updates:
        if "stationCode" not in item:
            continue
        station_codes_to_validate.append(
            require_account_code(item.get("stationCode"), field="stationCode")
        )
    if station_codes_to_validate:
        ensure_station_codes_exist(station_codes_to_validate)

    normalized_station_creates: list[dict] = []
    for item in station_creates:
        station_code = require_account_code(item.get("stationCode"), field="stationCode")
        normalized_station_creates.append(
            {
                "stationCode": station_code,
                "contactsSnapshot": _normalize_contacts_snapshot(item.get("contactsSnapshot")),
                "deliveryMethod": _normalize_optional_text(item.get("deliveryMethod"), field="deliveryMethod", max_length=128),
                "deliveryStatus": _normalize_enum(
                    item.get("deliveryStatus"),
                    field="deliveryStatus",
                    allowed_values=_DELIVERY_STATUS_VALUES,
                    default_value="",
                    allow_blank=True,
                ),
                "confirmedStatus": _normalize_enum(
                    item.get("confirmedStatus"),
                    field="confirmedStatus",
                    allowed_values=_CONFIRMED_STATUS_VALUES,
                    default_value="",
                    allow_blank=True,
                ),
                "note": _normalize_optional_note_text(item.get("note")),
            }
        )

    normalized_station_updates: list[dict] = []
    for item in station_updates:
        station_id_value = item.get("id")
        if station_id_value is None:
            raise ValueError("stationUpdates[].id is required")
        try:
            station_id = int(station_id_value)
        except (TypeError, ValueError) as exc:
            raise ValueError("stationUpdates[].id must be an unsigned integer") from exc
        if station_id < 0:
            raise ValueError("stationUpdates[].id must be an unsigned integer")
        if station_id not in existing_stations_by_id:
            raise NotFoundError("Station record not found for this traffic")

        fields: dict[str, object] = {}
        if "stationCode" in item:
            station_code = require_account_code(item.get("stationCode"), field="stationCode")
            fields["stationCode"] = station_code
        if "contactsSnapshot" in item:
            fields["contactsSnapshot"] = _normalize_contacts_snapshot(item.get("contactsSnapshot"))
        if "deliveryMethod" in item:
            fields["deliveryMethod"] = _normalize_optional_text(
                item.get("deliveryMethod"),
                field="deliveryMethod",
                max_length=128,
            )
        if "deliveryStatus" in item:
            fields["deliveryStatus"] = _normalize_enum(
                item.get("deliveryStatus"),
                field="deliveryStatus",
                allowed_values=_DELIVERY_STATUS_VALUES,
                default_value="",
                allow_blank=True,
            )
        if "confirmedStatus" in item:
            fields["confirmedStatus"] = _normalize_enum(
                item.get("confirmedStatus"),
                field="confirmedStatus",
                allowed_values=_CONFIRMED_STATUS_VALUES,
                default_value="",
                allow_blank=True,
            )
        if "note" in item:
            fields["note"] = _normalize_optional_note_text(item.get("note"))
        if not fields:
            continue
        normalized_station_updates.append(
            {
                "id": station_id,
                "fields": fields,
            }
        )

    email_item: dict | None = None
    if bool(payload.get("upsertEmail")):
        email_payload = payload.get("email")
        if not isinstance(email_payload, dict):
            raise ValueError("email is required when upsertEmail=true")

        sent_status = _normalize_enum(
            email_payload.get("sentStatus"),
            field="sentStatus",
            allowed_values=_EMAIL_SENT_STATUS_VALUES,
            default_value="draft",
        )
        require_ready_fields = sent_status == "ready"
        to_emails = _normalize_email_list(
            email_payload.get("toEmails"),
            field="toEmails",
            required=require_ready_fields,
        )
        cc_emails = _normalize_email_list(
            email_payload.get("ccEmails") or [],
            field="ccEmails",
            required=False,
        )
        bcc_emails = _normalize_email_list(
            email_payload.get("bccEmails") or [],
            field="bccEmails",
            required=False,
        )
        subject = _normalize_optional_text(email_payload.get("subject"), field="subject", max_length=500)
        body_text = str(email_payload.get("body") or "").strip()
        if require_ready_fields:
            if not subject:
                raise ValueError("subject is required when sentStatus is ready")
            if not body_text:
                raise ValueError("body is required when sentStatus is ready")
        if not subject:
            subject = "(draft)"
        if not body_text:
            body_text = ""

        sent_at_value = email_payload.get("sentAt")
        if sent_at_value is not None and str(sent_at_value).strip() != "":
            sent_at_text = str(sent_at_value).strip().replace("Z", "+00:00")
            try:
                _ = datetime.fromisoformat(sent_at_text)
            except ValueError as exc:
                raise ValueError("sentAt must be ISO datetime") from exc
        else:
            sent_at_text = None

        last_send_attempt_value = email_payload.get("lastSendAttemptAt")
        if last_send_attempt_value is not None and str(last_send_attempt_value).strip() != "":
            last_send_attempt_text = str(last_send_attempt_value).strip().replace("Z", "+00:00")
            try:
                _ = datetime.fromisoformat(last_send_attempt_text)
            except ValueError as exc:
                raise ValueError("lastSendAttemptAt must be ISO datetime") from exc
        else:
            last_send_attempt_text = None

        sent_by_value = _normalize_optional_text(
            email_payload.get("sentByUserId") or sent_by_user_id,
            field="sentByUserId",
            max_length=128,
        )
        smtp_message_id = _normalize_optional_text(email_payload.get("smtpMessageId"), field="smtpMessageId", max_length=500)
        last_send_error = _normalize_optional_text(email_payload.get("lastSendError"), field="lastSendError", max_length=8192)

        email_item = {
            "trafficId": normalized_traffic_id,
            "toEmails": json.dumps(to_emails),
            "ccEmails": json.dumps(cc_emails) if cc_emails else None,
            "bccEmails": json.dumps(bcc_emails) if bcc_emails else None,
            "subject": subject,
            "body": body_text,
            "sentStatus": sent_status,
            "sentAt": sent_at_text,
            "sentByUserId": sent_by_value,
            "smtpMessageId": smtp_message_id,
            "lastSendAttemptAt": last_send_attempt_text,
            "lastSendError": last_send_error,
        }

    _safe_db_call(
        save_traffic_bulk_changes,
        traffic_id=normalized_traffic_id,
        traffic_create_item=traffic_create_item,
        traffic_fields=traffic_fields if update_traffic else {},
        flight_creates=normalized_flight_creates,
        flight_updates=normalized_flight_updates,
        flight_deletes=normalized_flight_deletes,
        station_creates=normalized_station_creates,
        station_updates=normalized_station_updates,
        station_deletes=normalized_station_deletes,
        email_item=email_item,
    )

    detail = get_traffic_detail_data(traffic_id=normalized_traffic_id)
    return {
        "trafficId": normalized_traffic_id,
        "detail": detail,
        "trafficListItem": _build_traffic_list_item_from_detail(detail),
    }
