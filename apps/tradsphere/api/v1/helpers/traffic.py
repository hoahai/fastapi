from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import uuid

import mysql.connector

from apps.tradsphere.api.v1.helpers.accountValidation import (
    ensure_station_codes_exist,
    ensure_tradsphere_account_codes_exist,
    require_account_code,
)
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
    update_traffic,
    update_traffic_flight,
    update_traffic_station,
    upsert_traffic_email,
)
from apps.tradsphere.api.v1.helpers.stations import list_stations_data
from shared.tenant import get_tenant_id


_TRAFFIC_STATUSES = {"draft", "ready", "sent", "confirmed", "archived"}
_FLIGHT_MEDIA_VALUES = {"TV", "RA", "CA", "OD", "NP", "CINE"}
_DELIVERY_STATUS_VALUES = {
    "not_started",
    "needs_manual_upload",
    "ready_to_email",
    "sent",
    "skipped",
    "issue",
}
_CONFIRMED_STATUS_VALUES = {"pending", "confirmed", "issue", "not_required"}
_EMAIL_SENT_STATUS_VALUES = {"draft", "ready", "sent", "failed"}

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
) -> str:
    text = str(value or "").strip()
    if not text:
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


def _ensure_traffic_exists(traffic_id: str) -> dict:
    traffic_row = _safe_db_call(get_traffic_row, traffic_id=traffic_id)
    if not traffic_row:
        raise NotFoundError("Traffic record not found")
    return traffic_row


def _ensure_not_archived(traffic_row: dict, *, action: str) -> None:
    status = str(traffic_row.get("status") or "").strip().lower()
    if status == "archived":
        raise ValueError(f"Archived traffic cannot be modified ({action})")


def _build_detail_payload(*, traffic_row: dict) -> dict:
    traffic_id = str(traffic_row.get("id") or "").strip()
    flights_rows = _safe_db_call(list_traffic_flights, traffic_id=traffic_id)
    stations_rows = _safe_db_call(list_traffic_stations, traffic_id=traffic_id)
    email_row = _safe_db_call(get_traffic_email_row, traffic_id=traffic_id)

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
        out.append(
            {
                **_serialize_traffic_row(row),
                "flightCount": int(row.get("flightCount") or 0),
                "stationCount": int(row.get("stationCount") or 0),
                "emailSentStatus": row.get("emailSentStatus"),
                "emailSentAt": _to_iso_text(row.get("emailSentAt")),
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
) -> dict:
    normalized_account_code = require_account_code(account_code, field="accountCode")
    ensure_tradsphere_account_codes_exist([normalized_account_code])
    normalized_flight_start = _normalize_date(flight_start, field="flightStart")
    normalized_flight_end = _normalize_date(flight_end, field="flightEnd")
    if date.fromisoformat(normalized_flight_start) > date.fromisoformat(normalized_flight_end):
        raise ValueError("flightStart must be on or before flightEnd")

    rows = _safe_db_call(
        list_schedule_station_candidates_for_account_range,
        account_code=normalized_account_code,
        flight_start=normalized_flight_start,
        flight_end=normalized_flight_end,
    )

    station_codes: list[str] = []
    stations: list[dict] = []
    seen_codes: set[str] = set()
    for row in rows:
        station_code = str(row.get("stationCode") or "").strip().upper()
        if not station_code or station_code in seen_codes:
            continue
        seen_codes.add(station_code)
        station_codes.append(station_code)
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

    return {
        "accountCode": normalized_account_code,
        "flightStart": normalized_flight_start,
        "flightEnd": normalized_flight_end,
        "stations": stations,
        "summary": {
            "candidateCount": len(stations),
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
    note = _normalize_optional_text(payload.get("note"), field="note", max_length=2048)

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
        fields["note"] = _normalize_optional_text(payload.get("note"), field="note", max_length=2048)

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
    length = _normalize_unsigned_int(
        payload.get("length"),
        field="length",
        default=60,
        max_value=65535,
    )
    isci = _normalize_optional_text(payload.get("isci"), field="isci", max_length=128)
    rotation = _normalize_rotation(payload.get("rotation"))
    file_url = _normalize_required_text(payload.get("fileUrl"), field="fileUrl", max_length=2048)
    script_url = _normalize_optional_text(payload.get("scriptUrl"), field="scriptUrl", max_length=2048)
    note = _normalize_optional_text(payload.get("note"), field="note", max_length=2048)

    created_flight_id = _safe_db_call(
        insert_traffic_flight,
        {
            "trafficId": normalized_traffic_id,
            "flightStart": flight_start,
            "flightEnd": flight_end,
            "medium": medium,
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
        fields["fileUrl"] = _normalize_required_text(payload.get("fileUrl"), field="fileUrl", max_length=2048)
    if "scriptUrl" in payload:
        fields["scriptUrl"] = _normalize_optional_text(payload.get("scriptUrl"), field="scriptUrl", max_length=2048)
    if "note" in payload:
        fields["note"] = _normalize_optional_text(payload.get("note"), field="note", max_length=2048)

    if not fields:
        raise ValueError(
            "At least one updatable field is required: flightStart, flightEnd, medium, length, isci, rotation, fileUrl, scriptUrl, note"
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
        default_value="not_started",
    )
    confirmed_status = _normalize_enum(
        payload.get("confirmedStatus"),
        field="confirmedStatus",
        allowed_values=_CONFIRMED_STATUS_VALUES,
        default_value="pending",
    )
    note = _normalize_optional_text(payload.get("note"), field="note", max_length=2048)

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
        )
    if "confirmedStatus" in payload:
        fields["confirmedStatus"] = _normalize_enum(
            payload.get("confirmedStatus"),
            field="confirmedStatus",
            allowed_values=_CONFIRMED_STATUS_VALUES,
        )
    if "note" in payload:
        fields["note"] = _normalize_optional_text(payload.get("note"), field="note", max_length=2048)

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
