from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
import json
import re
import uuid

import mysql.connector

from apps.tradsphere.api.v1.helpers.accounts import list_accounts
from apps.tradsphere.api.v1.helpers.accountValidation import (
    ensure_est_nums_exist,
    ensure_master_account_codes_exist,
    ensure_station_codes_exist,
)
from apps.tradsphere.api.v1.helpers.stations import build_rep_contact_full_name, list_stations_data
from apps.tradsphere.api.v1.helpers.dbQueries import (
    count_inv_note_attachments,
    delete_inv_checklist,
    delete_inv_checklist_note,
    delete_inv_checklist_station,
    delete_inv_note_attachment,
    invalidate_inv_checklist_related_cache_for_bulk_write,
    list_inv_checklist_note_detail_rows,
    list_inv_checklist_notes,
    list_inv_checklist_note_rows_by_ids,
    list_inv_checklist_stations,
    list_inv_checklist_station_rows_by_ids,
    list_inv_checklist_station_rows_for_checklists,
    list_inv_checklist_station_search_rows,
    list_inv_checklist_rows_by_ids,
    get_inv_checklist_detail_rows,
    get_inv_checklist_note_row,
    get_inv_checklist_row,
    get_inv_checklist_station_row,
    get_inv_note_attachment_row,
    insert_inv_checklist,
    insert_inv_checklists_for_sync,
    insert_inv_checklist_note,
    insert_inv_checklist_station,
    insert_inv_checklist_stations_for_sync,
    insert_inv_note_attachment,
    list_inv_checklists,
    list_inv_note_attachments,
    list_schedule_invoice_checklist_expected_rows,
    save_inv_checklist_bulk_changes,
    soft_delete_app_attachments_for_invoice_note,
    update_inv_checklist,
    update_inv_checklist_note,
    update_inv_checklist_station,
    update_inv_note_attachment,
)
from shared.storage import (
    DeleteAssetInput,
    StorageUploadInput,
    delete_file,
    get_note_attachment_limit,
    upload_file,
    validate_attachment_payload,
)
from shared.tenant import get_tenant_id

_MAX_YEAR = 2155
_MIN_YEAR = 1901
_MAX_NOTE_AMOUNT = Decimal("9999999999.99")
_CENTS = Decimal("0.01")
_MONTH_LABELS = (
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)


class ConflictError(ValueError):
    pass


class NotFoundError(ValueError):
    pass


class InvalidReferenceError(ValueError):
    pass


class SafeDatabaseError(RuntimeError):
    pass


def _period_value(year: int, month: int) -> str:
    return f"{int(year):04d}-{int(month):02d}"


def _period_label(year: int, month: int) -> str:
    if 1 <= int(month) <= 12:
        return f"{_MONTH_LABELS[int(month)]} {int(year)}"
    return _period_value(year, month)


def _quarter_for_month(month: int | None) -> int | None:
    if month is None:
        return None
    parsed = int(month)
    if parsed < 1 or parsed > 12:
        return None
    return ((parsed - 1) // 3) + 1


def _ensure_required_text(
    value: object,
    *,
    field: str,
    max_length: int,
    uppercase: bool = False,
) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    if len(text) > max_length:
        raise ValueError(f"{field} must be <= {max_length} characters")
    return text.upper() if uppercase else text


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


def _ensure_required_year(value: object) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("year is required") from exc
    if parsed < _MIN_YEAR or parsed > _MAX_YEAR:
        raise ValueError(f"year must be between {_MIN_YEAR} and {_MAX_YEAR}")
    return parsed


def _ensure_required_month(value: object) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("month is required") from exc
    if parsed < 1 or parsed > 12:
        raise ValueError("month must be between 1 and 12")
    return parsed


def _ensure_required_unsigned_int(value: object, *, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} is required") from exc
    if parsed < 0:
        raise ValueError(f"{field} must be an unsigned integer")
    return parsed


def _ensure_optional_unsigned_int(value: object, *, field: str) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    parsed = _ensure_required_unsigned_int(value, field=field)
    return parsed


def _ensure_optional_amount(value: object) -> Decimal | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        amount = Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("amount must be a valid decimal number") from exc

    if not amount.is_finite():
        raise ValueError("amount must be a finite decimal number")
    if abs(amount) > _MAX_NOTE_AMOUNT:
        raise ValueError("amount must be within DECIMAL(12,2) range")

    quantized = amount.quantize(_CENTS)
    if quantized != amount:
        raise ValueError("amount must have at most 2 decimal places")
    return quantized


def _has_note_or_amount(*, note_value: object, amount_value: object) -> bool:
    if amount_value is not None:
        return True
    note_text = str(note_value or "").strip()
    return bool(note_text)


def _extract_db_error_message(exc: Exception) -> str:
    err_no = int(getattr(exc, "errno", 0) or 0)
    raw_message = str(exc).lower()
    if err_no == 1062:
        if "uq_tradsphere_invchecklist_account_year_month" in raw_message:
            return "Checklist already exists for this accountCode, year, and month"
        if "uq_tradsphere_invcheckliststation_checklist_estnum_station" in raw_message:
            return "Checklist station already exists for this checklistId, estNum, and stationCode"
        return "Duplicate record"
    if err_no in {1451, 1452}:
        return "Related parent/child record is missing or constrained"
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
    return SafeDatabaseError(friendly)


def _safe_db_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        mapped = _map_db_exception(exc)
        if isinstance(mapped, (ConflictError, InvalidReferenceError)):
            raise mapped from exc
        raise SafeDatabaseError("Database operation failed") from exc


def _serialize_datetime_fields(row: dict) -> dict:
    out = dict(row)
    for key in ("dateCreated", "dateUpdated"):
        out[key] = _to_iso_text(out.get(key))
    return out


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


def _normalized_text_values(values: list[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return sorted(normalized)


def _normalized_int_values(values: list[int]) -> list[int]:
    seen: set[int] = set()
    normalized: list[int] = []
    for value in values:
        parsed = int(value)
        if parsed in seen:
            continue
        seen.add(parsed)
        normalized.append(parsed)
    return sorted(normalized)


def _sanitize_storage_token(value: object) -> str:
    text = str(value or "").strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "", text)
    return normalized or "na"


def _build_invoice_note_storage_key(
    *,
    note_id: int,
    est_num: int | None,
    station_code: str | None,
    counter: int,
) -> str:
    station_token = _sanitize_storage_token(station_code)
    est_num_token = str(int(est_num)) if est_num is not None else "0"
    return f"{int(note_id)}_{est_num_token}_{station_token}_{int(counter)}"


def _build_stored_attachment_file_name(
    *,
    provider_public_id: str | None,
    provider_metadata: dict[str, object] | None,
    mime_type: str | None,
    fallback_name: str,
) -> str:
    fallback = str(fallback_name or "").strip() or "attachment"
    public_id = str(provider_public_id or "").strip()
    base_name = public_id.split("/")[-1] if public_id else ""
    base_name = base_name.strip()
    if not base_name:
        return fallback
    if "." in base_name:
        return base_name

    extension = ""
    if isinstance(provider_metadata, dict):
        extension = str(provider_metadata.get("format") or "").strip().lower()
    if not extension:
        normalized_mime = str(mime_type or "").strip().lower()
        if normalized_mime.startswith("image/"):
            extension = normalized_mime.split("/", 1)[1].strip()
    if extension:
        return f"{base_name}.{extension}"
    return base_name


def _normalize_checklist_payload(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    account_code = _ensure_required_text(
        payload.get("accountCode"),
        field="accountCode",
        max_length=10,
        uppercase=True,
    )
    ensure_master_account_codes_exist([account_code])
    return {
        "id": str(uuid.uuid4()),
        "accountCode": account_code,
        "year": _ensure_required_year(payload.get("year")),
        "month": _ensure_required_month(payload.get("month")),
        "status": _ensure_optional_text(payload.get("status"), field="status", max_length=32),
        "note": _ensure_optional_text(payload.get("note"), field="note", max_length=2048),
    }


def _normalize_checklist_updates(payload: dict) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    updates: dict[str, object] = {}
    if "accountCode" in payload:
        account_code = _ensure_required_text(
            payload.get("accountCode"),
            field="accountCode",
            max_length=10,
            uppercase=True,
        )
        ensure_master_account_codes_exist([account_code])
        updates["accountCode"] = account_code
    if "year" in payload:
        updates["year"] = _ensure_required_year(payload.get("year"))
    if "month" in payload:
        updates["month"] = _ensure_required_month(payload.get("month"))
    if "status" in payload:
        updates["status"] = _ensure_optional_text(payload.get("status"), field="status", max_length=32)
    if "note" in payload:
        updates["note"] = _ensure_optional_text(payload.get("note"), field="note", max_length=2048)
    if not updates:
        raise ValueError("At least one updatable field is required")
    return updates


def _normalize_station_payload(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    checklist_id_text = str(payload.get("checklistId") or "").strip()
    if not checklist_id_text:
        raise ValueError("checklistId is required")
    if _safe_db_call(get_inv_checklist_row, checklist_id=checklist_id_text) is None:
        raise NotFoundError(f"Checklist not found: {checklist_id_text}")

    est_num = _ensure_required_unsigned_int(payload.get("estNum"), field="estNum")
    ensure_est_nums_exist([est_num])

    station_code = _ensure_required_text(
        payload.get("stationCode"),
        field="stationCode",
        max_length=10,
        uppercase=True,
    )
    ensure_station_codes_exist([station_code])

    return {
        "checklistId": checklist_id_text,
        "estNum": est_num,
        "stationCode": station_code,
        "status": _ensure_optional_text(payload.get("status"), field="status", max_length=32),
    }


def _normalize_station_updates(payload: dict) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    updates: dict[str, object] = {}
    if "estNum" in payload:
        est_num = _ensure_required_unsigned_int(payload.get("estNum"), field="estNum")
        ensure_est_nums_exist([est_num])
        updates["estNum"] = est_num
    if "stationCode" in payload:
        station_code = _ensure_required_text(
            payload.get("stationCode"),
            field="stationCode",
            max_length=10,
            uppercase=True,
        )
        ensure_station_codes_exist([station_code])
        updates["stationCode"] = station_code
    if "status" in payload:
        updates["status"] = _ensure_optional_text(payload.get("status"), field="status", max_length=32)
    if not updates:
        raise ValueError("At least one updatable field is required")
    return updates


def _normalize_note_payload(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    station_row_id = _ensure_required_unsigned_int(
        payload.get("checklistStationId"),
        field="checklistStationId",
    )
    station_row = _safe_db_call(get_inv_checklist_station_row, station_row_id=station_row_id)
    if station_row is None:
        raise NotFoundError(f"Checklist station not found: {station_row_id}")

    amount_value = _ensure_optional_amount(payload.get("amount"))
    note_value = _ensure_optional_text(payload.get("note"), field="note", max_length=2048)
    if not _has_note_or_amount(note_value=note_value, amount_value=amount_value):
        raise ValueError("At least one of amount or note is required")

    return {
        "checklistStationId": int(station_row_id),
        "amount": amount_value,
        "note": note_value,
    }


def _normalize_note_updates(payload: dict) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    updates: dict[str, object] = {}
    if "amount" in payload:
        updates["amount"] = _ensure_optional_amount(payload.get("amount"))
    if "note" in payload:
        updates["note"] = _ensure_optional_text(payload.get("note"), field="note", max_length=2048)
    if not updates:
        raise ValueError("At least one updatable field is required")
    return updates


def _normalize_attachment_payload(payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    note_id = _ensure_required_unsigned_int(payload.get("noteId"), field="noteId")
    note_row = _safe_db_call(get_inv_checklist_note_row, note_id=note_id)
    if note_row is None:
        raise NotFoundError(f"Checklist note not found: {note_id}")

    return {
        "noteId": int(note_id),
        "url": _ensure_required_text(payload.get("url"), field="url", max_length=2048),
        "fileName": _ensure_optional_text(payload.get("fileName"), field="fileName", max_length=255),
        "fileType": _ensure_optional_text(payload.get("fileType"), field="fileType", max_length=100),
    }


def _normalize_attachment_updates(payload: dict) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    updates: dict[str, object] = {}
    if "url" in payload:
        updates["url"] = _ensure_required_text(payload.get("url"), field="url", max_length=2048)
    if "fileName" in payload:
        updates["fileName"] = _ensure_optional_text(payload.get("fileName"), field="fileName", max_length=255)
    if "fileType" in payload:
        updates["fileType"] = _ensure_optional_text(payload.get("fileType"), field="fileType", max_length=100)
    if not updates:
        raise ValueError("At least one updatable field is required")
    return updates


def _build_attachment_view_url(*, attachment_id: int) -> str:
    return f"/api/tradsphere/v1/invoice-note-attachments/open?attachmentId={int(attachment_id)}"


def _serialize_attachment_row(row: dict) -> dict:
    attachment_id = int(row.get("attachmentId"))
    note_id = row.get("attachmentNoteId")
    parsed_note_id = int(note_id) if note_id is not None else int(row.get("noteId"))
    file_name = row.get("attachmentOriginalFileName")
    if file_name is None:
        file_name = row.get("attachmentFileName")
    mime_type = row.get("attachmentMimeType")
    if mime_type is None:
        mime_type = row.get("attachmentFileType")
    file_size_value = row.get("attachmentFileSize")
    try:
        file_size = int(file_size_value) if file_size_value is not None else None
    except (TypeError, ValueError):
        file_size = None
    provider_metadata_raw = row.get("attachmentProviderMetadata")
    provider_metadata: dict | list | None
    if isinstance(provider_metadata_raw, (dict, list)):
        provider_metadata = provider_metadata_raw
    elif isinstance(provider_metadata_raw, str) and provider_metadata_raw.strip():
        try:
            parsed_metadata = json.loads(provider_metadata_raw)
            provider_metadata = parsed_metadata if isinstance(parsed_metadata, (dict, list)) else None
        except Exception:
            provider_metadata = None
    else:
        provider_metadata = None

    storage_key = row.get("attachmentStorageKey")
    provider_public_id = row.get("attachmentProviderPublicId") or storage_key
    provider_resource_type = row.get("attachmentProviderResourceType")
    if provider_resource_type is None and isinstance(provider_metadata, dict):
        provider_resource_type = provider_metadata.get("resource_type")

    output = {
        "id": attachment_id,
        "noteId": parsed_note_id,
        "url": _build_attachment_view_url(attachment_id=attachment_id),
        "fileName": file_name,
        "fileType": mime_type,
        "mimeType": mime_type,
        "fileSize": file_size,
        "storageProvider": row.get("attachmentStorageProvider"),
        "storageKey": storage_key,
        "providerMetadata": provider_metadata,
        "providerAssetId": row.get("attachmentProviderAssetId"),
        "providerPublicId": provider_public_id,
        "providerResourceType": provider_resource_type,
        "accessUrl": row.get("attachmentAccessUrl"),
        "uploadedBy": row.get("attachmentUploadedBy"),
        "tenantSlug": row.get("attachmentTenantSlug"),
        "ownerEntityType": row.get("attachmentOwnerEntityType"),
        "ownerEntityId": row.get("attachmentOwnerEntityId"),
        "source": row.get("attachmentSource"),
        "dateCreated": _to_iso_text(row.get("attachmentDateCreated")),
        "dateUpdated": _to_iso_text(row.get("attachmentDateUpdated")),
    }
    return output


def _resolve_attachment_delete_payload(row: dict) -> tuple[str | None, str | None, str | None, str | None]:
    storage_provider = str(row.get("attachmentStorageProvider") or "").strip().lower() or None
    storage_key = str(
        row.get("attachmentStorageKey")
        or row.get("attachmentProviderPublicId")
        or ""
    ).strip() or None
    provider_resource_type = str(row.get("attachmentProviderResourceType") or "").strip() or None

    provider_metadata_raw = row.get("attachmentProviderMetadata")
    if provider_resource_type is None and isinstance(provider_metadata_raw, dict):
        provider_resource_type = str(provider_metadata_raw.get("resource_type") or "").strip() or None
    elif provider_resource_type is None and isinstance(provider_metadata_raw, str) and provider_metadata_raw.strip():
        try:
            provider_metadata = json.loads(provider_metadata_raw)
            if isinstance(provider_metadata, dict):
                provider_resource_type = str(provider_metadata.get("resource_type") or "").strip() or None
        except Exception:
            provider_resource_type = None

    mime_type = str(
        row.get("attachmentMimeType")
        or row.get("attachmentFileType")
        or ""
    ).strip() or None

    return storage_provider, storage_key, provider_resource_type, mime_type


def _serialize_note_row(row: dict) -> dict:
    out = _serialize_datetime_fields(row)
    amount_value = out.get("amount")
    out["amount"] = float(amount_value) if amount_value is not None else None
    return out


def _build_checklist_detail(
    rows: list[dict],
    *,
    include_stations: bool,
    include_notes: bool,
    include_attachments: bool,
) -> dict:
    if not rows:
        raise NotFoundError("Checklist not found")

    include_stations_value = bool(include_stations or include_notes or include_attachments)
    include_notes_value = bool(include_notes or include_attachments)
    include_attachments_value = bool(include_attachments)

    first = rows[0]
    checklist = {
        "id": first.get("checklistId"),
        "accountCode": first.get("checklistAccountCode"),
        "year": int(first.get("checklistYear")),
        "month": int(first.get("checklistMonth")),
        "status": first.get("checklistStatus"),
        "note": first.get("checklistNote"),
        "dateCreated": _to_iso_text(first.get("checklistDateCreated")),
        "dateUpdated": _to_iso_text(first.get("checklistDateUpdated")),
    }
    if not include_stations_value:
        return checklist

    checklist["stations"] = []
    stations_by_id: dict[int, dict] = {}
    notes_by_id: dict[int, dict] = {}

    for row in rows:
        station_row_id = row.get("stationRowId")
        if station_row_id is None:
            continue
        station_id = int(station_row_id)
        station_entry = stations_by_id.get(station_id)
        if station_entry is None:
            station_entry = {
                "id": station_id,
                "checklistId": row.get("checklistId"),
                "estNum": int(row.get("stationEstNum")),
                "stationCode": row.get("stationCode"),
                "status": row.get("stationStatus"),
                "dateCreated": _to_iso_text(row.get("stationDateCreated")),
                "dateUpdated": _to_iso_text(row.get("stationDateUpdated")),
            }
            if include_notes_value:
                station_entry["notes"] = []
            stations_by_id[station_id] = station_entry
            checklist["stations"].append(station_entry)

        if not include_notes_value:
            continue

        note_row_id = row.get("noteId")
        if note_row_id is None:
            continue
        note_id = int(note_row_id)
        note_entry = notes_by_id.get(note_id)
        if note_entry is None:
            amount_value = row.get("noteAmount")
            note_entry = {
                "id": note_id,
                "checklistStationId": station_id,
                "amount": float(amount_value) if amount_value is not None else None,
                "note": row.get("noteText"),
                "dateCreated": _to_iso_text(row.get("noteDateCreated")),
                "dateUpdated": _to_iso_text(row.get("noteDateUpdated")),
            }
            if include_attachments_value:
                note_entry["attachments"] = []
            notes_by_id[note_id] = note_entry
            station_entry["notes"].append(note_entry)

    if include_attachments_value and notes_by_id:
        tenant_slug = str(get_tenant_id() or "").strip().lower() or None
        for note_id, note_entry in notes_by_id.items():
            attachment_rows = _safe_db_call(
                list_inv_note_attachments,
                note_id=int(note_id),
                tenant_slug=tenant_slug,
            )
            note_entry["attachments"] = [
                _serialize_attachment_row(attachment_row)
                for attachment_row in attachment_rows
                if attachment_row.get("attachmentId") is not None
            ]

    return checklist


def list_invoice_checklists_data(
    *,
    account_code: str | None = None,
    year: int | None = None,
    month: int | None = None,
    status: str | None = None,
) -> list[dict]:
    normalized_account = None
    if account_code is not None:
        normalized_account = _ensure_required_text(
            account_code,
            field="accountCode",
            max_length=10,
            uppercase=True,
        )
    normalized_year = _ensure_optional_unsigned_int(year, field="year")
    if normalized_year is not None and (normalized_year < _MIN_YEAR or normalized_year > _MAX_YEAR):
        raise ValueError(f"year must be between {_MIN_YEAR} and {_MAX_YEAR}")

    normalized_month = _ensure_optional_unsigned_int(month, field="month")
    if normalized_month is not None and (normalized_month < 1 or normalized_month > 12):
        raise ValueError("month must be between 1 and 12")

    normalized_status = None
    if status is not None:
        normalized_status = _ensure_optional_text(status, field="status", max_length=32)

    rows = _safe_db_call(
        list_inv_checklists,
        account_code=normalized_account,
        year=normalized_year,
        month=normalized_month,
        status=normalized_status,
    )
    return [_serialize_datetime_fields(row) for row in rows]


def get_invoice_checklists_data(
    *,
    checklist_id: str | None = None,
    account_code: str | None = None,
    year: int | None = None,
    month: int | None = None,
    status: str | None = None,
    include_stations: bool = False,
    include_notes: bool = False,
    include_attachments: bool = False,
) -> dict | list[dict]:
    checklist_id_text = str(checklist_id or "").strip()
    if not checklist_id_text:
        if include_stations or include_notes or include_attachments:
            raise ValueError(
                "includeStations/includeNotes/includeAttachments require checklistId"
            )
        return list_invoice_checklists_data(
            account_code=account_code,
            year=year,
            month=month,
            status=status,
        )

    if include_stations or include_notes or include_attachments:
        rows = _safe_db_call(get_inv_checklist_detail_rows, checklist_id=checklist_id_text)
        if not rows:
            raise NotFoundError(f"Checklist not found: {checklist_id_text}")
        return _build_checklist_detail(
            rows,
            include_stations=include_stations,
            include_notes=include_notes,
            include_attachments=include_attachments,
        )

    row = _safe_db_call(get_inv_checklist_row, checklist_id=checklist_id_text)
    if row is None:
        raise NotFoundError(f"Checklist not found: {checklist_id_text}")
    return _serialize_datetime_fields(row)


def create_invoice_checklist_data(payload: dict) -> dict:
    normalized = _normalize_checklist_payload(payload)
    try:
        insert_inv_checklist(normalized)
    except Exception as exc:
        raise _map_db_exception(exc) from exc

    row = _safe_db_call(get_inv_checklist_row, checklist_id=normalized["id"])
    if row is None:
        raise SafeDatabaseError("Failed to read created checklist")
    return _serialize_datetime_fields(row)


def update_invoice_checklist_data(*, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    checklist_id_text = str(payload.get("checklistId") or "").strip()
    if not checklist_id_text:
        raise ValueError("checklistId is required")
    if _safe_db_call(get_inv_checklist_row, checklist_id=checklist_id_text) is None:
        raise NotFoundError(f"Checklist not found: {checklist_id_text}")

    updates = _normalize_checklist_updates(payload)
    try:
        update_inv_checklist(checklist_id=checklist_id_text, fields=updates)
    except Exception as exc:
        raise _map_db_exception(exc) from exc

    row = _safe_db_call(get_inv_checklist_row, checklist_id=checklist_id_text)
    if row is None:
        raise NotFoundError(f"Checklist not found: {checklist_id_text}")
    return _serialize_datetime_fields(row)


def delete_invoice_checklist_data(*, checklist_id: str) -> dict:
    checklist_id_text = str(checklist_id or "").strip()
    if not checklist_id_text:
        raise ValueError("checklistId is required")
    existing = _safe_db_call(get_inv_checklist_row, checklist_id=checklist_id_text)
    if existing is None:
        raise NotFoundError(f"Checklist not found: {checklist_id_text}")

    station_rows = _safe_db_call(list_inv_checklist_stations, checklist_id=checklist_id_text)
    for station_row in station_rows:
        station_row_id = station_row.get("id")
        if station_row_id is None:
            continue
        delete_invoice_checklist_station_data(station_row_id=int(station_row_id))

    try:
        deleted = delete_inv_checklist(checklist_id=checklist_id_text)
    except Exception as exc:
        raise _map_db_exception(exc) from exc
    return {"deleted": int(deleted > 0), "id": checklist_id_text}


def list_invoice_checklist_stations_data(
    *,
    checklist_id: str | None = None,
    station_row_id: int | None = None,
    est_num: int | None = None,
    station_code: str | None = None,
    status: str | None = None,
) -> list[dict]:
    checklist_id_value = str(checklist_id or "").strip() or None
    station_row_id_value = _ensure_optional_unsigned_int(station_row_id, field="stationRowId")
    est_num_value = _ensure_optional_unsigned_int(est_num, field="estNum")
    station_code_value = None
    if station_code is not None:
        station_code_value = _ensure_required_text(
            station_code,
            field="stationCode",
            max_length=10,
            uppercase=True,
        )
    status_value = None
    if status is not None:
        status_value = _ensure_optional_text(status, field="status", max_length=32)

    rows = _safe_db_call(
        list_inv_checklist_stations,
        checklist_id=checklist_id_value,
        station_row_id=station_row_id_value,
        est_num=est_num_value,
        station_code=station_code_value,
        status=status_value,
    )
    return [_serialize_datetime_fields(row) for row in rows]


def create_invoice_checklist_station_data(*, payload: dict) -> dict:
    normalized = _normalize_station_payload(payload)
    try:
        station_id = insert_inv_checklist_station(normalized)
    except Exception as exc:
        raise _map_db_exception(exc) from exc

    row = _safe_db_call(get_inv_checklist_station_row, station_row_id=station_id)
    if row is None:
        raise SafeDatabaseError("Failed to read created checklist station")
    return _serialize_datetime_fields(row)


def update_invoice_checklist_station_data(*, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    station_id = _ensure_required_unsigned_int(payload.get("stationRowId"), field="stationRowId")
    if _safe_db_call(get_inv_checklist_station_row, station_row_id=station_id) is None:
        raise NotFoundError(f"Checklist station not found: {station_id}")

    updates = _normalize_station_updates(payload)
    try:
        update_inv_checklist_station(station_row_id=station_id, fields=updates)
    except Exception as exc:
        raise _map_db_exception(exc) from exc

    row = _safe_db_call(get_inv_checklist_station_row, station_row_id=station_id)
    if row is None:
        raise NotFoundError(f"Checklist station not found: {station_id}")
    return _serialize_datetime_fields(row)


def delete_invoice_checklist_station_data(*, station_row_id: int) -> dict:
    station_id = _ensure_required_unsigned_int(station_row_id, field="stationRowId")
    if _safe_db_call(get_inv_checklist_station_row, station_row_id=station_id) is None:
        raise NotFoundError(f"Checklist station not found: {station_id}")

    note_rows = _safe_db_call(list_inv_checklist_notes, checklist_station_id=station_id)
    for note_row in note_rows:
        note_id_value = note_row.get("id")
        if note_id_value is None:
            continue
        delete_invoice_checklist_note_data(note_id=int(note_id_value))

    try:
        deleted = delete_inv_checklist_station(station_row_id=station_id)
    except Exception as exc:
        raise _map_db_exception(exc) from exc
    return {"deleted": int(deleted > 0), "id": station_id}


def list_invoice_checklist_notes_data(
    *,
    checklist_station_id: int | None = None,
    note_id: int | None = None,
    est_num: int | None = None,
    station_code: str | None = None,
    checklist_id: str | None = None,
    include_attachments: bool = False,
    limit: int | None = None,
) -> list[dict]:
    checklist_station_id_value = _ensure_optional_unsigned_int(
        checklist_station_id,
        field="checklistStationId",
    )
    note_id_value = _ensure_optional_unsigned_int(note_id, field="noteId")
    est_num_value = _ensure_optional_unsigned_int(est_num, field="estNum")
    station_code_value = None
    if station_code is not None:
        station_code_value = _ensure_required_text(
            station_code,
            field="stationCode",
            max_length=10,
            uppercase=True,
        )
    checklist_id_value = str(checklist_id or "").strip() or None
    if (est_num_value is None) != (station_code_value is None):
        raise ValueError("estNum and stationCode must be provided together")
    limit_value = _ensure_optional_unsigned_int(limit, field="limit")
    if limit_value is not None and limit_value <= 0:
        raise ValueError("limit must be greater than 0")

    rows = _safe_db_call(
        list_inv_checklist_note_detail_rows,
        checklist_station_id=checklist_station_id_value,
        note_id=note_id_value,
        est_num=est_num_value,
        station_code=station_code_value,
        checklist_id=checklist_id_value,
        limit=limit_value,
    )
    notes_by_id: dict[int, dict] = {}
    ordered_note_ids: list[int] = []
    for row in rows:
        note_row_id = row.get("noteId")
        if note_row_id is None:
            continue
        parsed_note_id = int(note_row_id)
        note_entry = notes_by_id.get(parsed_note_id)
        if note_entry is None:
            ordered_note_ids.append(parsed_note_id)
            amount_value = row.get("amount")
            note_entry = {
                "id": parsed_note_id,
                "checklistStationId": int(row.get("checklistStationId")),
                "checklistId": str(row.get("checklistId") or "").strip() or None,
                "checklistYear": int(row.get("checklistYear")) if row.get("checklistYear") is not None else None,
                "checklistMonth": int(row.get("checklistMonth")) if row.get("checklistMonth") is not None else None,
                "estNum": int(row.get("estNum")) if row.get("estNum") is not None else None,
                "stationCode": str(row.get("stationCode") or "").strip().upper() or None,
                "amount": float(amount_value) if amount_value is not None else None,
                "note": row.get("note"),
                "dateCreated": _to_iso_text(row.get("dateCreated")),
                "dateUpdated": _to_iso_text(row.get("dateUpdated")),
            }
            if include_attachments:
                note_entry["attachments"] = []
            notes_by_id[parsed_note_id] = note_entry

    if include_attachments and notes_by_id:
        tenant_slug = str(get_tenant_id() or "").strip().lower() or None
        for note_id, note_entry in notes_by_id.items():
            attachment_rows = _safe_db_call(
                list_inv_note_attachments,
                note_id=int(note_id),
                tenant_slug=tenant_slug,
            )
            note_entry["attachments"] = [
                _serialize_attachment_row(attachment_row)
                for attachment_row in attachment_rows
                if attachment_row.get("attachmentId") is not None
            ]

    return [notes_by_id[key] for key in ordered_note_ids]


def create_invoice_checklist_note_data(*, payload: dict) -> dict:
    normalized = _normalize_note_payload(payload)
    try:
        note_id = insert_inv_checklist_note(normalized)
    except Exception as exc:
        raise _map_db_exception(exc) from exc

    row = _safe_db_call(get_inv_checklist_note_row, note_id=note_id)
    if row is None:
        raise SafeDatabaseError("Failed to read created checklist note")
    return _serialize_note_row(row)


def update_invoice_checklist_note_data(*, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    note_id_value = _ensure_required_unsigned_int(payload.get("noteId"), field="noteId")
    existing_row = _safe_db_call(get_inv_checklist_note_row, note_id=note_id_value)
    if existing_row is None:
        raise NotFoundError(f"Checklist note not found: {note_id_value}")

    updates = _normalize_note_updates(payload)
    next_amount = updates["amount"] if "amount" in updates else existing_row.get("amount")
    next_note = updates["note"] if "note" in updates else existing_row.get("note")
    if not _has_note_or_amount(note_value=next_note, amount_value=next_amount):
        raise ValueError("At least one of amount or note is required")

    try:
        update_inv_checklist_note(note_id=note_id_value, fields=updates)
    except Exception as exc:
        raise _map_db_exception(exc) from exc

    row = _safe_db_call(get_inv_checklist_note_row, note_id=note_id_value)
    if row is None:
        raise NotFoundError(f"Checklist note not found: {note_id_value}")
    return _serialize_note_row(row)


def delete_invoice_checklist_note_data(*, note_id: int) -> dict:
    note_id_value = _ensure_required_unsigned_int(note_id, field="noteId")
    if _safe_db_call(get_inv_checklist_note_row, note_id=note_id_value) is None:
        raise NotFoundError(f"Checklist note not found: {note_id_value}")
    tenant_slug = str(get_tenant_id() or "").strip().lower() or None
    attachment_rows = _safe_db_call(
        list_inv_note_attachments,
        note_id=note_id_value,
        tenant_slug=tenant_slug,
    )

    for attachment_row in attachment_rows:
        attachment_id_raw = attachment_row.get("attachmentId")
        if attachment_id_raw is None:
            continue
        attachment_id_value = int(attachment_id_raw)
        storage_provider, storage_key, provider_resource_type, attachment_mime_type = _resolve_attachment_delete_payload(attachment_row)
        if storage_provider and storage_key:
            try:
                delete_file(
                    app_name="TradSphere",
                    storage_provider=storage_provider,
                    payload=DeleteAssetInput(
                        provider_resource_type=provider_resource_type,
                        provider_public_id=storage_key,
                        mime_type=attachment_mime_type,
                    ),
                )
            except Exception:
                pass
        _safe_db_call(
            delete_inv_note_attachment,
            attachment_id=attachment_id_value,
            tenant_slug=tenant_slug,
        )
    _safe_db_call(
        soft_delete_app_attachments_for_invoice_note,
        note_id=note_id_value,
        tenant_slug=tenant_slug,
    )

    try:
        deleted = delete_inv_checklist_note(note_id=note_id_value)
    except Exception as exc:
        raise _map_db_exception(exc) from exc
    return {"deleted": int(deleted > 0), "id": note_id_value}


def list_invoice_note_attachments_data(
    *,
    note_id: int | None = None,
    attachment_id: int | None = None,
) -> list[dict]:
    tenant_slug = str(get_tenant_id() or "").strip().lower() or None
    note_id_value = _ensure_optional_unsigned_int(note_id, field="noteId")
    attachment_id_value = _ensure_optional_unsigned_int(attachment_id, field="attachmentId")
    rows = _safe_db_call(
        list_inv_note_attachments,
        note_id=note_id_value,
        attachment_id=attachment_id_value,
        tenant_slug=tenant_slug,
    )
    output: list[dict] = []
    for row in rows:
        normalized_row = dict(row)
        normalized_row["attachmentId"] = row.get("attachmentId")
        normalized_row["attachmentNoteId"] = row.get("attachmentNoteId")
        output.append(_serialize_attachment_row(normalized_row))
    return output


def create_invoice_note_attachment_data(*, payload: dict) -> dict:
    tenant_slug = str(get_tenant_id() or "").strip().lower() or None
    normalized = _normalize_attachment_payload(payload)
    existing_attachment_count = _safe_db_call(
        count_inv_note_attachments,
        note_id=int(normalized["noteId"]),
        include_deleted=False,
        tenant_slug=tenant_slug,
    )
    attachment_limit = get_note_attachment_limit()
    if int(existing_attachment_count) >= int(attachment_limit):
        raise ValueError(f"Attachment limit reached. Maximum {attachment_limit} files per note.")
    try:
        attachment_id = insert_inv_note_attachment(normalized)
    except Exception as exc:
        raise _map_db_exception(exc) from exc

    row = _safe_db_call(
        get_inv_note_attachment_row,
        attachment_id=attachment_id,
        tenant_slug=tenant_slug,
    )
    if row is None:
        raise SafeDatabaseError("Failed to read created note attachment")
    return _serialize_attachment_row(row)


def create_invoice_note_attachment_upload_data(
    *,
    note_id: int,
    filename: str,
    mime_type: str,
    file_bytes: bytes,
    uploaded_by: str | None,
) -> dict:
    note_id_value = _ensure_required_unsigned_int(note_id, field="noteId")
    tenant_slug = str(get_tenant_id() or "").strip().lower()
    if not tenant_slug:
        raise ValueError("Missing tenant context")
    note_row = _safe_db_call(get_inv_checklist_note_row, note_id=note_id_value)
    if note_row is None:
        raise NotFoundError(f"Checklist note not found: {note_id_value}")
    station_row = _safe_db_call(
        get_inv_checklist_station_row,
        station_row_id=int(note_row.get("checklistStationId")),
    )

    existing_attachment_count = _safe_db_call(
        count_inv_note_attachments,
        note_id=note_id_value,
        include_deleted=False,
        tenant_slug=tenant_slug,
    )
    attachment_limit = get_note_attachment_limit()
    if int(existing_attachment_count) >= int(attachment_limit):
        raise ValueError(f"Attachment limit reached. Maximum {attachment_limit} files per note.")
    total_attachment_count = _safe_db_call(
        count_inv_note_attachments,
        note_id=note_id_value,
        include_deleted=True,
        tenant_slug=tenant_slug,
    )
    next_attachment_counter = int(total_attachment_count or 0) + 1
    storage_key = _build_invoice_note_storage_key(
        note_id=note_id_value,
        est_num=int(station_row.get("estNum")) if station_row and station_row.get("estNum") is not None else None,
        station_code=station_row.get("stationCode") if station_row else None,
        counter=next_attachment_counter,
    )

    normalized_name, normalized_mime, normalized_size = validate_attachment_payload(
        file_bytes=file_bytes,
        filename=filename,
        mime_type=mime_type,
    )

    uploaded_by_value = str(uploaded_by or "").strip() or None
    stored_asset = upload_file(
        payload=StorageUploadInput(
            app_name="TradSphere",
            tenant_slug=tenant_slug,
            owner_entity_type="invoice_checklist_note",
            owner_entity_id=str(note_id_value),
            uploaded_by=uploaded_by_value,
            file_bytes=file_bytes,
            original_filename=normalized_name,
            mime_type=normalized_mime,
            storage_key=storage_key,
        )
    )
    asset_size = int(stored_asset.file_size or normalized_size)
    provider_metadata_payload = dict(stored_asset.provider_metadata or {})
    provider_metadata_payload.setdefault("asset_id", stored_asset.provider_asset_id)
    provider_metadata_payload.setdefault("public_id", stored_asset.provider_public_id)
    provider_metadata_payload.setdefault("resource_type", stored_asset.provider_resource_type)
    provider_metadata_payload.setdefault("bytes", asset_size)
    provider_metadata_payload.setdefault("uploaded_original_filename", normalized_name)
    stored_file_name = _build_stored_attachment_file_name(
        provider_public_id=stored_asset.provider_public_id,
        provider_metadata=provider_metadata_payload,
        mime_type=normalized_mime,
        fallback_name=normalized_name,
    )
    provider_metadata_payload.setdefault("stored_file_name", stored_file_name)

    row_payload = {
        "noteId": note_id_value,
        "url": stored_asset.access_url,
        "accessUrl": stored_asset.access_url,
        "fileName": stored_file_name,
        "fileType": normalized_mime,
        "originalFileName": stored_file_name,
        "mimeType": normalized_mime,
        "appCode": "tradsphere",
        "ownerEntityType": "invoice_checklist_note",
        "ownerEntityId": str(note_id_value),
        "storageProvider": stored_asset.storage_provider,
        "storageKey": stored_asset.provider_public_id,
        "providerMetadata": provider_metadata_payload,
        "providerAssetId": stored_asset.provider_asset_id,
        "providerPublicId": stored_asset.provider_public_id,
        "providerResourceType": stored_asset.provider_resource_type,
        "fileSize": asset_size,
        "uploadedBy": uploaded_by_value,
        "tenantSlug": tenant_slug,
    }

    try:
        attachment_id = insert_inv_note_attachment(row_payload)
    except Exception as exc:
        try:
            delete_file(
                app_name="TradSphere",
                storage_provider=stored_asset.storage_provider,
                payload=DeleteAssetInput(
                    provider_resource_type=stored_asset.provider_resource_type,
                    provider_public_id=stored_asset.provider_public_id,
                    mime_type=stored_asset.mime_type,
                ),
            )
        except Exception:
            pass
        raise _map_db_exception(exc) from exc

    row = _safe_db_call(
        get_inv_note_attachment_row,
        attachment_id=attachment_id,
        tenant_slug=tenant_slug,
    )
    if row is None:
        raise SafeDatabaseError("Failed to read created note attachment")
    return _serialize_attachment_row(row)


def update_invoice_note_attachment_data(*, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    tenant_slug = str(get_tenant_id() or "").strip().lower() or None
    attachment_id_value = _ensure_required_unsigned_int(payload.get("attachmentId"), field="attachmentId")
    if _safe_db_call(
        get_inv_note_attachment_row,
        attachment_id=attachment_id_value,
        tenant_slug=tenant_slug,
    ) is None:
        raise NotFoundError(f"Note attachment not found: {attachment_id_value}")

    updates = _normalize_attachment_updates(payload)
    try:
        update_inv_note_attachment(
            attachment_id=attachment_id_value,
            fields=updates,
            tenant_slug=tenant_slug,
        )
    except Exception as exc:
        raise _map_db_exception(exc) from exc

    row = _safe_db_call(
        get_inv_note_attachment_row,
        attachment_id=attachment_id_value,
        tenant_slug=tenant_slug,
    )
    if row is None:
        raise NotFoundError(f"Note attachment not found: {attachment_id_value}")
    return _serialize_attachment_row(row)


def delete_invoice_note_attachment_data(*, attachment_id: int) -> dict:
    tenant_slug = str(get_tenant_id() or "").strip().lower() or None
    attachment_id_value = _ensure_required_unsigned_int(attachment_id, field="attachmentId")
    row = _safe_db_call(
        get_inv_note_attachment_row,
        attachment_id=attachment_id_value,
        tenant_slug=tenant_slug,
    )
    if row is None:
        raise NotFoundError(f"Note attachment not found: {attachment_id_value}")

    storage_provider, storage_key, provider_resource_type, attachment_mime_type = _resolve_attachment_delete_payload(row)

    if storage_provider and storage_key:
        delete_file(
            app_name="TradSphere",
            storage_provider=storage_provider,
            payload=DeleteAssetInput(
                provider_resource_type=provider_resource_type,
                provider_public_id=storage_key,
                mime_type=attachment_mime_type,
            ),
        )

    try:
        deleted = delete_inv_note_attachment(
            attachment_id=attachment_id_value,
            tenant_slug=tenant_slug,
        )
    except Exception as exc:
        raise _map_db_exception(exc) from exc
    return {"deleted": int(deleted > 0), "id": attachment_id_value}


def _ensure_object_list(value: object, *, field: str) -> list[dict]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field} must be an array")
    rows: list[dict] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"{field}[{index}] must be an object")
        rows.append(item)
    return rows


def _resolve_bulk_checklist_id(
    *,
    row: dict,
    checklist_id_map: dict[str, str],
    field: str,
) -> str:
    direct_id = str(row.get("checklistId") or "").strip()
    if direct_id:
        if direct_id in checklist_id_map:
            return checklist_id_map[direct_id]
        return direct_id
    client_id = str(row.get("checklistClientId") or "").strip()
    if client_id and client_id in checklist_id_map:
        return checklist_id_map[client_id]
    raise ValueError(f"{field} requires checklistId or checklistClientId")


def bulk_save_invoice_checklists_data(*, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    selected_year = _ensure_required_year(payload.get("year"))
    selected_month = _ensure_required_month(payload.get("month"))

    create_checklists_raw = _ensure_object_list(payload.get("createChecklists"), field="createChecklists")
    checklist_updates_raw = _ensure_object_list(payload.get("checklistUpdates"), field="checklistUpdates")
    delete_checklist_ids = [
        str(item or "").strip()
        for item in (payload.get("deleteChecklistIds") or [])
        if str(item or "").strip()
    ]

    checklist_creates: list[dict] = []
    checklist_id_map: dict[str, str] = {}
    create_account_codes: list[str] = []
    for index, row in enumerate(create_checklists_raw):
        client_id = str(row.get("clientChecklistId") or "").strip()
        if not client_id:
            raise ValueError(f"createChecklists[{index}].clientChecklistId is required")
        account_code = _ensure_required_text(
            row.get("accountCode"),
            field=f"createChecklists[{index}].accountCode",
            max_length=10,
            uppercase=True,
        )
        row_year = _ensure_required_year(row.get("year"))
        row_month = _ensure_required_month(row.get("month"))
        if row_year != selected_year or row_month != selected_month:
            raise ValueError(
                f"createChecklists[{index}] period must match payload year/month"
            )
        checklist_id = str(uuid.uuid4())
        checklist_id_map[client_id] = checklist_id
        create_account_codes.append(account_code)
        checklist_creates.append(
            {
                "clientChecklistId": client_id,
                "id": checklist_id,
                "accountCode": account_code,
                "year": row_year,
                "month": row_month,
                "status": _ensure_optional_text(
                    row.get("status"),
                    field=f"createChecklists[{index}].status",
                    max_length=32,
                ),
                "note": _ensure_optional_text(
                    row.get("note"),
                    field=f"createChecklists[{index}].note",
                    max_length=2048,
                ),
            }
        )

    if create_account_codes:
        ensure_master_account_codes_exist(create_account_codes)

    checklist_updates: list[dict] = []
    checklist_ids_for_lookup: list[str] = []
    for index, row in enumerate(checklist_updates_raw):
        checklist_id = _resolve_bulk_checklist_id(
            row=row,
            checklist_id_map=checklist_id_map,
            field=f"checklistUpdates[{index}]",
        )
        update_row: dict[str, object] = {"checklistId": checklist_id}
        if "status" in row:
            update_row["status"] = _ensure_optional_text(
                row.get("status"),
                field=f"checklistUpdates[{index}].status",
                max_length=32,
            )
        if "note" in row:
            update_row["note"] = _ensure_optional_text(
                row.get("note"),
                field=f"checklistUpdates[{index}].note",
                max_length=2048,
            )
        if len(update_row) == 1:
            raise ValueError(
                f"No updatable fields provided for checklistUpdates[{index}]"
            )
        checklist_updates.append(update_row)
        checklist_ids_for_lookup.append(checklist_id)

    checklist_ids_to_validate = [
        checklist_id
        for checklist_id in _normalized_text_values(delete_checklist_ids + checklist_ids_for_lookup)
        if checklist_id not in checklist_id_map.values()
    ]
    if checklist_ids_to_validate:
        existing_checklists = _safe_db_call(
            list_inv_checklist_rows_by_ids,
            checklist_ids=checklist_ids_to_validate,
        )
        existing_checklist_ids = {
            str((row or {}).get("id") or "").strip()
            for row in (existing_checklists or [])
            if str((row or {}).get("id") or "").strip()
        }
        for checklist_id in checklist_ids_to_validate:
            if checklist_id not in existing_checklist_ids:
                raise NotFoundError(f"Checklist not found: {checklist_id}")

    station_creates_raw = _ensure_object_list(payload.get("createStations"), field="createStations")
    station_updates_raw = _ensure_object_list(payload.get("stationUpdates"), field="stationUpdates")
    delete_station_ids = [
        _ensure_required_unsigned_int(item, field="deleteStationIds[]")
        for item in (payload.get("deleteStationIds") or [])
    ]

    station_creates: list[dict] = []
    est_nums_for_create: list[int] = []
    station_codes_for_create: list[str] = []
    for index, row in enumerate(station_creates_raw):
        client_station_id = str(row.get("clientStationId") or "").strip()
        if not client_station_id:
            raise ValueError(f"createStations[{index}].clientStationId is required")
        checklist_id = _resolve_bulk_checklist_id(
            row=row,
            checklist_id_map=checklist_id_map,
            field=f"createStations[{index}]",
        )
        est_num = _ensure_required_unsigned_int(
            row.get("estNum"),
            field=f"createStations[{index}].estNum",
        )
        station_code = _ensure_required_text(
            row.get("stationCode"),
            field=f"createStations[{index}].stationCode",
            max_length=10,
            uppercase=True,
        )
        est_nums_for_create.append(est_num)
        station_codes_for_create.append(station_code)
        station_creates.append(
            {
                "clientStationId": client_station_id,
                "checklistId": checklist_id,
                "estNum": est_num,
                "stationCode": station_code,
                "status": _ensure_optional_text(
                    row.get("status"),
                    field=f"createStations[{index}].status",
                    max_length=32,
                ),
            }
        )

    if est_nums_for_create:
        ensure_est_nums_exist(est_nums_for_create)
    if station_codes_for_create:
        ensure_station_codes_exist(station_codes_for_create)

    station_updates: list[dict] = []
    station_ids_for_lookup: list[int] = []
    for index, row in enumerate(station_updates_raw):
        station_row_id = _ensure_required_unsigned_int(
            row.get("stationRowId"),
            field=f"stationUpdates[{index}].stationRowId",
        )
        update_row: dict[str, object] = {"stationRowId": station_row_id}
        if "status" in row:
            update_row["status"] = _ensure_optional_text(
                row.get("status"),
                field=f"stationUpdates[{index}].status",
                max_length=32,
            )
        if len(update_row) == 1:
            raise ValueError(
                f"No updatable fields provided for stationUpdates[{index}]"
            )
        station_updates.append(update_row)
        station_ids_for_lookup.append(station_row_id)

    station_ids_to_validate = _normalized_int_values(station_ids_for_lookup + delete_station_ids)
    if station_ids_to_validate:
        existing_stations = _safe_db_call(
            list_inv_checklist_station_rows_by_ids,
            station_row_ids=station_ids_to_validate,
        )
        existing_station_ids = {
            int((row or {}).get("id"))
            for row in (existing_stations or [])
            if (row or {}).get("id") is not None
        }
        for station_id in station_ids_to_validate:
            if station_id not in existing_station_ids:
                raise NotFoundError(f"Checklist station not found: {station_id}")

    note_creates_raw = _ensure_object_list(payload.get("createNotes"), field="createNotes")
    note_updates_raw = _ensure_object_list(payload.get("noteUpdates"), field="noteUpdates")
    delete_note_ids = [
        _ensure_required_unsigned_int(item, field="deleteNoteIds[]")
        for item in (payload.get("deleteNoteIds") or [])
    ]

    note_creates: list[dict] = []
    for index, row in enumerate(note_creates_raw):
        client_note_id = str(row.get("clientNoteId") or "").strip()
        if not client_note_id:
            raise ValueError(f"createNotes[{index}].clientNoteId is required")
        direct_station_id = row.get("checklistStationId")
        client_station_id = str(row.get("checklistStationClientId") or "").strip()
        if direct_station_id is None and not client_station_id:
            raise ValueError(
                f"createNotes[{index}] requires checklistStationId or checklistStationClientId"
            )
        checklist_station_id = None
        if direct_station_id is not None and str(direct_station_id).strip() != "":
            checklist_station_id = _ensure_required_unsigned_int(
                direct_station_id,
                field=f"createNotes[{index}].checklistStationId",
            )
        amount_value = _ensure_optional_amount(row.get("amount"))
        note_value = _ensure_optional_text(
            row.get("note"),
            field=f"createNotes[{index}].note",
            max_length=2048,
        )
        if not _has_note_or_amount(note_value=note_value, amount_value=amount_value):
            raise ValueError("At least one of amount or note is required")
        note_creates.append(
            {
                "clientNoteId": client_note_id,
                "checklistStationId": checklist_station_id,
                "checklistStationClientId": client_station_id or None,
                "amount": amount_value,
                "note": note_value,
            }
        )

    note_updates: list[dict] = []
    note_ids_for_lookup: list[int] = []
    for index, row in enumerate(note_updates_raw):
        note_id = _ensure_required_unsigned_int(
            row.get("noteId"),
            field=f"noteUpdates[{index}].noteId",
        )
        update_row: dict[str, object] = {"noteId": note_id}
        if "amount" in row:
            update_row["amount"] = _ensure_optional_amount(row.get("amount"))
        if "note" in row:
            update_row["note"] = _ensure_optional_text(
                row.get("note"),
                field=f"noteUpdates[{index}].note",
                max_length=2048,
            )
        next_amount = update_row.get("amount")
        next_note = update_row.get("note")
        if not _has_note_or_amount(note_value=next_note, amount_value=next_amount):
            raise ValueError("At least one of amount or note is required")
        note_updates.append(update_row)
        note_ids_for_lookup.append(note_id)

    note_ids_to_validate = _normalized_int_values(note_ids_for_lookup + delete_note_ids)
    if note_ids_to_validate:
        existing_notes = _safe_db_call(
            list_inv_checklist_note_rows_by_ids,
            note_ids=note_ids_to_validate,
        )
        existing_note_ids = {
            int((row or {}).get("id"))
            for row in (existing_notes or [])
            if (row or {}).get("id") is not None
        }
        for note_id in note_ids_to_validate:
            if note_id not in existing_note_ids:
                raise NotFoundError(f"Checklist note not found: {note_id}")

    tenant_slug = str(get_tenant_id() or "").strip().lower() or None
    result = _safe_db_call(
        save_inv_checklist_bulk_changes,
        checklist_creates=checklist_creates,
        checklist_updates=checklist_updates,
        checklist_deletes=_normalized_text_values(delete_checklist_ids),
        station_creates=station_creates,
        station_updates=station_updates,
        station_deletes=_normalized_int_values(delete_station_ids),
        note_creates=note_creates,
        note_updates=note_updates,
        note_deletes=_normalized_int_values(delete_note_ids),
        tenant_slug=tenant_slug,
    )

    selected_checklist_id_raw = str(payload.get("selectedChecklistId") or "").strip()
    selected_checklist_id = checklist_id_map.get(selected_checklist_id_raw, selected_checklist_id_raw)
    deleted_checklist_ids_set = set(_normalized_text_values(delete_checklist_ids))
    if selected_checklist_id in deleted_checklist_ids_set:
        selected_checklist_id = None

    selected_checklist = None
    if selected_checklist_id:
        try:
            selected_checklist = get_invoice_checklists_data(
                checklist_id=selected_checklist_id,
                include_stations=True,
                include_notes=True,
                include_attachments=True,
            )
        except NotFoundError:
            selected_checklist = None
            selected_checklist_id = None

    checklist_rows = list_invoice_checklists_data(
        year=selected_year,
        month=selected_month,
    )

    return {
        "year": selected_year,
        "month": selected_month,
        "checklists": checklist_rows,
        "selectedChecklistId": selected_checklist_id,
        "selectedChecklist": selected_checklist,
        "mappings": {
            "checklistIds": dict(result.get("checklistIds") or {}),
            "stationIds": dict(result.get("stationIds") or {}),
            "noteIds": dict(result.get("noteIds") or {}),
        },
        "deleted": {
            "checklistIds": _normalized_text_values(delete_checklist_ids),
            "stationIds": _normalized_int_values(delete_station_ids),
            "noteIds": _normalized_int_values(delete_note_ids),
        },
        "summary": {
            "createdChecklistsCount": len(checklist_creates),
            "updatedChecklistsCount": len(checklist_updates),
            "deletedChecklistsCount": len(delete_checklist_ids),
            "createdStationsCount": len(station_creates),
            "updatedStationsCount": len(station_updates),
            "deletedStationsCount": len(delete_station_ids),
            "createdNotesCount": len(note_creates),
            "updatedNotesCount": len(note_updates),
            "deletedNotesCount": len(delete_note_ids),
        },
    }


def get_invoice_note_attachment_open_data(*, attachment_id: int) -> dict:
    tenant_slug = str(get_tenant_id() or "").strip().lower() or None
    attachment_id_value = _ensure_required_unsigned_int(attachment_id, field="attachmentId")
    row = _safe_db_call(
        get_inv_note_attachment_row,
        attachment_id=attachment_id_value,
        tenant_slug=tenant_slug,
    )
    if row is None:
        raise NotFoundError(f"Note attachment not found: {attachment_id_value}")

    access_url = str(
        row.get("attachmentAccessUrl")
        or row.get("attachmentUrl")
        or ""
    ).strip()
    if not access_url:
        raise NotFoundError(f"Attachment URL not found: {attachment_id_value}")

    return {
        "id": attachment_id_value,
        "url": access_url,
        "fileName": row.get("attachmentOriginalFileName") or row.get("attachmentFileName"),
        "mimeType": row.get("attachmentMimeType") or row.get("attachmentFileType"),
    }


def _build_account_names_map(account_codes: list[str]) -> dict[str, str]:
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for raw in account_codes:
        code = str(raw or "").strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized_codes.append(code)
    if not normalized_codes:
        return {}

    rows = list_accounts(account_codes=normalized_codes, active=False)
    mapped: dict[str, str] = {}
    for row in rows:
        code = str(row.get("accountCode") or "").strip().upper()
        if not code:
            continue
        mapped[code] = str(row.get("name") or "").strip()
    return mapped


def _serialize_station_rep_contacts(rep_rows: object) -> list[dict]:
    if not isinstance(rep_rows, list):
        return []
    output: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for row in rep_rows:
        if not isinstance(row, dict):
            continue
        full_name = (
            str(row.get("name") or "").strip()
            or build_rep_contact_full_name(row)
        )
        email = str(row.get("email") or "").strip().lower()
        if not full_name and not email:
            continue
        fingerprint = (full_name.lower(), email)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        output.append(
            {
                "id": row.get("id"),
                "fullName": full_name,
                "email": email,
                "office": str(row.get("office") or "").strip(),
                "mobile": str(row.get("cell") or "").strip(),
                "jobTitle": str(row.get("jobTitle") or "").strip(),
                "company": str(row.get("company") or "").strip(),
                "primaryContact": bool(row.get("primaryContact")),
            }
        )
    output.sort(
        key=lambda item: (
            -int(bool(item.get("primaryContact"))),
            str(item.get("fullName") or "").lower(),
            str(item.get("email") or "").lower(),
        )
    )
    return output


def _build_station_metadata(station_codes: list[str]) -> dict[str, dict]:
    normalized_codes: list[str] = []
    seen: set[str] = set()
    for raw in station_codes:
        code = str(raw or "").strip().upper()
        if not code or code in seen:
            continue
        seen.add(code)
        normalized_codes.append(code)
    if not normalized_codes:
        return {}

    station_rows = list_stations_data(
        codes=normalized_codes,
        delivery_method_detail=False,
        contact_detail=True,
    )
    mapped: dict[str, dict] = {}
    for row in station_rows:
        code = str(row.get("code") or "").strip().upper()
        if not code:
            continue
        contacts = row.get("contacts")
        rep_rows = contacts.get("REP") if isinstance(contacts, dict) else []
        mapped[code] = {
            "stationCode": code,
            "stationName": str(row.get("name") or "").strip(),
            "mediaType": str(row.get("mediaType") or "").strip().upper(),
            "repContacts": _serialize_station_rep_contacts(rep_rows),
        }
    return mapped


def _build_checklist_station_search_map(checklist_ids: list[str]) -> dict[str, list[dict]]:
    rows = _safe_db_call(
        list_inv_checklist_station_search_rows,
        checklist_ids=checklist_ids,
    )
    mapped: dict[str, list[dict]] = {}
    dedupe: dict[str, set[tuple[int, str]]] = {}
    for row in rows:
        checklist_id = str(row.get("checklistId") or "").strip()
        if not checklist_id:
            continue
        est_num = _ensure_optional_unsigned_int(row.get("estNum"), field="estNum")
        station_code = str(row.get("stationCode") or "").strip().upper()
        if est_num is None or not station_code:
            continue
        seen = dedupe.setdefault(checklist_id, set())
        fingerprint = (int(est_num), station_code)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        mapped.setdefault(checklist_id, []).append(
            {
                "estNum": int(est_num),
                "stationCode": station_code,
            }
        )
    return mapped


def _build_expected_schedule_pairs_by_account(
    *,
    year: int,
    month: int,
) -> dict[str, set[tuple[int, str]]]:
    rows = _safe_db_call(
        list_schedule_invoice_checklist_expected_rows,
        broadcast_year=int(year),
        broadcast_month=int(month),
    )
    mapped: dict[str, set[tuple[int, str]]] = {}
    for row in rows:
        account_code = str(row.get("accountCode") or "").strip().upper()
        est_num = _ensure_optional_unsigned_int(row.get("estNum"), field="estNum")
        station_code = str(row.get("stationCode") or "").strip().upper()
        if not account_code or est_num is None or not station_code:
            continue
        mapped.setdefault(account_code, set()).add((int(est_num), station_code))
    return mapped


def _build_mismatch_rows_for_period(
    *,
    checklist_rows: list[dict],
    expected_pairs_by_account: dict[str, set[tuple[int, str]]],
) -> list[dict]:
    checklist_ids: list[str] = []
    checklist_by_id: dict[str, dict] = {}
    for row in checklist_rows:
        checklist_id = str(row.get("id") or "").strip()
        if not checklist_id:
            continue
        checklist_ids.append(checklist_id)
        checklist_by_id[checklist_id] = row

    if not checklist_ids:
        return []

    station_rows = _safe_db_call(
        list_inv_checklist_station_rows_for_checklists,
        checklist_ids=checklist_ids,
    )

    mismatches: list[dict] = []
    for station_row in station_rows:
        checklist_id = str(station_row.get("checklistId") or "").strip()
        checklist = checklist_by_id.get(checklist_id)
        if not checklist:
            continue
        account_code = str(checklist.get("accountCode") or "").strip().upper()
        est_num = _ensure_optional_unsigned_int(station_row.get("estNum"), field="estNum")
        station_code = str(station_row.get("stationCode") or "").strip().upper()
        if not account_code or est_num is None or not station_code:
            continue
        expected_pairs = expected_pairs_by_account.get(account_code, set())
        if (int(est_num), station_code) in expected_pairs:
            continue
        mismatches.append(
            {
                "stationRowId": int(station_row.get("id")),
                "checklistId": checklist_id,
                "accountCode": account_code,
                "estNum": int(est_num),
                "stationCode": station_code,
                "status": str(station_row.get("status") or "").strip(),
                "reasonCode": "NOT_IN_CURRENT_PERIOD_SCHEDULE",
            }
        )
    return mismatches


def sync_invoice_checklists_for_period_data(
    *,
    year: object,
    month: object,
    preview_only: bool = False,
) -> dict:
    selected_year = _ensure_required_year(year)
    selected_month = _ensure_required_month(month)

    expected_pairs_by_account = _build_expected_schedule_pairs_by_account(
        year=selected_year,
        month=selected_month,
    )

    existing_before = list_invoice_checklists_data(
        year=selected_year,
        month=selected_month,
    )
    had_existing_checklists = len(existing_before) > 0

    checklist_by_account: dict[str, dict] = {}
    for row in existing_before:
        account_code = str(row.get("accountCode") or "").strip().upper()
        if not account_code:
            continue
        checklist_by_account[account_code] = row

    planned_checklists: list[dict] = []
    for account_code in sorted(expected_pairs_by_account.keys()):
        if account_code in checklist_by_account:
            continue
        planned_checklists.append(
            {
                "accountCode": account_code,
                "year": selected_year,
                "month": selected_month,
            }
        )

    checklist_rows_for_plan = existing_before
    checklist_id_to_row_for_plan = {
        str(row.get("id") or "").strip(): row
        for row in checklist_rows_for_plan
        if str(row.get("id") or "").strip()
    }
    planned_station_rows: list[dict] = []
    if checklist_id_to_row_for_plan:
        station_rows_for_plan = _safe_db_call(
            list_inv_checklist_station_rows_for_checklists,
            checklist_ids=list(checklist_id_to_row_for_plan.keys()),
        )
    else:
        station_rows_for_plan = []

    existing_station_pairs_by_checklist_for_plan: dict[str, set[tuple[int, str]]] = {}
    for station_row in station_rows_for_plan:
        checklist_id = str(station_row.get("checklistId") or "").strip()
        est_num = _ensure_optional_unsigned_int(station_row.get("estNum"), field="estNum")
        station_code = str(station_row.get("stationCode") or "").strip().upper()
        if not checklist_id or est_num is None or not station_code:
            continue
        existing_station_pairs_by_checklist_for_plan.setdefault(checklist_id, set()).add((int(est_num), station_code))

    checklist_id_by_account_for_plan: dict[str, str] = {}
    for checklist_id, checklist in checklist_id_to_row_for_plan.items():
        account_code = str(checklist.get("accountCode") or "").strip().upper()
        if not account_code:
            continue
        checklist_id_by_account_for_plan[account_code] = checklist_id

    for account_code, expected_pairs in expected_pairs_by_account.items():
        checklist_id = checklist_id_by_account_for_plan.get(account_code)
        if not checklist_id:
            for est_num, station_code in sorted(expected_pairs, key=lambda item: (item[0], item[1])):
                planned_station_rows.append(
                    {
                        "checklistId": None,
                        "accountCode": account_code,
                        "estNum": est_num,
                        "stationCode": station_code,
                    }
                )
            continue
        existing_pairs = existing_station_pairs_by_checklist_for_plan.get(checklist_id, set())
        for est_num, station_code in sorted(expected_pairs, key=lambda item: (item[0], item[1])):
            if (est_num, station_code) in existing_pairs:
                continue
            planned_station_rows.append(
                {
                    "checklistId": checklist_id,
                    "accountCode": account_code,
                    "estNum": est_num,
                    "stationCode": station_code,
                }
            )

    created_checklists: list[dict] = []
    if preview_only:
        mismatch_rows_preview = _build_mismatch_rows_for_period(
            checklist_rows=existing_before,
            expected_pairs_by_account=expected_pairs_by_account,
        )
        return {
            "action": "generate" if not had_existing_checklists else "update",
            "previewOnly": True,
            "period": {
                "year": selected_year,
                "month": selected_month,
                "quarter": _quarter_for_month(selected_month),
                "value": _period_value(selected_year, selected_month),
                "label": _period_label(selected_year, selected_month),
            },
            "expectedAccountsCount": len(expected_pairs_by_account),
            "expectedStationRowsCount": sum(len(pairs) for pairs in expected_pairs_by_account.values()),
            "plannedChecklistsCount": len(planned_checklists),
            "plannedStationsCount": len(planned_station_rows),
            "mismatchStationCount": len(mismatch_rows_preview),
            "plannedChecklists": planned_checklists,
            "plannedStations": planned_station_rows,
            "mismatchStations": mismatch_rows_preview,
        }

    batch_checklists_to_insert: list[dict] = []
    expected_new_checklist_ids: set[str] = set()
    for account_code in sorted(expected_pairs_by_account.keys()):
        if account_code in checklist_by_account:
            continue
        checklist_id = str(uuid.uuid4())
        expected_new_checklist_ids.add(checklist_id)
        batch_checklists_to_insert.append(
            {
                "id": checklist_id,
                "accountCode": account_code,
                "year": selected_year,
                "month": selected_month,
                "status": None,
                "note": None,
            }
        )

    if batch_checklists_to_insert:
        _safe_db_call(insert_inv_checklists_for_sync, batch_checklists_to_insert)

    checklist_rows = list_invoice_checklists_data(
        year=selected_year,
        month=selected_month,
    )
    checklist_id_to_row = {
        str(row.get("id") or "").strip(): row
        for row in checklist_rows
        if str(row.get("id") or "").strip()
    }

    station_rows_before_insert = _safe_db_call(
        list_inv_checklist_station_rows_for_checklists,
        checklist_ids=list(checklist_id_to_row.keys()),
    )
    existing_station_pairs_by_checklist: dict[str, set[tuple[int, str]]] = {}
    for station_row in station_rows_before_insert:
        checklist_id = str(station_row.get("checklistId") or "").strip()
        est_num = _ensure_optional_unsigned_int(station_row.get("estNum"), field="estNum")
        station_code = str(station_row.get("stationCode") or "").strip().upper()
        if not checklist_id or est_num is None or not station_code:
            continue
        existing_station_pairs_by_checklist.setdefault(checklist_id, set()).add((int(est_num), station_code))

    stations_to_insert: list[dict] = []
    station_insert_keys: list[tuple[str, int, str]] = []
    created_station_rows: list[dict] = []
    for checklist_id, checklist in checklist_id_to_row.items():
        account_code = str(checklist.get("accountCode") or "").strip().upper()
        if not account_code:
            continue
        expected_pairs = expected_pairs_by_account.get(account_code, set())
        existing_pairs = existing_station_pairs_by_checklist.get(checklist_id, set())
        for est_num, station_code in sorted(expected_pairs, key=lambda item: (item[0], item[1])):
            if (est_num, station_code) in existing_pairs:
                continue
            stations_to_insert.append(
                {
                    "checklistId": checklist_id,
                    "estNum": est_num,
                    "stationCode": station_code,
                    "status": None,
                }
            )
            station_insert_keys.append((checklist_id, int(est_num), station_code))
            existing_pairs.add((est_num, station_code))

    if stations_to_insert:
        _safe_db_call(insert_inv_checklist_stations_for_sync, stations_to_insert)

    station_rows_after_insert = _safe_db_call(
        list_inv_checklist_station_rows_for_checklists,
        checklist_ids=list(checklist_id_to_row.keys()),
    )
    station_row_by_key: dict[tuple[str, int, str], dict] = {}
    for station_row in station_rows_after_insert:
        checklist_id = str(station_row.get("checklistId") or "").strip()
        est_num = _ensure_optional_unsigned_int(station_row.get("estNum"), field="estNum")
        station_code = str(station_row.get("stationCode") or "").strip().upper()
        if not checklist_id or est_num is None or not station_code:
            continue
        station_row_by_key[(checklist_id, int(est_num), station_code)] = station_row

    for checklist_id, est_num, station_code in station_insert_keys:
        row = station_row_by_key.get((checklist_id, est_num, station_code))
        if not row:
            continue
        checklist = checklist_id_to_row.get(checklist_id)
        if not checklist:
            continue
        created_station_rows.append(
            {
                "id": int(row.get("id")),
                "checklistId": checklist_id,
                "accountCode": str(checklist.get("accountCode") or "").strip().upper(),
                "estNum": int(est_num),
                "stationCode": station_code,
            }
        )

    for row in checklist_rows:
        checklist_id = str(row.get("id") or "").strip()
        if checklist_id in expected_new_checklist_ids:
            created_checklists.append(row)

    if created_checklists or created_station_rows:
        _safe_db_call(invalidate_inv_checklist_related_cache_for_bulk_write)

    final_checklist_rows = list_invoice_checklists_data(
        year=selected_year,
        month=selected_month,
    )
    mismatch_rows = _build_mismatch_rows_for_period(
        checklist_rows=final_checklist_rows,
        expected_pairs_by_account=expected_pairs_by_account,
    )

    return {
        "action": "generate" if not had_existing_checklists else "update",
        "previewOnly": False,
        "period": {
            "year": selected_year,
            "month": selected_month,
            "quarter": _quarter_for_month(selected_month),
            "value": _period_value(selected_year, selected_month),
            "label": _period_label(selected_year, selected_month),
        },
        "expectedAccountsCount": len(expected_pairs_by_account),
        "expectedStationRowsCount": sum(len(pairs) for pairs in expected_pairs_by_account.values()),
        "createdChecklistsCount": len(created_checklists),
        "createdStationsCount": len(created_station_rows),
        "mismatchStationCount": len(mismatch_rows),
        "plannedChecklistsCount": len(planned_checklists),
        "plannedStationsCount": len(planned_station_rows),
        "createdChecklists": created_checklists,
        "createdStations": created_station_rows,
        "plannedChecklists": planned_checklists,
        "plannedStations": planned_station_rows,
        "mismatchStations": mismatch_rows,
    }


def get_invoice_checklists_ui_load_data(
    *,
    year: int | None = None,
    month: int | None = None,
    checklist_id: str | None = None,
    include_selected_detail: bool = True,
) -> dict:
    if (year is None) != (month is None):
        raise ValueError("year and month must be provided together")

    normalized_year: int | None = None
    normalized_month: int | None = None
    if year is not None and month is not None:
        normalized_year = _ensure_optional_unsigned_int(year, field="year")
        normalized_month = _ensure_optional_unsigned_int(month, field="month")
        if normalized_year is None or normalized_month is None:
            raise ValueError("year and month are required")
        if normalized_year < _MIN_YEAR or normalized_year > _MAX_YEAR:
            raise ValueError(f"year must be between {_MIN_YEAR} and {_MAX_YEAR}")
        if normalized_month < 1 or normalized_month > 12:
            raise ValueError("month must be between 1 and 12")

    all_checklists = list_invoice_checklists_data()
    period_seen: set[str] = set()
    periods: list[dict] = []
    for row in all_checklists:
        row_year = _ensure_optional_unsigned_int(row.get("year"), field="year")
        row_month = _ensure_optional_unsigned_int(row.get("month"), field="month")
        if row_year is None or row_month is None:
            continue
        if row_month < 1 or row_month > 12:
            continue
        value = _period_value(row_year, row_month)
        if value in period_seen:
            continue
        period_seen.add(value)
        periods.append(
            {
                "year": row_year,
                "month": row_month,
                "quarter": _quarter_for_month(row_month),
                "value": value,
                "label": _period_label(row_year, row_month),
            }
        )
    periods.sort(
        key=lambda item: (
            -int(item.get("year") or 0),
            -int(item.get("month") or 0),
        )
    )

    selected_year: int
    selected_month: int
    if normalized_year is not None and normalized_month is not None:
        selected_year = normalized_year
        selected_month = normalized_month
    elif periods:
        selected_year = int(periods[0]["year"])
        selected_month = int(periods[0]["month"])
    else:
        today = date.today()
        selected_year = int(today.year)
        selected_month = int(today.month)

    selected_period = {
        "year": selected_year,
        "month": selected_month,
        "quarter": _quarter_for_month(selected_month),
        "value": _period_value(selected_year, selected_month),
        "label": _period_label(selected_year, selected_month),
    }

    selected_checklists = list_invoice_checklists_data(
        year=selected_year,
        month=selected_month,
    )
    account_name_by_code = _build_account_names_map(
        [str(item.get("accountCode") or "") for item in selected_checklists]
    )
    checklist_station_search_map = _build_checklist_station_search_map(
        [str(item.get("id") or "").strip() for item in selected_checklists]
    )
    expected_pairs_by_account = _build_expected_schedule_pairs_by_account(
        year=selected_year,
        month=selected_month,
    )
    mismatch_rows_for_period = _build_mismatch_rows_for_period(
        checklist_rows=selected_checklists,
        expected_pairs_by_account=expected_pairs_by_account,
    )
    mismatch_count_by_checklist: dict[str, int] = {}
    for row in mismatch_rows_for_period:
        checklist_row_id = str(row.get("checklistId") or "").strip()
        if not checklist_row_id:
            continue
        mismatch_count_by_checklist[checklist_row_id] = mismatch_count_by_checklist.get(checklist_row_id, 0) + 1

    checklist_summaries: list[dict] = []
    for item in selected_checklists:
        row = dict(item)
        checklist_row_id = str(row.get("id") or "").strip()
        account_code = str(row.get("accountCode") or "").strip().upper()
        row["accountCode"] = account_code
        row["accountName"] = account_name_by_code.get(account_code, "")
        row["quarter"] = _quarter_for_month(_ensure_optional_unsigned_int(row.get("month"), field="month"))
        row["searchStations"] = checklist_station_search_map.get(checklist_row_id, [])
        row["expectedStationCount"] = len(expected_pairs_by_account.get(account_code, set()))
        row["mismatchStationCount"] = mismatch_count_by_checklist.get(checklist_row_id, 0)
        row["hasScheduleMismatch"] = row["mismatchStationCount"] > 0
        checklist_summaries.append(row)

    selected_checklist_id = str(checklist_id or "").strip() or None
    if selected_checklist_id is None and checklist_summaries:
        first_id = str(checklist_summaries[0].get("id") or "").strip()
        selected_checklist_id = first_id or None

    selected_checklist: dict | None = None
    if selected_checklist_id and include_selected_detail:
        detail = get_invoice_checklists_data(
            checklist_id=selected_checklist_id,
            include_stations=True,
            include_notes=True,
            include_attachments=True,
        )
        if not isinstance(detail, dict):
            raise SafeDatabaseError("Failed to load checklist detail")
        selected_checklist = dict(detail)

        account_code = str(selected_checklist.get("accountCode") or "").strip().upper()
        selected_checklist["accountCode"] = account_code
        selected_checklist["accountName"] = account_name_by_code.get(
            account_code,
            _build_account_names_map([account_code]).get(account_code, ""),
        )
        selected_checklist["quarter"] = _quarter_for_month(
            _ensure_optional_unsigned_int(selected_checklist.get("month"), field="month")
        )

        station_codes = [
            str(item.get("stationCode") or "").strip().upper()
            for item in (selected_checklist.get("stations") or [])
            if isinstance(item, dict)
        ]
        station_metadata = _build_station_metadata(station_codes)
        stations: list[dict] = []
        expected_pairs_for_selected_checklist = expected_pairs_by_account.get(account_code, set())
        selected_checklist_mismatch_count = 0
        for station_row in (selected_checklist.get("stations") or []):
            if not isinstance(station_row, dict):
                continue
            station = dict(station_row)
            station_est_num = _ensure_optional_unsigned_int(station.get("estNum"), field="estNum")
            station_code = str(station.get("stationCode") or "").strip().upper()
            meta = station_metadata.get(station_code, {})
            station["stationCode"] = station_code
            station["stationName"] = str(meta.get("stationName") or "").strip()
            station["mediaType"] = str(meta.get("mediaType") or "").strip().upper()
            station["repContacts"] = meta.get("repContacts") if isinstance(meta.get("repContacts"), list) else []
            in_current_schedule = bool(
                station_est_num is not None
                and station_code
                and (int(station_est_num), station_code) in expected_pairs_for_selected_checklist
            )
            station["inCurrentSchedule"] = in_current_schedule
            station["scheduleMismatch"] = not in_current_schedule
            station["scheduleMismatchReason"] = (
                "NOT_IN_CURRENT_PERIOD_SCHEDULE" if not in_current_schedule else None
            )
            if not in_current_schedule:
                selected_checklist_mismatch_count += 1
            stations.append(station)
        selected_checklist["stations"] = stations
        selected_checklist["expectedStationCount"] = len(expected_pairs_for_selected_checklist)
        selected_checklist["mismatchStationCount"] = selected_checklist_mismatch_count
        selected_checklist["hasScheduleMismatch"] = selected_checklist_mismatch_count > 0

    return {
        "periods": periods,
        "selectedPeriod": selected_period,
        "checklists": checklist_summaries,
        "selectedChecklistId": selected_checklist_id,
        "selectedChecklist": selected_checklist,
        "mismatchStations": mismatch_rows_for_period,
    }
