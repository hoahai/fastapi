from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
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
    delete_inv_checklist,
    delete_inv_checklist_note,
    delete_inv_checklist_station,
    delete_inv_note_attachment,
    invalidate_inv_checklist_related_cache_for_bulk_write,
    list_inv_checklist_note_detail_rows,
    list_inv_checklist_stations,
    list_inv_checklist_station_rows_for_checklists,
    list_inv_checklist_station_search_rows,
    get_inv_checklist_detail_rows,
    get_inv_checklist_note_row,
    get_inv_checklist_row,
    get_inv_checklist_station_row,
    get_inv_note_attachment_row,
    insert_inv_checklist,
    insert_inv_checklist_note,
    insert_inv_checklist_station,
    insert_inv_note_attachment,
    list_inv_checklists,
    list_inv_note_attachments,
    list_schedule_invoice_checklist_expected_rows,
    update_inv_checklist,
    update_inv_checklist_note,
    update_inv_checklist_station,
    update_inv_note_attachment,
)

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


def _ensure_required_amount(value: object) -> Decimal:
    text = str(value or "").strip()
    if not text:
        raise ValueError("amount is required")
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
        value = out.get(key)
        if hasattr(value, "isoformat"):
            out[key] = value.isoformat()
    return out


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

    return {
        "checklistStationId": int(station_row_id),
        "amount": _ensure_required_amount(payload.get("amount")),
        "note": _ensure_required_text(payload.get("note"), field="note", max_length=2048),
    }


def _normalize_note_updates(payload: dict) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    updates: dict[str, object] = {}
    if "amount" in payload:
        updates["amount"] = _ensure_required_amount(payload.get("amount"))
    if "note" in payload:
        updates["note"] = _ensure_required_text(payload.get("note"), field="note", max_length=2048)
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


def _serialize_note_row(row: dict) -> dict:
    out = _serialize_datetime_fields(row)
    amount_value = out.get("amount")
    if amount_value is not None:
        out["amount"] = float(amount_value)
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
        "dateCreated": first.get("checklistDateCreated").isoformat() if first.get("checklistDateCreated") else None,
        "dateUpdated": first.get("checklistDateUpdated").isoformat() if first.get("checklistDateUpdated") else None,
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
                "dateCreated": row.get("stationDateCreated").isoformat() if row.get("stationDateCreated") else None,
                "dateUpdated": row.get("stationDateUpdated").isoformat() if row.get("stationDateUpdated") else None,
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
                "amount": float(amount_value) if amount_value is not None else 0.0,
                "note": row.get("noteText"),
                "dateCreated": row.get("noteDateCreated").isoformat() if row.get("noteDateCreated") else None,
                "dateUpdated": row.get("noteDateUpdated").isoformat() if row.get("noteDateUpdated") else None,
            }
            if include_attachments_value:
                note_entry["attachments"] = []
            notes_by_id[note_id] = note_entry
            station_entry["notes"].append(note_entry)

        if not include_attachments_value:
            continue

        attachment_row_id = row.get("attachmentId")
        if attachment_row_id is None:
            continue
        note_entry["attachments"].append(
            {
                "id": int(attachment_row_id),
                "noteId": note_id,
                "url": row.get("attachmentUrl"),
                "fileName": row.get("attachmentFileName"),
                "fileType": row.get("attachmentFileType"),
                "dateCreated": row.get("attachmentDateCreated").isoformat() if row.get("attachmentDateCreated") else None,
                "dateUpdated": row.get("attachmentDateUpdated").isoformat() if row.get("attachmentDateUpdated") else None,
            }
        )

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
                "estNum": int(row.get("estNum")) if row.get("estNum") is not None else None,
                "stationCode": str(row.get("stationCode") or "").strip().upper() or None,
                "amount": float(amount_value) if amount_value is not None else 0.0,
                "note": row.get("note"),
                "dateCreated": row.get("dateCreated").isoformat() if row.get("dateCreated") else None,
                "dateUpdated": row.get("dateUpdated").isoformat() if row.get("dateUpdated") else None,
            }
            if include_attachments:
                note_entry["attachments"] = []
            notes_by_id[parsed_note_id] = note_entry

        if not include_attachments:
            continue
        attachment_row_id = row.get("attachmentId")
        if attachment_row_id is None:
            continue
        note_entry["attachments"].append(
            {
                "id": int(attachment_row_id),
                "noteId": int(row.get("attachmentNoteId")),
                "url": row.get("attachmentUrl"),
                "fileName": row.get("attachmentFileName"),
                "fileType": row.get("attachmentFileType"),
                "dateCreated": row.get("attachmentDateCreated").isoformat() if row.get("attachmentDateCreated") else None,
                "dateUpdated": row.get("attachmentDateUpdated").isoformat() if row.get("attachmentDateUpdated") else None,
            }
        )

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
    if _safe_db_call(get_inv_checklist_note_row, note_id=note_id_value) is None:
        raise NotFoundError(f"Checklist note not found: {note_id_value}")

    updates = _normalize_note_updates(payload)
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
    note_id_value = _ensure_optional_unsigned_int(note_id, field="noteId")
    attachment_id_value = _ensure_optional_unsigned_int(attachment_id, field="attachmentId")
    rows = _safe_db_call(
        list_inv_note_attachments,
        note_id=note_id_value,
        attachment_id=attachment_id_value,
    )
    return [_serialize_datetime_fields(row) for row in rows]


def create_invoice_note_attachment_data(*, payload: dict) -> dict:
    normalized = _normalize_attachment_payload(payload)
    try:
        attachment_id = insert_inv_note_attachment(normalized)
    except Exception as exc:
        raise _map_db_exception(exc) from exc

    row = _safe_db_call(get_inv_note_attachment_row, attachment_id=attachment_id)
    if row is None:
        raise SafeDatabaseError("Failed to read created note attachment")
    return _serialize_datetime_fields(row)


def update_invoice_note_attachment_data(*, payload: dict) -> dict:
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")
    attachment_id_value = _ensure_required_unsigned_int(payload.get("attachmentId"), field="attachmentId")
    if _safe_db_call(get_inv_note_attachment_row, attachment_id=attachment_id_value) is None:
        raise NotFoundError(f"Note attachment not found: {attachment_id_value}")

    updates = _normalize_attachment_updates(payload)
    try:
        update_inv_note_attachment(attachment_id=attachment_id_value, fields=updates)
    except Exception as exc:
        raise _map_db_exception(exc) from exc

    row = _safe_db_call(get_inv_note_attachment_row, attachment_id=attachment_id_value)
    if row is None:
        raise NotFoundError(f"Note attachment not found: {attachment_id_value}")
    return _serialize_datetime_fields(row)


def delete_invoice_note_attachment_data(*, attachment_id: int) -> dict:
    attachment_id_value = _ensure_required_unsigned_int(attachment_id, field="attachmentId")
    if _safe_db_call(get_inv_note_attachment_row, attachment_id=attachment_id_value) is None:
        raise NotFoundError(f"Note attachment not found: {attachment_id_value}")
    try:
        deleted = delete_inv_note_attachment(attachment_id=attachment_id_value)
    except Exception as exc:
        raise _map_db_exception(exc) from exc
    return {"deleted": int(deleted > 0), "id": attachment_id_value}


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

    for account_code in sorted(expected_pairs_by_account.keys()):
        if account_code in checklist_by_account:
            continue
        was_created = False
        try:
            created = create_invoice_checklist_data(
                {
                    "accountCode": account_code,
                    "year": selected_year,
                    "month": selected_month,
                    "status": None,
                    "note": None,
                }
            )
            was_created = True
        except ConflictError:
            rows = list_invoice_checklists_data(
                account_code=account_code,
                year=selected_year,
                month=selected_month,
            )
            created = rows[0] if rows else None
        if not created:
            continue
        if was_created:
            created_checklists.append(created)
        checklist_by_account[account_code] = created

    checklist_rows = list_invoice_checklists_data(
        year=selected_year,
        month=selected_month,
    )
    checklist_id_to_row = {
        str(row.get("id") or "").strip(): row
        for row in checklist_rows
        if str(row.get("id") or "").strip()
    }

    station_rows = _safe_db_call(
        list_inv_checklist_station_rows_for_checklists,
        checklist_ids=list(checklist_id_to_row.keys()),
    )
    existing_station_pairs_by_checklist: dict[str, set[tuple[int, str]]] = {}
    for station_row in station_rows:
        checklist_id = str(station_row.get("checklistId") or "").strip()
        est_num = _ensure_optional_unsigned_int(station_row.get("estNum"), field="estNum")
        station_code = str(station_row.get("stationCode") or "").strip().upper()
        if not checklist_id or est_num is None or not station_code:
            continue
        existing_station_pairs_by_checklist.setdefault(checklist_id, set()).add((int(est_num), station_code))

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
            try:
                station_id = insert_inv_checklist_station(
                    {
                        "checklistId": checklist_id,
                        "estNum": est_num,
                        "stationCode": station_code,
                        "status": None,
                    }
                )
            except Exception as exc:
                mapped = _map_db_exception(exc)
                if isinstance(mapped, ConflictError):
                    existing_pairs.add((est_num, station_code))
                    continue
                raise mapped from exc
            created_station_rows.append(
                {
                    "id": int(station_id),
                    "checklistId": checklist_id,
                    "accountCode": account_code,
                    "estNum": est_num,
                    "stationCode": station_code,
                }
            )
            existing_pairs.add((est_num, station_code))

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
    if selected_checklist_id:
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
