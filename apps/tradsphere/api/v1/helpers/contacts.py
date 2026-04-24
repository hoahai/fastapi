from __future__ import annotations

import re

from apps.tradsphere.api.v1.helpers.accountValidation import (
    ensure_contact_ids_exist,
    ensure_station_codes_exist,
    ensure_stations_contact_ids_exist,
    invalidate_validation_cache,
)
from apps.tradsphere.api.v1.helpers.config import get_contact_types, get_default_contact_type
from apps.tradsphere.api.v1.helpers.dbQueries import (
    find_existing_emails,
    get_contacts,
    get_contacts_by_station_codes,
    get_stations_contacts,
    insert_contacts,
    insert_stations_contacts,
    update_contacts,
    update_stations_contacts,
)

_DUPLICATE_IN_PAYLOAD = "duplicate_in_payload"
_EMAIL_ALREADY_EXISTS = "email_already_exists"
_EMAIL_FORMAT_RE = re.compile(
    r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+$"
)
_OFFICE_EXT_RE = re.compile(r"^(?P<base>.+?)(?:\s*x(?P<ext>\d{1,6}))?$", re.IGNORECASE)
_PHONE_ALLOWED_RE = re.compile(r"^[\d\s().+-]+$")


class DuplicateContactsError(ValueError):
    def __init__(self, duplicated_contacts: list[dict]):
        super().__init__("Duplicate contacts found")
        self.duplicated_contacts = duplicated_contacts


def _ensure_list(payload: list[dict] | dict) -> list[dict]:
    if isinstance(payload, dict):
        return [payload]
    if not isinstance(payload, list):
        raise ValueError("Payload must be an object or an array of objects")
    return payload


def _normalize_email(value: object) -> str:
    return str(value or "").strip().lower()


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


def _ensure_email(value: object, *, field: str = "email") -> str:
    text = str(value or "").strip().lower()
    if not text:
        raise ValueError(f"{field} is required")
    if len(text) > 255:
        raise ValueError(f"{field} must be <= 255 characters")
    if not _EMAIL_FORMAT_RE.fullmatch(text):
        raise ValueError(f"{field} must be a valid email format")
    return text


def _ensure_first_name(value: object, *, required: bool) -> str | None:
    if value is None and required:
        return ""
    if value is None:
        return None
    text = str(value).strip()
    if len(text) > 255:
        raise ValueError("firstName must be <= 255 characters")
    return text


def _is_valid_us_phone_base(value: str) -> bool:
    if not value:
        return False
    if not _PHONE_ALLOWED_RE.fullmatch(value):
        return False
    if value.count("+") > 1:
        return False
    if "+" in value and not value.strip().startswith("+"):
        return False
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) == 10:
        return True
    return len(digits) == 11 and digits.startswith("1")


def _ensure_phone(
    value: object,
    *,
    field: str,
    max_length: int,
    allow_extension: bool,
) -> str | None:
    text = _ensure_optional_text(value, field=field, max_length=max_length)
    if text is None:
        return None

    base = text
    if allow_extension:
        match = _OFFICE_EXT_RE.fullmatch(text)
        if not match:
            raise ValueError(f"{field} must be a US phone format; optional extension x####")
        base = str(match.group("base") or "").strip()
    elif re.search(r"\bx\d+\s*$", text, flags=re.IGNORECASE):
        raise ValueError(f"{field} cannot include extension; use office for x####")

    if not _is_valid_us_phone_base(base):
        raise ValueError(f"{field} must be all digits (10/11) or valid US phone format")
    return text


def _parse_contact_name(value: object) -> tuple[str | None, str | None]:
    text = str(value or "").strip()
    if not text:
        return None, None
    parts = [part for part in text.split() if part]
    if not parts:
        return None, None
    first_name = parts[0]
    last_name = " ".join(parts[1:]) if len(parts) > 1 else None
    return first_name, last_name


def _resolve_contact_name_values(
    row: dict,
    *,
    required_first_name: bool,
) -> tuple[str | None, str | None]:
    has_first_name = "firstName" in row
    has_last_name = "lastName" in row
    use_name_field = "name" in row and not (has_first_name and has_last_name)

    parsed_first_name: str | None = None
    parsed_last_name: str | None = None
    if use_name_field:
        parsed_first_name, parsed_last_name = _parse_contact_name(row.get("name"))

    first_name_source: object = row.get("firstName") if has_first_name else parsed_first_name
    last_name_source: object = row.get("lastName") if has_last_name else parsed_last_name

    normalized_first_name = _ensure_first_name(
        first_name_source,
        required=required_first_name,
    )
    normalized_last_name = _ensure_optional_text(
        last_name_source,
        field="lastName",
        max_length=255,
    )
    return normalized_first_name, normalized_last_name


def _normalize_contact_type(value: object) -> str:
    normalized = str(value or "").strip().upper()
    allowed = set(get_contact_types())
    if not normalized:
        raise ValueError("contactType is required")
    if normalized not in allowed:
        raise ValueError(
            f"Invalid contactType: {normalized}. Allowed values: {', '.join(sorted(allowed))}"
        )
    return normalized


def _build_duplicate_contact_item(
    *,
    index: int,
    row: dict,
    reason: str,
    existing_contact_id: int | None = None,
) -> dict:
    payload = {
        "index": index,
        "reason": reason,
        "contact": dict(row),
    }
    email = _normalize_email(row.get("email"))
    if email:
        payload["email"] = email
    if "id" in row:
        payload["id"] = int(row.get("id"))
    if existing_contact_id is not None:
        payload["existingContactId"] = int(existing_contact_id)
    return payload


def _split_create_rows_and_duplicates(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    email_counts: dict[str, int] = {}
    for row in rows:
        email = _normalize_email(row.get("email"))
        if not email:
            raise ValueError("email is required")
        email_counts[email] = email_counts.get(email, 0) + 1

    rows_to_insert: list[dict] = []
    duplicated_contacts: list[dict] = []
    pending_rows: list[tuple[int, dict, str]] = []
    for index, row in enumerate(rows):
        email = _normalize_email(row.get("email"))
        if email_counts.get(email, 0) > 1:
            duplicated_contacts.append(
                _build_duplicate_contact_item(
                    index=index,
                    row=row,
                    reason=_DUPLICATE_IN_PAYLOAD,
                )
            )
            continue
        pending_rows.append((index, row, email))

    existing_rows = find_existing_emails(emails=[email for _, _, email in pending_rows])
    existing_by_email = {_normalize_email(item.get("email")): int(item.get("id")) for item in existing_rows}
    for index, row, email in pending_rows:
        existing_id = existing_by_email.get(email)
        if existing_id is not None:
            duplicated_contacts.append(
                _build_duplicate_contact_item(
                    index=index,
                    row=row,
                    reason=_EMAIL_ALREADY_EXISTS,
                    existing_contact_id=existing_id,
                )
            )
            continue
        rows_to_insert.append(row)
    return rows_to_insert, duplicated_contacts


def _split_update_rows_and_duplicates(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    email_to_ids: dict[str, set[int]] = {}
    rows_with_email: list[tuple[int, dict, str]] = []
    for index, row in enumerate(rows):
        row_id = row.get("id")
        if row_id is None:
            raise ValueError("id is required for contacts update")
        if "email" not in row:
            continue
        email = _normalize_email(row.get("email"))
        if not email:
            raise ValueError("email cannot be empty")
        rows_with_email.append((index, row, email))
        email_to_ids.setdefault(email, set()).add(int(row_id))

    duplicate_payload_emails = {
        email for email, contact_ids in email_to_ids.items() if len(contact_ids) > 1
    }
    duplicated_indexes: set[int] = set()
    duplicated_contacts: list[dict] = []
    rows_to_check_existing: list[tuple[int, dict, str]] = []
    for index, row, email in rows_with_email:
        if email in duplicate_payload_emails:
            duplicated_indexes.add(index)
            duplicated_contacts.append(
                _build_duplicate_contact_item(
                    index=index,
                    row=row,
                    reason=_DUPLICATE_IN_PAYLOAD,
                )
            )
            continue
        rows_to_check_existing.append((index, row, email))

    existing = find_existing_emails(emails=[email for _, _, email in rows_to_check_existing])
    existing_by_email = {_normalize_email(row.get("email")): int(row.get("id")) for row in existing}
    for index, row, email in rows_to_check_existing:
        existing_id = existing_by_email.get(email)
        if existing_id is None or int(row.get("id")) == existing_id:
            continue
        duplicated_indexes.add(index)
        duplicated_contacts.append(
            _build_duplicate_contact_item(
                index=index,
                row=row,
                reason=_EMAIL_ALREADY_EXISTS,
                existing_contact_id=existing_id,
            )
        )

    rows_to_update = [row for index, row in enumerate(rows) if index not in duplicated_indexes]
    return rows_to_update, duplicated_contacts


def list_contacts_data(
    *,
    emails: list[str] | None = None,
    name: str | None = None,
    contact_type: str | None = None,
    active: bool | None = None,
) -> list[dict]:
    rows = get_contacts(
        emails=emails or [],
        name=name,
        contact_type=_normalize_contact_type(contact_type) if contact_type else None,
        active=active,
    )
    for row in rows:
        station_codes = [
            str(code or "").strip().upper()
            for code in str(row.get("stationCodes") or "").split(",")
            if str(code or "").strip()
        ]
        row["stationCodes"] = list(dict.fromkeys(station_codes))
    return rows


def list_contacts_by_station_codes_data(
    *,
    station_codes: list[str],
    contact_types: list[str] | None = None,
) -> list[dict]:
    if not station_codes:
        raise ValueError("codes is required")
    normalized_station_codes = [str(code or "").strip().upper() for code in station_codes]
    normalized_station_codes = [code for code in normalized_station_codes if code]
    if not normalized_station_codes:
        raise ValueError("codes is required")
    ensure_station_codes_exist(normalized_station_codes)

    normalized_contact_types: list[str] = []
    if contact_types:
        normalized_contact_types = [_normalize_contact_type(value) for value in contact_types]
        normalized_contact_types = list(dict.fromkeys(normalized_contact_types))

    rows = get_contacts_by_station_codes(
        station_codes=normalized_station_codes,
        contact_types=normalized_contact_types,
    )
    grouped: dict[str, list[dict]] = {code: [] for code in normalized_station_codes}
    for row in rows:
        station_code = str(row.get("stationCode") or "").strip().upper()
        if not station_code:
            continue
        grouped.setdefault(station_code, []).append(
            {
                "id": row.get("id"),
                "email": row.get("email"),
                "firstName": row.get("firstName"),
                "lastName": row.get("lastName"),
                "company": row.get("company"),
                "jobTitle": row.get("jobTitle"),
                "office": row.get("office"),
                "cell": row.get("cell"),
                "active": row.get("active"),
                "note": row.get("note"),
                "contactType": row.get("contactType"),
                "primaryContact": row.get("primaryContact"),
                "contactTypeNote": row.get("contactTypeNote"),
            }
        )
    return [{"stationCode": code, "contacts": grouped.get(code, [])} for code in normalized_station_codes]


def create_contacts_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    if not rows:
        return {"inserted": 0}
    normalized_rows: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each contacts item must be an object")
        email = _ensure_email(row.get("email"))
        first_name, last_name = _resolve_contact_name_values(
            row,
            required_first_name=True,
        )
        normalized_rows.append(
            {
                "email": email,
                "firstName": first_name,
                "lastName": last_name,
                "company": _ensure_optional_text(
                    row.get("company"),
                    field="company",
                    max_length=255,
                ),
                "jobTitle": _ensure_optional_text(
                    row.get("jobTitle"),
                    field="jobTitle",
                    max_length=255,
                ),
                "office": _ensure_phone(
                    row.get("office"),
                    field="office",
                    max_length=35,
                    allow_extension=True,
                ),
                "cell": _ensure_phone(
                    row.get("cell"),
                    field="cell",
                    max_length=20,
                    allow_extension=False,
                ),
                "active": row.get("active"),
                "note": _ensure_optional_text(
                    row.get("note"),
                    field="note",
                    max_length=2048,
                ),
            }
        )

    rows_to_insert, duplicated_contacts = _split_create_rows_and_duplicates(normalized_rows)
    if duplicated_contacts:
        raise DuplicateContactsError(duplicated_contacts)
    inserted = insert_contacts(rows_to_insert)
    if inserted > 0:
        invalidate_validation_cache()
    return {"inserted": inserted}


def modify_contacts_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    if not rows:
        return {"updated": 0}
    normalized_rows: list[dict] = []
    ids_to_validate: list[int] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each contacts item must be an object")
        row_id = row.get("id")
        if row_id is None:
            raise ValueError("id is required for contacts update")
        item: dict[str, object] = {"id": int(row_id)}
        ids_to_validate.append(int(row_id))
        has_first_name = "firstName" in row
        has_last_name = "lastName" in row
        use_name_field = "name" in row and not (has_first_name and has_last_name)

        normalized_first_name: str | None = None
        normalized_last_name: str | None = None
        if has_first_name or has_last_name or use_name_field:
            normalized_first_name, normalized_last_name = _resolve_contact_name_values(
                row,
                required_first_name=False,
            )

        if "email" in row:
            item["email"] = _ensure_email(row.get("email"), field="email")
        if has_first_name or use_name_field:
            item["firstName"] = (
                normalized_first_name if normalized_first_name is not None else ""
            )
        if has_last_name or use_name_field:
            item["lastName"] = normalized_last_name
        if "company" in row:
            item["company"] = _ensure_optional_text(
                row.get("company"),
                field="company",
                max_length=255,
            )
        if "jobTitle" in row:
            item["jobTitle"] = _ensure_optional_text(
                row.get("jobTitle"),
                field="jobTitle",
                max_length=255,
            )
        if "office" in row:
            item["office"] = _ensure_phone(
                row.get("office"),
                field="office",
                max_length=35,
                allow_extension=True,
            )
        if "cell" in row:
            item["cell"] = _ensure_phone(
                row.get("cell"),
                field="cell",
                max_length=20,
                allow_extension=False,
            )
        if "active" in row:
            item["active"] = row.get("active")
        if "note" in row:
            item["note"] = _ensure_optional_text(
                row.get("note"),
                field="note",
                max_length=2048,
            )
        if len(item) == 1:
            raise ValueError(f"No updatable fields provided for contact id '{row_id}'")
        normalized_rows.append(item)

    ensure_contact_ids_exist(ids_to_validate)
    rows_to_update, duplicated_contacts = _split_update_rows_and_duplicates(normalized_rows)
    if duplicated_contacts:
        raise DuplicateContactsError(duplicated_contacts)
    updated = update_contacts(rows_to_update)
    if updated > 0:
        invalidate_validation_cache()
    return {"updated": updated}


def list_stations_contacts_data(
    *,
    ids: list[int] | None = None,
    station_codes: list[str] | None = None,
    contact_ids: list[int] | None = None,
    active: bool | None = None,
) -> list[dict]:
    normalized_station_codes = [
        str(code or "").strip().upper()
        for code in (station_codes or [])
        if str(code or "").strip()
    ]
    return get_stations_contacts(
        ids=ids or [],
        station_codes=normalized_station_codes,
        contact_ids=contact_ids or [],
        active=active,
    )


def create_stations_contacts_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    if not rows:
        return {"inserted": 0}

    normalized_rows: list[dict] = []
    station_codes_to_validate: list[str] = []
    contact_ids_to_validate: list[int] = []
    default_contact_type = get_default_contact_type()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each stationsContacts item must be an object")
        station_code = str(row.get("stationCode") or "").strip().upper()
        if not station_code:
            raise ValueError("stationCode is required")
        contact_id = row.get("contactId")
        if contact_id is None:
            raise ValueError("contactId is required")
        if "contactType" in row and str(row.get("contactType") or "").strip():
            contact_type = _normalize_contact_type(row.get("contactType"))
        else:
            contact_type = default_contact_type

        station_codes_to_validate.append(station_code)
        contact_ids_to_validate.append(int(contact_id))
        normalized_rows.append(
            {
                "stationCode": station_code,
                "contactId": int(contact_id),
                "contactType": contact_type,
                "primaryContact": row.get("primaryContact"),
                "note": row.get("note"),
                "active": row.get("active"),
            }
        )

    ensure_station_codes_exist(station_codes_to_validate)
    ensure_contact_ids_exist(contact_ids_to_validate)
    inserted = insert_stations_contacts(normalized_rows)
    invalidate_validation_cache()
    return {"inserted": inserted}


def modify_stations_contacts_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_list(payload)
    if not rows:
        return {"updated": 0}

    normalized_rows: list[dict] = []
    ids_to_validate: list[int] = []
    station_codes_to_validate: list[str] = []
    contact_ids_to_validate: list[int] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each stationsContacts item must be an object")
        row_id = row.get("id")
        if row_id is None:
            raise ValueError("id is required for stationsContacts update")
        ids_to_validate.append(int(row_id))
        item: dict[str, object] = {"id": int(row_id)}

        if "stationCode" in row:
            station_code = str(row.get("stationCode") or "").strip().upper()
            if not station_code:
                raise ValueError("stationCode cannot be empty")
            item["stationCode"] = station_code
            station_codes_to_validate.append(station_code)

        if "contactId" in row:
            contact_id = row.get("contactId")
            if contact_id is None:
                raise ValueError("contactId cannot be null")
            item["contactId"] = int(contact_id)
            contact_ids_to_validate.append(int(contact_id))

        if "contactType" in row:
            item["contactType"] = _normalize_contact_type(row.get("contactType"))
        if "primaryContact" in row:
            item["primaryContact"] = row.get("primaryContact")
        if "note" in row:
            item["note"] = row.get("note")
        if "active" in row:
            item["active"] = row.get("active")

        if len(item) == 1:
            raise ValueError(f"No updatable fields provided for stationsContacts id '{row_id}'")
        normalized_rows.append(item)

    ensure_stations_contact_ids_exist(ids_to_validate)
    if station_codes_to_validate:
        ensure_station_codes_exist(station_codes_to_validate)
    if contact_ids_to_validate:
        ensure_contact_ids_exist(contact_ids_to_validate)

    existing_rows = get_stations_contacts(ids=ids_to_validate)
    existing_by_id: dict[int, dict] = {
        int(row.get("id")): row
        for row in existing_rows
        if row.get("id") is not None
    }

    rows_to_update: list[dict] = []
    rows_to_deactivate: list[dict] = []
    rows_to_insert: list[dict] = []

    for item in normalized_rows:
        row_id = int(item["id"])
        existing = existing_by_id.get(row_id)
        if not isinstance(existing, dict):
            raise ValueError(f"stationsContacts id '{row_id}' not found")

        next_station_code = str(
            item.get("stationCode", existing.get("stationCode")) or ""
        ).strip().upper()
        next_contact_id = int(item.get("contactId", existing.get("contactId")))
        next_contact_type = str(
            item.get("contactType", existing.get("contactType")) or ""
        ).strip().upper()

        current_station_code = str(existing.get("stationCode") or "").strip().upper()
        current_contact_id = int(existing.get("contactId"))
        current_contact_type = str(existing.get("contactType") or "").strip().upper()

        identity_changed = (
            next_station_code != current_station_code
            or next_contact_id != current_contact_id
            or next_contact_type != current_contact_type
        )

        if not identity_changed:
            rows_to_update.append(item)
            continue

        rows_to_deactivate.append({"id": row_id, "active": False})
        rows_to_insert.append(
            {
                "stationCode": next_station_code,
                "contactId": next_contact_id,
                "contactType": next_contact_type,
                "primaryContact": item.get(
                    "primaryContact",
                    existing.get("primaryContact"),
                ),
                "note": item.get("note", existing.get("note")),
                "active": item.get("active", existing.get("active")),
            }
        )

    updated = 0
    if rows_to_insert:
        updated += insert_stations_contacts(rows_to_insert)
    if rows_to_deactivate:
        updated += update_stations_contacts(rows_to_deactivate)
    if rows_to_update:
        updated += update_stations_contacts(rows_to_update)

    if updated > 0:
        invalidate_validation_cache()
    return {"updated": updated}


def deactivate_stations_contacts_data(*, ids: list[int]) -> dict[str, int]:
    normalized_ids = [int(item) for item in (ids or [])]
    normalized_ids = list(dict.fromkeys(normalized_ids))
    if not normalized_ids:
        raise ValueError("ids is required")

    ensure_stations_contact_ids_exist(normalized_ids)
    rows_to_deactivate = [{"id": row_id, "active": False} for row_id in normalized_ids]
    updated = update_stations_contacts(rows_to_deactivate)

    if updated > 0:
        invalidate_validation_cache()
    return {"updated": updated}
