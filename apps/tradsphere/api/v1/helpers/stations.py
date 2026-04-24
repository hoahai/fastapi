from __future__ import annotations

from apps.tradsphere.api.v1.helpers.accountValidation import (
    ensure_delivery_method_ids_exist,
    ensure_station_codes_exist,
    invalidate_validation_cache,
)
from apps.tradsphere.api.v1.helpers.config import get_media_types
from apps.tradsphere.api.v1.helpers.dbQueries import (
    get_contacts_by_station_codes,
    get_delivery_methods,
    get_station_media_types,
    get_stations,
    insert_delivery_methods,
    insert_stations,
    update_delivery_methods,
    update_stations,
)

_LANGUAGE_VALUES = {
    "EN": "English",
    "ENGLISH": "English",
    "ES": "Spanish",
    "SPANISH": "Spanish",
}


def _ensure_media_type(value: object) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise ValueError("mediaType is required")
    allowed = set(get_media_types())
    if normalized not in allowed:
        raise ValueError(
            f"Invalid mediaType: {normalized}. Allowed values: {', '.join(sorted(allowed))}"
        )
    return normalized


def _ensure_language(value: object) -> str:
    normalized = str(value or "").strip().upper()
    if not normalized:
        raise ValueError("language is required")
    mapped = _LANGUAGE_VALUES.get(normalized)
    if not mapped:
        raise ValueError("language must be English/Spanish (or EN/ES)")
    return mapped


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


def _ensure_optional_unsigned_int(value: object, *, field: str) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an unsigned integer") from exc
    if parsed < 0:
        raise ValueError(f"{field} must be an unsigned integer")
    return parsed


def _normalize_delivery_method_inline(inline: dict) -> dict:
    if not isinstance(inline, dict):
        raise ValueError("deliveryMethod must be an object when provided")
    return {
        "name": _ensure_required_text(
            inline.get("name"),
            field="deliveryMethod.name",
            max_length=255,
        ),
        "url": _ensure_required_text(
            inline.get("url"),
            field="deliveryMethod.url",
            max_length=2048,
        ),
        "username": _ensure_required_text(
            inline.get("username"),
            field="deliveryMethod.username",
            max_length=255,
        ),
        "password": inline.get("password"),
        "deadline": _ensure_required_text(
            inline.get("deadline") or "10 AM",
            field="deliveryMethod.deadline",
            max_length=50,
        ),
        "note": _ensure_optional_text(
            inline.get("note"),
            field="deliveryMethod.note",
            max_length=2048,
        ),
    }


def _ensure_payload_list(payload: list[dict] | dict, *, allow_object: bool = True) -> list[dict]:
    if isinstance(payload, dict):
        if not allow_object:
            raise ValueError("Payload must be an array of objects")
        return [payload]
    if not isinstance(payload, list):
        raise ValueError("Payload must be an object or an array of objects")
    return payload


def _resolve_station_delivery_method_id(
    row: dict,
    *,
    required: bool,
) -> int | None:
    if "deliveryMethod" in row:
        raise ValueError(
            "Inline deliveryMethod is not supported in /stations. "
            "Use /stations/deliveryMethods and pass deliveryMethodId."
        )

    if "deliveryMethodId" not in row or row.get("deliveryMethodId") is None:
        if required:
            raise ValueError("deliveryMethodId is required")
        return None

    delivery_method_id = _ensure_optional_unsigned_int(
        row.get("deliveryMethodId"),
        field="deliveryMethodId",
    )
    if delivery_method_id is None:
        if required:
            raise ValueError("deliveryMethodId is required")
        return None
    ensure_delivery_method_ids_exist([delivery_method_id])
    return delivery_method_id


def _serialize_syscode_for_media_type(
    media_type: object,
    syscode: object,
) -> object | None:
    if str(media_type or "").strip().upper() != "CA":
        return None
    return syscode


def _ensure_syscode_required_for_ca(
    *,
    media_type: str,
    syscode: int | None,
) -> None:
    if media_type == "CA" and syscode is None:
        raise ValueError("syscode is required when mediaType is CA")


def _validate_syscode_for_media_type(
    *,
    media_type: str,
    has_syscode_field: bool,
    syscode: int | None,
    require_syscode_for_ca: bool,
) -> None:
    if media_type == "CA":
        if require_syscode_for_ca and not has_syscode_field:
            raise ValueError("syscode is required when mediaType is CA")
        if has_syscode_field:
            _ensure_syscode_required_for_ca(media_type=media_type, syscode=syscode)
        return
    if has_syscode_field:
        raise ValueError("syscode is only allowed when mediaType is CA")


def _serialize_station_row(
    row: dict,
    *,
    delivery_method_detail: bool,
) -> dict:
    delivery_method_id = row.get("deliveryMethodId")
    station: dict[str, object] = {
        "code": row.get("code"),
        "name": row.get("name"),
        "affiliation": row.get("affiliation"),
        "mediaType": row.get("mediaType"),
        "language": row.get("language"),
        "ownership": row.get("ownership"),
        "deliveryMethodId": delivery_method_id,
        "note": row.get("note"),
    }
    delivery_method = None
    if delivery_method_id is not None:
        delivery_method = {
            "id": delivery_method_id,
            "name": row.get("deliveryMethodName"),
        }
        if delivery_method_detail:
            delivery_method["url"] = row.get("deliveryMethodUrl")
            delivery_method["username"] = row.get("deliveryMethodUsername")
            delivery_method["deadline"] = row.get("deliveryMethodDeadline")
            delivery_method["note"] = row.get("deliveryMethodNote")
    station["deliveryMethod"] = delivery_method

    serialized_syscode = _serialize_syscode_for_media_type(
        row.get("mediaType"),
        row.get("syscode"),
    )
    if serialized_syscode is not None:
        station["syscode"] = serialized_syscode
    return station


def _build_rep_contact_short_name(row: dict) -> str:
    first_name = str(row.get("firstName") or "").strip()
    last_name = str(row.get("lastName") or "").strip()
    full_name = " ".join(part for part in [first_name, last_name] if part).strip()
    return full_name


def _build_station_contacts_map(
    station_codes: list[str],
    *,
    contact_detail: bool,
) -> dict[str, dict[str, list[object]]]:
    normalized_codes = [str(code or "").strip().upper() for code in station_codes]
    normalized_codes = [code for code in normalized_codes if code]
    normalized_codes = list(dict.fromkeys(normalized_codes))
    if not normalized_codes:
        return {}

    rows = get_contacts_by_station_codes(
        station_codes=normalized_codes,
        contact_types=[],
    )
    grouped: dict[str, dict[str, list[object]]] = {code: {} for code in normalized_codes}
    for row in rows:
        station_code = str(row.get("stationCode") or "").strip().upper()
        if not station_code:
            continue
        contact_type = str(row.get("contactType") or "").strip().upper() or "UNKNOWN"
        station_group = grouped.setdefault(station_code, {})
        contact_bucket = station_group.setdefault(contact_type, [])
        email = str(row.get("email") or "").strip()

        if contact_type != "REP":
            if email and email not in contact_bucket:
                contact_bucket.append(email)
            continue

        if not contact_detail:
            contact_bucket.append(
                {
                    "id": row.get("id"),
                    "name": _build_rep_contact_short_name(row),
                    "email": email,
                }
            )
            continue

        contact_bucket.append(
            {
                "id": row.get("id"),
                "email": email,
                "firstName": row.get("firstName"),
                "lastName": row.get("lastName"),
                "company": row.get("company"),
                "jobTitle": row.get("jobTitle"),
                "office": row.get("office"),
                "cell": row.get("cell"),
                "active": row.get("active"),
                "note": row.get("note"),
                "primaryContact": row.get("primaryContact"),
                "contactTypeNote": row.get("contactTypeNote"),
            }
        )
    return grouped


def map_station_names_by_codes(codes: list[str] | None = None) -> dict[str, str]:
    normalized_codes = [
        str(code or "").strip().upper()
        for code in (codes or [])
        if str(code or "").strip()
    ]
    normalized_codes = list(dict.fromkeys(normalized_codes))
    if not normalized_codes:
        return {}

    rows = get_stations(
        codes=normalized_codes,
        account_codes=[],
        est_nums=[],
        delivery_method_detail=False,
    )
    mapped: dict[str, str] = {}
    for row in rows:
        code = str(row.get("code") or "").strip().upper()
        if not code:
            continue
        mapped[code] = str(row.get("name") or "").strip()
    return mapped


def list_stations_data(
    *,
    codes: list[str] | None = None,
    account_code: str | None = None,
    est_num: int | None = None,
    delivery_method_detail: bool = False,
    contact_detail: bool = False,
) -> list[dict]:
    normalized_codes = [
        str(code or "").strip().upper()
        for code in (codes or [])
        if str(code or "").strip()
    ]
    normalized_codes = list(dict.fromkeys(normalized_codes))

    normalized_account_code = str(account_code or "").strip().upper()

    normalized_est_num: int | None = None
    if est_num is not None:
        try:
            parsed = int(est_num)
        except (TypeError, ValueError) as exc:
            raise ValueError("estNum must be an unsigned integer") from exc
        if parsed < 0 or parsed > 4294967295:
            raise ValueError("estNum must be an unsigned integer")
        normalized_est_num = parsed

    if (
        not normalized_codes
        and not normalized_account_code
        and normalized_est_num is None
    ):
        raise ValueError(
            "At least one of codes, accountCode, estNum is required"
        )

    rows = get_stations(
        codes=normalized_codes,
        account_codes=[normalized_account_code] if normalized_account_code else [],
        est_nums=[normalized_est_num] if normalized_est_num is not None else [],
        delivery_method_detail=delivery_method_detail,
    )
    serialized_rows = [
        _serialize_station_row(
            row,
            delivery_method_detail=delivery_method_detail,
        )
        for row in rows
    ]
    if serialized_rows:
        station_contacts = _build_station_contacts_map(
            [
                str(item.get("code") or "").strip().upper()
                for item in serialized_rows
            ],
            contact_detail=contact_detail,
        )
        for station in serialized_rows:
            station_code = str(station.get("code") or "").strip().upper()
            station["contacts"] = station_contacts.get(station_code, {})
    return serialized_rows


def list_delivery_methods_data(*, ids: list[int] | None = None) -> list[dict]:
    return get_delivery_methods(ids=ids or [])


def create_delivery_methods_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_payload_list(payload)
    if not rows:
        return {"inserted": 0}

    normalized_rows: list[dict] = []
    for row in rows:
        normalized_rows.append(_normalize_delivery_method_inline(row))

    inserted = insert_delivery_methods(normalized_rows)
    invalidate_validation_cache()
    return {"inserted": inserted}


def create_stations_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_payload_list(payload, allow_object=False)
    if not rows:
        return {"inserted": 0}

    normalized_rows: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each stations item must be an object")
        code = str(row.get("code") or "").strip().upper()
        if not code:
            raise ValueError("code is required")
        media_type = _ensure_media_type(row.get("mediaType"))
        has_syscode_field = "syscode" in row
        syscode = (
            _ensure_optional_unsigned_int(row.get("syscode"), field="syscode")
            if has_syscode_field
            else None
        )
        _validate_syscode_for_media_type(
            media_type=media_type,
            has_syscode_field=has_syscode_field,
            syscode=syscode,
            require_syscode_for_ca=True,
        )
        normalized_rows.append(
            {
                "code": code,
                "name": _ensure_required_text(row.get("name"), field="name", max_length=255),
                "affiliation": _ensure_optional_text(
                    row.get("affiliation"),
                    field="affiliation",
                    max_length=255,
                ),
                "mediaType": media_type,
                "syscode": syscode,
                "language": _ensure_language(row.get("language")),
                "ownership": _ensure_optional_text(
                    row.get("ownership"),
                    field="ownership",
                    max_length=255,
                ),
                "deliveryMethodId": _resolve_station_delivery_method_id(
                    row,
                    required=True,
                ),
                "note": _ensure_optional_text(
                    row.get("note"),
                    field="note",
                    max_length=2048,
                ),
            }
        )

    inserted = insert_stations(normalized_rows)
    invalidate_validation_cache()
    return {"inserted": inserted}


def modify_stations_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_payload_list(payload)
    if not rows:
        return {"updated": 0}

    prepared_rows: list[tuple[dict, str]] = []
    station_codes: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each stations item must be an object")
        if "contacts" in row:
            raise ValueError(
                "Field 'contacts' is not supported in PUT /stations. Use /contacts/stationsContacts endpoints."
            )
        if "deliveryMethod" in row:
            raise ValueError(
                "Inline deliveryMethod is not supported in /stations. "
                "Use /stations/deliveryMethods and pass deliveryMethodId."
            )
        code = str(row.get("code") or "").strip().upper()
        if not code:
            raise ValueError("code is required for stations update")
        prepared_rows.append((row, code))
        station_codes.append(code)

    ensure_station_codes_exist(station_codes)
    existing_media_types = get_station_media_types(codes=station_codes)

    normalized_rows: list[dict] = []
    for row, code in prepared_rows:
        item: dict[str, object] = {"code": code}
        media_type: str | None = None
        has_syscode_update = False
        syscode: int | None = None
        current_media_type = str(existing_media_types.get(code) or "").strip().upper()
        if "name" in row:
            item["name"] = _ensure_required_text(
                row.get("name"),
                field="name",
                max_length=255,
            )
        if "affiliation" in row:
            item["affiliation"] = _ensure_optional_text(
                row.get("affiliation"),
                field="affiliation",
                max_length=255,
            )
        if "mediaType" in row:
            media_type = _ensure_media_type(row.get("mediaType"))
            item["mediaType"] = media_type
        if "syscode" in row:
            has_syscode_update = True
            syscode = _ensure_optional_unsigned_int(
                row.get("syscode"),
                field="syscode",
            )
            item["syscode"] = syscode
        if media_type is not None:
            _validate_syscode_for_media_type(
                media_type=media_type,
                has_syscode_field=has_syscode_update,
                syscode=syscode,
                require_syscode_for_ca=True,
            )
        elif has_syscode_update:
            if not current_media_type:
                raise ValueError(f"Unable to resolve current mediaType for station '{code}'")
            _validate_syscode_for_media_type(
                media_type=current_media_type,
                has_syscode_field=has_syscode_update,
                syscode=syscode,
                require_syscode_for_ca=False,
            )
        if "language" in row:
            item["language"] = _ensure_language(row.get("language"))
        if "ownership" in row:
            item["ownership"] = _ensure_optional_text(
                row.get("ownership"),
                field="ownership",
                max_length=255,
            )
        if "note" in row:
            item["note"] = _ensure_optional_text(
                row.get("note"),
                field="note",
                max_length=2048,
            )

        if "deliveryMethodId" in row:
            delivery_method_id = _resolve_station_delivery_method_id(
                row,
                required=False,
            )
            if delivery_method_id is None:
                raise ValueError("deliveryMethodId cannot be null")
            item["deliveryMethodId"] = delivery_method_id

        if len(item) == 1:
            raise ValueError(f"No updatable fields provided for station '{code}'")
        normalized_rows.append(item)

    updated = update_stations(normalized_rows)
    invalidate_validation_cache()
    return {"updated": updated}


def modify_delivery_methods_data(payload: list[dict] | dict) -> dict[str, int]:
    rows = _ensure_payload_list(payload)
    if not rows:
        return {"updated": 0}
    normalized_rows: list[dict] = []
    ids: list[int] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Each deliveryMethods item must be an object")
        delivery_method_id = row.get("id")
        if delivery_method_id is None:
            raise ValueError("id is required for delivery method update")
        item: dict[str, object] = {"id": int(delivery_method_id)}
        ids.append(int(delivery_method_id))
        if "name" in row:
            item["name"] = _ensure_required_text(
                row.get("name"),
                field="name",
                max_length=255,
            )
        if "url" in row:
            item["url"] = _ensure_required_text(
                row.get("url"),
                field="url",
                max_length=2048,
            )
        if "username" in row:
            item["username"] = _ensure_required_text(
                row.get("username"),
                field="username",
                max_length=255,
            )
        if "password" in row:
            item["password"] = row.get("password")
        if "deadline" in row:
            item["deadline"] = _ensure_required_text(
                row.get("deadline"),
                field="deadline",
                max_length=50,
            )
        if "note" in row:
            item["note"] = _ensure_optional_text(
                row.get("note"),
                field="note",
                max_length=2048,
            )
        if len(item) == 1:
            raise ValueError(
                f"No updatable fields provided for delivery method id '{delivery_method_id}'"
            )
        normalized_rows.append(item)

    ensure_delivery_method_ids_exist(ids)
    updated = update_delivery_methods(normalized_rows)
    invalidate_validation_cache()
    return {"updated": updated}
