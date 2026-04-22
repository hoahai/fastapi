from __future__ import annotations

from apps.tradsphere.api.v1.helpers.accountValidation import (
    ensure_delivery_method_ids_exist,
    ensure_station_codes_exist,
    invalidate_validation_cache,
)
from apps.tradsphere.api.v1.helpers.config import get_media_types
from apps.tradsphere.api.v1.helpers.dbQueries import (
    get_delivery_methods,
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


def _normalize_language_filters(values: list[str] | None) -> list[str]:
    normalized: list[str] = []
    for value in values or []:
        mapped = _LANGUAGE_VALUES.get(str(value or "").strip().upper())
        if not mapped:
            raise ValueError("languages values must be English/Spanish (or EN/ES)")
        normalized.append(mapped.upper())
    return list(dict.fromkeys(normalized))


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


def _serialize_station_row(row: dict) -> dict:
    delivery_method_id = row.get("deliveryMethodId")
    delivery_method = None
    if delivery_method_id is not None:
        delivery_method = {
            "id": delivery_method_id,
            "name": row.get("deliveryMethodName"),
            "url": row.get("deliveryMethodUrl"),
            "username": row.get("deliveryMethodUsername"),
            "deadline": row.get("deliveryMethodDeadline"),
            "note": row.get("deliveryMethodNote"),
        }
    return {
        "code": row.get("code"),
        "name": row.get("name"),
        "affiliation": row.get("affiliation"),
        "mediaType": row.get("mediaType"),
        "syscode": row.get("syscode"),
        "language": row.get("language"),
        "ownership": row.get("ownership"),
        "deliveryMethodId": delivery_method_id,
        "note": row.get("note"),
        "deliveryMethod": delivery_method,
    }


def list_stations_data(
    *,
    codes: list[str],
    media_types: list[str] | None = None,
    languages: list[str] | None = None,
) -> list[dict]:
    if not codes:
        raise ValueError("codes is required")

    normalized_codes = [str(code or "").strip().upper() for code in codes if str(code or "").strip()]
    if not normalized_codes:
        raise ValueError("codes is required")

    normalized_media_types: list[str] = []
    if media_types:
        allowed = set(get_media_types())
        for item in media_types:
            media_type = str(item or "").strip().upper()
            if not media_type:
                continue
            if media_type not in allowed:
                raise ValueError(
                    f"Invalid mediaType: {media_type}. Allowed values: {', '.join(sorted(allowed))}"
                )
            normalized_media_types.append(media_type)
        normalized_media_types = list(dict.fromkeys(normalized_media_types))

    normalized_languages = _normalize_language_filters(languages)

    rows = get_stations(
        codes=normalized_codes,
        media_types=normalized_media_types,
        languages=normalized_languages,
    )
    return [_serialize_station_row(row) for row in rows]


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
        normalized_rows.append(
            {
                "code": code,
                "name": _ensure_required_text(row.get("name"), field="name", max_length=255),
                "affiliation": _ensure_optional_text(
                    row.get("affiliation"),
                    field="affiliation",
                    max_length=255,
                ),
                "mediaType": _ensure_media_type(row.get("mediaType")),
                "syscode": _ensure_optional_unsigned_int(row.get("syscode"), field="syscode"),
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

    normalized_rows: list[dict] = []
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

        item: dict[str, object] = {"code": code}
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
            item["mediaType"] = _ensure_media_type(row.get("mediaType"))
        if "syscode" in row:
            item["syscode"] = _ensure_optional_unsigned_int(
                row.get("syscode"),
                field="syscode",
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

    ensure_station_codes_exist([row["code"] for row in normalized_rows])
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
