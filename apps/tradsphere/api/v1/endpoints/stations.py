from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.queryParsing import parse_csv_values
from apps.tradsphere.api.v1.helpers.stations import (
    create_stations_data,
    list_stations_data,
    modify_stations_data,
)

router = APIRouter(prefix="/stations")


@router.get("")
def get_stations_route(
    codes: list[str] | None = Query(None, alias="codes"),
    code: list[str] | None = Query(None, alias="code"),
    media_types: list[str] | None = Query(None, alias="mediaTypes"),
    media_type: list[str] | None = Query(None, alias="mediaType"),
    languages: list[str] | None = Query(None, alias="languages"),
    language: list[str] | None = Query(None, alias="language"),
):
    """
    Return station rows for required station codes with optional media/language filters.

    Example request:
        GET /api/tradsphere/v1/stations?codes=KABC,WXYZ

    Example request (with optional filters):
        GET /api/tradsphere/v1/stations?codes=KABC,WXYZ&mediaTypes=TV,OTT&languages=English,Spanish

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 3},
          "data": [
            {
              "code": "KABC",
              "name": "KABC Los Angeles",
              "affiliation": "ABC",
              "mediaType": "TV",
              "syscode": 1001,
              "language": "English",
              "ownership": "Owned",
              "deliveryMethodId": 12,
              "note": "Primary station",
              "deliveryMethod": {
                "id": 12,
                "name": "Station Portal",
                "url": "https://delivery.example.com",
                "username": "ops_user",
                "deadline": "17:00",
                "note": "Standard daily upload"
              }
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - codes/code is required and accepts comma-separated values
        - mediaTypes/mediaType and languages/language are optional comma-separated filters
        - languages accepts English/Spanish (aliases EN/ES)
        - deliveryMethodIds filtering is intentionally not supported on this route
    """
    normalized_codes = parse_csv_values(codes, code, uppercase=True)
    normalized_media_types = parse_csv_values(media_types, media_type, uppercase=True)
    normalized_languages = parse_csv_values(languages, language, uppercase=True)
    try:
        return list_stations_data(
            codes=normalized_codes,
            media_types=normalized_media_types,
            languages=normalized_languages,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("")
def create_stations_route(
    payload: list[dict] = Body(...),
):
    """
    Create one or many station rows.

    Example request (deliveryMethodId only):
        POST /api/tradsphere/v1/stations
        [
          {
            "code": "KABC",
            "name": "KABC Los Angeles",
            "affiliation": "ABC",
            "mediaType": "TV",
            "syscode": 1001,
            "language": "English",
            "ownership": "Owned",
            "note": "Primary station",
            "deliveryMethodId": 12
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 3},
          "data": {"inserted": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Payload must be an array of station objects
        - code, name, mediaType, language are required per item
        - deliveryMethodId is required per item and must exist
        - deliveryMethodId is the only supported delivery-method input on this route
        - syscode must be unsigned integer when provided
        - note max length is 2048
        - Inline deliveryMethod is not supported on this route
        - Create/update delivery methods via /stations/deliveryMethods
    """
    try:
        return create_stations_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("")
def update_stations_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Update stations by station code.

    Example request:
        PUT /api/tradsphere/v1/stations
        [
          {
            "code": "KABC",
            "mediaType": "OTT",
            "deliveryMethodId": 12,
            "language": "Spanish",
            "note": "Moved to OTT package"
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 2},
          "data": {"updated": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - code is required per item
        - When updating delivery method, provide deliveryMethodId only (must exist)
        - Inline deliveryMethod is not supported on this route
        - Create/update delivery methods via /stations/deliveryMethods
        - language accepts English/Spanish (aliases EN/ES)
        - syscode must be unsigned integer when provided
        - note max length is 2048
        - Field contacts is rejected; use /contacts/stationsContacts instead
    """
    try:
        return modify_stations_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
