from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.queryParsing import parse_csv_values, parse_int_list
from apps.tradsphere.api.v1.helpers.stations import (
    create_station_detail_data,
    create_stations_data,
    get_station_detail_data,
    list_stations_data,
    update_station_detail_data,
    modify_stations_data,
)

router = APIRouter(prefix="/stations")


@router.get("")
def get_stations_route(
    codes: list[str] | None = Query(None, alias="codes"),
    code: list[str] | None = Query(None, alias="code"),
    account_code: list[str] | None = Query(None, alias="accountCode"),
    est_num: list[str] | None = Query(None, alias="estNum"),
    name: str | None = Query(None, alias="name"),
    affiliation: str | None = Query(None, alias="affiliation"),
    media_types: list[str] | None = Query(None, alias="mediaTypes"),
    media_type: list[str] | None = Query(None, alias="mediaType"),
    languages: list[str] | None = Query(None, alias="languages"),
    language: list[str] | None = Query(None, alias="language"),
    delivery_method_detail: bool = Query(False, alias="deliveryMethodDetail"),
    contact_detail: bool = Query(False, alias="contactDetail"),
):
    """
    Return station rows with optional filters.

    Example request:
        GET /api/tradsphere/v1/stations?codes=KABC,WXYZ

    Example request (filter by accountCode and estNum, no codes):
        GET /api/tradsphere/v1/stations?accountCode=TAAA&estNum=1001

    Example request (summary details):
        GET /api/tradsphere/v1/stations?estNum=1957&deliveryMethodDetail=false&contactDetail=false

    Example request (station name filter):
        GET /api/tradsphere/v1/stations?name=los%20angeles

    Example request (media/language filters):
        GET /api/tradsphere/v1/stations?mediaTypes=TV,CA&languages=English

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 3},
          "data": [
            {
              "code": "KABC",
              "name": "KABC Los Angeles",
              "affiliation": "ABC",
              "mediaType": "CA",
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
              },
              "contacts": {
                "REP": [
                  {
                    "id": 88,
                    "email": "rep@kabc.com",
                    "firstName": "Mina",
                    "lastName": "Tran",
                    "company": "KABC",
                    "jobTitle": "Sales Rep",
                    "office": "213-555-0100",
                    "cell": "213-555-0101",
                    "active": 1,
                    "note": null,
                    "primaryContact": 1,
                    "contactTypeNote": null
                  }
                ],
                "TRAFFIC": ["traffic@kabc.com"],
                "BILLING": ["billing@kabc.com"]
              }
            }
          ]
        }

    Example response (contactDetail=false):
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 3},
          "data": [
            {
              "code": "KABC",
              "deliveryMethodId": 12,
              "deliveryMethod": {"id": 12, "name": "Station Portal"},
              "contacts": {
                "REP": [
                  {"id": 88, "name": "Mina Tran", "email": "rep@kabc.com"}
                ],
                "TRAFFIC": ["traffic@kabc.com"]
              }
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - At least one of codes/code, accountCode, estNum, name, affiliation, mediaType(s), language(s) is required
        - codes/code accepts comma-separated values (multiple supported)
        - accountCode accepts only one value
        - estNum accepts only one unsigned-integer value
        - name performs case-insensitive partial match on station name
        - affiliation performs case-insensitive partial match
        - mediaType/mediaTypes accepts comma-separated values and validates against tenant mediaType enum
        - language/languages accepts comma-separated values and supports English/Spanish (aliases EN/ES)
        - deliveryMethodDetail controls deliveryMethod object detail (default false)
        - when deliveryMethodDetail=false, deliveryMethod returns id and name only
        - contactDetail controls REP contact detail (default false)
        - contacts are grouped by contactType
        - REP returns contact objects; non-REP returns email lists
        - when contactDetail=false, REP objects are short: {id, name, email}
        - syscode is returned only when mediaType is CA; for other media types, syscode is omitted
        - deliveryMethodIds filtering is intentionally not supported on this route
    """
    normalized_codes = parse_csv_values(codes, code, uppercase=True)
    normalized_account_codes = parse_csv_values(account_code, uppercase=True)
    normalized_est_nums = parse_int_list(est_num)
    normalized_media_types = parse_csv_values(media_types, media_type, uppercase=True)
    normalized_languages = parse_csv_values(languages, language, uppercase=True)

    if len(normalized_account_codes) > 1:
        raise HTTPException(status_code=400, detail="accountCode accepts only one value")
    if len(normalized_est_nums) > 1:
        raise HTTPException(status_code=400, detail="estNum accepts only one value")

    selected_account_code = normalized_account_codes[0] if normalized_account_codes else None
    selected_est_num = normalized_est_nums[0] if normalized_est_nums else None

    try:
        return list_stations_data(
            codes=normalized_codes,
            account_code=selected_account_code,
            est_num=selected_est_num,
            station_name=name,
            affiliation=affiliation,
            media_types=normalized_media_types,
            languages=normalized_languages,
            delivery_method_detail=delivery_method_detail,
            contact_detail=contact_detail,
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
        - syscode must be an unsigned integer when provided
        - syscode is required when mediaType is CA
        - syscode is not allowed when mediaType is not CA
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
            "mediaType": "CA",
            "syscode": 1001,
            "deliveryMethodId": 12,
            "language": "Spanish",
            "note": "Updated CA station details"
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
        - syscode must be an unsigned integer when provided
        - when mediaType is set to CA in PUT payload, syscode is required in the same item
        - when mediaType is set to non-CA in PUT payload, syscode is not allowed in the same item
        - when syscode is updated without mediaType, existing station mediaType must be CA
        - note max length is 2048
        - Field contacts is rejected; use /contacts/stationsContacts instead
    """
    try:
        return modify_stations_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{code}/detail")
def get_station_detail_route(code: str):
    """
    Return full station detail payload in one request.

    Example request:
        GET /api/tradsphere/v1/stations/KABC/detail

    Example response:
        {
          "meta": {"timestamp": "2026-05-06T10:00:00+07:00", "duration_ms": 4},
          "data": {
            "station": {
              "code": "KABC",
              "name": "KABC Los Angeles",
              "affiliation": "ABC",
              "mediaType": "TV",
              "language": "English",
              "ownership": "Owned",
              "deliveryMethodId": 12,
              "note": "Primary station"
            },
            "deliveryMethod": {
              "id": 12,
              "name": "Station Portal",
              "url": "https://delivery.example.com",
              "username": "ops_user",
              "password": "secret",
              "deadline": "10 AM",
              "note": "Primary endpoint"
            },
            "contacts": [
              {
                "id": 88,
                "email": "rep@kabc.com",
                "firstName": "Mina",
                "lastName": "Tran",
                "company": "KABC",
                "jobTitle": "Sales Rep",
                "office": "213-555-0100",
                "cell": "213-555-0101",
                "active": 1,
                "note": null
              }
            ],
            "contactLinks": [
              {
                "id": 44,
                "stationCode": "KABC",
                "contactId": 88,
                "contactType": "REP",
                "primaryContact": 1,
                "note": "Primary booking rep",
                "active": 1
              }
            ]
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - code must exist in stations
        - Returns station + delivery method + linked contacts + station-contact links
        - Contact links include active links only
    """
    try:
        return get_station_detail_data(code)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/detail")
def create_station_detail_route(payload: dict = Body(...)):
    """
    Create station detail (station + delivery method + contacts + links) in one transaction.

    Example request:
        POST /api/tradsphere/v1/stations/detail
        {
          "station": {
            "code": "KABC",
            "name": "KABC Los Angeles",
            "affiliation": "ABC",
            "mediaType": "TV",
            "language": "English",
            "ownership": "Owned",
            "note": "Primary station"
          },
          "deliveryMethod": {
            "name": "Station Portal",
            "url": "https://delivery.example.com",
            "username": "ops_user",
            "password": "secret",
            "deadline": "10 AM",
            "note": "Primary endpoint"
          },
          "contacts": [
            {
              "clientKey": "rep-1",
              "email": "rep@kabc.com",
              "firstName": "Mina",
              "lastName": "Tran",
              "company": "KABC",
              "jobTitle": "Sales Rep",
              "office": "213-555-0100",
              "cell": "213-555-0101",
              "active": true
            }
          ],
          "contactLinks": [
            {
              "contactClientKey": "rep-1",
              "contactType": "REP",
              "primaryContact": true,
              "note": "Primary booking rep",
              "active": true
            }
          ]
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-06T10:00:00+07:00", "duration_ms": 8},
          "data": {
            "station": {"code": "KABC"},
            "deliveryMethod": {"id": 12},
            "contacts": [{"id": 88}],
            "contactLinks": [{"id": 44}],
            "summary": {
              "deliveryMethodCreated": true,
              "deliveryMethodUpdated": false,
              "stationCreated": true,
              "stationUpdated": false,
              "contactsCreated": 1,
              "contactsUpdated": 0,
              "linksCreatedOrReactivated": 1,
              "linksUpdated": 0,
              "linksDeactivated": 0
            }
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - station object is required
        - deliveryMethod object is optional; when provided, name/url/username/deadline are required
        - Uses one transaction; any error rolls back all changes
        - Removed links are not deleted (active links are managed by contactLinks desired set)
        - Uses contactId or contactClientKey to resolve link targets
        - Duplicate/ambiguous contact link references return 400
    """
    try:
        return create_station_detail_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/{code}/detail")
def update_station_detail_route(
    code: str,
    payload: dict = Body(...),
):
    """
    Update station detail (station + delivery method + contacts + links) in one transaction.

    Example request:
        PUT /api/tradsphere/v1/stations/KABC/detail
        {
          "station": {
            "code": "KABC",
            "name": "KABC Los Angeles",
            "affiliation": "ABC",
            "mediaType": "TV",
            "language": "Spanish",
            "ownership": "Owned",
            "note": "Updated note"
          },
          "deliveryMethod": {
            "id": 12,
            "name": "Station Portal",
            "url": "https://delivery.example.com/v2",
            "username": "ops_user",
            "deadline": "17:00",
            "note": "Updated endpoint"
          },
          "contacts": [
            {
              "id": 88,
              "email": "rep@kabc.com",
              "firstName": "Mina",
              "lastName": "Tran",
              "office": "213-555-0111",
              "cell": "213-555-0101"
            }
          ],
          "contactLinks": [
            {
              "contactId": 88,
              "contactType": "REP",
              "primaryContact": true,
              "active": true
            }
          ]
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-06T10:00:00+07:00", "duration_ms": 8},
          "data": {
            "station": {"code": "KABC"},
            "deliveryMethod": {"id": 12},
            "contacts": [{"id": 88}],
            "contactLinks": [{"id": 44}],
            "summary": {
              "deliveryMethodCreated": false,
              "deliveryMethodUpdated": true,
              "stationCreated": false,
              "stationUpdated": true,
              "contactsCreated": 0,
              "contactsUpdated": 1,
              "linksCreatedOrReactivated": 0,
              "linksUpdated": 1,
              "linksDeactivated": 1
            }
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Path code must match payload station.code
        - deliveryMethod object is optional; when provided, name/url/username/deadline are required
        - Uses one transaction; any error rolls back all changes
        - Existing active links omitted from contactLinks are deactivated (not deleted)
        - If delivery method is shared by multiple stations, it is not updated in place
    """
    try:
        return update_station_detail_data(station_code=code, payload=payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
