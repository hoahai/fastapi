from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.queryParsing import parse_csv_values, parse_int_list
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
    account_code: list[str] | None = Query(None, alias="accountCode"),
    est_num: list[str] | None = Query(None, alias="estNum"),
    name: str | None = Query(None, alias="name"),
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
        - At least one of codes/code, accountCode, estNum, name is required
        - codes/code accepts comma-separated values (multiple supported)
        - accountCode accepts only one value
        - estNum accepts only one unsigned-integer value
        - name performs case-insensitive partial match on station name
        - deliveryMethodDetail controls deliveryMethod object detail (default false)
        - when deliveryMethodDetail=false, deliveryMethod returns id and name only
        - contactDetail controls REP contact detail (default false)
        - contacts are grouped by contactType
        - REP returns contact objects; non-REP returns email lists
        - when contactDetail=false, REP objects are short: {id, name, email}
        - mediaTypes/mediaType and languages/language are not supported on this route
        - syscode is returned only when mediaType is CA; for other media types, syscode is omitted
        - deliveryMethodIds filtering is intentionally not supported on this route
    """
    normalized_codes = parse_csv_values(codes, code, uppercase=True)
    normalized_account_codes = parse_csv_values(account_code, uppercase=True)
    normalized_est_nums = parse_int_list(est_num)

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
