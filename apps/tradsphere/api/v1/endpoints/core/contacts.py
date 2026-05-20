from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Path, Query

from apps.tradsphere.api.v1.helpers.contacts import (
    DuplicateContactsError,
    create_contacts_data,
    get_contact_usage_bundle_data,
    list_contacts_selector_data,
    list_contacts_data,
    modify_contacts_data,
    search_contact_station_codes_data,
)
from apps.tradsphere.api.v1.helpers.queryParsing import parse_csv_values

router = APIRouter(prefix="/contacts")


@router.get("")
def get_contacts_route(
    emails: list[str] | None = Query(None, alias="emails"),
    email: list[str] | None = Query(None, alias="email"),
    name: str | None = Query(None),
    company: str | None = Query(None),
    phone: str | None = Query(None),
    station: str | None = Query(None),
    contact_types: str | None = Query(None, alias="contactTypes"),
    contact_type: str | None = Query(None, alias="contactType"),
    contact_type_lower: str | None = Query(None, alias="contacttype"),
    active: bool | None = Query(None),
):
    """
    Return contact rows filtered by keyword fields, contact type, and/or active.

    Example request (partial email, multiple values):
        GET /api/tradsphere/v1/contacts?emails=ops,billing@station.com&active=true

    Example request (partial name/contact type):
        GET /api/tradsphere/v1/contacts?name=ari&contactType=REP

    Example request (station and phone partial match):
        GET /api/tradsphere/v1/contacts?station=KABC&phone=8324886150

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 3},
          "data": [
            {
              "id": 12,
              "email": "ops@station.com",
              "firstName": "Ari",
              "lastName": "Nguyen",
              "company": "ABC Media",
              "jobTitle": "Traffic Manager",
              "office": "+1-555-1000",
              "cell": "+1-555-2000",
              "stationCodes": ["KABC", "WXYZ"],
              "active": 1,
              "note": "Preferred contact for logs"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - At least one filter is required: emails/email, name, company, phone, station, contactType/contactTypes/contacttype, or active
        - emails/email accepts comma-separated values
        - emails/email uses case-insensitive partial match on contact email
        - when multiple emails/email values are provided, they are combined with OR (match any value)
        - name uses case-insensitive partial match across firstName, lastName, and full name
        - company uses case-insensitive partial match
        - station uses case-insensitive partial match on station code or station name
        - phone uses non-exact match and ignores common formatting characters (for example: 8324886150, 832-488-6150, (832) 488-6150)
        - contactType/contactTypes/contacttype accepts exactly one value
        - contactType values must match tenant enum tradsphere.ENUMS.contactType
    """
    try:
        normalized_emails = parse_csv_values(emails, email, lowercase=True)
        normalized_contact_types = parse_csv_values(
            contact_types,
            contact_type,
            contact_type_lower,
            uppercase=True,
        )
        if len(normalized_contact_types) > 1:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Only one contactType is allowed for /contacts. "
                    "Use one of: contactType, contactTypes, or contacttype."
                ),
            )
        normalized_name = str(name or "").strip() or None
        normalized_company = str(company or "").strip() or None
        normalized_phone = str(phone or "").strip() or None
        normalized_station = str(station or "").strip() or None
        if (
            not normalized_emails
            and not normalized_name
            and not normalized_company
            and not normalized_phone
            and not normalized_station
            and not normalized_contact_types
            and active is None
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    "At least one filter is required: emails/email, name, company, phone, station, "
                    "contactType/contactTypes/contacttype, or active"
                ),
            )
        normalized_contact_type = normalized_contact_types[0] if normalized_contact_types else None
        return list_contacts_data(
            emails=normalized_emails,
            name=normalized_name,
            company=normalized_company,
            phone=normalized_phone,
            station=normalized_station,
            contact_type=normalized_contact_type,
            active=active,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise


@router.get("/selector")
def get_contacts_selector_route(
    active: bool = Query(True),
):
    """
    Return lightweight contacts selector rows for station/contact selector UIs.

    Example request:
        GET /api/tradsphere/v1/contacts/selector

    Example request (include inactive contacts):
        GET /api/tradsphere/v1/contacts/selector?active=false

    Example response:
        {
          "meta": {"timestamp": "2026-05-21T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "contactId": 12,
              "firstName": "Mina",
              "lastName": "Tran",
              "contactName": "Mina Tran",
              "contactEmail": "rep@kabc.com",
              "office": "213-555-0100",
              "cell": "213-555-0101",
              "company": "KABC",
              "jobTitle": "Sales Rep",
              "note": "",
              "active": 1
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - active defaults to true
        - active=true returns active contacts only
        - active=false includes inactive contacts
        - Lightweight selector payload; full filtered contact search remains on `/contacts`
    """
    try:
        return list_contacts_selector_data(active=active)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{contact_id}/usage")
def get_contact_usage_route(
    contact_id: int = Path(..., ge=1),
    active: bool = Query(True),
):
    """
    Return bundled usage/detail datasets for one contact to avoid frontend request fan-out.

    Example request:
        GET /api/tradsphere/v1/contacts/12/usage

    Example request (include inactive station-contact links):
        GET /api/tradsphere/v1/contacts/12/usage?active=false

    Example response:
        {
          "meta": {"timestamp": "2026-05-20T19:00:00+07:00", "duration_ms": 6},
          "data": {
            "contactId": 12,
            "usageRows": [
              {
                "id": 44,
                "contactId": 12,
                "stationCode": "KABC",
                "stationName": "KABC Los Angeles",
                "mediaType": "CA",
                "language": "English",
                "syscode": 1001,
                "affiliation": "ABC",
                "market": null,
                "contactType": "REP",
                "primaryContact": 1,
                "active": 1
              }
            ],
            "stationScheduleRows": [
              {"stationCode": "KABC", "estNum": 26001}
            ],
            "estNumUsageMetaRows": [
              {
                "estNum": 26001,
                "accountCode": "TAAA",
                "month": null,
                "quarter": 2,
                "year": 2026,
                "periodLabel": "APR,MAY'26",
                "mediaType": "TV",
                "broadcastMonths": [4, 5],
                "broadcastYears": [2026]
              }
            ],
            "accountRows": [
              {"accountCode": "TAAA", "name": "Alpha Motors"}
            ]
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - contactId path value is required
        - contactId must exist in contacts table
        - active=true (default) returns only active station-contact links
        - Existing /contacts, /contacts/stationsContacts, /schedules, /estNums, and /accounts routes remain unchanged
    """
    try:
        return get_contact_usage_bundle_data(contact_id=contact_id, active=active)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/station-codes")
def get_contact_station_codes_route(
    q: str = Query(..., min_length=1),
    active: bool = Query(True),
):
    """
    Return unique station codes linked to contacts matched by a contact query.

    Example request:
        GET /api/tradsphere/v1/contacts/station-codes?q=mina

    Example request (email-style query):
        GET /api/tradsphere/v1/contacts/station-codes?q=rep@kabc.com

    Example response:
        {
          "meta": {"timestamp": "2026-05-20T20:00:00+07:00", "duration_ms": 4},
          "data": {
            "query": "mina",
            "active": true,
            "stationCodes": ["KABC", "WXYZ"]
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - q is required and must be non-empty
        - Uses name query matching, and email partial matching when q contains '@'
        - active=true (default) limits linked station-contact rows to active links
        - Returned stationCodes are uppercase and deduplicated
    """
    try:
        station_codes = search_contact_station_codes_data(
            query=q,
            active=active,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "query": str(q or "").strip(),
        "active": bool(active),
        "stationCodes": station_codes,
    }


@router.post("")
def create_contacts_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Create one or many contact rows.

    Example request:
        POST /api/tradsphere/v1/contacts
        [
          {
            "email": "ops@station.com",
            "name": "Ari Nguyen",
            "company": "ABC Media",
            "jobTitle": "Traffic Manager",
            "office": "+1-555-1000",
            "cell": "+1-555-2000",
            "active": true,
            "note": "Preferred contact for logs"
          }
        ]

    Example request (firstName/lastName take precedence over name):
        POST /api/tradsphere/v1/contacts
        [
          {
            "email": "ops@station.com",
            "name": "Ignored Name",
            "firstName": "Ari",
            "lastName": "Nguyen"
          }
        ]

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 2},
          "data": {"inserted": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - email is required per item
        - email must be a valid email format
        - Optional name is accepted and auto-parsed into firstName/lastName when firstName/lastName are not both provided
        - If firstName and lastName are both provided, name is ignored
        - firstName defaults to empty string when omitted
        - office/cell accept all-digit (10/11 digits) or US phone format
        - office may include optional extension suffix like x1234; cell cannot include extension
        - office max length 35; cell max length 20
        - note max length 2048
        - Duplicate emails return HTTP 400 with duplicatedContacts details (payload + DB, case-insensitive)

    Example error response (duplicate email):
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 2},
          "error": {
            "message": "Duplicate contacts found",
            "duplicatedContacts": [
              {
                "index": 1,
                "email": "ops@station.com",
                "reason": "email_already_exists",
                "existingContactId": 12,
                "contact": {"email": "ops@station.com", "firstName": "Ari"}
              }
            ]
          }
        }
    """
    try:
        return create_contacts_data(payload)
    except DuplicateContactsError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Duplicate contacts found",
                "duplicatedContacts": exc.duplicated_contacts,
            },
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("")
def update_contacts_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Update contact rows by id.

    Example request:
        PUT /api/tradsphere/v1/contacts
        [
          {
            "id": 12,
            "name": "Ari Tran"
          }
        ]

    Example request (firstName/lastName take precedence over name):
        PUT /api/tradsphere/v1/contacts
        [
          {
            "id": 12,
            "name": "Ignored Name",
            "firstName": "Ari",
            "lastName": "Nguyen"
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
        - id is required per item
        - email must be a valid email format when provided
        - Optional name is accepted and auto-parsed into firstName/lastName when firstName/lastName are not both provided
        - If firstName and lastName are both provided, name is ignored
        - office/cell accept all-digit (10/11 digits) or US phone format when provided
        - office may include optional extension suffix like x1234; cell cannot include extension
        - office max length 35; cell max length 20
        - note max length 2048
        - Duplicate emails return HTTP 400 with duplicatedContacts details (payload + DB, case-insensitive)

    Example error response (duplicate email):
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 2},
          "error": {
            "message": "Duplicate contacts found",
            "duplicatedContacts": [
              {
                "index": 0,
                "id": 18,
                "email": "ops@station.com",
                "reason": "duplicate_in_payload",
                "contact": {"id": 18, "email": "ops@station.com"}
              }
            ]
          }
        }
    """
    try:
        return modify_contacts_data(payload)
    except DuplicateContactsError as exc:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Duplicate contacts found",
                "duplicatedContacts": exc.duplicated_contacts,
            },
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
