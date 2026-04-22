from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.contacts import (
    DuplicateContactsError,
    create_contacts_data,
    list_contacts_by_station_codes_data,
    list_contacts_data,
    modify_contacts_data,
)
from apps.tradsphere.api.v1.helpers.queryParsing import parse_csv_values

router = APIRouter(prefix="/contacts")


@router.get("")
def get_contacts_route(
    emails: list[str] | None = Query(None, alias="emails"),
    email: list[str] | None = Query(None, alias="email"),
    active: bool | None = Query(None),
):
    """
    Return contact rows with optional email/active filters.

    Example request:
        GET /api/tradsphere/v1/contacts

    Example request (filtered):
        GET /api/tradsphere/v1/contacts?emails=ops@station.com,billing@station.com&active=true

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
              "active": 1,
              "note": "Preferred contact for logs"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - emails/email accepts comma-separated values
        - blank filter values return all for that filter
    """
    try:
        normalized_emails = parse_csv_values(emails, email, lowercase=True)
        return list_contacts_data(
            emails=normalized_emails,
            active=active,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise


@router.get("/byStationCodes")
def get_contacts_by_station_codes_route(
    codes: list[str] | None = Query(None, alias="codes"),
    code: list[str] | None = Query(None, alias="code"),
    contact_types: list[str] | None = Query(None, alias="contactTypes"),
    contact_type: list[str] | None = Query(None, alias="contactType"),
    contact_type_lower: list[str] | None = Query(None, alias="contacttype"),
):
    """
    Return active contacts grouped by station code, including station-contact fields.

    Example request:
        GET /api/tradsphere/v1/contacts/byStationCodes?codes=KABC,WXYZ

    Example request (with contact type filter):
        GET /api/tradsphere/v1/contacts/byStationCodes?codes=KABC&contactTypes=REP,TRAFFIC

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 4},
          "data": [
            {
              "stationCode": "KABC",
              "contacts": [
                {
                  "id": 12,
                  "email": "ops@station.com",
                  "firstName": "Ari",
                  "lastName": "Nguyen",
                  "company": "ABC Media",
                  "jobTitle": "Traffic Manager",
                  "office": "+1-555-1000",
                  "cell": "+1-555-2000",
                  "active": 1,
                  "note": "Preferred contact for logs",
                  "contactType": "REP",
                  "primaryContact": 1,
                  "contactTypeNote": "Primary booking rep"
                }
              ]
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - codes/code is required and accepts comma-separated station codes
        - Optional contact type aliases: contactTypes, contactType, contacttype
        - contactType values must match tenant enum tradsphere.ENUMS.contactType
    """
    normalized_codes = parse_csv_values(codes, code, uppercase=True)
    if not normalized_codes:
        raise HTTPException(status_code=400, detail="codes/code is required")
    normalized_contact_types = parse_csv_values(
        contact_types,
        contact_type,
        contact_type_lower,
        uppercase=True,
    )
    try:
        return list_contacts_by_station_codes_data(
            station_codes=normalized_codes,
            contact_types=normalized_contact_types,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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
            "firstName": "Ari",
            "lastName": "Nguyen",
            "company": "ABC Media",
            "jobTitle": "Traffic Manager",
            "office": "+1-555-1000",
            "cell": "+1-555-2000",
            "active": true,
            "note": "Preferred contact for logs"
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
        - firstName defaults to empty string when omitted
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
            "email": "traffic@station.com",
            "jobTitle": "Traffic Lead",
            "office": "+1-555-1200",
            "active": true
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
