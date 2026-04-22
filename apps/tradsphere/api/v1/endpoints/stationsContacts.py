from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.contacts import (
    create_stations_contacts_data,
    list_stations_contacts_data,
    modify_stations_contacts_data,
)
from apps.tradsphere.api.v1.helpers.queryParsing import parse_csv_values, parse_int_list

router = APIRouter(prefix="/contacts/stationsContacts")


@router.get("")
def get_stations_contacts_route(
    ids: list[str] | None = Query(None, alias="ids"),
    row_id: list[str] | None = Query(None, alias="id"),
    station_codes: list[str] | None = Query(None, alias="stationCodes"),
    station_code: list[str] | None = Query(None, alias="stationCode"),
    contact_ids: list[str] | None = Query(None, alias="contactIds"),
    contact_id: list[str] | None = Query(None, alias="contactId"),
    contact_types: list[str] | None = Query(None, alias="contactTypes"),
    contact_type: list[str] | None = Query(None, alias="contactType"),
    active: bool | None = Query(None),
):
    """
    Return station-contact linking rows with optional filters.

    Example request:
        GET /api/tradsphere/v1/contacts/stationsContacts

    Example request (filtered):
        GET /api/tradsphere/v1/contacts/stationsContacts?stationCodes=KABC&contactIds=12&contactTypes=REP&active=true

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": 44,
              "stationCode": "KABC",
              "contactId": 12,
              "contactType": "REP",
              "primaryContact": 1,
              "note": "Primary booking rep",
              "active": 1
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - ids/id, contactIds/contactId accept comma-separated integers
        - stationCodes/stationCode and contactTypes/contactType accept comma-separated values
    """
    try:
        normalized_ids = parse_int_list(ids, row_id)
        normalized_station_codes = parse_csv_values(
            station_codes,
            station_code,
            uppercase=True,
        )
        normalized_contact_ids = parse_int_list(contact_ids, contact_id)
        normalized_contact_types = parse_csv_values(
            contact_types,
            contact_type,
            uppercase=True,
        )
        return list_stations_contacts_data(
            ids=normalized_ids,
            station_codes=normalized_station_codes,
            contact_ids=normalized_contact_ids,
            contact_types=normalized_contact_types,
            active=active,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise


@router.post("")
def create_stations_contacts_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Create one or many station-contact link rows.

    Example request:
        POST /api/tradsphere/v1/contacts/stationsContacts
        [
          {
            "stationCode": "KABC",
            "contactId": 12,
            "contactType": "REP",
            "primaryContact": true,
            "note": "Primary booking rep",
            "active": true
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
        - stationCode and contactId are required
        - stationCode/contactId must exist
        - contactType must match tenant enum; defaults to REP if available, else first configured type
    """
    try:
        return create_stations_contacts_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("")
def update_stations_contacts_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Update station-contact link rows by id.

    Example request:
        PUT /api/tradsphere/v1/contacts/stationsContacts
        [
          {
            "id": 44,
            "contactType": "TRAFFIC",
            "primaryContact": false,
            "note": "Use traffic team for logs"
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
        - stationCode/contactId/contactType are validated when provided
        - If stationCode/contactId/contactType is changed, old link row is set inactive and new link is created/reactivated
        - updated count reflects DB affected rows; reassignment can affect more than one row
    """
    try:
        return modify_stations_contacts_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
