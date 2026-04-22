from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.queryParsing import parse_int_list
from apps.tradsphere.api.v1.helpers.stations import (
    create_delivery_methods_data,
    list_delivery_methods_data,
    modify_delivery_methods_data,
)

router = APIRouter(prefix="/stations/deliveryMethods")


@router.get("")
def get_delivery_methods_route(
    ids: list[str] | None = Query(None, alias="ids"),
    row_id: list[str] | None = Query(None, alias="id"),
):
    """
    Return station delivery-method rows, optionally filtered by ids.

    Example request:
        GET /api/tradsphere/v1/stations/deliveryMethods

    Example request (filtered):
        GET /api/tradsphere/v1/stations/deliveryMethods?ids=1,2,3

    Example response:
        {
          "meta": {"timestamp": "2026-04-22T17:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": 12,
              "name": "Station Portal",
              "url": "https://delivery.example.com",
              "username": "ops_user",
              "deadline": "17:00",
              "note": "Standard daily upload"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - ids/id accepts comma-separated integer values
        - blank ids/id returns all delivery methods
    """
    try:
        normalized_ids = parse_int_list(ids, row_id)
        return list_delivery_methods_data(ids=normalized_ids)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise


@router.post("")
def create_delivery_methods_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Create one or many delivery-method rows.

    Example request:
        POST /api/tradsphere/v1/stations/deliveryMethods
        [
          {
            "name": "Station Portal",
            "url": "https://delivery.example.com",
            "username": "ops_user",
            "password": "secret",
            "deadline": "10 AM",
            "note": "Primary delivery endpoint"
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
        - Payload accepts object or array of objects
        - name, url, username are required per item
        - deadline defaults to "10 AM" when omitted
        - Duplicate (url, username, deadline) values are deduped by DB unique key hash
    """
    try:
        return create_delivery_methods_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("")
def update_delivery_methods_route(
    payload: list[dict] | dict = Body(...),
):
    """
    Update delivery-method rows by id.

    Example request:
        PUT /api/tradsphere/v1/stations/deliveryMethods
        [
          {
            "id": 12,
            "name": "Station Portal v2",
            "url": "https://delivery.example.com/v2",
            "deadline": "18:00",
            "note": "Updated endpoint"
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
        - At least one updatable delivery-method field is required
        - name/url/username/deadline cannot be empty when provided
    """
    try:
        return modify_delivery_methods_data(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
