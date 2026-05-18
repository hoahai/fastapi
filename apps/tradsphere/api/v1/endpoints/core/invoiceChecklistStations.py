from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.invoiceChecklists import (
    ConflictError,
    InvalidReferenceError,
    NotFoundError,
    SafeDatabaseError,
    create_invoice_checklist_station_data,
    delete_invoice_checklist_station_data,
    list_invoice_checklist_stations_data,
    update_invoice_checklist_station_data,
)

router = APIRouter(prefix="/invoice-checklist-stations")


@router.get("")
def get_invoice_checklist_stations_route(
    checklist_id: str | None = Query(None, alias="checklistId"),
    station_row_id: int | None = Query(None, alias="stationRowId", ge=0),
    est_num: int | None = Query(None, alias="estNum", ge=0),
    station_code: str | None = Query(None, alias="stationCode"),
    status: str | None = Query(None),
):
    """
    Return checklist station rows filtered by query parameters.

    Example request:
        GET /api/tradsphere/v1/invoice-checklist-stations?checklistId=f3f4502f-b0c3-4ef7-b315-72f9850e36d2

    Example request (single station):
        GET /api/tradsphere/v1/invoice-checklist-stations?stationRowId=12

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": 12,
              "checklistId": "f3f4502f-b0c3-4ef7-b315-72f9850e36d2",
              "estNum": 26001,
              "stationCode": "KABC",
              "status": "PENDING"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - stationRowId and estNum must be unsigned integers when provided
        - stationCode max length is 10 when provided
        - status max length is 32 when provided
        - Unknown query params are rejected (400)
    """
    try:
        return list_invoice_checklist_stations_data(
            checklist_id=checklist_id,
            station_row_id=station_row_id,
            est_num=est_num,
            station_code=station_code,
            status=status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to load checklist stations")


@router.post("")
def create_invoice_checklist_station_route(
    payload: dict = Body(...),
):
    """
    Create a checklist station row.

    Example request:
        POST /api/tradsphere/v1/invoice-checklist-stations
        {
          "checklistId": "f3f4502f-b0c3-4ef7-b315-72f9850e36d2",
          "estNum": 26001,
          "stationCode": "KABC",
          "status": "PENDING"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": 12,
            "checklistId": "f3f4502f-b0c3-4ef7-b315-72f9850e36d2",
            "estNum": 26001,
            "stationCode": "KABC",
            "status": "PENDING"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - checklistId is required
        - estNum is required and must be unsigned integer
        - stationCode is required, max length 10
        - status is nullable, max length 32
        - checklistId must exist
        - estNum must exist in TradSphere_EstNums
        - stationCode must exist in TradSphere_Stations
        - Duplicate (checklistId, estNum, stationCode) returns HTTP 409
    """
    try:
        return create_invoice_checklist_station_data(payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to create checklist station")


@router.put("")
def update_invoice_checklist_station_route(
    payload: dict = Body(...),
):
    """
    Update a checklist station row.

    Example request:
        PUT /api/tradsphere/v1/invoice-checklist-stations
        {
          "stationRowId": 12,
          "status": "MATCHED"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": {"id": 12, "status": "MATCHED"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - stationRowId is required and must be unsigned integer
        - At least one updatable field is required
        - estNum and stationCode are validated when provided
        - Duplicate (checklistId, estNum, stationCode) returns HTTP 409
        - Unknown stationRowId returns HTTP 404
    """
    try:
        return update_invoice_checklist_station_data(payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to update checklist station")


@router.delete("")
def delete_invoice_checklist_station_route(
    station_row_id: int = Query(..., alias="stationRowId", ge=0),
):
    """
    Delete one checklist station row. Child notes and attachments are cascade-deleted.

    Example request:
        DELETE /api/tradsphere/v1/invoice-checklist-stations?stationRowId=12

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": {"deleted": 1, "id": 12}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - stationRowId query param is required
        - Unknown stationRowId returns HTTP 404
    """
    try:
        return delete_invoice_checklist_station_data(station_row_id=station_row_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to delete checklist station")
