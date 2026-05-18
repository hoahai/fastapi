from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.invoiceChecklists import (
    InvalidReferenceError,
    NotFoundError,
    SafeDatabaseError,
    create_invoice_checklist_note_data,
    delete_invoice_checklist_note_data,
    list_invoice_checklist_notes_data,
    update_invoice_checklist_note_data,
)

router = APIRouter(prefix="/invoice-checklist-notes")


@router.get("")
def get_invoice_checklist_notes_route(
    checklist_station_id: int | None = Query(None, alias="checklistStationId", ge=0),
    note_id: int | None = Query(None, alias="noteId", ge=0),
):
    """
    Return checklist note rows filtered by query parameters.

    Example request:
        GET /api/tradsphere/v1/invoice-checklist-notes?checklistStationId=12

    Example request (single note):
        GET /api/tradsphere/v1/invoice-checklist-notes?noteId=5

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": 5,
              "checklistStationId": 12,
              "amount": -125.5,
              "note": "Credit memo expected"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - checklistStationId and noteId must be unsigned integers when provided
        - Unknown query params are rejected (400)
    """
    try:
        return list_invoice_checklist_notes_data(
            checklist_station_id=checklist_station_id,
            note_id=note_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to load checklist notes")


@router.post("")
def create_invoice_checklist_note_route(
    payload: dict = Body(...),
):
    """
    Create a checklist note row.

    Example request:
        POST /api/tradsphere/v1/invoice-checklist-notes
        {
          "checklistStationId": 12,
          "amount": -125.50,
          "note": "Credit memo expected"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": 5,
            "checklistStationId": 12,
            "amount": -125.5,
            "note": "Credit memo expected"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - checklistStationId must exist
        - amount is required and must fit DECIMAL(12,2)
        - amount supports positive and negative values
        - note is required, max length 2048
    """
    try:
        return create_invoice_checklist_note_data(payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to create checklist note")


@router.put("")
def update_invoice_checklist_note_route(
    payload: dict = Body(...),
):
    """
    Update one checklist note row.

    Example request:
        PUT /api/tradsphere/v1/invoice-checklist-notes
        {
          "noteId": 5,
          "amount": 0,
          "note": "Issue resolved"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": {"id": 5, "amount": 0.0, "note": "Issue resolved"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - noteId is required and must exist
        - At least one updatable field is required
        - amount validation uses DECIMAL(12,2) rules
        - note max length is 2048
    """
    try:
        return update_invoice_checklist_note_data(payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to update checklist note")


@router.delete("")
def delete_invoice_checklist_note_route(
    note_id: int = Query(..., alias="noteId", ge=0),
):
    """
    Delete one checklist note row. Child attachments are cascade-deleted.

    Example request:
        DELETE /api/tradsphere/v1/invoice-checklist-notes?noteId=5

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 1},
          "data": {"deleted": 1, "id": 5}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - noteId query param is required
        - Unknown noteId returns HTTP 404
    """
    try:
        return delete_invoice_checklist_note_data(note_id=note_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to delete checklist note")
