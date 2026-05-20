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
    est_num: int | None = Query(None, alias="estNum", ge=0),
    station_code: str | None = Query(None, alias="stationCode"),
    checklist_id: str | None = Query(None, alias="checklistId"),
    include_attachments: bool = Query(False, alias="includeAttachments"),
    limit: int | None = Query(None, ge=1),
):
    """
    Return checklist note rows filtered by query parameters.

    Example request:
        GET /api/tradsphere/v1/invoice-checklist-notes?checklistStationId=12

    Example request (single note):
        GET /api/tradsphere/v1/invoice-checklist-notes?noteId=5

    Example request (station match across checklists):
        GET /api/tradsphere/v1/invoice-checklist-notes?estNum=26001&stationCode=KABC&includeAttachments=true&limit=10

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": 5,
              "checklistStationId": 12,
              "checklistId": "f3f4502f-b0c3-4ef7-b315-72f9850e36d2",
              "checklistYear": 2026,
              "checklistMonth": 4,
              "estNum": 26001,
              "stationCode": "KABC",
              "amount": -125.5,
              "note": "Credit memo expected",
              "attachments": [
                {
                  "id": 21,
                  "noteId": 5,
                  "url": "/api/tradsphere/v1/invoice-note-attachments/open?attachmentId=21",
                  "fileName": "invoice-123.pdf",
                  "fileType": "application/pdf"
                }
              ]
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - checklistStationId, noteId, estNum, and limit must be unsigned integers when provided
        - stationCode max length is 10 when provided
        - estNum and stationCode must be provided together
        - checklistYear/checklistMonth are included when the note query joins checklist context
        - includeAttachments=true includes attachment arrays per note
        - Unknown query params are rejected (400)
    """
    try:
        return list_invoice_checklist_notes_data(
            checklist_station_id=checklist_station_id,
            note_id=note_id,
            est_num=est_num,
            station_code=station_code,
            checklist_id=checklist_id,
            include_attachments=include_attachments,
            limit=limit,
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
          "note": "Credit memo expected"
        }

    Example request (amount only):
        POST /api/tradsphere/v1/invoice-checklist-notes
        {
          "checklistStationId": 12,
          "amount": -125.50
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": 5,
            "checklistStationId": 12,
            "amount": null,
            "note": "Credit memo expected"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - checklistStationId must exist
        - At least one of amount or note is required
        - amount is optional and must fit DECIMAL(12,2) when provided
        - amount supports positive and negative values
        - note is optional and max length is 2048 when provided
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
          "note": "Issue resolved"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": {"id": 5, "amount": null, "note": "Issue resolved"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - noteId is required and must exist
        - At least one updatable field is required
        - After update, at least one of amount or note must remain populated
        - amount validation uses DECIMAL(12,2) rules when provided
        - note max length is 2048 when provided
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
