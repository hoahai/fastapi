from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.invoiceChecklists import (
    InvalidReferenceError,
    NotFoundError,
    SafeDatabaseError,
    create_invoice_note_attachment_data,
    delete_invoice_note_attachment_data,
    list_invoice_note_attachments_data,
    update_invoice_note_attachment_data,
)

router = APIRouter(prefix="/invoice-note-attachments")


@router.get("")
def get_invoice_note_attachments_route(
    note_id: int | None = Query(None, alias="noteId", ge=0),
    attachment_id: int | None = Query(None, alias="attachmentId", ge=0),
):
    """
    Return checklist note-attachment rows filtered by query parameters.

    Example request:
        GET /api/tradsphere/v1/invoice-note-attachments?noteId=5

    Example request (single attachment):
        GET /api/tradsphere/v1/invoice-note-attachments?attachmentId=21

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": 21,
              "noteId": 5,
              "url": "https://cdn.example.com/docs/invoice-123.pdf",
              "fileName": "invoice-123.pdf",
              "fileType": "application/pdf"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - noteId and attachmentId must be unsigned integers when provided
        - Unknown query params are rejected (400)
    """
    try:
        return list_invoice_note_attachments_data(
            note_id=note_id,
            attachment_id=attachment_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to load note attachments")


@router.post("")
def create_invoice_note_attachment_route(
    payload: dict = Body(...),
):
    """
    Create one attachment row for a checklist note.

    Example request:
        POST /api/tradsphere/v1/invoice-note-attachments
        {
          "noteId": 5,
          "url": "https://cdn.example.com/docs/invoice-123.pdf",
          "fileName": "invoice-123.pdf",
          "fileType": "application/pdf"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": 21,
            "noteId": 5,
            "url": "https://cdn.example.com/docs/invoice-123.pdf",
            "fileName": "invoice-123.pdf",
            "fileType": "application/pdf"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - noteId is required and must exist
        - url is required, max length 2048
        - fileName is nullable, max length 255
        - fileType is nullable, max length 100
    """
    try:
        return create_invoice_note_attachment_data(payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to create note attachment")


@router.put("")
def update_invoice_note_attachment_route(
    payload: dict = Body(...),
):
    """
    Update one note attachment row.

    Example request:
        PUT /api/tradsphere/v1/invoice-note-attachments
        {
          "attachmentId": 21,
          "fileName": "invoice-123-v2.pdf"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 1},
          "data": {"id": 21, "fileName": "invoice-123-v2.pdf"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - attachmentId is required and must exist
        - At least one updatable field is required
        - url/fileName/fileType max lengths are enforced
    """
    try:
        return update_invoice_note_attachment_data(payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to update note attachment")


@router.delete("")
def delete_invoice_note_attachment_route(
    attachment_id: int = Query(..., alias="attachmentId", ge=0),
):
    """
    Delete one note attachment row.

    Example request:
        DELETE /api/tradsphere/v1/invoice-note-attachments?attachmentId=21

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 1},
          "data": {"deleted": 1, "id": 21}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - attachmentId query param is required
        - Unknown attachmentId returns HTTP 404
    """
    try:
        return delete_invoice_note_attachment_data(attachment_id=attachment_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to delete note attachment")
