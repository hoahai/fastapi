from __future__ import annotations

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import RedirectResponse

from apps.tradsphere.api.v1.helpers.invoiceChecklists import (
    InvalidReferenceError,
    NotFoundError,
    SafeDatabaseError,
    create_invoice_note_attachment_upload_data,
    create_invoice_note_attachment_data,
    delete_invoice_note_attachment_data,
    get_invoice_note_attachment_open_data,
    list_invoice_note_attachments_data,
    update_invoice_note_attachment_data,
)
from shared.storage import (
    StorageConfigError,
    StorageDeleteError,
    StorageUploadError,
    StorageValidationError,
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
              "url": "/api/tradsphere/v1/invoice-note-attachments/open?attachmentId=21",
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
          "url": "https://legacy.example.com/docs/invoice-123.pdf",
          "fileName": "invoice-123.pdf",
          "fileType": "application/pdf"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": 21,
            "noteId": 5,
            "url": "/api/tradsphere/v1/invoice-note-attachments/open?attachmentId=21",
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


@router.post("/upload")
async def upload_invoice_note_attachment_route(
    request: Request,
    note_id: int = Form(..., alias="noteId"),
    attachment: UploadFile = File(..., alias="file"),
):
    """
    Upload one attachment file for a checklist note and persist provider metadata.

    Example request:
        POST /api/tradsphere/v1/invoice-note-attachments/upload
        Content-Type: multipart/form-data
        form-data:
          noteId=5
          file=@\"/path/to/invoice-proof.png\"

    Example response:
        {
          "meta": {"timestamp": "2026-05-20T10:00:00+07:00", "duration_ms": 180},
          "data": {
            "id": 21,
            "noteId": 5,
            "url": "/api/tradsphere/v1/invoice-note-attachments/open?attachmentId=21",
            "fileName": "invoice-proof.png",
            "mimeType": "image/png",
            "fileSize": 582103,
            "storageProvider": "cloudinary"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - noteId must exist
        - file is required
        - Only PNG/JPG/JPEG/WEBP/PDF are allowed
        - File size and attachment-count limits are validated on backend
    """
    principal = getattr(request.state, "auth_principal", None)
    uploaded_by = None
    if principal is not None:
        uploaded_by = str(getattr(principal, "user_id", "") or getattr(principal, "email", "") or "").strip() or None

    try:
        file_bytes = await attachment.read()
        return create_invoice_note_attachment_upload_data(
            note_id=note_id,
            filename=str(attachment.filename or ""),
            mime_type=str(attachment.content_type or ""),
            file_bytes=file_bytes,
            uploaded_by=uploaded_by,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (StorageValidationError, InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except StorageConfigError:
        raise HTTPException(status_code=500, detail="Storage provider is not configured")
    except StorageUploadError:
        raise HTTPException(status_code=502, detail="File upload failed")
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


@router.get("/open")
def open_invoice_note_attachment_route(
    attachment_id: int = Query(..., alias="attachmentId", ge=0),
):
    """
    Resolve an attachment URL through backend authorization and redirect to the provider URL.

    Example request:
        GET /api/tradsphere/v1/invoice-note-attachments/open?attachmentId=21

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key / bearer session
        - attachmentId must exist
    """
    try:
        payload = get_invoice_note_attachment_open_data(attachment_id=attachment_id)
        return RedirectResponse(url=str(payload["url"]), status_code=307)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to open note attachment")


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
    except StorageDeleteError:
        raise HTTPException(status_code=502, detail="File delete failed")
    except StorageConfigError:
        raise HTTPException(status_code=500, detail="Storage provider is not configured")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to delete note attachment")
