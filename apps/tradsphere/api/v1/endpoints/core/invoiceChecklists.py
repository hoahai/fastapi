from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query

from apps.tradsphere.api.v1.helpers.invoiceChecklists import (
    ConflictError,
    InvalidReferenceError,
    NotFoundError,
    SafeDatabaseError,
    create_invoice_checklist_data,
    delete_invoice_checklist_data,
    get_invoice_checklists_data,
    sync_invoice_checklists_for_period_data,
    update_invoice_checklist_data,
)

router = APIRouter(prefix="/invoice-checklists")


@router.get("")
def get_invoice_checklists_route(
    checklist_id: str | None = Query(None, alias="checklistId"),
    account_code: str | None = Query(None, alias="accountCode"),
    year: int | None = Query(None),
    month: int | None = Query(None),
    status: str | None = Query(None),
    include_stations: bool = Query(False, alias="includeStations"),
    include_notes: bool = Query(False, alias="includeNotes"),
    include_attachments: bool = Query(False, alias="includeAttachments"),
):
    """
    Return invoice checklist rows in list mode or detail mode.

    Example request:
        GET /api/tradsphere/v1/invoice-checklists?accountCode=TAAA&year=2026&month=4

    Example request (detail mode):
        GET /api/tradsphere/v1/invoice-checklists?checklistId=f3f4502f-b0c3-4ef7-b315-72f9850e36d2

    Example request (detail mode with related data):
        GET /api/tradsphere/v1/invoice-checklists?checklistId=f3f4502f-b0c3-4ef7-b315-72f9850e36d2&includeStations=true&includeNotes=true&includeAttachments=true

    Example response (detail mode with related data):
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 4},
          "data": {
            "id": "f3f4502f-b0c3-4ef7-b315-72f9850e36d2",
            "accountCode": "TAAA",
            "year": 2026,
            "month": 4,
            "status": "OPEN",
            "note": "April invoice review",
            "dateCreated": "2026-05-16T09:00:00",
            "dateUpdated": "2026-05-16T09:10:00",
            "stations": [
              {
                "id": 12,
                "checklistId": "f3f4502f-b0c3-4ef7-b315-72f9850e36d2",
                "estNum": 26001,
                "stationCode": "KABC",
                "status": "PENDING",
                "dateCreated": "2026-05-16T09:01:00",
                "dateUpdated": "2026-05-16T09:06:00",
                "notes": [
                  {
                    "id": 5,
                    "checklistStationId": 12,
                    "amount": -125.5,
                    "note": "Credit memo expected",
                    "dateCreated": "2026-05-16T09:02:00",
                    "dateUpdated": "2026-05-16T09:04:00",
                    "attachments": [
                      {
                        "id": 21,
                        "noteId": 5,
                        "url": "/api/tradsphere/v1/invoice-note-attachments/open?attachmentId=21",
                        "fileName": "invoice-123.pdf",
                        "fileType": "application/pdf",
                        "dateCreated": "2026-05-16T09:03:00",
                        "dateUpdated": "2026-05-16T09:03:00"
                      }
                    ]
                  }
                ]
              }
            ]
          }
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 3},
          "data": [
            {
              "id": "b6c95b0f-7c48-4c6e-9f63-007a6583f245",
              "accountCode": "TAAA",
              "year": 2026,
              "month": 4,
              "status": "IN_PROGRESS",
              "note": "Waiting on station backup",
              "dateCreated": "2026-05-16T09:00:00",
              "dateUpdated": "2026-05-16T09:10:00",
              "stationCount": 4
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - Optional filters: checklistId, accountCode, year, month, status
        - accountCode max length is 10
        - year must be between 1901 and 2155 when provided
        - month must be between 1 and 12 when provided
        - status max length is 32 when provided
        - includeStations/includeNotes/includeAttachments are supported in checklistId detail mode
        - Unknown query params are rejected (400)
    """
    try:
        return get_invoice_checklists_data(
            checklist_id=checklist_id,
            account_code=account_code,
            year=year,
            month=month,
            status=status,
            include_stations=include_stations,
            include_notes=include_notes,
            include_attachments=include_attachments,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to load checklists")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("")
def create_invoice_checklist_route(
    payload: dict = Body(...),
):
    """
    Create an invoice reconciliation checklist row.

    Example request:
        POST /api/tradsphere/v1/invoice-checklists
        {
          "accountCode": "TAAA",
          "year": 2026,
          "month": 4,
          "status": "OPEN",
          "note": "April invoice review"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": "f3f4502f-b0c3-4ef7-b315-72f9850e36d2",
            "accountCode": "TAAA",
            "year": 2026,
            "month": 4,
            "status": "OPEN",
            "note": "April invoice review",
            "dateCreated": "2026-05-16T09:00:00",
            "dateUpdated": "2026-05-16T09:00:00"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - accountCode is required, max length 10
        - year is required (1901-2155)
        - month is required (1-12)
        - status is nullable, max length 32
        - note is nullable, max length 2048
        - Duplicate (accountCode, year, month) returns HTTP 409
    """
    try:
        return create_invoice_checklist_data(payload)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to create checklist")


@router.post("/sync-period")
def sync_invoice_checklists_period_route(
    payload: dict = Body(...),
):
    """
    Generate or update checklist account/station rows for one selected invoice period.

    Example request:
        POST /api/tradsphere/v1/invoice-checklists/sync-period
        {
          "year": 2026,
          "month": 4,
          "previewOnly": true
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-19T10:00:00+07:00", "duration_ms": 12},
          "data": {
            "action": "update",
            "period": {"year": 2026, "month": 4, "quarter": 2, "value": "2026-04", "label": "April 2026"},
            "expectedAccountsCount": 3,
            "expectedStationRowsCount": 27,
            "previewOnly": true,
            "plannedChecklistsCount": 1,
            "plannedStationsCount": 4,
            "mismatchStationCount": 2,
            "plannedChecklists": [
              {
                "accountCode": "TAAA",
                "year": 2026,
                "month": 4
              }
            ],
            "plannedStations": [
              {
                "checklistId": null,
                "accountCode": "TAAA",
                "estNum": 2042,
                "stationCode": "KXYZ"
              }
            ],
            "mismatchStations": [
              {
                "stationRowId": 19,
                "checklistId": "f3f4502f-b0c3-4ef7-b315-72f9850e36d2",
                "accountCode": "TAAA",
                "estNum": 2042,
                "stationCode": "KXYZ",
                "status": "Matched",
                "reasonCode": "NOT_IN_CURRENT_PERIOD_SCHEDULE"
              }
            ]
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - year and month are required in request body
        - previewOnly is optional (default false)
        - year must be between 1901 and 2155
        - month must be between 1 and 12
        - Generates expected rows from schedules where broadcastYear/year and broadcastMonth/month exactly match the selected period
        - Does not delete checklist rows or overwrite existing checklist/station status, notes, or attachments
        - Adds only missing checklist accounts/station rows required by the selected period schedule
    """
    try:
        if not isinstance(payload, dict):
            raise ValueError("Payload must be an object")
        return sync_invoice_checklists_for_period_data(
            year=payload.get("year"),
            month=payload.get("month"),
            preview_only=bool(payload.get("previewOnly")),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to sync checklist rows")


@router.put("")
def update_invoice_checklist_route(
    payload: dict = Body(...),
):
    """
    Update checklist fields.

    Example request:
        PUT /api/tradsphere/v1/invoice-checklists
        {
          "checklistId": "f3f4502f-b0c3-4ef7-b315-72f9850e36d2",
          "status": "DONE",
          "note": "Reviewed and reconciled"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "id": "f3f4502f-b0c3-4ef7-b315-72f9850e36d2",
            "status": "DONE"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - checklistId is required in request body
        - At least one updatable field is required
        - accountCode/year/month updates are validated
        - Duplicate (accountCode, year, month) returns HTTP 409
        - Unknown checklistId returns HTTP 404
    """
    try:
        return update_invoice_checklist_data(payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except (InvalidReferenceError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to update checklist")


@router.delete("")
def delete_invoice_checklist_route(
    checklist_id: str = Query(..., alias="checklistId"),
):
    """
    Delete one checklist row. Child rows are cascade-deleted by the database.

    Example request:
        DELETE /api/tradsphere/v1/invoice-checklists?checklistId=f3f4502f-b0c3-4ef7-b315-72f9850e36d2

    Example response:
        {
          "meta": {"timestamp": "2026-05-16T10:00:00+07:00", "duration_ms": 2},
          "data": {"deleted": 1, "id": "f3f4502f-b0c3-4ef7-b315-72f9850e36d2"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - checklistId query param is required
        - Unknown checklistId returns HTTP 404
    """
    try:
        return delete_invoice_checklist_data(checklist_id=checklist_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except SafeDatabaseError:
        raise HTTPException(status_code=500, detail="Failed to delete checklist")
