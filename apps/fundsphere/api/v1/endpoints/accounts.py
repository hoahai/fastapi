from __future__ import annotations

import mysql.connector
from fastapi import APIRouter, Body, File, Form, HTTPException, Query, Request, UploadFile

from apps.fundsphere.api.v1.endpoints._direct_common import translate_mysql_error
from apps.fundsphere.api.v1.helpers.directDbQueries import (
    ConflictError,
    NotFoundError,
    create_account,
    list_accounts,
    update_account,
)
from apps.fundsphere.api.v1.helpers.directUploads import upload_account_logo
from shared.storage import StorageConfigError, StorageUploadError, StorageValidationError

router = APIRouter(prefix="/accounts")


@router.post("/logo/upload")
async def upload_account_logo_route(
    request: Request,
    account_code: str = Form(..., alias="accountCode"),
    file: UploadFile = File(..., alias="file"),
):
    """
    Upload one FundSphere account logo image and return the stored image URL.

    Example request:
        POST /api/fundsphere/v1/accounts/logo/upload
        Content-Type: multipart/form-data
        form-data:
          accountCode="ACME01"
          file=@"/path/to/logo.png"

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": {
            "logoUrl": "https://res.cloudinary.com/.../acme01-logo.png",
            "fileName": "acme01-logo.png",
            "mimeType": "image/png",
            "storageProvider": "cloudinary"
          }
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - accountCode is required for generated file naming
        - file is required
        - Only PNG, JPG, JPEG, and WEBP files are allowed
        - File size limits are validated on backend
    """
    try:
        principal = getattr(request.state, "auth_principal", None)
        uploaded_by = None
        if principal is not None:
            uploaded_by = str(getattr(principal, "user_id", "") or getattr(principal, "email", "") or "").strip() or None
        file_bytes = await file.read()
        result = upload_account_logo(
            account_code=account_code,
            filename=str(file.filename or ""),
            mime_type=str(file.content_type or ""),
            file_bytes=file_bytes,
            uploaded_by=uploaded_by,
        )
        return result
    except (StorageValidationError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except StorageConfigError:
        raise HTTPException(status_code=500, detail="Storage provider is not configured")
    except StorageUploadError:
        raise HTTPException(status_code=502, detail="File upload failed")


@router.get("")
def list_accounts_route(
    code: str | None = Query(None, alias="code"),
    name: str | None = Query(None, alias="name"),
    ae_name: str | None = Query(None, alias="aeName"),
    status: str | None = Query(None, alias="status"),
    active: bool = Query(True),
):
    """
    Return FundSphere account reference rows.

    Example request:
        GET /api/fundsphere/v1/accounts

    Example request (code and name filters):
        GET /api/fundsphere/v1/accounts?code=ACME&name=Acme

    Example request (AE and status filters):
        GET /api/fundsphere/v1/accounts?aeName=Alex%20Chen&status=inactive

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "code": "ACME01",
              "name": "Acme Media",
              "logoUrl": null,
              "conseroId": "12345",
              "conseroName": "Acme Media",
              "strataName": "Acme",
              "active": 1,
              "endDate": null
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - code, name, aeName, and status are optional filters
        - status accepts active, inactive, or all; when omitted, the legacy active flag is used
        - active=true filters by active accounts only when status is omitted
        - active=false returns active and inactive rows when status is omitted
        - aeName requires DB_TABLES.employees to be configured so AE names can be resolved from accountReps
        - Unknown query params are rejected (400)
    """
    try:
        return list_accounts(code=code, name=name, ae_name=ae_name, status=status, active=active)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("")
def create_accounts_route(payload: dict = Body(...)):
    """
    Create one FundSphere account row.

    Example request:
        POST /api/fundsphere/v1/accounts
        {
          "code": "ACME01",
          "name": "Acme Media",
          "active": true
        }

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": {"inserted": 1, "code": "ACME01"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - code and name are required
        - Accounts are insert-only in Phase 1; no hard delete route is exposed
    """
    try:
        return create_account(payload)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except mysql.connector.Error as exc:
        raise translate_mysql_error(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("")
def update_accounts_route(
    code: str | None = Query(None, alias="code"),
    payload: dict = Body(...),
):
    """
    Update one FundSphere account row by code.

    Example request:
        PUT /api/fundsphere/v1/accounts?code=ACME01
        {
          "name": "Acme Media Group",
          "active": false,
          "endDate": "2026-12-31"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": {"updated": 1, "code": "ACME01"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - code query param is required
        - At least one updatable field is required
        - Accounts are soft-disabled through active/endDate in Phase 1
    """
    try:
        return update_account(code=code or "", payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except mysql.connector.Error as exc:
        raise translate_mysql_error(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
