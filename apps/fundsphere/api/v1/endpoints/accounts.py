from __future__ import annotations

import mysql.connector
from fastapi import APIRouter, Body, HTTPException, Query

from apps.fundsphere.api.v1.endpoints._direct_common import translate_mysql_error
from apps.fundsphere.api.v1.helpers.directDbQueries import (
    ConflictError,
    NotFoundError,
    create_account,
    get_account,
    list_accounts,
    update_account,
)

router = APIRouter(prefix="/accounts")


@router.get("")
def list_accounts_route(
    code: str | None = Query(None, alias="code"),
    active: bool = Query(True),
):
    """
    Return FundSphere account reference rows.

    Example request:
        GET /api/fundsphere/v1/accounts

    Example request (single account):
        GET /api/fundsphere/v1/accounts?code=ACME01

    Example request (include inactive accounts):
        GET /api/fundsphere/v1/accounts?active=false

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
        - code is optional; when present, returns the matching account row
        - active defaults to true
        - active=true filters by active accounts only
        - active=false returns active and inactive rows
        - Unknown query params are rejected (400)
    """
    try:
        if code:
            account = get_account(code=code)
            if account is None:
                raise HTTPException(status_code=404, detail="Account not found")
            return account
        return list_accounts(code=None, active=active)
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
