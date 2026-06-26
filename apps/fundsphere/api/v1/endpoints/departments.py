from __future__ import annotations

import mysql.connector
from fastapi import APIRouter, Body, HTTPException, Query

from apps.fundsphere.api.v1.endpoints._direct_common import translate_mysql_error
from apps.fundsphere.api.v1.helpers.directDbQueries import (
    ConflictError,
    NotFoundError,
    create_department,
    get_department,
    list_departments,
    update_department,
)

router = APIRouter(prefix="/departments")


@router.get("")
def list_departments_route(
    code: str | None = Query(None, alias="code"),
    name: str | None = Query(None, alias="name"),
):
    """
    Return FundSphere department reference rows.

    Example request:
        GET /api/fundsphere/v1/departments

    Example request (single department):
        GET /api/fundsphere/v1/departments?code=MKT

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "code": "MKT",
              "name": "Marketing",
              "color": "#0088cc",
              "listingOrder": 10
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - code and name are optional filters
        - Unknown query params are rejected (400)
        - Departments are insert/update only in Phase 1; no delete route is exposed
    """
    try:
        if code:
            department = get_department(code=code)
            if department is None:
                raise HTTPException(status_code=404, detail="Department not found")
            return department
        return list_departments(code=code, name=name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("")
def create_departments_route(payload: dict = Body(...)):
    """
    Create one FundSphere department row.

    Example request:
        POST /api/fundsphere/v1/departments
        {
          "code": "MKT",
          "name": "Marketing",
          "color": "#0088cc",
          "listingOrder": 10
        }

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": {"inserted": 1, "code": "MKT"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - code and name are required
        - Departments are insert-only in Phase 1
    """
    try:
        return create_department(payload)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except mysql.connector.Error as exc:
        raise translate_mysql_error(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("")
def update_departments_route(
    code: str | None = Query(None, alias="code"),
    payload: dict = Body(...),
):
    """
    Update one FundSphere department row by code.

    Example request:
        PUT /api/fundsphere/v1/departments?code=MKT
        {
          "name": "Brand Marketing",
          "listingOrder": 20
        }

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": {"updated": 1, "code": "MKT"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - code query param is required
        - At least one updatable field is required
        - Departments are safe reference data; no delete route is exposed
    """
    try:
        return update_department(code=code or "", payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except mysql.connector.Error as exc:
        raise translate_mysql_error(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
