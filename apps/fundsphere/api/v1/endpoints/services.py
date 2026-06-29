from __future__ import annotations

import mysql.connector
from fastapi import APIRouter, Body, HTTPException, Query

from apps.fundsphere.api.v1.endpoints._direct_common import translate_mysql_error
from apps.fundsphere.api.v1.helpers.directDbQueries import (
    ConflictError,
    NotFoundError,
    create_service,
    get_service,
    list_services,
    update_service,
)

router = APIRouter(prefix="/services")


@router.get("")
def list_services_route(
    id: str | None = Query(None, alias="id"),
    name: str | None = Query(None, alias="name"),
    department_code: str | None = Query(None, alias="departmentCode"),
    status: str | None = Query(None, alias="status"),
    active: bool = Query(True),
    summary: bool = Query(False),
):
    """
    Return FundSphere service reference rows joined to departments.

    Example request:
        GET /api/fundsphere/v1/services

    Example request (single service):
        GET /api/fundsphere/v1/services?id=7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7

    Example request (name and department filters):
        GET /api/fundsphere/v1/services?name=Paid%20Media&departmentCode=MKT&status=active

    Example request (inactive services):
        GET /api/fundsphere/v1/services?status=inactive

    Example request (summary rows for search):
        GET /api/fundsphere/v1/services?status=active&summary=true

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7",
              "name": "Paid Media",
              "departmentCode": "MKT",
              "departmentName": "Marketing",
              "departmentListingOrder": 10,
              "description": null,
              "commission": 0,
              "netAdjustment": 0,
              "active": 1
            }
          ]
        }

    Example response (summary):
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7",
              "name": "Paid Media",
              "departmentCode": "MKT",
              "departmentName": "Marketing",
              "departmentListingOrder": 10,
              "description": null,
              "active": 1
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - id, name, departmentCode, and status are optional filters
        - active defaults to true when status is not provided
        - status=active filters active services only
        - status=inactive filters inactive services only
        - status=all or active=false returns active and inactive services
        - summary=true returns only the fields needed for the page search list
        - Unknown query params are rejected (400)
        - Services are insert/update only in Phase 1; no delete route is exposed
    """
    try:
        if id:
            service = get_service(service_id=id)
            if service is None:
                raise HTTPException(status_code=404, detail="Service not found")
            return service
        return list_services(
            id=id,
            name=name,
            department_code=department_code,
            status=status,
            active=active,
            summary=summary,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("")
def create_services_route(payload: dict = Body(...)):
    """
    Create one FundSphere service row.

    Example request:
        POST /api/fundsphere/v1/services
        {
          "id": "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7",
          "name": "Paid Media",
          "departmentCode": "MKT",
          "commission": 0,
          "netAdjustment": 0,
          "active": true
        }

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": {"inserted": 1, "id": "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - name and departmentCode are required
        - Services are insert-only in Phase 1
        - departmentCode must reference an existing department
    """
    try:
        return create_service(payload)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except mysql.connector.Error as exc:
        raise translate_mysql_error(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("")
def update_services_route(
    id: str | None = Query(None, alias="id"),
    payload: dict = Body(...),
):
    """
    Update one FundSphere service row by id.

    Example request:
        PUT /api/fundsphere/v1/services?id=7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7
        {
          "name": "Paid Media - Search",
          "departmentCode": "MKT",
          "active": false
        }

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": {"updated": 1, "id": "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - id query param is required
        - At least one updatable field is required
        - Services are soft-disabled through active in Phase 1
    """
    try:
        return update_service(service_id=id or "", payload=payload)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except mysql.connector.Error as exc:
        raise translate_mysql_error(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
