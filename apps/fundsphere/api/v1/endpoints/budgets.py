from __future__ import annotations

import mysql.connector
from fastapi import APIRouter, Body, HTTPException, Query, Request

from apps.fundsphere.api.v1.endpoints._direct_common import parse_optional_int_query, translate_mysql_error
from apps.fundsphere.api.v1.helpers.directDbQueries import (
    ConflictError,
    NotFoundError,
    create_budget,
    get_budget,
    list_budgets,
    update_budget,
)

router = APIRouter(prefix="/budgets")


@router.get("")
def list_budgets_route(
    id: str | None = Query(None, alias="id"),
    account_code: str | None = Query(None, alias="accountCode"),
    service_id: str | None = Query(None, alias="serviceId"),
    department_code: str | None = Query(None, alias="departmentCode"),
    month: str | None = Query(None, alias="month"),
    year: str | None = Query(None, alias="year"),
    sub_service: str | None = Query(None, alias="subService"),
):
    """
    Return FundSphere budget rows joined to account, service, and department data.

    Example request:
        GET /api/fundsphere/v1/budgets

    Example request (single budget):
        GET /api/fundsphere/v1/budgets?id=3a8f40df-9490-4a7d-a2ff-7d2863e95b1f

    Example request (period and reference filters):
        GET /api/fundsphere/v1/budgets?accountCode=ACME01&departmentCode=MKT&month=6&year=2026

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": "3a8f40df-9490-4a7d-a2ff-7d2863e95b1f",
              "accountCode": "ACME01",
              "accountName": "Acme Media",
              "month": 6,
              "year": 2026,
              "serviceId": "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7",
              "serviceName": "Paid Media",
              "departmentCode": "MKT",
              "departmentName": "Marketing",
              "subService": "",
              "grossAmount": 50000,
              "commission": 5,
              "netAdjustment": 0,
              "netAmount": 47500,
              "note": null
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - id is optional; when present, returns the matching budget row
        - accountCode, serviceId, departmentCode, month, year, and subService are optional filters
        - month/year are accepted independently
        - Unknown query params are rejected (400)
        - Budgets are list/create/update only in Phase 1
    """
    try:
        month_value = parse_optional_int_query(month, field="month")
        year_value = parse_optional_int_query(year, field="year")
        if id:
            budget = get_budget(budget_id=id)
            if budget is None:
                raise HTTPException(status_code=404, detail="Budget not found")
            return budget
        return list_budgets(
            id=id,
            account_code=account_code,
            service_id=service_id,
            department_code=department_code,
            month=month_value,
            year=year_value,
            sub_service=sub_service,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("")
def create_budgets_route(
    request: Request,
    payload: dict = Body(...),
):
    """
    Create one FundSphere budget row and write an audit history record.

    Example request:
        POST /api/fundsphere/v1/budgets
        {
          "accountCode": "ACME01",
          "month": 6,
          "year": 2026,
          "serviceId": "7e4e0c9d-f8b8-4d2f-8d1d-6ce8b1f5b7f7",
          "subService": "",
          "grossAmount": 50000,
          "commission": 5,
          "netAdjustment": 0,
          "note": "Initial plan"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": {"inserted": 1, "id": "3a8f40df-9490-4a7d-a2ff-7d2863e95b1f"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - accountCode, month, year, serviceId, and grossAmount are required
        - Budget creates write a BudgetChangeHistories audit row
        - Duplicate accountCode + month + year + serviceId + subService pairs are rejected
    """
    try:
        return create_budget(payload=payload, request=request)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except mysql.connector.Error as exc:
        raise translate_mysql_error(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("")
def update_budgets_route(
    request: Request,
    id: str | None = Query(None, alias="id"),
    payload: dict = Body(...),
):
    """
    Update one FundSphere budget row by id and write an audit history record.

    Example request:
        PUT /api/fundsphere/v1/budgets?id=3a8f40df-9490-4a7d-a2ff-7d2863e95b1f
        {
          "grossAmount": 60000,
          "note": "Updated after planning review",
          "changeNote": "Client scope expanded"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": {"updated": 1, "id": "3a8f40df-9490-4a7d-a2ff-7d2863e95b1f"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - id query param is required
        - At least one mutable field is required
        - Allowed mutable fields: subService, grossAmount, commission, netAdjustment, note
        - Updates write a BudgetChangeHistories audit row
    """
    try:
        return update_budget(budget_id=id or "", payload=payload, request=request)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except mysql.connector.Error as exc:
        raise translate_mysql_error(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

