from __future__ import annotations

import mysql.connector
from fastapi import APIRouter, Body, HTTPException, Query

from apps.fundsphere.api.v1.endpoints._direct_common import translate_mysql_error
from apps.fundsphere.api.v1.helpers.directDbQueries import (
    ConflictError,
    NotFoundError,
    create_account_rep,
    delete_account_rep,
    get_account_rep,
    list_account_reps,
)

router = APIRouter(prefix="/accountReps")


@router.get("")
def list_account_reps_route(
    id: str | None = Query(None, alias="id"),
    account_code: str | None = Query(None, alias="accountCode"),
    employee_id: str | None = Query(None, alias="employeeId"),
):
    """
    Return FundSphere account-rep assignment rows.

    Example request:
        GET /api/fundsphere/v1/accountReps

    Example request (single assignment):
        GET /api/fundsphere/v1/accountReps?id=7eb0f4eb-8b3e-48c1-9a2a-2ec13d437bd8

    Example request (relationship filters):
        GET /api/fundsphere/v1/accountReps?accountCode=ACME01&employeeId=13f6b22f-0a86-43b8-946d-cbba67642e8b

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": [
            {
              "id": "7eb0f4eb-8b3e-48c1-9a2a-2ec13d437bd8",
              "accountCode": "ACME01",
              "employeeId": "13f6b22f-0a86-43b8-946d-cbba67642e8b"
            }
          ]
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - id, accountCode, and employeeId are optional filters
        - Unknown query params are rejected (400)
    """
    try:
        if id:
            assignment = get_account_rep(rep_id=id)
            if assignment is None:
                raise HTTPException(status_code=404, detail="Account rep not found")
            return assignment
        return list_account_reps(id=id, account_code=account_code, employee_id=employee_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("")
def create_account_reps_route(payload: dict = Body(...)):
    """
    Create one FundSphere account-rep assignment.

    Example request:
        POST /api/fundsphere/v1/accountReps
        {
          "accountCode": "ACME01",
          "employeeId": "13f6b22f-0a86-43b8-946d-cbba67642e8b"
        }

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": {"inserted": 1, "id": "7eb0f4eb-8b3e-48c1-9a2a-2ec13d437bd8"}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - accountCode and employeeId are required
        - Employee validation is optional when DB_TABLES.employees is not configured
        - Duplicate accountCode + employeeId pairs are rejected
    """
    try:
        return create_account_rep(payload)
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except mysql.connector.Error as exc:
        raise translate_mysql_error(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("")
def delete_account_reps_route(
    id: str | None = Query(None, alias="id"),
    account_code: str | None = Query(None, alias="accountCode"),
    employee_id: str | None = Query(None, alias="employeeId"),
):
    """
    Remove one FundSphere account-rep assignment.

    Example request:
        DELETE /api/fundsphere/v1/accountReps?id=7eb0f4eb-8b3e-48c1-9a2a-2ec13d437bd8

    Example request (relationship keys):
        DELETE /api/fundsphere/v1/accountReps?accountCode=ACME01&employeeId=13f6b22f-0a86-43b8-946d-cbba67642e8b

    Example response:
        {
          "meta": {"timestamp": "2026-06-26T10:00:00+07:00", "duration_ms": 2},
          "data": {"deleted": 1}
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key or bearer token in compat mode
        - id or accountCode + employeeId is required
        - Assignment rows are deleted directly because this is a relationship table
    """
    try:
        return delete_account_rep(rep_id=id, account_code=account_code, employee_id=employee_id)
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except mysql.connector.Error as exc:
        raise translate_mysql_error(exc) from exc
