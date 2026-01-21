from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query, Request

from apps.shiftzy.api.v1.helpers.db_queries import get_employees, insert_employees
from shared.utils import with_meta

router = APIRouter()


# ============================================================
# EMPLOYEES
# ============================================================


@router.get("/employees")
def list_employees(request: Request, employee_id: str | None = Query(None, alias="id")):
    data = get_employees(employee_id)
    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.post("/employees")
def create_employees(
    request: Request,
    payload: list[dict] | dict = Body(...),
):
    try:
        inserted = insert_employees(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return with_meta(
        data={"inserted": inserted},
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )
