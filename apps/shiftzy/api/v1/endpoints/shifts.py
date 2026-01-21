from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query, Request

from apps.shiftzy.api.v1.helpers.db_queries import get_shifts, insert_shifts
from shared.utils import with_meta

router = APIRouter()


# ============================================================
# SHIFTS
# ============================================================


@router.get("/shifts")
def list_shifts(request: Request, shift_id: int | None = Query(None, alias="id")):
    data = get_shifts(shift_id)
    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.post("/shifts")
def create_shifts(
    request: Request,
    payload: list[dict] | dict = Body(...),
):
    try:
        inserted = insert_shifts(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return with_meta(
        data={"inserted": inserted},
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )
