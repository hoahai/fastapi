from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query, Request

from apps.shiftzy.api.v1.helpers.db_queries import get_positions, insert_positions
from shared.utils import with_meta

router = APIRouter()


# ============================================================
# POSITIONS
# ============================================================


@router.get("/positions")
def list_positions(
    request: Request,
    position_id: str | None = Query(None, alias="id"),
    code: str | None = Query(None),
):
    code_value = position_id or code
    data = get_positions(code_value)
    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@router.post("/positions")
def create_positions(
    request: Request,
    payload: list[dict] | dict = Body(...),
):
    try:
        inserted = insert_positions(payload)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return with_meta(
        data={"inserted": inserted},
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )
