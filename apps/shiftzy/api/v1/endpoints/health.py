from fastapi import APIRouter, Request

from shared.utils import with_meta

router = APIRouter(prefix="/shiftzy")


@router.get("/ping")
def ping(request: Request):
    return with_meta(
        data={"status": "ok"},
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )
