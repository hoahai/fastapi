from contextlib import asynccontextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request

from shared.utils import with_meta, load_env
from shared.exception_handlers import register_exception_handlers
from shared.logger import log_run_start
from shared.tenant import get_timezone

from apps.shiftzy.api.v1.router import router as v1_router

load_env()


@asynccontextmanager
async def lifespan(app: FastAPI):
    log_run_start()
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(v1_router)

register_exception_handlers(app, logger_name="Shiftzy API")


@app.get("/")
def root(request: Request):
    return with_meta(
        data={"status": "Shiftzy API"},
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@app.get("/wake-up")
def wake_up(request: Request):
    client_id = getattr(request.state, "client_id", "Not Found")
    request_id = getattr(request.state, "request_id", "Not Found")

    data = {
        "message": "I'm awake. Let's do this.",
        "called_at": datetime.now(ZoneInfo(get_timezone())).isoformat(),
        "request_id": request_id,
        "method": request.method,
        "path": request.url.path,
    }

    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=client_id,
    )
