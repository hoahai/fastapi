import os
import traceback

from contextlib import asynccontextmanager
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from shared.middleware import (
    timing_middleware,
    api_key_auth_middleware,
    request_response_logger_middleware,
    tenant_context_middleware,
)
from shared.utils import with_meta, load_env
from shared.logger import log_run_start, get_logger
from shared.tenant import TenantConfigError, get_timezone

from apps.shiftzy.api.v1.router import router as v1_router

load_env()


@asynccontextmanager
async def lifespan(app: FastAPI):
    log_run_start()
    yield


app = FastAPI(lifespan=lifespan)
app.middleware("http")(timing_middleware)
app.middleware("http")(api_key_auth_middleware)
app.middleware("http")(request_response_logger_middleware)
app.middleware("http")(tenant_context_middleware)
app.include_router(v1_router)

logger = get_logger("Shiftzy API")


@app.exception_handler(TenantConfigError)
async def tenant_config_exception_handler(request: Request, exc: TenantConfigError):
    return JSONResponse(
        status_code=400,
        content={"detail": str(exc)},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    logger.error(
        "Unhandled exception",
        extra={
            "extra_fields": {
                "path": str(request.url.path),
                "method": request.method,
                "error": str(exc),
            }
        },
    )

    response_content = {
        "error": "Internal Server Error",
        "message": "Something went wrong. Please try again later.",
        "detail": str(exc),
        "error_type": exc.__class__.__name__,
        "path": str(request.url.path),
        "method": request.method,
        "request_id": getattr(request.state, "request_id", None),
    }

    if os.getenv("APP_ENV", "").lower() in {"local", "dev", "development"}:
        response_content["traceback"] = traceback.format_exc().splitlines()

    return JSONResponse(
        status_code=500,
        content=response_content,
    )


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
