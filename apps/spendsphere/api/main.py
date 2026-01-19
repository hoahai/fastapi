# main.py
import os
import traceback
from contextlib import asynccontextmanager

from shared.utils import with_meta, load_env
from shared.tenant import TenantConfigError

load_env()

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from shared.middleware import (
    timing_middleware,
    api_key_auth_middleware,
    request_response_logger_middleware,
    tenant_context_middleware,
)
from apps.spendsphere.api.v1.router import router as v1_router
from apps.spendsphere.api.v2.router import router as v2_router
from apps.spendsphere.api.v1.helpers.config import validate_tenant_config

from shared.logger import log_run_start, get_logger

@asynccontextmanager
async def lifespan(app: FastAPI):
    log_run_start()
    yield


app = FastAPI(lifespan=lifespan)
app.state.tenant_validator = validate_tenant_config
app.middleware("http")(timing_middleware)
app.middleware("http")(api_key_auth_middleware)
app.middleware("http")(request_response_logger_middleware)
app.middleware("http")(tenant_context_middleware)
app.include_router(v1_router)
app.include_router(v2_router)

logger = get_logger("API")


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

# =========================================================
# ROOT
# =========================================================


@app.get("/")
def root(request: Request):
    return with_meta(
        data={"status": "Hello World!"},
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


# =========================================================
# LOCAL DEV
# =========================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
