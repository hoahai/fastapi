from __future__ import annotations

import os
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from shared.logger import get_logger
from shared.tenant import TenantConfigError


def register_exception_handlers(app: FastAPI, *, logger_name: str) -> None:
    logger = get_logger(logger_name)

    @app.exception_handler(TenantConfigError)
    async def tenant_config_exception_handler(
        request: Request,
        exc: TenantConfigError,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={"detail": str(exc)},
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
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
