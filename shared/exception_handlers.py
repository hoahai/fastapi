from __future__ import annotations

import os
import traceback

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from shared.logger import get_logger
from shared.response import wrap_error
from shared.tenant import (
    TenantConfigError,
    TenantConfigValidationError,
    build_tenant_config_payload,
)


def register_exception_handlers(app: FastAPI, *, logger_name: str) -> None:
    logger = get_logger(logger_name)

    @app.exception_handler(TenantConfigError)
    async def tenant_config_exception_handler(
        request: Request,
        exc: TenantConfigError,
    ) -> JSONResponse:
        if isinstance(exc, TenantConfigValidationError):
            app_name = exc.app_name or getattr(request.state, "tenant_app", None)
            payload = build_tenant_config_payload(
                app_name,
                missing=exc.missing,
                invalid=exc.invalid,
            )
            return JSONResponse(
                status_code=400,
                content=wrap_error(payload, request),
            )
        return JSONResponse(
            status_code=400,
            content=wrap_error(str(exc), request),
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
            content=wrap_error(response_content, request),
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_exception_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        errors = exc.errors()
        def _format_loc(loc: object) -> str:
            if not isinstance(loc, (list, tuple)):
                return str(loc)
            parts: list[str] = []
            for item in loc:
                if item == "body":
                    continue
                if isinstance(item, int):
                    if parts:
                        parts[-1] = f"{parts[-1]}[{item}]"
                    else:
                        parts.append(f"[{item}]")
                else:
                    parts.append(str(item))
            return ".".join(parts) if parts else "body"

        messages: list[str] = []
        missing_fields: list[str] = []
        for err in errors:
            loc = _format_loc(err.get("loc"))
            msg = err.get("msg") or "Invalid value"
            if msg == "Field required":
                if loc and loc != "body":
                    missing_fields.append(loc)
                else:
                    missing_fields.append("body")
                continue
            if loc and loc != "body":
                messages.append(f"{loc}: {msg}")
            else:
                messages.append(msg)

        if missing_fields:
            missing_fields_sorted = sorted(dict.fromkeys(missing_fields))
            missing_joined = ", ".join(missing_fields_sorted)
            suffix = "are required" if len(missing_fields_sorted) > 1 else "is required"
            messages.insert(0, f"{missing_joined} {suffix}")

        message = "; ".join(messages) if messages else "Invalid request payload"
        payload = {
            "error": "Invalid payload",
            "message": message,
            "messages": messages,
            "errors": errors,
        }
        return JSONResponse(
            status_code=422,
            content=wrap_error(payload, request),
        )
