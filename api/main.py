# main.py
from pathlib import Path
import os
import traceback
from dotenv import load_dotenv


def _load_env() -> None:
    for path in (Path("/etc/secrets/.env"), Path("secrets/.env")):
        if path.is_file():
            load_dotenv(path)
            return


_load_env()

from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from api.middleware import (
    timing_middleware,
    api_key_auth_middleware,
    request_response_logger_middleware,
)
from contextlib import asynccontextmanager

from functions.utils import with_meta, get_current_period
from functions.logger import log_run_start, get_logger
from functions.constants import TIMEZONE

from functions.db_queries import (
    get_accounts,
    get_masterbudgets,
    get_allocations,
    get_rollbreakdowns,
)

from functions.ggSheet import get_rollovers
from functions.spendsphere import run_google_ads_budget_pipeline

@asynccontextmanager
async def lifespan(app: FastAPI):
    log_run_start()
    yield


app = FastAPI(lifespan=lifespan)
app.middleware("http")(timing_middleware)
app.middleware("http")(api_key_auth_middleware)
app.middleware("http")(request_response_logger_middleware)

logger = get_logger("API")


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
# HELPERS
# =========================================================


def validate_account_codes(account_codes: str | list[str] | None) -> list[dict]:
    """
    Validate accountCodes against DB.

    Rules:
    - None / ""     → all accounts
    - "TAAA"        → single account
    - ["TAAA","X"]  → multiple accounts
    """

    accounts = get_accounts(account_codes)
    all_codes = {a["code"].upper() for a in accounts}

    if not account_codes:
        return accounts

    requested = [account_codes] if isinstance(account_codes, str) else account_codes

    requested_set = {c.strip().upper() for c in requested if c.strip()}
    missing = sorted(requested_set - all_codes)

    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Invalid accountCodes",
                "invalid_codes": missing,
                "valid_codes": sorted(all_codes),
            },
        )

    return accounts


def require_account_code(account_code: str) -> str:
    if not account_code or not account_code.strip():
        raise HTTPException(status_code=400, detail="account_code is required")
    return account_code.strip().upper()


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
# WAKE-UP
# =========================================================


@app.get("/wake-up")
def wake_up(request: Request):
    client_id = getattr(request.state, "client_id", "Not Found")
    request_id = getattr(request.state, "request_id", "Not Found")
    forwarded_for = request.headers.get("x-forwarded-for")
    caller_ip = (
        forwarded_for.split(",")[0].strip()
        if forwarded_for
        else request.headers.get("x-real-ip")
        or (request.client.host if request.client else "Unknown")
    )

    data = {
        "message": "I'm awake. Let's do this.",
        "called_at": datetime.now(ZoneInfo(TIMEZONE)).isoformat(),
        "request_id": request_id,
        "caller_ip": caller_ip,
        "method": request.method,
        "path": request.url.path,
        "user_agent": request.headers.get("user-agent", "Unknown"),
    }

    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=client_id,
    )


# =========================================================
# UTILS
# =========================================================


@app.get("/api/utils/current-period")
def getCurrentPeriod(request: Request):
    return with_meta(
        data=get_current_period(),
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


# =========================================================
# BUDGETS
# =========================================================


@app.get("/api/budgets/{account_code}")
def getBudgets(account_code: str, request: Request):
    account_code = require_account_code(account_code)

    data = get_masterbudgets(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No budgets found for account_code '{account_code}'",
        )

    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


# =========================================================
# ALLOCATIONS
# =========================================================


@app.get("/api/allocations/{account_code}")
def getAllocations(account_code: str, request: Request):
    account_code = require_account_code(account_code)

    data = get_allocations(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No allocations found for account_code '{account_code}'",
        )

    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


# =========================================================
# ROLLOVERS
# =========================================================


@app.get("/api/rollovers/{account_code}")
def getRollovers(account_code: str, request: Request):
    account_code = require_account_code(account_code)

    data = get_rollovers(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No rollovers found for account_code '{account_code}'",
        )

    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


@app.get("/api/rollovers/breakdown/{account_code}")
def getRolloversBreakDown(account_code: str, request: Request):
    account_code = require_account_code(account_code)

    data = get_rollbreakdowns(account_code)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No rollovers breakdown found for account_code '{account_code}'",
        )

    return with_meta(
        data=data,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


# =========================================================
# SPENDSPHERE UPDATE
# =========================================================


class GoogleAdsUpdateRequest(BaseModel):
    accountCodes: str | list[str] | None = None
    dryRun: bool = False


@app.post("/api/update/")
def update_google_ads(payload: GoogleAdsUpdateRequest, request: Request):
    # -----------------------------------------
    # Validate accountCodes (REUSABLE)
    # -----------------------------------------
    validate_account_codes(payload.accountCodes)

    # -----------------------------------------
    # Run pipeline
    # -----------------------------------------
    result = run_google_ads_budget_pipeline(
        account_codes=payload.accountCodes,
        dry_run=payload.dryRun,
    )

    return with_meta(
        data=result,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )


# =========================================================
# LOCAL DEV
# =========================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
