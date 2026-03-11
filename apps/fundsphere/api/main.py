from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from apps.fundsphere.api.v1.router import router as v1_router
from shared.exception_handlers import register_exception_handlers
from shared.logger import log_run_start
from shared.middleware import response_envelope_middleware, timing_middleware
from shared.request_validation import validate_query_params
from shared.utils import load_env

load_env()


@asynccontextmanager
async def lifespan(app: FastAPI):
    log_run_start()
    yield


app = FastAPI(
    lifespan=lifespan,
    dependencies=[Depends(validate_query_params)],
)
app.middleware("http")(timing_middleware)
app.middleware("http")(response_envelope_middleware)
app.include_router(v1_router)

register_exception_handlers(app, logger_name="FundSphere API")


# =========================================================
# ROOT
# =========================================================


@app.get("/")
def root():
    """
    Return a basic status payload for FundSphere API.

    Example request:
        GET /api/fundsphere/

    Example response:
        {
          "status": "FundSphere API"
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
    """
    return {"status": "FundSphere API"}
