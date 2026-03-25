from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from shared.utils import load_env
from shared.exceptionHandlers import register_exception_handlers
from shared.logger import log_run_start
from shared.middleware import response_envelope_middleware, timing_middleware
from shared.requestValidation import validate_query_params

from apps.fundsphere.api.v1.router import router as v1_router

load_env()


# ============================================================
# LIFESPAN
# ============================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    log_run_start()
    yield


# ============================================================
# APP SETUP
# ============================================================

app = FastAPI(lifespan=lifespan, dependencies=[Depends(validate_query_params)])
app.middleware("http")(timing_middleware)
app.middleware("http")(response_envelope_middleware)
app.include_router(v1_router)

register_exception_handlers(app, logger_name="FundSphere API")


# ============================================================
# ROOT
# ============================================================


@app.get("/")
def root():
    return {"status": "FundSphere API"}
