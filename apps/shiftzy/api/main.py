from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request

from shared.utils import with_meta, load_env
from shared.exception_handlers import register_exception_handlers
from shared.logger import log_run_start
from shared.request_validation import validate_query_params

from apps.shiftzy.api.v1.router import router as v1_router

load_env()


@asynccontextmanager
async def lifespan(app: FastAPI):
    log_run_start()
    yield


app = FastAPI(lifespan=lifespan, dependencies=[Depends(validate_query_params)])
app.include_router(v1_router)

register_exception_handlers(app, logger_name="Shiftzy API")


# =========================================================
# ROOT
# =========================================================


@app.get("/")
def root(request: Request):
    return with_meta(
        data={"status": "Shiftzy API"},
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )
