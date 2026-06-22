from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from apps.leavesphere.api.v1.endpoints.publicApproval import router as public_approval_router
from apps.leavesphere.api.v1.router import router as v1_router
from shared.exceptionHandlers import register_exception_handlers
from shared.logger import log_run_start
from shared.middleware import response_envelope_middleware, timing_middleware
from shared.requestValidation import validate_query_params
from shared.utils import load_env

load_env()


@asynccontextmanager
async def lifespan(app: FastAPI):
    log_run_start()
    yield


app = FastAPI(
    lifespan=lifespan,
    redirect_slashes=False,
    dependencies=[Depends(validate_query_params)],
)
app.middleware("http")(timing_middleware)
app.middleware("http")(response_envelope_middleware)
app.state.public_path_prefixes = ("/api/leavesphere/v1/public/approval",)
app.include_router(public_approval_router)
app.include_router(v1_router)

register_exception_handlers(app, logger_name="LeaveSphere API")


@app.get("/")
def root():
    return {"status": "LeaveSphere API"}
