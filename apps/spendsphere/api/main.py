# main.py
from contextlib import asynccontextmanager

from shared.utils import load_env

load_env()

from fastapi import Depends, FastAPI

from apps.spendsphere.api.v1.router import router as v1_router

from shared.exception_handlers import register_exception_handlers
from shared.logger import log_run_start
from shared.middleware import response_envelope_middleware, timing_middleware
from shared.request_validation import validate_query_params


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
app.include_router(v1_router)

register_exception_handlers(app, logger_name="SpendSphere API")

# =========================================================
# ROOT
# =========================================================


@app.get("/")
def root():
    return {"status": "Hello World!"}


# =========================================================
# LOCAL DEV
# =========================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
