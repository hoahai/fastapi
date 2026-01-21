# main.py
from contextlib import asynccontextmanager

from shared.utils import with_meta, load_env

load_env()

from fastapi import FastAPI, Request

from apps.spendsphere.api.v1.router import router as v1_router
from apps.spendsphere.api.v2.router import router as v2_router

from shared.exception_handlers import register_exception_handlers
from shared.logger import log_run_start


@asynccontextmanager
async def lifespan(app: FastAPI):
    log_run_start()
    yield


app = FastAPI(lifespan=lifespan, redirect_slashes=False)
app.include_router(v1_router)
app.include_router(v2_router)

register_exception_handlers(app, logger_name="SpendSphere API")

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
