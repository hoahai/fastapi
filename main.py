from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from apps.spendsphere.api.main import app as spendsphere_app
from apps.shiftzy.api.main import app as shiftzy_app
from apps.fundsphere.api.main import app as fundsphere_app
from apps.tradsphere.api.main import app as tradsphere_app
from apps.opssphere.api.main import app as opssphere_app
from apps.opssphere.public.router import router as opssphere_public_router
from apps.spendsphere.api.v1.helpers.config import (
    validate_tenant_config as validate_spendsphere_tenant_config,
)
from apps.shiftzy.api.v1.helpers.config import (
    validate_tenant_config as validate_shiftzy_tenant_config,
)
from apps.fundsphere.api.v1.helpers.config import (
    validate_tenant_config as validate_fundsphere_tenant_config,
)
from apps.tradsphere.api.v1.helpers.config import (
    validate_tenant_config as validate_tradsphere_tenant_config,
)
from apps.opssphere.api.helpers.config import (
    validate_tenant_config as validate_opssphere_tenant_config,
)
from shared.exceptionHandlers import register_exception_handlers
from shared.middleware import (
    timing_middleware,
    api_key_auth_middleware,
    request_response_logger_middleware,
    tenant_context_middleware,
)


app = FastAPI()
_STATIC_DIR = Path(__file__).resolve().parent / "static"
_TRADSPHERE_FE_DIST = Path(__file__).resolve().parent / "apps" / "tradsphere" / "ui_dist"
app.state.public_paths = {"/", "/ping"}
app.state.tenant_validator_registry = [
    (
        ("/api/spendsphere", "/spendsphere/api"),
        "SpendSphere",
        validate_spendsphere_tenant_config,
    ),
    (("/api/shiftzy",), "Shiftzy", validate_shiftzy_tenant_config),
    (("/api/fundsphere",), "FundSphere", validate_fundsphere_tenant_config),
    (("/api/tradsphere",), "TradSphere", validate_tradsphere_tenant_config),
    (("/api/opssphere",), "OpsSphere", validate_opssphere_tenant_config),
]
app.middleware("http")(timing_middleware)
app.middleware("http")(api_key_auth_middleware)
app.middleware("http")(request_response_logger_middleware)
app.middleware("http")(tenant_context_middleware)
register_exception_handlers(app, logger_name="Root")

# Mount app-specific APIs under distinct prefixes.
app.mount("/api/spendsphere", spendsphere_app)
app.mount("/api/shiftzy", shiftzy_app)
app.mount("/api/fundsphere", fundsphere_app)
app.mount("/api/tradsphere", tradsphere_app)
app.mount("/api/opssphere", opssphere_app)
if _TRADSPHERE_FE_DIST.exists():
    app.mount(
        "/fe/assets",
        StaticFiles(directory=_TRADSPHERE_FE_DIST / "assets", check_dir=False),
        name="fe-assets",
    )
app.include_router(opssphere_public_router)


@app.get("/")
def root():
    html_path = _STATIC_DIR / "index.html"
    return FileResponse(html_path)


@app.get("/ping")
def ping():
    return {"status": "ok"}


if _TRADSPHERE_FE_DIST.exists():
    @app.get("/fe")
    @app.get("/fe/")
    def serve_fe_root():
        return FileResponse(_TRADSPHERE_FE_DIST / "index.html")


    @app.get("/fe/{full_path:path}")
    def serve_fe_app(full_path: str):
        target = _TRADSPHERE_FE_DIST / full_path
        if target.exists() and target.is_file():
            return FileResponse(target)
        return FileResponse(_TRADSPHERE_FE_DIST / "index.html")
