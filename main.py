from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from apps.spendsphere.api.main import app as spendsphere_app
from apps.shiftzy.api.main import app as shiftzy_app
from apps.fundsphere.api.main import app as fundsphere_app
from apps.tradsphere.api.main import app as tradsphere_app
from apps.leavesphere.api.main import app as leavesphere_app
from apps.opssphere.api.main import app as opssphere_app
from apps.auth.api.main import app as auth_app
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
from apps.leavesphere.api.v1.helpers.config import (
    validate_tenant_config as validate_leavesphere_tenant_config,
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
_FRONTEND_INDEX = _TRADSPHERE_FE_DIST / "index.html"
app.state.public_paths = {"/", "/ping"}
app.state.public_path_prefixes = {
    "/assets/",
    "/fe/assets/",
    "/auth/",
    "/api/leavesphere/v1/public/approval",
    "/api/leavesphere/v1/public/google-calendar/oauth",
    "/public/opssphere/advWebsiteReport/reports/cta",
}
app.state.tenant_validator_registry = [
    (
        ("/api/spendsphere", "/spendsphere/api"),
        "SpendSphere",
        validate_spendsphere_tenant_config,
    ),
    (("/api/shiftzy",), "Shiftzy", validate_shiftzy_tenant_config),
    (("/api/fundsphere",), "FundSphere", validate_fundsphere_tenant_config),
    (("/api/tradsphere",), "TradSphere", validate_tradsphere_tenant_config),
    (("/api/leavesphere",), "LeaveSphere", validate_leavesphere_tenant_config),
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
app.mount("/api/leavesphere", leavesphere_app)
app.mount("/api/opssphere", opssphere_app)
app.mount("/api/auth", auth_app)
if _TRADSPHERE_FE_DIST.exists():
    app.mount(
        "/assets",
        StaticFiles(directory=_TRADSPHERE_FE_DIST / "assets", check_dir=False),
        name="assets",
    )
    app.mount(
        "/fe/assets",
        StaticFiles(directory=_TRADSPHERE_FE_DIST / "assets", check_dir=False),
        name="fe-assets-compat",
    )
app.include_router(opssphere_public_router)


def _should_redirect_frontend_path(full_path: str) -> bool:
    normalized_path = f"/{str(full_path or '').lstrip('/')}"
    if normalized_path == "/":
        return False
    if normalized_path == "/api" or normalized_path.startswith("/api/"):
        return False
    if normalized_path == "/assets" or normalized_path.startswith("/assets/"):
        return False
    if normalized_path == "/fe/assets" or normalized_path.startswith("/fe/assets/"):
        return False
    if normalized_path.endswith("/"):
        return False
    return True


def _redirect_to_frontend_canonical_path(full_path: str, query_string: str = "") -> RedirectResponse:
    normalized_path = f"/{str(full_path or '').lstrip('/')}".rstrip("/")
    suffix = f"?{query_string}" if query_string else ""
    return RedirectResponse(url=f"{normalized_path}/{suffix}", status_code=307)


def _frontend_index_response() -> FileResponse:
    if _FRONTEND_INDEX.exists():
        return FileResponse(_FRONTEND_INDEX)

    html_path = _STATIC_DIR / "index.html"
    if html_path.exists():
        return FileResponse(html_path)

    raise HTTPException(status_code=404, detail="Frontend build not found")


@app.get("/")
def root():
    return _frontend_index_response()


@app.get("/ping")
def ping():
    return {"status": "ok"}


if _TRADSPHERE_FE_DIST.exists():
    @app.get("/fe")
    @app.get("/fe/")
    def serve_fe_root():
        return RedirectResponse(url="/", status_code=307)


    @app.get("/fe/{full_path:path}")
    def serve_fe_app(request: Request, full_path: str):
        normalized_path = full_path.lstrip("/")
        if not normalized_path:
            return RedirectResponse(url="/", status_code=307)
        return _redirect_to_frontend_canonical_path(normalized_path, request.url.query)


@app.get("/shiftzy/home")
def redirect_shiftzy_home():
    return RedirectResponse(url="/shiftzy/home/", status_code=307)


@app.get("/{full_path:path}")
def serve_frontend_app(request: Request, full_path: str):
    if full_path == "api" or full_path.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not Found")

    target = _TRADSPHERE_FE_DIST / full_path
    if target.exists() and target.is_file():
        return FileResponse(target)

    if _should_redirect_frontend_path(full_path):
        return _redirect_to_frontend_canonical_path(full_path, request.url.query)

    return _frontend_index_response()
