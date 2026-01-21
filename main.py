from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse

from apps.spendsphere.api.main import app as spendsphere_app
from apps.shiftzy.api.main import app as shiftzy_app
from apps.spendsphere.api.v1.helpers.config import (
    validate_tenant_config as validate_spendsphere_tenant_config,
)
from apps.shiftzy.api.v1.helpers.config import (
    validate_tenant_config as validate_shiftzy_tenant_config,
)
from shared.exception_handlers import register_exception_handlers
from shared.middleware import (
    timing_middleware,
    api_key_auth_middleware,
    request_response_logger_middleware,
    tenant_context_middleware,
)


app = FastAPI()
app.state.public_paths = {"/", "/ping"}
app.state.tenant_validator_registry = [
    (
        ("/api/spendsphere", "/spendsphere/api"),
        "SpendSphere",
        validate_spendsphere_tenant_config,
    ),
    (("/api/shiftzy",), "Shiftzy", validate_shiftzy_tenant_config),
]
app.middleware("http")(timing_middleware)
app.middleware("http")(api_key_auth_middleware)
app.middleware("http")(request_response_logger_middleware)
app.middleware("http")(tenant_context_middleware)
register_exception_handlers(app, logger_name="Root")

# Mount app-specific APIs under distinct prefixes.
app.mount("/api/spendsphere", spendsphere_app)
app.mount("/api/shiftzy", shiftzy_app)


@app.get("/")
def root():
    html_path = Path(__file__).resolve().parent / "static" / "index.html"
    return FileResponse(html_path)


@app.get("/ping")
def ping():
    return {"status": "ok"}
