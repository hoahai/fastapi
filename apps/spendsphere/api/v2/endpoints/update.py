from datetime import datetime
import time
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from fastapi import APIRouter, BackgroundTasks, Request
from pydantic import BaseModel, ConfigDict, Field

from apps.spendsphere.api.v2.helpers.pipeline import run_google_ads_budget_pipeline
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
    normalize_query_params,
    should_validate_account_codes,
    validate_account_codes,
)
from shared.logger import get_logger, set_request_id, reset_request_id
from shared.response import ensure_request_id, wrap_success
from shared.tenant import get_timezone, set_tenant_context, reset_tenant_context
from shared.utils import dump_model

router = APIRouter()
_API_LOGGER = get_logger("api")


# ============================================================
# UPDATES
# ============================================================


class GoogleAdsUpdateRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    accountCodes: str | list[str] | None = None
    dryRun: bool = False
    includeTransformResults: bool = False
    includeAll: bool = Field(default=False, alias="include_all")


@router.post("/update/")
def update_google_ads(request_payload: GoogleAdsUpdateRequest):
    if should_validate_account_codes(request_payload.accountCodes):
        validate_account_codes(
            request_payload.accountCodes,
            include_all=request_payload.includeAll,
        )

    result = run_google_ads_budget_pipeline(
        account_codes=request_payload.accountCodes,
        dry_run=request_payload.dryRun,
        include_transform_results=request_payload.includeTransformResults,
    )

    return result


def _run_update_job(
    *,
    request_id: str,
    tenant_id: str | None,
    request_payload: GoogleAdsUpdateRequest,
    log_context: dict[str, object | None],
) -> None:
    request_token = set_request_id(request_id)
    tenant_token = None
    start_time = time.perf_counter()
    try:
        if tenant_id:
            tenant_token = set_tenant_context(tenant_id)
        result = run_google_ads_budget_pipeline(
            account_codes=request_payload.accountCodes,
            dry_run=request_payload.dryRun,
            include_transform_results=request_payload.includeTransformResults,
        )
        _log_async_update_response(
            request_id=request_id,
            tenant_id=tenant_id,
            response_body=result,
            duration_s=time.perf_counter() - start_time,
            **log_context,
        )
    finally:
        if tenant_token:
            reset_tenant_context(tenant_token)
        reset_request_id(request_token)


@router.post("/update/async")
def update_google_ads_async(
    request_payload: GoogleAdsUpdateRequest,
    background_tasks: BackgroundTasks,
    request: Request,
):
    if should_validate_account_codes(request_payload.accountCodes):
        validate_account_codes(
            request_payload.accountCodes,
            include_all=request_payload.includeAll,
        )

    request_id = ensure_request_id(request)
    tenant_id = getattr(request.state, "tenant_id", None)
    client_id = getattr(request.state, "client_id", None)

    request_host = request.headers.get("x-forwarded-host") or request.headers.get(
        "host"
    )
    request_scheme = request.headers.get("x-forwarded-proto") or request.url.scheme
    request_path = request.url.path

    background_tasks.add_task(
        _run_update_job,
        request_id=request_id,
        tenant_id=tenant_id,
        request_payload=request_payload,
        log_context={
            "client_id": client_id,
            "request_host": request_host,
            "request_scheme": request_scheme,
            "request_path": request_path,
            "request_body": dump_model(request_payload),
            "request_params": normalize_query_params(request.query_params),
        },
    )

    return {"request_id": request_id, "status": "accepted"}


def _log_async_update_response(
    *,
    request_id: str,
    tenant_id: str | None,
    client_id: str | None,
    request_host: str | None,
    request_scheme: str | None,
    request_path: str,
    request_body: object | None,
    request_params: dict[str, object] | None,
    response_body: object,
    duration_s: float,
) -> None:
    request_state = SimpleNamespace(
        request_id=request_id,
        client_id=client_id or "Not Found",
    )
    fake_request = SimpleNamespace(state=request_state)

    wrapped_response = wrap_success(
        response_body,
        fake_request,
        duration_s=duration_s,
    )

    _API_LOGGER.info(
        "HTTP request/response",
        extra={
            "extra_fields": {
                "event": "http_request_response",
                "timestamp": datetime.now(ZoneInfo(get_timezone())).isoformat(),
                "method": "POST",
                "path": request_path,
                "status_code": 200,
                "duration_ms": int(duration_s * 1000),
                "client_id": client_id,
                "tenant_id": tenant_id,
                "request_host": request_host,
                "request_scheme": request_scheme,
                "request_body": request_body,
                "request_params": request_params,
                "response_body": wrapped_response,
            }
        },
    )
