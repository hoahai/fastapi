from fastapi import APIRouter, BackgroundTasks, Request
from pydantic import BaseModel

from apps.spendsphere.api.v2.helpers.pipeline import run_google_ads_budget_pipeline
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import validate_account_codes
from shared.logger import set_request_id, reset_request_id
from shared.response import ensure_request_id
from shared.tenant import set_tenant_context, reset_tenant_context

router = APIRouter()


# ============================================================
# UPDATES
# ============================================================


class GoogleAdsUpdateRequest(BaseModel):
    accountCodes: str | list[str] | None = None
    dryRun: bool = False
    includeTransformResults: bool = False


@router.post("/update/")
def update_google_ads(payload: GoogleAdsUpdateRequest):
    validate_account_codes(payload.accountCodes)

    result = run_google_ads_budget_pipeline(
        account_codes=payload.accountCodes,
        dry_run=payload.dryRun,
        include_transform_results=payload.includeTransformResults,
    )

    return result


def _run_update_job(
    *,
    request_id: str,
    tenant_id: str | None,
    payload: GoogleAdsUpdateRequest,
) -> None:
    request_token = set_request_id(request_id)
    tenant_token = None
    try:
        if tenant_id:
            tenant_token = set_tenant_context(tenant_id)
        run_google_ads_budget_pipeline(
            account_codes=payload.accountCodes,
            dry_run=payload.dryRun,
            include_transform_results=payload.includeTransformResults,
        )
    finally:
        if tenant_token:
            reset_tenant_context(tenant_token)
        reset_request_id(request_token)


@router.post("/update/async")
def update_google_ads_async(
    payload: GoogleAdsUpdateRequest,
    background_tasks: BackgroundTasks,
    request: Request,
):
    validate_account_codes(payload.accountCodes)

    request_id = ensure_request_id(request)
    tenant_id = getattr(request.state, "tenant_id", None)

    background_tasks.add_task(
        _run_update_job,
        request_id=request_id,
        tenant_id=tenant_id,
        payload=payload,
    )

    return {"request_id": request_id, "status": "accepted"}
