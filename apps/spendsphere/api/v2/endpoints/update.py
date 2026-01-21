from fastapi import APIRouter, Request
from pydantic import BaseModel

from apps.spendsphere.api.v2.helpers.pipeline import run_google_ads_budget_pipeline
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import validate_account_codes
from shared.utils import with_meta

router = APIRouter()


# ============================================================
# UPDATES
# ============================================================


class GoogleAdsUpdateRequest(BaseModel):
    accountCodes: str | list[str] | None = None
    dryRun: bool = False


@router.post("/update/")
def update_google_ads(payload: GoogleAdsUpdateRequest, request: Request):
    validate_account_codes(payload.accountCodes)

    result = run_google_ads_budget_pipeline(
        account_codes=payload.accountCodes,
        dry_run=payload.dryRun,
    )

    return with_meta(
        data=result,
        start_time=request.state.start_time,
        client_id=getattr(request.state, "client_id", "Not Found"),
    )
