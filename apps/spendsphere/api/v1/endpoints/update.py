from fastapi import APIRouter
from pydantic import BaseModel

from apps.spendsphere.api.v1.helpers.pipeline import run_google_ads_budget_pipeline
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import validate_account_codes

router = APIRouter()


# ============================================================
# UPDATES
# ============================================================


class GoogleAdsUpdateRequest(BaseModel):
    accountCodes: str | list[str] | None = None
    dryRun: bool = False
    includeTransformResults: bool = False


@router.post("/updateBudget")
def update_google_ads(payload: GoogleAdsUpdateRequest):
    validate_account_codes(payload.accountCodes)

    result = run_google_ads_budget_pipeline(
        account_codes=payload.accountCodes,
        dry_run=payload.dryRun,
        include_transform_results=payload.includeTransformResults,
    )

    return result
