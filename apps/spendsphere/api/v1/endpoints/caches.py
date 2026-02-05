from fastapi import APIRouter, HTTPException, Query

from apps.spendsphere.api.v1.helpers.ggAd import (
    get_ggad_accounts,
    get_ggad_budgets,
    get_ggad_campaigns,
)
from apps.spendsphere.api.v1.helpers.ggSheet import refresh_google_sheet_cache
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
    refresh_account_codes_cache,
)

router = APIRouter()

_CACHE_ALIASES = {
    "account_codes": "account_codes",
    "accountcode": "account_codes",
    "accountcodes": "account_codes",
    "google_ads_clients": "google_ads_clients",
    "google_ads": "google_ads_clients",
    "googleadsclients": "google_ads_clients",
    "google_ads_budgets": "google_ads_budgets",
    "google_ads_budget": "google_ads_budgets",
    "googleadsbudgets": "google_ads_budgets",
    "google_ads_campaigns": "google_ads_campaigns",
    "google_ads_campaign": "google_ads_campaigns",
    "googleadscampaigns": "google_ads_campaigns",
    "google_sheets": "google_sheets",
    "googlesheets": "google_sheets",
}


def _normalize_cache_requests(values: list[str] | None) -> list[str]:
    if not values:
        return [
            "account_codes",
            "google_ads_clients",
            "google_ads_budgets",
            "google_ads_campaigns",
            "google_sheets",
        ]

    requested: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        chunks = [v.strip() for v in value.split(",") if v.strip()]
        for chunk in chunks:
            key = chunk.replace("-", "_").replace(" ", "").lower()
            alias = _CACHE_ALIASES.get(key)
            if alias and alias not in requested:
                requested.append(alias)
            elif not alias:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown cache name: {chunk}",
                )

    if not requested:
        return [
            "account_codes",
            "google_ads_clients",
            "google_ads_budgets",
            "google_ads_campaigns",
            "google_sheets",
        ]

    return requested


@router.post(
    "/cache/refresh",
    summary="Refresh SpendSphere caches",
    description=(
        "Refreshes account code, Google Ads clients, budgets, campaigns, and "
        "Google Sheets caches for the current tenant."
    ),
)
def refresh_cache_route(
    caches: list[str] | None = Query(
        None,
        description=(
            "Optional cache list. Valid values: account_codes, "
            "google_ads_clients, google_ads_budgets, google_ads_campaigns, "
            "google_sheets. Can be repeated or comma-separated."
        ),
    ),
):
    """
    Example request:
        POST /api/spendsphere/v1/cache/refresh
        Header: X-Tenant-Id: acme

    Example request (partial):
        POST /api/spendsphere/v1/cache/refresh?caches=google_ads_clients

    Valid cache values:
        account_codes
        google_ads_clients
        google_ads_budgets
        google_ads_campaigns
        google_sheets

    Example response:
        {
          "accountCodes": {
            "active": 42,
            "all": 50
          },
          "googleAdsClients": 12,
          "googleAdsBudgets": 120,
          "googleAdsCampaigns": 240,
          "googleSheets": {
            "rollovers": 120,
            "activePeriod": 55
          }
        }
    """
    requested = _normalize_cache_requests(caches)
    response: dict[str, object] = {}

    if "account_codes" in requested:
        active_accounts = refresh_account_codes_cache(include_all=False)
        all_accounts = refresh_account_codes_cache(include_all=True)
        response["accountCodes"] = {
            "active": len(active_accounts),
            "all": len(all_accounts),
        }

    accounts: list[dict] | None = None
    if (
        "google_ads_clients" in requested
        or "google_ads_budgets" in requested
        or "google_ads_campaigns" in requested
    ):
        accounts = get_ggad_accounts(refresh_cache=True)

    if "google_ads_clients" in requested:
        response["googleAdsClients"] = len(accounts or [])

    if "google_ads_budgets" in requested:
        budgets = get_ggad_budgets(accounts or [], refresh_cache=True)
        response["googleAdsBudgets"] = len(budgets)

    if "google_ads_campaigns" in requested:
        campaigns = get_ggad_campaigns(accounts or [], refresh_cache=True)
        response["googleAdsCampaigns"] = len(campaigns)

    if "google_sheets" in requested:
        rollovers = refresh_google_sheet_cache("rollovers")
        active_period = refresh_google_sheet_cache("active_period")
        response["googleSheets"] = {
            "rollovers": len(rollovers),
            "activePeriod": len(active_period),
        }

    return response
