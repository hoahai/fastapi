from fastapi import APIRouter, HTTPException, Query, Request

from apps.spendsphere.api.v1.endpoints.custom.budgetManagements import (
    refresh_budget_management_overview_cache,
)
from apps.spendsphere.api.v1.helpers.ggAd import (
    get_ggad_accounts,
    get_ggad_budgets,
    get_ggad_campaigns,
)
from apps.spendsphere.api.v1.helpers.ggSheet import refresh_google_sheet_cache
from apps.spendsphere.api.v1.helpers.spendsphere_helpers import (
    clear_google_ads_warning_cache,
    refresh_account_codes_cache,
    refresh_services_cache,
)
from shared.request_validation import allow_unknown_query_params

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
    "google_ads_warnings": "google_ads_warnings",
    "google_ads_warning": "google_ads_warnings",
    "googleadswarnings": "google_ads_warnings",
    "google_ads_failures": "google_ads_warnings",
    "google_ads_failure": "google_ads_warnings",
    "googleadsfailures": "google_ads_warnings",
    "google_ads_issues": "google_ads_warnings",
    "googleadsissues": "google_ads_warnings",
    "google_sheets": "google_sheets",
    "googlesheets": "google_sheets",
    "budget_management_overview": "budget_management_overview",
    "budget_management": "budget_management_overview",
    "budget_managements": "budget_management_overview",
    "budgetmanagementoverview": "budget_management_overview",
    "service": "services",
    "services": "services",
}

_DEFAULT_CACHES = [
    "account_codes",
    "google_ads_clients",
    "google_ads_budgets",
    "google_ads_campaigns",
    "google_ads_warnings",
    "google_sheets",
    "budget_management_overview",
    "services",
]

_CACHE_FLAG_TRUE = {"", "1", "true", "t", "yes", "y", "on"}
_CACHE_FLAG_FALSE = {"0", "false", "f", "no", "n", "off"}


def _normalize_cache_key(value: str) -> str:
    return value.replace("-", "_").replace(" ", "").lower()


def _parse_cache_flag(value: str | None, *, field: str) -> bool:
    cleaned = str(value or "").strip().lower()
    if cleaned in _CACHE_FLAG_TRUE:
        return True
    if cleaned in _CACHE_FLAG_FALSE:
        return False
    raise HTTPException(
        status_code=400,
        detail=(
            f"Invalid value for {field}: {value}. "
            "Use true/false (or 1/0), or provide no value to enable."
        ),
    )


def _normalize_cache_requests(
    values: list[str] | None,
    *,
    request: Request | None = None,
) -> list[str]:
    requested: list[str] = []

    if values:
        for value in values:
            if not isinstance(value, str):
                continue
            chunks = [v.strip() for v in value.split(",") if v.strip()]
            for chunk in chunks:
                key = _normalize_cache_key(chunk)
                alias = _CACHE_ALIASES.get(key)
                if alias and alias not in requested:
                    requested.append(alias)
                elif not alias:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Unknown cache name: {chunk}",
                    )

    saw_cache_flag = False
    if request is not None:
        for raw_key in request.query_params.keys():
            if _normalize_cache_key(raw_key) == "caches":
                continue

            alias = _CACHE_ALIASES.get(_normalize_cache_key(raw_key))
            if not alias:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown cache flag: {raw_key}",
                )

            saw_cache_flag = True
            raw_values = request.query_params.getlist(raw_key)
            enabled = _parse_cache_flag(
                raw_values[-1] if raw_values else "",
                field=raw_key,
            )

            if enabled and alias not in requested:
                requested.append(alias)
            if not enabled and alias in requested:
                requested.remove(alias)

    if saw_cache_flag:
        return requested

    if not requested:
        return list(_DEFAULT_CACHES)

    return requested


@router.post(
    "/cache/refresh",
    summary="Refresh SpendSphere caches",
    description=(
        "Refreshes account code, Google Ads clients, budgets, campaigns, and "
        "warning/failure + Google Sheets + budget-management overview + "
        "service caches for the current tenant."
    ),
)
@allow_unknown_query_params
def refresh_cache_route(
    request: Request,
    caches: list[str] | None = Query(
        None,
        description=(
            "Optional cache list (legacy). Valid values: account_codes, "
            "google_ads_clients, google_ads_budgets, google_ads_campaigns, "
            "google_ads_warnings (or google_ads_failures / google_ads_issues), "
            "google_sheets, budget_management_overview, services. Can be repeated or "
            "comma-separated. Prefer using query flags such as "
            "?account_codes&google_ads_clients=false."
        ),
    ),
):
    """
    Example request:
        POST /api/spendsphere/v1/cache/refresh
        Header: X-Tenant-Id: acme

    Example request (partial):
        POST /api/spendsphere/v1/cache/refresh?caches=google_ads_clients

    Example request (query-flag style):
        POST /api/spendsphere/v1/cache/refresh?account_codes&google_ads_clients

    Example request (explicit disable):
        POST /api/spendsphere/v1/cache/refresh?account_codes=true&google_ads_clients=false

    Valid cache values:
        account_codes
        google_ads_clients
        google_ads_budgets
        google_ads_campaigns
        google_ads_warnings (also clears deduped failures)
        google_sheets
        budget_management_overview
        services

    Example response:
        {
          "accountCodes": {
            "active": 42,
            "all": 50
          },
          "googleAdsClients": 12,
          "googleAdsBudgets": 120,
          "googleAdsCampaigns": 240,
          "googleAdsWarnings": 25,
          "googleSheets": {
            "rollovers": 120,
            "activePeriod": 55
          },
          "budgetManagementOverview": {
            "period": {"month": 3, "year": 2026},
            "tableData": 120,
            "spentData": 20,
            "recommended": 85
          },
          "services": 6
        }
    """
    requested = _normalize_cache_requests(caches, request=request)
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

    if "google_ads_warnings" in requested:
        response["googleAdsWarnings"] = clear_google_ads_warning_cache()

    if "google_sheets" in requested:
        rollovers = refresh_google_sheet_cache("rollovers")
        active_period = refresh_google_sheet_cache("active_period")
        response["googleSheets"] = {
            "rollovers": len(rollovers),
            "activePeriod": len(active_period),
        }

    if "budget_management_overview" in requested:
        response["budgetManagementOverview"] = (
            refresh_budget_management_overview_cache()
        )

    if "services" in requested:
        services = refresh_services_cache(department_code="DIGM")
        response["services"] = len(services)

    return response
