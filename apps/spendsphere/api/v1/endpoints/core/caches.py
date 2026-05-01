from fastapi import APIRouter, HTTPException, Query, Request

from apps.spendsphere.api.v1.endpoints.custom.budgetManagements import (
    refresh_budget_management_cache,
    refresh_budget_management_recommended_sheet_cache,
)
from apps.spendsphere.api.v1.helpers.ggAd import (
    get_ggad_accounts,
    get_ggad_budgets,
    get_ggad_campaigns,
    get_ggad_spents,
)
from apps.spendsphere.api.v1.helpers.ggSheet import refresh_google_sheet_cache
from apps.spendsphere.api.v1.helpers.spendsphereHelpers import (
    clear_all_tenant_cache_entries,
    cleanup_stale_cache_entries,
    clear_google_ads_warning_cache,
    get_services,
    refresh_account_codes_cache,
)
from shared.requestValidation import allow_unknown_query_params

router = APIRouter()

_DEFAULT_CACHES = [
    "account_codes",
    "google_ads_clients",
    "google_ads_budgets",
    "google_ads_campaigns",
    "google_ads_spent",
    "google_ads_warnings",
    "google_sheets",
    "budget_management",
    "services",
]
_VALID_CACHE_KEYS = set(_DEFAULT_CACHES)

_CACHE_FLAG_TRUE = {"", "1", "true", "t", "yes", "y", "on"}
_CACHE_FLAG_FALSE = {"0", "false", "f", "no", "n", "off"}


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
    ignore_query_keys: set[str] | None = None,
) -> list[str]:
    requested: list[str] = []
    ignored_keys = {
        str(key).strip()
        for key in (ignore_query_keys or set())
        if str(key).strip()
    }

    if values:
        for value in values:
            if not isinstance(value, str):
                continue
            chunks = [v.strip() for v in value.split(",") if v.strip()]
            for chunk in chunks:
                if chunk not in _VALID_CACHE_KEYS:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Unknown cache name: {chunk}",
                    )
                if chunk not in requested:
                    requested.append(chunk)

    saw_cache_flag = False
    if request is not None:
        for raw_key in request.query_params.keys():
            if raw_key == "caches":
                continue
            if raw_key in ignored_keys:
                continue

            if raw_key not in _VALID_CACHE_KEYS:
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

            if enabled and raw_key not in requested:
                requested.append(raw_key)
            if not enabled and raw_key in requested:
                requested.remove(raw_key)

    if saw_cache_flag:
        return requested

    if not requested:
        return list(_DEFAULT_CACHES)

    return requested


def _has_cache_selection_params(
    values: list[str] | None,
    *,
    request: Request | None = None,
) -> bool:
    if values is not None:
        return True
    if request is None:
        return False
    return any(
        raw_key == "caches" or raw_key in _VALID_CACHE_KEYS
        for raw_key in request.query_params.keys()
    )


@router.post(
    "/cache/refresh",
    summary="Refresh SpendSphere caches",
    description=(
        "Refreshes account code, Google Ads clients, budgets, campaigns, and "
        "spend + warning/failure + Google Sheets + budget-management + "
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
            "google_ads_spent, "
            "google_ads_warnings, "
            "google_sheets, budget_management, services. "
            "Can be repeated or "
            "comma-separated. Prefer using query flags such as "
            "?account_codes&google_ads_clients=false."
        ),
    ),
    month: int | None = Query(
        None,
        description=(
            "Optional month (1-12) for period-scoped cache refreshes such as "
            "budget_management and recommended budget sheet refresh."
        ),
    ),
    year: int | None = Query(
        None,
        description=(
            "Optional year (e.g., 2026) for period-scoped cache refreshes. "
            "Must be provided together with month."
        ),
    ),
):
    """
    Example request:
        POST /api/spendsphere/v1/cache/refresh
        Header: X-Tenant-Id: acme
        (Clears all tenant cache buckets first, then refreshes default caches)

    Example request (partial):
        POST /api/spendsphere/v1/cache/refresh?caches=google_ads_clients

    Example request (query-flag style):
        POST /api/spendsphere/v1/cache/refresh?account_codes&google_ads_clients

    Example request (explicit disable):
        POST /api/spendsphere/v1/cache/refresh?account_codes=true&google_ads_clients=false

    Example request (target period for budget-management caches):
        POST /api/spendsphere/v1/cache/refresh?budget_management&google_sheets&month=5&year=2026

    Valid cache values:
        account_codes
        google_ads_clients
        google_ads_budgets
        google_ads_campaigns
        google_ads_spent
        google_ads_warnings (also clears deduped failures)
        google_sheets
        budget_management
        services

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
        - If no cache flags/`caches` params are provided, all tenant-scoped
          cache buckets are cleared first (including non-selectable internal buckets).

    Example response:
        {
          "accountCodes": {
            "active": 42,
            "all": 50
          },
          "googleAdsClients": 12,
          "googleAdsBudgets": 120,
          "googleAdsCampaigns": 240,
          "googleAdsSpent": 640,
          "googleAdsWarnings": 25,
          "googleSheets": {
            "rollovers": 120,
            "activePeriod": 55,
            "recommendedBudget": {
              "period": {"month": 3, "year": 2026},
              "rows": 85
            }
          },
          "budgetManagement": {
            "period": {"month": 3, "year": 2026},
            "tableData": 120,
            "spentData": 20,
            "recommended": 85
          },
          "services": 6
        }
    """
    if not _has_cache_selection_params(caches, request=request):
        clear_all_tenant_cache_entries()

    requested = _normalize_cache_requests(
        caches,
        request=request,
        ignore_query_keys={"month", "year"},
    )
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
        or "google_ads_spent" in requested
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

    if "google_ads_spent" in requested:
        spent_rows = get_ggad_spents(accounts or [], refresh_cache=True)
        response["googleAdsSpent"] = len(spent_rows)

    if "google_ads_warnings" in requested:
        response["googleAdsWarnings"] = clear_google_ads_warning_cache()

    if "google_sheets" in requested:
        rollovers = refresh_google_sheet_cache("rollovers")
        active_period = refresh_google_sheet_cache("active_period")
        recommended_budget = refresh_budget_management_recommended_sheet_cache(
            month=month,
            year=year,
        )
        response["googleSheets"] = {
            "rollovers": len(rollovers),
            "activePeriod": len(active_period),
            "recommendedBudget": recommended_budget,
        }

    if "budget_management" in requested:
        refreshed_budget_management = refresh_budget_management_cache(
            month=month,
            year=year,
            fresh_data=True,
            fresh_spent_data=False,
        )
        response["budgetManagement"] = refreshed_budget_management

    if "services" in requested:
        services = get_services(department_code="DIGM", refresh_cache=True)
        response["services"] = len(services)

    return response


@router.post(
    "/cache/cleanup",
    summary="Clean up stale SpendSphere caches",
    description=(
        "Removes stale cache entries (based on cache-specific TTL rules) "
        "for the current tenant across all SpendSphere cache types, "
        "including budget-management caches."
    ),
)
def cleanup_cache_route():
    """
    Removes stale cache entries for the current tenant.

    Example request:
        POST /api/spendsphere/v1/cache/cleanup
        Header: X-Tenant-Id: acme

    Example response:
        {
          "removed": {
            "accountCodes": 1,
            "googleAdsClients": 1,
            "googleAdsBudgets": 3,
            "googleAdsCampaigns": 3,
            "googleAdsSpent": 4,
            "googleAdsWarnings": 2,
            "googleSheets": 1,
            "budgetManagement": 1,
            "services": 1
          },
          "totalRemoved": 17
        }

    Requirements:
        - Requires X-Tenant-Id header
        - Requires valid API key
    """
    removed = cleanup_stale_cache_entries()
    payload = {
        "accountCodes": int(removed.get("account_codes", 0) or 0),
        "googleAdsClients": int(removed.get("google_ads_clients", 0) or 0),
        "googleAdsBudgets": int(removed.get("google_ads_budgets", 0) or 0),
        "googleAdsCampaigns": int(removed.get("google_ads_campaigns", 0) or 0),
        "googleAdsSpent": int(removed.get("google_ads_spent", 0) or 0),
        "googleAdsWarnings": int(removed.get("google_ads_warnings", 0) or 0),
        "googleSheets": int(removed.get("google_sheets", 0) or 0),
        "budgetManagement": int(removed.get("budget_management", 0) or 0),
        "services": int(removed.get("services", 0) or 0),
    }
    return {
        "removed": payload,
        "totalRemoved": sum(payload.values()),
    }
