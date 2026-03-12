# Cache Behavior (SpendSphere)

This document describes the SpendSphere cache behavior for account codes, Google Ads clients, budgets, campaigns, and spend.

## Cache file
- Location: `fastapi/caches.json`
- Override via env: `SPENDSPHERE_ACCOUNT_CODE_CACHE_PATH`
- Single shared file for all tenants and cache types.

## Tenant scoping
- Cache entries are stored per tenant key (derived from `X-Tenant-Id`).
- Each tenant has its own account code and Google Ads client data.

## TTL behavior
- Default TTL: 86400 seconds (24 hours).
- Tenant default: `spendsphere -> CACHE -> ttl_time` in `tenant.yaml`.
- Cache-specific tenant overrides in `spendsphere -> CACHE`:
  - `account_codes_ttl_time`
  - `google_ads_clients_ttl_time`
  - `google_ads_budgets_ttl_time`
  - `google_ads_campaigns_ttl_time`
  - `google_ads_spent_ttl_time`
  - `budget_managements_ttl_time`
  - `budget_management_spent_ttl_time`
  - `google_sheet_ttl_time`
  - `services_ttl_time`
- Optional env override (highest priority): `SPENDSPHERE_GOOGLE_ADS_CLIENTS_CACHE_TTL_SECONDS`.

If a cache entry is stale, the API **blocks and refreshes** from the source before returning data.

## Account code cache
- Stored under `account_codes` per tenant.
- For account codes, two scopes are stored per tenant:
  - `active` for `include_all=false`
  - `all` for `include_all=true`
- Each scope has its own `updated_at` timestamp and TTL.

## Google Ads clients cache
- Stored under `google_ads_clients` per tenant.
- Reads return cached data when fresh.
- If stale or `refresh_cache=true`, data is fetched from Google Ads and the cache is updated.

## Google Ads budgets cache
- Stored under `google_ads_budgets` per tenant (per accountCode).
- Default TTL: 300 seconds (5 minutes).
- Reads return cached data when fresh.
- If stale, data is fetched from Google Ads and the cache is updated.

## Google Ads campaigns cache
- Stored under `google_ads_campaigns` per tenant (per accountCode).
- Default TTL: 300 seconds (5 minutes).
- Reads return cached data when fresh.
- If stale, data is fetched from Google Ads and the cache is updated.

## Google Ads spent cache
- Stored under `google_ads_spent` per tenant (per accountCode + period `YYYY-MM`).
- Default TTL: 300 seconds (5 minutes).
- Reads return cached data when fresh.
- If stale, data is fetched from Google Ads and the cache is updated.

Example tenant override:
```yaml
spendsphere:
  CACHE:
    google_ads_budgets_ttl_time: 300
    google_ads_campaigns_ttl_time: 300
    google_ads_spent_ttl_time: 300
    services_ttl_time: 86400
```

## Services cache
- Stored under `services` per tenant.
- Default TTL: 86400 seconds (24 hours).
- Reads return cached data when fresh.
- If stale, data is fetched from DB and the cache is updated.

## Budget Management cache
- Stored under `budget_managements` per tenant.
- Cached keys include:
  - `budget_management_overview::YYYY-MM`
  - `budget_management_spend_by_adtype::YYYY-MM::<digest>`
- Overview default TTL: 86400 seconds (24 hours).
- Overview tenant override:
  - `spendsphere.CACHE.budget_managements_ttl_time`
- Spend-by-adtype default TTL:
  - follows overview TTL unless overridden.
- Spend-by-adtype tenant override:
  - `spendsphere.CACHE.budget_management_spent_ttl_time`
- Reads return cached data when fresh.
- If stale, data is fetched/recomputed and the cache is updated.

## Google Sheets cache
- Stored under `google_sheets` per tenant.
- Sheets cached:
  - `rollovers`
  - `active_period`
- Reads return cached data when fresh.
- If stale, data is fetched from Google Sheets and the cache is updated.

## Refresh controls
- `GET /api/spendsphere/v1/google-ads?refresh_cache=true`
- `GET /api/spendsphere/v1/uis/selections?refresh_cache=true`
- `GET /api/spendsphere/v1/uis/load?fresh_data=true`
- `GET /api/spendsphere/v1/uis/budgetManagament/load?fresh_data=true`
- `GET /api/spendsphere/v1/uis/budgetManagament/load?fresh_spent_data=true`
- `POST /api/spendsphere/v1/google-ads/refresh`
- `POST /api/spendsphere/v1/cache/refresh`
- `POST /api/spendsphere/v1/cache/cleanup` (remove stale entries for current tenant)

Notes:
- `GET /api/spendsphere/v1/uis/load?fresh_data=true`
  forces fresh reads for cache-backed data used by the route:
  Google Ads (`clients/campaigns/budgets/spend`) and
  Google Sheets (`rollovers`, `active_period`).
- `POST /api/spendsphere/v1/cache/refresh?budget_management`
  forces fresh recompute for overview (`fresh_data=true`) before
  updating cached `tableData`/`spentData`/`recommended`.
- `POST /api/spendsphere/v1/cache/refresh?budget_management_spent`
  forces fresh spend recompute (`fresh_spent_data=true`) before
  updating cached values.
- `POST /api/spendsphere/v1/cache/cleanup` removes stale entries from
  all cache buckets, including budget-management overview and spend entries.
