# Cache Behavior (SpendSphere)

This document describes the SpendSphere cache behavior for account codes, Google Ads clients, budgets, and campaigns.

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
  - `google_sheet_ttl_time`
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

Example tenant override:
```yaml
spendsphere:
  CACHE:
    google_ads_budgets_ttl_time: 300
    google_ads_campaigns_ttl_time: 300
```

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
- `POST /api/spendsphere/v1/google-ads/refresh`
- `POST /api/spendsphere/v1/cache/refresh`
